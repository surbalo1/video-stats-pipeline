"""Bronze -> silver -> gold pipeline for short-video stats, in plain SQLite.

Bronze (raw, never edited):
  - video_public_stats: one row per video per weekly snapshot (views, likes, comments).
  - video_retention_stats: one row per video per snapshot (average % viewed).
  - video_catalog: what each video is about (topic, pillar, series, publish time).

Silver (clean, one row per thing):
  - silver_dim_video: one row per video.
  - silver_fact_video_week: one row per video and snapshot, with the change in views
    against the previous snapshot (LAG window function) and the video's age in days.

Gold (SQL views ready for a report):
  - gold_first_week_views_by_pillar
  - gold_retention_by_series
  - gold_views_by_hour

The load is idempotent: running it twice leaves exactly the same data (full reload
inside one transaction). Only the standard library is used.
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

MAX_DAYS_WITHOUT_SNAPSHOT = 8   # snapshots are weekly: older than 8 days = stale data
VIEW_DROP_TOLERANCE = 0.02      # platforms purge invalid views; drops under 2% are fine
MAX_RETENTION_PCT = 150.0       # looped videos report 300-500% and would skew averages
UTC_TO_LOCAL_H = -6             # Mexico City, no daylight saving since 2022

BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS video_public_stats (
    video_id      TEXT NOT NULL,
    snapshot_date TEXT NOT NULL,
    title         TEXT,
    published_at  TEXT,
    duration_s    INTEGER,
    views         INTEGER,
    likes         INTEGER,
    comments      INTEGER,
    PRIMARY KEY (video_id, snapshot_date)
);
CREATE TABLE IF NOT EXISTS video_retention_stats (
    video_id      TEXT NOT NULL,
    snapshot_date TEXT NOT NULL,
    avg_view_pct  REAL,
    PRIMARY KEY (video_id, snapshot_date)
);
CREATE TABLE IF NOT EXISTS video_catalog (
    video_id     TEXT PRIMARY KEY,
    topic_id     TEXT,
    pillar       TEXT,
    series_key   TEXT,
    episode_num  INTEGER,
    published_at TEXT
);
"""

SILVER_DDL = """
CREATE TABLE IF NOT EXISTS silver_dim_video (
    video_id     TEXT PRIMARY KEY,
    title        TEXT,
    published_at TEXT,
    duration_s   INTEGER,
    topic_id     TEXT,
    pillar       TEXT,
    series_key   TEXT,
    episode_num  INTEGER,
    local_hour   INTEGER
);
CREATE TABLE IF NOT EXISTS silver_fact_video_week (
    video_id      TEXT NOT NULL,
    snapshot_date TEXT NOT NULL,
    views         INTEGER,
    likes         INTEGER,
    comments      INTEGER,
    views_delta   INTEGER,
    age_days      INTEGER,
    PRIMARY KEY (video_id, snapshot_date)
);
"""

# One row per video: its views in the first snapshot taken at 7+ days old.
_FIRST_WEEK = """
    SELECT f.video_id, f.views
    FROM silver_fact_video_week f
    WHERE f.age_days >= 7
      AND f.snapshot_date = (SELECT MIN(g.snapshot_date) FROM silver_fact_video_week g
                             WHERE g.video_id = f.video_id AND g.age_days >= 7)
"""

GOLD_VIEWS = {
    "gold_first_week_views_by_pillar": f"""
        SELECT COALESCE(d.pillar, 'no_pillar') AS pillar,
               COUNT(*) AS videos,
               CAST(ROUND(AVG(p.views)) AS INTEGER) AS avg_views
        FROM ({_FIRST_WEEK}) p
        JOIN silver_dim_video d ON d.video_id = p.video_id
        GROUP BY 1
    """,
    "gold_retention_by_series": f"""
        SELECT d.series_key AS series_key,
               COUNT(*) AS videos,
               ROUND(AVG(r.avg_view_pct), 1) AS avg_retention_pct
        FROM video_retention_stats r
        JOIN silver_dim_video d ON d.video_id = r.video_id
        WHERE d.series_key IS NOT NULL
          AND r.avg_view_pct IS NOT NULL
          AND r.avg_view_pct <= {MAX_RETENTION_PCT}
          AND r.snapshot_date = (SELECT MAX(snapshot_date) FROM video_retention_stats
                                 WHERE video_id = r.video_id)
        GROUP BY 1
    """,
    "gold_views_by_hour": f"""
        SELECT d.local_hour AS local_hour,
               COUNT(*) AS videos,
               CAST(ROUND(AVG(p.views)) AS INTEGER) AS avg_views
        FROM ({_FIRST_WEEK}) p
        JOIN silver_dim_video d ON d.video_id = p.video_id
        WHERE d.local_hour IS NOT NULL
        GROUP BY 1
    """,
}


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(BRONZE_DDL + SILVER_DDL)
    for name, sql in GOLD_VIEWS.items():
        conn.execute(f"DROP VIEW IF EXISTS {name}")
        conn.execute(f"CREATE VIEW {name} AS {sql}")


def local_hour(published_at: Any) -> Optional[int]:
    """Local publish hour. Accepts naive local time or UTC ending in 'Z' / with an offset."""
    text = str(published_at or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            dt = datetime.fromisoformat(text[:-1]) + timedelta(hours=UTC_TO_LOCAL_H)
        else:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is not None:
                dt = (dt - dt.utcoffset()).replace(tzinfo=None) + timedelta(hours=UTC_TO_LOCAL_H)
        return dt.hour
    except ValueError:
        return None


def build_silver(conn: sqlite3.Connection) -> Dict[str, int]:
    """Full reload of the silver layer inside one transaction (idempotent)."""
    ensure_schema(conn)
    latest = conn.execute("""
        SELECT s.video_id, s.title, s.published_at, s.duration_s,
               c.topic_id, c.pillar, c.series_key, c.episode_num, c.published_at
        FROM video_public_stats s
        LEFT JOIN video_catalog c ON c.video_id = s.video_id
        WHERE s.snapshot_date = (SELECT MAX(snapshot_date) FROM video_public_stats
                                 WHERE video_id = s.video_id)
    """).fetchall()
    dim_rows = [row[:8] + (local_hour(row[8] or row[2]),) for row in latest]

    with conn:  # all or nothing: on failure the previous silver data stays intact
        conn.execute("DELETE FROM silver_dim_video")
        conn.executemany("INSERT INTO silver_dim_video VALUES (?,?,?,?,?,?,?,?,?)", dim_rows)
        conn.execute("DELETE FROM silver_fact_video_week")
        conn.execute("""
            INSERT INTO silver_fact_video_week
            SELECT video_id, snapshot_date, views, likes, comments,
                   views - LAG(views) OVER (PARTITION BY video_id ORDER BY snapshot_date),
                   CAST(julianday(snapshot_date) - julianday(substr(published_at, 1, 10)) AS INTEGER)
            FROM video_public_stats
        """)
    fact = conn.execute("SELECT COUNT(*) FROM silver_fact_video_week").fetchone()[0]
    return {"dim": len(dim_rows), "fact": fact}


def run_quality_checks(conn: sqlite3.Connection, now: Optional[datetime] = None) -> List[str]:
    """Returns the problems found (empty list = all good)."""
    now = now or datetime.now()
    issues: List[str] = []
    bronze, videos, last = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT video_id), MAX(snapshot_date) FROM video_public_stats"
    ).fetchone()
    fact = conn.execute("SELECT COUNT(*) FROM silver_fact_video_week").fetchone()[0]
    dim = conn.execute("SELECT COUNT(*) FROM silver_dim_video").fetchone()[0]

    # 1. No duplicates or losses between layers.
    if fact != bronze:
        issues.append(f"Snapshot rows: bronze {bronze} vs silver {fact}.")
    if dim != videos:
        issues.append(f"Videos: bronze {videos} vs silver {dim}.")

    # 2. Views should not go down (small purges are tolerated).
    drops = conn.execute("""
        SELECT COUNT(*) FROM silver_fact_video_week
        WHERE views_delta < 0 AND -views_delta > ? * (views - views_delta)
    """, (VIEW_DROP_TOLERANCE,)).fetchone()[0]
    if drops:
        issues.append(f"{drops} snapshot(s) where views dropped more than {int(VIEW_DROP_TOLERANCE * 100)}%.")

    # 3. Freshness.
    if not last:
        issues.append("No snapshots at all.")
    else:
        try:
            days = (now.date() - datetime.strptime(last, "%Y-%m-%d").date()).days
        except ValueError:
            days = None
        if days is None:
            issues.append(f"Unreadable snapshot date: {last}.")
        elif days > MAX_DAYS_WITHOUT_SNAPSHOT:
            issues.append(f"Last snapshot is {days} days old ({last}).")

    # 4. Required fields.
    nulls = conn.execute(
        "SELECT COUNT(*) FROM silver_dim_video WHERE published_at IS NULL OR published_at = ''"
    ).fetchone()[0]
    if nulls:
        issues.append(f"{nulls} video(s) without a publish date.")
    return issues


def run_pipeline(conn: sqlite3.Connection, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Bronze -> silver -> gold -> quality checks."""
    counts = build_silver(conn)
    return {**counts, "issues": run_quality_checks(conn, now)}
