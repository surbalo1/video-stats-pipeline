"""Synthetic sample data (made up, not real channel numbers) and a small CLI demo."""

import random
import sqlite3
from datetime import date, datetime, timedelta

from pipeline.medallion import ensure_schema, run_pipeline

PILLARS = ["savings", "credit", "taxes"]
SERIES = ["money_dictionary", "hidden_fees", None]
HOURS_UTC = ["20:00:00Z", "01:00:00Z"]  # 14:00 and 19:00 local


def load_sample(conn: sqlite3.Connection, videos: int = 12, weeks: int = 4,
                start: date = date(2026, 1, 4), seed: int = 7) -> None:
    rng = random.Random(seed)
    ensure_schema(conn)
    with conn:
        for i in range(videos):
            vid = f"vid{i:03d}"
            published = start + timedelta(days=i)
            published_at = f"{published.isoformat()}T{HOURS_UTC[i % 2]}"
            conn.execute("INSERT INTO video_catalog VALUES (?,?,?,?,?,?)", (
                vid, f"topic_{i}", PILLARS[i % 3], SERIES[i % 3], i // 3 + 1, published_at))
            views = 0
            for w in range(1, weeks + 1):
                snap = published + timedelta(days=7 * w)
                views += rng.randint(100, 900)
                conn.execute("INSERT INTO video_public_stats VALUES (?,?,?,?,?,?,?,?)", (
                    vid, snap.isoformat(), f"Sample video {i}", published_at, 70,
                    views, views // 20, views // 100))
                conn.execute("INSERT INTO video_retention_stats VALUES (?,?,?)", (
                    vid, snap.isoformat(), round(rng.uniform(40, 95), 1)))


if __name__ == "__main__":
    conn = sqlite3.connect(":memory:")
    load_sample(conn)
    print(run_pipeline(conn, now=datetime(2026, 2, 14)))
    for view in ("gold_first_week_views_by_pillar", "gold_retention_by_series", "gold_views_by_hour"):
        print(f"\n{view}")
        for row in conn.execute(f"SELECT * FROM {view} ORDER BY 1"):
            print("  ", row)
