import sqlite3
from datetime import datetime

import pytest

from pipeline.medallion import build_silver, ensure_schema, local_hour, run_quality_checks
from pipeline.sample_data import load_sample


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    load_sample(c, videos=6, weeks=3)
    yield c
    c.close()


NOW = datetime(2026, 2, 1)


def test_silver_counts_match_bronze(conn):
    counts = build_silver(conn)
    assert counts == {"dim": 6, "fact": 18}


def test_reload_is_idempotent(conn):
    build_silver(conn)
    first = conn.execute("SELECT * FROM silver_fact_video_week ORDER BY 1, 2").fetchall()
    build_silver(conn)
    assert conn.execute("SELECT * FROM silver_fact_video_week ORDER BY 1, 2").fetchall() == first


def test_views_delta_uses_previous_snapshot(conn):
    build_silver(conn)
    rows = conn.execute("""SELECT views, views_delta FROM silver_fact_video_week
                           WHERE video_id = 'vid000' ORDER BY snapshot_date""").fetchall()
    assert rows[0][1] is None
    assert rows[1][1] == rows[1][0] - rows[0][0]


def test_quality_clean_data_has_no_issues(conn):
    build_silver(conn)
    assert run_quality_checks(conn, NOW) == []


def test_quality_flags_big_view_drop(conn):
    conn.execute("""UPDATE video_public_stats SET views = 1
                    WHERE video_id = 'vid001' AND snapshot_date = (
                        SELECT MAX(snapshot_date) FROM video_public_stats WHERE video_id = 'vid001')""")
    build_silver(conn)
    assert any("dropped" in i for i in run_quality_checks(conn, NOW))


def test_quality_flags_stale_data(conn):
    build_silver(conn)
    assert any("days old" in i for i in run_quality_checks(conn, datetime(2026, 6, 1)))


def test_retention_view_ignores_looped_videos(conn):
    conn.execute("UPDATE video_retention_stats SET avg_view_pct = 480 WHERE video_id = 'vid000'")
    build_silver(conn)
    rows = dict((r[0], r[1]) for r in conn.execute("SELECT series_key, videos FROM gold_retention_by_series"))
    assert rows["money_dictionary"] == 1  # vid000 excluded, vid003 kept


def test_views_by_hour_groups_local_hours(conn):
    build_silver(conn)
    hours = {r[0] for r in conn.execute("SELECT local_hour FROM gold_views_by_hour")}
    assert hours == {14, 19}


@pytest.mark.parametrize("value,expected", [
    ("2026-01-04T20:00:00Z", 14),
    ("2026-01-04T19:00:00", 19),
    ("2026-01-04T20:00:00+00:00", 14),
    ("", None),
    ("not a date", None),
])
def test_local_hour(value, expected):
    assert local_hour(value) == expected


def test_empty_database_reports_no_snapshots():
    c = sqlite3.connect(":memory:")
    ensure_schema(c)
    build_silver(c)
    assert "No snapshots at all." in run_quality_checks(c, NOW)
