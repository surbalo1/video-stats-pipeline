import sqlite3
from datetime import datetime

import pytest

duckdb = pytest.importorskip("duckdb")

from pipeline.export import GOLD_SCHEMAS, export_gold_to_parquet, query_parquet  # noqa: E402
from pipeline.medallion import GOLD_VIEWS, ensure_schema, run_pipeline  # noqa: E402
from pipeline.sample_data import load_sample  # noqa: E402


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    load_sample(c, videos=6, weeks=3)
    run_pipeline(c, now=datetime(2026, 2, 1))
    yield c
    c.close()


def test_every_gold_view_has_a_schema():
    assert set(GOLD_SCHEMAS) == set(GOLD_VIEWS)


def test_parquet_row_counts_match_gold_views(conn, tmp_path):
    written = export_gold_to_parquet(conn, tmp_path)
    for view in GOLD_VIEWS:
        expected = conn.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
        assert written[view] == expected
        assert query_parquet(tmp_path, f"SELECT COUNT(*) FROM {view}")[0][0] == expected


def test_parquet_values_match_gold_views(conn, tmp_path):
    export_gold_to_parquet(conn, tmp_path)
    for view in GOLD_VIEWS:
        sqlite_rows = conn.execute(f"SELECT * FROM {view} ORDER BY 1").fetchall()
        assert query_parquet(tmp_path, f"SELECT * FROM {view} ORDER BY 1") == sqlite_rows


def test_parquet_keeps_declared_types(conn, tmp_path):
    export_gold_to_parquet(conn, tmp_path)
    path = (tmp_path / "gold_retention_by_series.parquet").as_posix()
    types = duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
    assert [(t[0], t[1]) for t in types] == GOLD_SCHEMAS["gold_retention_by_series"]


def test_export_is_idempotent(conn, tmp_path):
    first = export_gold_to_parquet(conn, tmp_path)
    assert export_gold_to_parquet(conn, tmp_path) == first
    assert query_parquet(tmp_path, "SELECT SUM(videos) FROM gold_views_by_hour")[0][0] == 6


def test_empty_database_writes_empty_typed_files(tmp_path):
    c = sqlite3.connect(":memory:")
    ensure_schema(c)
    assert set(export_gold_to_parquet(c, tmp_path).values()) == {0}
    assert query_parquet(tmp_path, "SELECT COUNT(*) FROM gold_first_week_views_by_pillar") == [(0,)]


def test_schema_drift_is_caught(conn, tmp_path, monkeypatch):
    broken = dict(GOLD_SCHEMAS, gold_views_by_hour=[("hour", "BIGINT"), ("videos", "BIGINT"),
                                                    ("avg_views", "BIGINT")])
    monkeypatch.setattr("pipeline.export.GOLD_SCHEMAS", broken)
    with pytest.raises(ValueError, match="do not match schema"):
        export_gold_to_parquet(conn, tmp_path)
