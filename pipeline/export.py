"""Export the gold layer to Parquet files and query them with DuckDB.

SQLite stays the working database. Parquet is the hand-off format: a columnar file that
DuckDB, Spark, pandas or a BI tool can read without knowing anything about SQLite.

Each gold view gets an explicit schema below, so even an empty view produces a valid
Parquet file with the right column types (type inference from zero rows is impossible).

Needs the optional dependency: pip install duckdb
"""

import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple, Union

from pipeline.medallion import GOLD_VIEWS

# Column name -> DuckDB type, in the same order as the SELECT of each gold view.
GOLD_SCHEMAS: Dict[str, List[Tuple[str, str]]] = {
    "gold_first_week_views_by_pillar": [
        ("pillar", "VARCHAR"), ("videos", "BIGINT"), ("avg_views", "BIGINT")],
    "gold_retention_by_series": [
        ("series_key", "VARCHAR"), ("videos", "BIGINT"), ("avg_retention_pct", "DOUBLE")],
    "gold_views_by_hour": [
        ("local_hour", "BIGINT"), ("videos", "BIGINT"), ("avg_views", "BIGINT")],
}


def _duckdb():
    try:
        import duckdb
    except ImportError as exc:  # keep the core pipeline dependency-free
        raise RuntimeError("Parquet export needs DuckDB: pip install duckdb") from exc
    return duckdb


def export_gold_to_parquet(conn: sqlite3.Connection, out_dir: Union[str, Path]) -> Dict[str, int]:
    """Writes one <view>.parquet per gold view. Returns rows written per view.

    Files are overwritten, so running it twice gives the same files (idempotent).
    """
    duckdb = _duckdb()
    missing = set(GOLD_VIEWS) - set(GOLD_SCHEMAS)
    if missing:
        raise ValueError(f"No Parquet schema for gold view(s): {sorted(missing)}")

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written: Dict[str, int] = {}
    with duckdb.connect() as duck:  # in-memory: only used to build the files
        for view, schema in GOLD_SCHEMAS.items():
            cursor = conn.execute(f"SELECT * FROM {view}")
            columns = [d[0] for d in cursor.description]
            expected = [name for name, _ in schema]
            if columns != expected:
                raise ValueError(f"{view}: columns {columns} do not match schema {expected}")
            rows = cursor.fetchall()

            duck.execute(f"DROP TABLE IF EXISTS {view}")
            duck.execute(f"CREATE TABLE {view} ({', '.join(f'{n} {t}' for n, t in schema)})")
            if rows:
                marks = ", ".join("?" * len(schema))
                duck.executemany(f"INSERT INTO {view} VALUES ({marks})", rows)
            target = (out / f"{view}.parquet").as_posix().replace("'", "''")
            duck.execute(f"COPY {view} TO '{target}' (FORMAT parquet)")
            written[view] = len(rows)
    return written


def query_parquet(out_dir: Union[str, Path], sql: str) -> List[tuple]:
    """Runs SQL with DuckDB where each gold view name points at its Parquet file."""
    duckdb = _duckdb()
    out = Path(out_dir)
    with duckdb.connect() as duck:
        for view in GOLD_SCHEMAS:
            path = (out / f"{view}.parquet").as_posix().replace("'", "''")
            duck.execute(f"CREATE VIEW {view} AS SELECT * FROM read_parquet('{path}')")
        return duck.execute(sql).fetchall()


if __name__ == "__main__":
    import sys
    from datetime import datetime

    from pipeline.medallion import run_pipeline
    from pipeline.sample_data import load_sample

    target = sys.argv[1] if len(sys.argv) > 1 else "out"
    sqlite = sqlite3.connect(":memory:")
    load_sample(sqlite)
    run_pipeline(sqlite, now=datetime(2026, 2, 14))
    print(export_gold_to_parquet(sqlite, target))
    print(query_parquet(target, """
        SELECT local_hour, avg_views FROM gold_views_by_hour ORDER BY avg_views DESC
    """))
