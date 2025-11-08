from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict

import duckdb


LOGGER = logging.getLogger(__name__)

RAW_EXPORTS = {
    "shift_notes": "SELECT * FROM source.shift_notes",
    "incident_reports": "SELECT * FROM source.incident_reports",
    "incident_investigations": "SELECT * FROM source.incident_investigations",
}

ANALYTIC_QUERIES = {
    "shift_daily_metrics": """
        SELECT
            note_date,
            COUNT(*) AS total_notes,
            SUM(CASE WHEN bm_occurred = 1 THEN 1 ELSE 0 END) AS bm_yes_count,
            SUM(CASE WHEN bm_occurred = 0 THEN 1 ELSE 0 END) AS bm_no_count
        FROM source.shift_notes
        WHERE note_date IS NOT NULL
        GROUP BY note_date
        ORDER BY note_date
    """,
    "incident_prn_usage": """
        SELECT
            'incident_report' AS source,
            id,
            prn_name,
            prn_admin_time,
            prn_authorised,
            prn_recurrence
        FROM source.incident_reports
        WHERE prn_name IS NOT NULL
        UNION ALL
        SELECT
            'incident_investigation' AS source,
            id,
            prn_name,
            prn_admin_time,
            prn_authorised,
            prn_recurrence
        FROM source.incident_investigations
        WHERE prn_name IS NOT NULL
    """,
}


def _copy_to_parquet(con: duckdb.DuckDBPyConnection, query: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()
    con.execute(
        f"COPY ({query}) TO '{output_path}' (FORMAT 'parquet', COMPRESSION 'zstd')"
    )


def generate_reports(sqlite_path: Path, output_dir: Path) -> Dict[str, Path]:
    """
    Produce Parquet exports for downstream analytics.
    Returns mapping of report name -> file path.
    """
    sqlite_path = sqlite_path.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL sqlite;")
    con.execute("LOAD sqlite;")
    con.execute(f"ATTACH '{sqlite_path}' AS source (TYPE SQLITE);")

    report_paths: Dict[str, Path] = {}

    for name, query in RAW_EXPORTS.items():
        dest = (output_dir / f"{name}.parquet").resolve()
        LOGGER.info("Exporting %s to %s", name, dest)
        _copy_to_parquet(con, query, dest)
        report_paths[name] = dest

    for name, query in ANALYTIC_QUERIES.items():
        dest = (output_dir / f"{name}.parquet").resolve()
        LOGGER.info("Exporting analytic view %s", name)
        _copy_to_parquet(con, query, dest)
        report_paths[name] = dest

    con.close()
    return report_paths

