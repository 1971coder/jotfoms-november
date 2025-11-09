from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, TYPE_CHECKING

try:  # pragma: no cover - environment dependent import
    import duckdb  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    duckdb = None  # type: ignore

if TYPE_CHECKING:  # pragma: no cover - typing aid only
    from duckdb import DuckDBPyConnection
else:  # pragma: no cover - runtime fallback
    DuckDBPyConnection = Any


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
    "hydration_daily_summary": """
        WITH hydration AS (
            SELECT
                note_date,
                COALESCE(participant_name, 'Unknown') AS participant_name,
                hydration_intake,
                LOWER(COALESCE(hydration_intake, '')) AS hydration_lower,
                REGEXP_REPLACE(
                    LOWER(COALESCE(hydration_intake, '')),
                    '^[:\\s]+',
                    ''
                ) AS normalized_text,
                TRY_CAST(
                    REGEXP_EXTRACT(
                        LOWER(COALESCE(hydration_intake, '')),
                        '([0-9]+(?:\\.[0-9]+)?)'
                    ) AS DOUBLE
                ) AS numeric_value
            FROM source.shift_notes
            WHERE note_date IS NOT NULL
        ),
        prepared AS (
            SELECT
                note_date,
                participant_name,
                hydration_intake,
                normalized_text,
                CASE
                    WHEN normalized_text = '' THEN NULL
                    WHEN normalized_text LIKE '%ml%' THEN numeric_value
                    WHEN normalized_text LIKE '%litre%' OR normalized_text LIKE '%liter%' OR normalized_text LIKE '% l%'
                        THEN numeric_value * 1000
                    WHEN normalized_text LIKE '%cup%' THEN numeric_value * 250
                    WHEN normalized_text LIKE '%glass%' THEN numeric_value * 250
                    WHEN normalized_text LIKE '%bottle%' THEN numeric_value * 600
                    ELSE NULL
                END AS hydration_ml,
                normalized_text LIKE '%water%' AS mentions_water
            FROM hydration
        )
        SELECT
            note_date,
            participant_name,
            COUNT(*) AS entries,
            LIST(DISTINCT hydration_intake)
                FILTER (WHERE hydration_intake IS NOT NULL AND hydration_intake <> '') AS hydration_entries,
            SUM(CASE WHEN normalized_text <> '' THEN 1 ELSE 0 END) AS logged_entries,
            SUM(CASE WHEN mentions_water THEN 1 ELSE 0 END) AS water_mentions,
            AVG(hydration_ml) AS avg_hydration_ml
        FROM prepared
        GROUP BY note_date, participant_name
        ORDER BY note_date, participant_name
    """,
    "shift_hydration_summary": """
        WITH hydration AS (
            SELECT
                note_date,
                LOWER(COALESCE(hydration_intake, '')) AS hydration_lower,
                REGEXP_REPLACE(LOWER(COALESCE(hydration_intake, '')), '^[:\\s]+', '') AS normalized_text,
                TRY_CAST(
                    REGEXP_EXTRACT(
                        LOWER(COALESCE(hydration_intake, '')),
                        '([0-9]+(?:\\.[0-9]+)?)'
                    )
                    AS DOUBLE
                ) AS numeric_value
            FROM source.shift_notes
            WHERE note_date IS NOT NULL
        ),
        prepared AS (
            SELECT
                note_date,
                normalized_text,
                CASE
                    WHEN normalized_text = '' THEN NULL
                    WHEN normalized_text LIKE '%ml%' THEN numeric_value
                    WHEN normalized_text LIKE '%litre%' OR normalized_text LIKE '%liter%' OR normalized_text LIKE '% l%'
                        THEN numeric_value * 1000
                    WHEN normalized_text LIKE '%cup%' THEN numeric_value * 250
                    WHEN normalized_text LIKE '%glass%' THEN numeric_value * 250
                    WHEN normalized_text LIKE '%bottle%' THEN numeric_value * 600
                    ELSE NULL
                END AS hydration_ml,
                normalized_text LIKE '%water%' AS mentions_water
            FROM hydration
        )
        SELECT
            note_date,
            COUNT(*) AS total_notes,
            SUM(CASE WHEN normalized_text <> '' THEN 1 ELSE 0 END) AS hydration_logged_count,
            SUM(CASE WHEN normalized_text = '' THEN 1 ELSE 0 END) AS hydration_missing_count,
            SUM(COALESCE(hydration_ml, 0)) AS total_hydration_ml,
            AVG(hydration_ml) AS avg_hydration_ml,
            SUM(CASE WHEN mentions_water THEN 1 ELSE 0 END) AS water_mentions,
            LIST(normalized_text) FILTER (WHERE normalized_text <> '') AS hydration_entries
        FROM prepared
        GROUP BY note_date
        ORDER BY note_date
    """,
    "shift_sleep_quality": """
        SELECT
            note_date,
            COUNT(*) AS total_notes,
            SUM(CASE WHEN sleep_disturbance = 1 THEN 1 ELSE 0 END) AS disturbance_count,
            SUM(CASE WHEN sleep_disturbance = 0 THEN 1 ELSE 0 END) AS settled_count,
            SUM(CASE WHEN sleep_start_time IS NOT NULL THEN 1 ELSE 0 END) AS recorded_bedtime_count
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
    "prn_baseline_deltas": """
        WITH combined AS (
            SELECT
                'incident_report' AS source,
                id,
                participant_name,
                prn_name,
                prn_admin_time,
                prn_authorised,
                prn_recurrence,
                prn_time_period,
                prn_time_window,
                prn_baseline_duration
            FROM source.incident_reports
            UNION ALL
            SELECT
                'incident_investigation' AS source,
                id,
                participant_name,
                prn_name,
                prn_admin_time,
                prn_authorised,
                prn_recurrence,
                prn_time_period,
                prn_time_window,
                prn_baseline_duration
            FROM source.incident_investigations
        ),
        prepared AS (
            SELECT
                *,
                LOWER(COALESCE(prn_baseline_duration, '')) AS duration_lower,
                TRY_CAST(
                    REGEXP_EXTRACT(
                        LOWER(COALESCE(prn_baseline_duration, '')),
                        '([0-9]+(?:\\.[0-9]+)?)'
                    ) AS DOUBLE
                ) AS numeric_value
            FROM combined
            WHERE prn_name IS NOT NULL OR prn_baseline_duration IS NOT NULL
        ),
        enriched AS (
            SELECT
                source,
                id,
                participant_name,
                prn_name,
                prn_admin_time,
                prn_authorised,
                prn_recurrence,
                prn_time_period,
                prn_time_window,
                prn_baseline_duration,
                CASE
                    WHEN prn_baseline_duration IS NULL OR prn_baseline_duration = '' THEN NULL
                    WHEN prn_baseline_duration ILIKE '%did not return%' THEN 'not_returned'
                    WHEN prn_baseline_duration ILIKE '%over%' THEN 'over'
                    WHEN prn_baseline_duration ILIKE '%within%' THEN 'within'
                    WHEN prn_baseline_duration ILIKE '%less%' THEN 'less'
                    ELSE 'reported'
                END AS baseline_descriptor,
                CASE
                    WHEN duration_lower LIKE '%hour%' OR duration_lower LIKE '%hr%' THEN numeric_value * 60
                    WHEN duration_lower LIKE '%min%' THEN numeric_value
                    WHEN duration_lower LIKE '%sec%' THEN numeric_value / 60
                    WHEN duration_lower LIKE '%day%' THEN numeric_value * 60 * 24
                    ELSE numeric_value
                END AS baseline_minutes_estimate
            FROM prepared
        )
        SELECT
            source,
            id,
            participant_name,
            prn_name,
            prn_admin_time,
            prn_authorised,
            prn_recurrence,
            prn_time_period,
            prn_time_window,
            prn_baseline_duration,
            baseline_descriptor,
            baseline_minutes_estimate,
            CASE
                WHEN baseline_minutes_estimate IS NULL THEN NULL
                ELSE baseline_minutes_estimate - 45
            END AS delta_vs_45_minutes
        FROM enriched
        ORDER BY participant_name, prn_admin_time
    """,
    "incident_training_requests": """
        WITH parsed AS (
            SELECT
                TRY_CAST(awareness_timestamp AS TIMESTAMP) AS awareness_ts,
                training_request
            FROM source.incident_reports
        )
        SELECT
            DATE(awareness_ts) AS awareness_date,
            COUNT(*) AS incident_count,
            SUM(CASE WHEN training_request = 1 THEN 1 ELSE 0 END) AS training_requested_count,
            CASE
                WHEN COUNT(*) = 0 THEN NULL
                ELSE SUM(CASE WHEN training_request = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
            END AS training_request_rate
        FROM parsed
        WHERE awareness_ts IS NOT NULL
        GROUP BY awareness_date
        ORDER BY awareness_date
    """,
}



def _copy_to_parquet(con: "DuckDBPyConnection", query: str, output_path: Path) -> None:
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
    if duckdb is None:
        raise RuntimeError(
            "DuckDB is required to generate reports. Install the 'duckdb' package to continue."
        )
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
