from __future__ import annotations

from pathlib import Path

import duckdb

from analytics.reporting import generate_reports
from .sample_data import build_sample_database


def test_generate_reports(tmp_path, samples_dir: Path):
    db_path = build_sample_database(tmp_path, samples_dir)
    output_dir = tmp_path / "reports"

    outputs = generate_reports(db_path, output_dir)

    expected_keys = {
        "shift_notes",
        "incident_reports",
        "incident_investigations",
        "shift_daily_metrics",
        "incident_prn_usage",
    }
    assert expected_keys.issubset(outputs.keys())

    con = duckdb.connect()

    metrics_path = outputs["shift_daily_metrics"]
    result = con.execute(
        "SELECT total_notes, bm_yes_count FROM read_parquet(?) WHERE note_date = '2024-03-26'",
        [str(metrics_path)],
    ).fetchone()
    assert result is not None
    assert result[0] >= 1

    prn_path = outputs["incident_prn_usage"]
    prn_count = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(prn_path)],
    ).fetchone()[0]
    assert prn_count >= 1

    con.close()
