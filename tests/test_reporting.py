from __future__ import annotations

from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

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
        "hydration_daily_summary",
        "incident_prn_usage",
        "prn_baseline_deltas",
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

    hydration_path = outputs["hydration_daily_summary"]
    hydration_row = con.execute(
        """
        SELECT hydration_entries, water_mentions
        FROM read_parquet(?)
        WHERE note_date = '2024-03-26'
        LIMIT 1
        """,
        [str(hydration_path)],
    ).fetchone()
    assert hydration_row is not None
    entries, water_mentions = hydration_row
    assert entries, "expected hydration entries to be populated"
    assert water_mentions >= 1

    baseline_path = outputs["prn_baseline_deltas"]
    baseline_row = con.execute(
        """
        SELECT baseline_descriptor, delta_vs_45_minutes
        FROM read_parquet(?)
        WHERE prn_baseline_duration IS NOT NULL
        LIMIT 1
        """,
        [str(baseline_path)],
    ).fetchone()
    assert baseline_row is not None
    descriptor, delta = baseline_row
    assert descriptor in {"within", "over", "less", "reported", "not_returned"}
    assert delta is None or isinstance(delta, (int, float))

    con.close()
