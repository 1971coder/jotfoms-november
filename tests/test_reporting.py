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
        "shift_hydration_summary",
        "shift_sleep_quality",
        "incident_prn_usage",
        "prn_baseline_deltas",
        "incident_training_requests",
    }
    assert expected_keys.issubset(outputs.keys())

    con = duckdb.connect()

    metrics_path = outputs["shift_daily_metrics"]
    result = con.execute(
        "SELECT total_notes, bm_yes_count FROM read_parquet(?) ORDER BY note_date LIMIT 1",
        [str(metrics_path)],
    ).fetchone()
    assert result is not None
    total_notes, bm_yes = result
    assert total_notes >= 1
    assert bm_yes >= 0

    sleep_path = outputs["shift_sleep_quality"]
    sleep_row = con.execute(
        "SELECT total_notes, disturbance_count FROM read_parquet(?) ORDER BY note_date LIMIT 1",
        [str(sleep_path)],
    ).fetchone()
    assert sleep_row is not None
    assert sleep_row[0] >= sleep_row[1]

    prn_path = outputs["incident_prn_usage"]
    prn_count = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(prn_path)],
    ).fetchone()[0]
    assert prn_count >= 1

    hydration_shift_path = outputs["shift_hydration_summary"]
    hydration_shift = con.execute(
        "SELECT total_hydration_ml, hydration_logged_count "
        "FROM read_parquet(?) ORDER BY note_date LIMIT 1",
        [str(hydration_shift_path)],
    ).fetchone()
    assert hydration_shift is not None
    total_hydration_ml, logged_count = hydration_shift
    assert logged_count >= 0
    assert total_hydration_ml is not None

    hydration_daily_path = outputs["hydration_daily_summary"]
    hydration_daily = con.execute(
        "SELECT hydration_entries, water_mentions "
        "FROM read_parquet(?) "
        "WHERE hydration_entries IS NOT NULL LIMIT 1",
        [str(hydration_daily_path)],
    ).fetchone()
    assert hydration_daily is not None
    entries, mentions = hydration_daily
    assert entries
    assert mentions >= 0

    baseline_path = outputs["prn_baseline_deltas"]
    baseline_row = con.execute(
        "SELECT baseline_descriptor, delta_vs_45_minutes "
        "FROM read_parquet(?) "
        "WHERE baseline_descriptor IS NOT NULL LIMIT 1",
        [str(baseline_path)],
    ).fetchone()
    assert baseline_row is not None
    descriptor, delta = baseline_row
    assert descriptor in {"within", "over", "less", "reported", "not_returned"}
    assert delta is None or isinstance(delta, (int, float))

    training_path = outputs["incident_training_requests"]
    training_row = con.execute(
        "SELECT training_request_rate FROM read_parquet(?) LIMIT 1",
        [str(training_path)],
    ).fetchone()
    assert training_row is not None
    request_rate = training_row[0]
    assert request_rate is None or 0 <= request_rate <= 100

    con.close()
