from __future__ import annotations

import sqlite3
from pathlib import Path

from .sample_data import SAMPLE_FILES, build_sample_database


def test_extraction_pipeline(tmp_path, samples_dir: Path):
    db_path = build_sample_database(tmp_path, samples_dir)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    shift_notes = conn.execute(
        "SELECT author_name, shift_window, bm_occurred FROM shift_notes"
    ).fetchall()
    assert len(shift_notes) == 2
    lower_authors = {
        (row["author_name"] or "").lower() for row in shift_notes if row["author_name"]
    }
    assert any("stacy moses" in name for name in lower_authors)

    incident_report = conn.execute(
        "SELECT participant_name, immediate_actions FROM incident_reports"
    ).fetchone()
    assert incident_report is not None
    assert incident_report["participant_name"] == "Will White"
    assert incident_report["immediate_actions"] is not None

    investigation = conn.execute(
        "SELECT incident_classification, ndis_reporting_status FROM incident_investigations"
    ).fetchone()
    assert investigation is not None
    assert investigation["ndis_reporting_status"] == "Monthly Reporting"

    processed_rows = conn.execute(
        "SELECT COUNT(*) FROM processed_entities WHERE status='success'"
    ).fetchone()[0]
    assert processed_rows == len(SAMPLE_FILES)

    conn.close()
