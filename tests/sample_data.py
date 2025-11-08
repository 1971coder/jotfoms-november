from __future__ import annotations

from pathlib import Path

from ingestion.pipeline import EmailIngestionPipeline
from extraction.pipeline import run_extraction

SAMPLE_FILES = {
    "automated_daily_shift_note": "Will's automated daily shift note- 2024-03-26.eml",
    "jotform_shift_note": "Re- Will White - The Hive SILC Shift Notes - Graeme Kolomalu 7.eml",
    "incident_report": "Incident Report Notification - Will White 66.eml",
    "incident_investigation": "EDIT- Incident Investigation Completed - Incident Dated 2024-08-24 3-00 PM.eml",
}


def build_sample_database(tmp_path: Path, samples_dir: Path) -> Path:
    """
    Create a SQLite DB populated with canonical entities using fixtures.
    Returns the database path.
    """
    db_path = tmp_path / "pipeline.db"
    raw_dir = tmp_path / "raw"
    attachment_dir = tmp_path / "attachments"

    ingest = EmailIngestionPipeline(db_path, raw_dir, attachment_dir)
    try:
        for filename in SAMPLE_FILES.values():
            ingest.ingest_file(samples_dir / filename)
    finally:
        ingest.close()

    run_extraction(db_path)
    return db_path

