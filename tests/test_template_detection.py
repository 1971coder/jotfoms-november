from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import pytest

from parsers.message_loader import load_message
from parsers.template_detection import detect_template

TEMPLATE_FIXTURES = {
    "automated_daily_shift_note": [
        "Will's automated daily shift note- 2024-03-26.eml",
        "Will's automated daily shift note- 2024-04-09.eml",
        "Will's automated daily shift note- 2024-08-09.eml",
    ],
    "jotform_shift_note": [
        "Re- Will White - The Hive SILC Shift Notes - David Baraza 19.eml",
        "Re- Will White - The Hive SILC Shift Notes - David Baraza 63.eml",
        "Re- Will White - The Hive SILC Shift Notes - Diana East 20.eml",
        "Re- Will White - The Hive SILC Shift Notes - Graeme Kolomalu 14.eml",
        "Re- Will White - The Hive SILC Shift Notes - Graeme Kolomalu 7.eml",
        "Re- Will White - The Hive SILC Shift Notes - Ina Maraeara 31.eml",
    ],
    "jotform_incident_notification": [
        "Incident Report Notification - Will White 12.eml",
        "Incident Report Notification - Will White 35.eml",
        "Incident Report Notification - Will White 49.eml",
        "Incident Report Notification - Will White 66.eml",
        "Incident Report Notification - Will White 70.eml",
        "Incident Report Notification - Will White 76.eml",
        "Incident Report Notification - Will White 79.eml",
        "Incident Report Notification - Will White 103.eml",
        "Incident Report Notification - Will White 104.eml",
    ],
    "incident_investigation_update": [
        "EDIT- Incident Investigation Completed - Incident Dated 2024-03-08 4-20 PM.eml",
        "EDIT- Incident Investigation Completed - Incident Dated 2024-08-24 3-00 PM.eml",
        "EDIT- Incident Investigation Completed - Incident Dated 2024-08-25 2-30 PM.eml",
        "EDIT- Incident Investigation Completed - Incident Dated 2024-11-07 3-50 PM.eml",
        "EDIT- Incident Investigation Completed - Incident Dated 2025-02-01 1-30 PM.eml",
        "EDIT- Incident Investigation Completed - Incident Dated 2025-04-11 6-37 PM.eml",
    ],
}


def _expand_cases() -> List[Tuple[str, str]]:
    cases: List[Tuple[str, str]] = []
    for template_id, filenames in TEMPLATE_FIXTURES.items():
        for filename in filenames:
            cases.append((filename, template_id))
    return cases


@pytest.mark.parametrize(
    ("eml_filename", "expected_template"),
    _expand_cases(),
)
def test_template_detection(samples_dir: Path, eml_filename: str, expected_template: str):
    envelope = load_message(samples_dir / eml_filename)
    template_id, confidence = detect_template(envelope)
    assert (
        template_id == expected_template
    ), f"{eml_filename} classified as {template_id}"
    assert 0 < confidence <= 1.0

