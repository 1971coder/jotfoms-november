from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from .message_loader import MessageEnvelope


@dataclass(frozen=True)
class TemplateRule:
    template_id: str
    subject_keywords: Tuple[str, ...]


TEMPLATE_RULES = (
    TemplateRule(
        template_id="incident_investigation_update",
        subject_keywords=("incident investigation completed",),
    ),
    TemplateRule(
        template_id="jotform_incident_notification",
        subject_keywords=("incident report notification",),
    ),
    TemplateRule(
        template_id="jotform_shift_note",
        subject_keywords=("the hive silc shift notes",),
    ),
    TemplateRule(
        template_id="automated_daily_shift_note",
        subject_keywords=("automated daily shift note",),
    ),
)


def detect_template(envelope: MessageEnvelope) -> Tuple[Optional[str], float]:
    """
    Return (template_id, confidence) based on subject heuristics.
    Confidence is a simple ratio of matched keywords to rule keywords.
    """
    subject = (envelope.subject or "").lower()

    best_match: Optional[str] = None
    best_confidence = 0.0

    for rule in TEMPLATE_RULES:
        matches = sum(1 for kw in rule.subject_keywords if kw in subject)
        if matches:
            confidence = matches / len(rule.subject_keywords)
            if confidence > best_confidence:
                best_confidence = confidence
                best_match = rule.template_id

    return best_match, best_confidence

