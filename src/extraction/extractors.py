from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from parsers.html_tables import extract_question_rows
from parsers.message_loader import MessageEnvelope
from parsers.text_sections import SectionParser
from parsers.value_normalizers import (
    parse_bool,
    parse_bullet_list,
    parse_date,
    parse_datetime,
    parse_int,
    parse_json_list,
    parse_time,
    split_multivalue,
)


@dataclass
class ExtractionResult:
    entity_type: str
    canonical: Dict[str, Any]
    additional_fields: Dict[str, Any]


class BaseExtractor:
    template_id: str
    entity_type: str

    def extract(self, envelope: MessageEnvelope) -> ExtractionResult:
        raise NotImplementedError


def _pop_value(field_map: Dict[str, List[str]], label: str) -> Optional[str]:
    values = field_map.get(label)
    if not values:
        return None
    value = values.pop(0).strip()
    if not values:
        field_map.pop(label, None)
    return value


def _remaining_additional(field_map: Dict[str, List[str]]) -> Dict[str, str]:
    additional: Dict[str, str] = {}
    for label, values in field_map.items():
        for idx, value in enumerate(values, start=1):
            key = label if len(values) == 1 else f"{label} ({idx})"
            additional[key] = value
    return additional


class AutomatedDailyShiftNoteExtractor(BaseExtractor):
    template_id = "automated_daily_shift_note"
    entity_type = "shift_note"

    SECTION_LABELS = [
        "Date",
        "Written by",
        "Description of activities",
        "Description of mood",
        "What did the participant drink today",
        "Kilometres walked today",
        "What did the participant eat today",
        "Did will have a bowel movement?",
        "What rating on the Bristol Stool Chart was it?",
    ]

    SECTION_PARSER = SectionParser(SECTION_LABELS)

    def extract(self, envelope: MessageEnvelope) -> ExtractionResult:
        if not envelope.text_body:
            raise ValueError("Plain text body required for automated shift note")

        sections = self.SECTION_PARSER.parse(envelope.text_body)
        canonical: Dict[str, Any] = {}

        date_value = sections.pop("Date", None)
        parsed_date = parse_date(date_value) if date_value else None
        if parsed_date:
            canonical["note_date"] = parsed_date
        elif envelope.sent_at:
            canonical["note_date"] = envelope.sent_at.date().isoformat()

        writer = sections.pop("Written by", None)
        if writer:
            canonical["author_name"] = writer

        activities = sections.pop("Description of activities", None)
        if activities:
            canonical["activities_summary"] = activities

        mood = sections.pop("Description of mood", None)
        if mood:
            canonical["mood_summary"] = mood

        hydration = sections.pop("What did the participant drink today", None)
        if hydration:
            canonical["hydration_intake"] = hydration

        kms = sections.pop("Kilometres walked today", None)
        if kms:
            canonical["kilometres_walked"] = kms

        meals = sections.pop("What did the participant eat today", None)
        meal_list = parse_json_list(meals) or split_multivalue(meals)
        if meal_list:
            canonical["meals_consumed"] = meal_list
        elif meals:
            canonical["meals_consumed"] = [meals]

        bm_value = sections.pop("Did will have a bowel movement?", None)
        bm_flag = parse_bool(bm_value)
        if bm_flag is not None:
            canonical["bm_occurred"] = bm_flag

        bm_rating = sections.pop("What rating on the Bristol Stool Chart was it?", None)
        if bm_rating:
            canonical["bm_rating"] = bm_rating

        additional = {label: value for label, value in sections.items() if value}

        return ExtractionResult(
            entity_type=self.entity_type,
            canonical=canonical,
            additional_fields=additional,
        )


@dataclass(frozen=True)
class FieldMapping:
    canonical_key: str
    value_type: str  # text, date, bool, list, datetime, int, time, bullets


def _transform_value(value: Optional[str], value_type: str) -> Any:
    if value is None:
        return None

    if value_type == "text":
        return " ".join(value.split())
    if value_type == "date":
        return parse_date(value)
    if value_type == "bool":
        return parse_bool(value)
    if value_type == "list":
        return split_multivalue(value)
    if value_type == "json_list":
        return parse_json_list(value) or split_multivalue(value)
    if value_type == "datetime":
        return parse_datetime(value)
    if value_type == "int":
        return parse_int(value)
    if value_type == "time":
        return parse_time(value)
    if value_type == "bullets":
        return parse_bullet_list(value)
    return value


class JotformShiftNoteExtractor(BaseExtractor):
    template_id = "jotform_shift_note"
    entity_type = "shift_note"

    FIELD_MAP = {
        "Who is this report about?": FieldMapping("participant_name", "text"),
        "Shift date (date your shift ended)": FieldMapping("note_date", "date"),
        "What day of the week is it?": FieldMapping("day_of_week", "text"),
        "Which shift are you reporting on?": FieldMapping("shift_window", "text"),
        "This report was prepared by": FieldMapping("author_name", "text"),
        "Did you provide personal care to Will?": FieldMapping(
            "personal_care_provided", "bool"
        ),
        "Did the resident seem well/unwell during the shift?": FieldMapping(
            "resident_wellness", "text"
        ),
        "Did Will have a Bowel Movement (BM) during your shift?": FieldMapping(
            "bm_occurred", "bool"
        ),
        "Did Will require support to manage his emotions or behaviour?": FieldMapping(
            "emotional_support_required", "text"
        ),
        "Did will struggle with transitions inside the house?": FieldMapping(
            "transition_difficulty", "text"
        ),
        "Did will struggle to accept/manage a change, or if something was unavailable?": FieldMapping(
            "change_management_difficulty", "text"
        ),
        "Select the below shift duties which you completed while on shift": FieldMapping(
            "shift_duties_completed", "list"
        ),
        "Was the Will involved in jobs around the house?": FieldMapping(
            "house_jobs_participation", "text"
        ),
        "Did any injuries or incidents occur during your shift?": FieldMapping(
            "incidents_occurred", "bool"
        ),
        "Were there any near misses on this shift? (When William escalated but you were able to calm him down)": FieldMapping(
            "near_misses", "text"
        ),
        "Were any hazards identified during your shift? (e.g a hazard is anything which might have cause an injury)": FieldMapping(
            "hazards_identified", "text"
        ),
        "Was there any visitors during this shift?": FieldMapping(
            "visitors_present", "text"
        ),
        "Any other issues, concerns or successes you would like to share?": FieldMapping(
            "issues_or_successes", "text"
        ),
        "is there anything you need your colleague to follow up on next shift or over the next few days?": FieldMapping(
            "follow_up_requests", "text"
        ),
        "Which of the following (if any) did you feel due to your shift?": FieldMapping(
            "staff_emotions", "list"
        ),
        "What time did the resident go to bed?": FieldMapping(
            "sleep_start_time", "text"
        ),
        "Was the resident's  sleep disturbed to the point that they required staff support to settle them back to bed?": FieldMapping(
            "sleep_disturbance", "bool"
        ),
    }

    def extract(self, envelope: MessageEnvelope) -> ExtractionResult:
        if not envelope.html_body:
            raise ValueError("HTML body required for Jotform shift notes")
        fields = extract_question_rows(envelope.html_body)
        canonical: Dict[str, Any] = {}

        for label, mapping in self.FIELD_MAP.items():
            raw_value = _pop_value(fields, label)
            transformed = _transform_value(raw_value, mapping.value_type)
            if transformed is not None:
                canonical[mapping.canonical_key] = transformed

        if "note_date" not in canonical and envelope.sent_at:
            canonical["note_date"] = envelope.sent_at.date().isoformat()

        additional = _remaining_additional(fields)

        return ExtractionResult(
            entity_type=self.entity_type,
            canonical=canonical,
            additional_fields=additional,
        )


class JotformIncidentExtractor(BaseExtractor):
    template_id = "jotform_incident_notification"
    entity_type = "incident_report"

    FIELD_MAP = {
        "Who is this incident report about?": FieldMapping("participant_name", "text"),
        "Incident Management Stage": FieldMapping("incident_stage", "text"),
        "Date & time you became aware of the incident": FieldMapping(
            "awareness_timestamp", "datetime"
        ),
        "How many staff were present at the time of the incident": FieldMapping(
            "staff_present_count", "int"
        ),
        "Who was, or potentially was impacted by the incident? If there was more than one person with disability impacted by the incident, a separate form must be completed for each participant": FieldMapping(
            "impacted_role", "text"
        ),
        "Name of person impacted (This includes residents, staff, family and community members)": FieldMapping(
            "impacted_person_name", "text"
        ),
        "What were the circumstances leading up to the incident? (What had the resident been doing? How was the participant's mood? What were the other resident doing? What were staff doing? How many staff were present? What were the likely triggers?)": FieldMapping(
            "pre_incident_context", "text"
        ),
        "Describe the incident/ allegation (Please provide all details including names of staff, location of incident (e.g which room in the House or venue), actions by all involved)": FieldMapping(
            "incident_description", "text"
        ),
        "Immediate action taken (Provide details of the immediate steps taken)": FieldMapping(
            "immediate_actions", "bullets"
        ),
        "Given the behaviour displayed in this incident, what does the behaviour support plan say to do?": FieldMapping(
            "bsp_guidance", "text"
        ),
        "Were these strategies effective?": FieldMapping(
            "strategy_effectiveness", "text"
        ),
        "Would you like more training on the behaviour support plan directly from the behaviour support clinician and your manager? (If you select yes, an email will be sent to your manager and behaviour support clinician.)": FieldMapping(
            "training_request", "bool"
        ),
        "If not, why not?": FieldMapping("training_rationale", "text"),
        "What could have been differently? Suggested action that be can be taken to lower the risk of future incidents.": FieldMapping(
            "preventative_actions", "text"
        ),
        "Type of incident (Tick all that apply)": FieldMapping(
            "incident_types", "list"
        ),
        "Was a restraint used on the resident to manage the incident?": FieldMapping(
            "restraint_used", "bool"
        ),
        "Name of PRN/ Chemical restraint": FieldMapping("prn_name", "text"),
        "Dosage administered": FieldMapping("prn_dosage", "text"),
        "Name of person who administered the PRN": FieldMapping(
            "prn_admin_person", "text"
        ),
        "What time was PRN administered": FieldMapping("prn_admin_time", "text"),
        "Is this restrictive practice (chemical restraint) authorised?": FieldMapping(
            "prn_authorised", "bool"
        ),
        "Was it a one-off emergency use? or is it likely to recur?": FieldMapping(
            "prn_recurrence", "text"
        ),
        "Is there a subject of allegation?": FieldMapping(
            "subject_of_allegation", "bool"
        ),
        "Was there any Witnesses": FieldMapping("witnesses_present", "text"),
        "Name of person completing the form": FieldMapping("reporter_name", "text"),
        "Role": FieldMapping("reporter_role", "text"),
        "Email": FieldMapping("reporter_email", "text"),
    }

    def extract(self, envelope: MessageEnvelope) -> ExtractionResult:
        if not envelope.html_body:
            raise ValueError("HTML body required for Jotform incident reports")
        fields = extract_question_rows(envelope.html_body)
        canonical: Dict[str, Any] = {}

        for label, mapping in self.FIELD_MAP.items():
            raw_value = _pop_value(fields, label)
            transformed = _transform_value(raw_value, mapping.value_type)
            if transformed is not None:
                canonical[mapping.canonical_key] = transformed

        additional = _remaining_additional(fields)
        return ExtractionResult(
            entity_type=self.entity_type,
            canonical=canonical,
            additional_fields=additional,
        )


class IncidentInvestigationExtractor(JotformIncidentExtractor):
    template_id = "incident_investigation_update"
    entity_type = "incident_investigation"

    EXTRA_FIELDS = {
        "NDIS Quality and Safeguard Reporting Status": FieldMapping(
            "ndis_reporting_status", "text"
        ),
        "Incident Classification": FieldMapping(
            "incident_classification", "text"
        ),
        "Brief Incident Description": FieldMapping(
            "brief_description", "text"
        ),
        "Additional information not included in the initial incident report.": FieldMapping(
            "additional_context", "text"
        ),
        "PRN: Where did you administer PRN?": FieldMapping("prn_location", "text"),
        "Which behaviour did you primarily administer the PRN for?": FieldMapping(
            "prn_primary_behaviour", "text"
        ),
        "When did you administer PRN?": FieldMapping("prn_time_period", "text"),
        "Afternoon/PM - What time did you administer PRN 1?": FieldMapping(
            "prn_time_window", "text"
        ),
        "How long did it take for the resident to return to baseline after taking PRN?": FieldMapping(
            "prn_baseline_duration", "text"
        ),
        "Status of the investigation": FieldMapping(
            "investigation_status", "text"
        ),
        "What factors System Factors contributed to the incident? (P3)": FieldMapping(
            "system_factor_list", "list"
        ),
        "Other": FieldMapping("investigator_confirmation", "text"),
        "Name": FieldMapping("investigation_lead_name", "text"),
    }

    def extract(self, envelope: MessageEnvelope) -> ExtractionResult:
        if not envelope.html_body:
            raise ValueError("HTML body required for investigation extract")
        fields = extract_question_rows(envelope.html_body)
        canonical: Dict[str, Any] = {}

        combined_map = {**self.FIELD_MAP, **self.EXTRA_FIELDS}
        for label, mapping in combined_map.items():
            raw_value = _pop_value(fields, label)
            transformed = _transform_value(raw_value, mapping.value_type)
            if transformed is not None:
                canonical[mapping.canonical_key] = transformed

        additional = _remaining_additional(fields)
        return ExtractionResult(
            entity_type=self.entity_type,
            canonical=canonical,
            additional_fields=additional,
        )


EXTRACTOR_REGISTRY = {
    AutomatedDailyShiftNoteExtractor.template_id: AutomatedDailyShiftNoteExtractor(),
    JotformShiftNoteExtractor.template_id: JotformShiftNoteExtractor(),
    JotformIncidentExtractor.template_id: JotformIncidentExtractor(),
    IncidentInvestigationExtractor.template_id: IncidentInvestigationExtractor(),
}


def get_extractor(template_id: Optional[str]) -> Optional[BaseExtractor]:
    if not template_id:
        return None
    return EXTRACTOR_REGISTRY.get(template_id)
