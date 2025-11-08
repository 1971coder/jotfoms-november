from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Optional

from extraction.extractors import ExtractionResult, get_extractor
from parsers.message_loader import load_message


def _to_bool_int(value):
    if value is None:
        return None
    return 1 if value else 0


def _to_json(value):
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


class ExtractionPipeline:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._ensure_schema()

    def close(self) -> None:
        self.conn.close()

    def _ensure_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS processed_entities (
                raw_message_id INTEGER PRIMARY KEY,
                entity_type TEXT,
                entity_id INTEGER,
                status TEXT NOT NULL,
                error TEXT,
                processed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(raw_message_id) REFERENCES raw_messages(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS shift_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_message_id INTEGER UNIQUE NOT NULL,
                note_date TEXT,
                day_of_week TEXT,
                shift_window TEXT,
                author_name TEXT,
                participant_name TEXT,
                activities_summary TEXT,
                mood_summary TEXT,
                hydration_intake TEXT,
                meals_consumed TEXT,
                kilometres_walked TEXT,
                bm_occurred INTEGER,
                bm_rating TEXT,
                sleep_start_time TEXT,
                sleep_disturbance INTEGER,
                personal_care_provided INTEGER,
                resident_wellness TEXT,
                emotional_support_required TEXT,
                transition_difficulty TEXT,
                change_management_difficulty TEXT,
                shift_duties_completed TEXT,
                house_jobs_participation TEXT,
                incidents_occurred INTEGER,
                near_misses TEXT,
                hazards_identified TEXT,
                visitors_present TEXT,
                issues_or_successes TEXT,
                follow_up_requests TEXT,
                staff_emotions TEXT,
                additional_fields TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(raw_message_id) REFERENCES raw_messages(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS incident_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_message_id INTEGER UNIQUE NOT NULL,
                participant_name TEXT,
                incident_stage TEXT,
                awareness_timestamp TEXT,
                staff_present_count INTEGER,
                impacted_role TEXT,
                impacted_person_name TEXT,
                pre_incident_context TEXT,
                incident_description TEXT,
                immediate_actions TEXT,
                bsp_guidance TEXT,
                strategy_effectiveness TEXT,
                training_request INTEGER,
                training_rationale TEXT,
                preventative_actions TEXT,
                incident_types TEXT,
                restraint_used INTEGER,
                prn_name TEXT,
                prn_dosage TEXT,
                prn_admin_person TEXT,
                prn_admin_time TEXT,
                prn_authorised INTEGER,
                prn_recurrence TEXT,
                prn_location TEXT,
                prn_primary_behaviour TEXT,
                prn_time_period TEXT,
                prn_time_window TEXT,
                prn_baseline_duration TEXT,
                subject_of_allegation INTEGER,
                witnesses_present TEXT,
                reporter_name TEXT,
                reporter_role TEXT,
                reporter_email TEXT,
                additional_fields TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(raw_message_id) REFERENCES raw_messages(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS incident_investigations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_message_id INTEGER UNIQUE NOT NULL,
                participant_name TEXT,
                incident_stage TEXT,
                awareness_timestamp TEXT,
                staff_present_count INTEGER,
                impacted_role TEXT,
                impacted_person_name TEXT,
                pre_incident_context TEXT,
                incident_description TEXT,
                immediate_actions TEXT,
                bsp_guidance TEXT,
                strategy_effectiveness TEXT,
                training_request INTEGER,
                training_rationale TEXT,
                preventative_actions TEXT,
                incident_types TEXT,
                restraint_used INTEGER,
                prn_name TEXT,
                prn_dosage TEXT,
                prn_admin_person TEXT,
                prn_admin_time TEXT,
                prn_authorised INTEGER,
                prn_recurrence TEXT,
                prn_location TEXT,
                prn_primary_behaviour TEXT,
                prn_time_period TEXT,
                prn_time_window TEXT,
                prn_baseline_duration TEXT,
                subject_of_allegation INTEGER,
                witnesses_present TEXT,
                reporter_name TEXT,
                reporter_role TEXT,
                reporter_email TEXT,
                ndis_reporting_status TEXT,
                incident_classification TEXT,
                brief_description TEXT,
                additional_context TEXT,
                system_factor_list TEXT,
                investigator_confirmation TEXT,
                investigation_status TEXT,
                investigation_lead_name TEXT,
                additional_fields TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(raw_message_id) REFERENCES raw_messages(id) ON DELETE CASCADE
            );
            """
        )
        self.conn.commit()

    def process_all(self, limit: Optional[int] = None) -> int:
        params = ()
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ?"
            params = (limit,)

        rows = self.conn.execute(
            f"""
            SELECT rm.id, rm.template_id, rm.stored_path
            FROM raw_messages rm
            LEFT JOIN processed_entities pe ON pe.raw_message_id = rm.id
            WHERE pe.raw_message_id IS NULL
            ORDER BY rm.id
            {limit_clause}
            """,
            params,
        ).fetchall()

        processed = 0
        for row in rows:
            processed += 1
            raw_id = row["id"]
            template_id = row["template_id"]
            extractor = get_extractor(template_id)
            if not extractor:
                self._record_status(raw_id, None, None, "skipped", "no extractor")
                continue

            stored_path = Path(row["stored_path"])
            if not stored_path.is_absolute():
                stored_path = (self.db_path.parent / stored_path).resolve()

            try:
                envelope = load_message(stored_path)
                result = extractor.extract(envelope)
                entity_id = self._persist_entity(raw_id, result)
                self._record_status(raw_id, result.entity_type, entity_id, "success", None)
            except Exception as exc:  # pylint: disable=broad-except
                self._record_status(raw_id, extractor.entity_type, None, "error", str(exc))

        self.conn.commit()
        return processed

    def _record_status(
        self,
        raw_message_id: int,
        entity_type: Optional[str],
        entity_id: Optional[int],
        status: str,
        error: Optional[str],
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO processed_entities (raw_message_id, entity_type, entity_id, status, error)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(raw_message_id) DO UPDATE SET
                entity_type=excluded.entity_type,
                entity_id=excluded.entity_id,
                status=excluded.status,
                error=excluded.error,
                processed_at=CURRENT_TIMESTAMP
            """,
            (raw_message_id, entity_type, entity_id, status, error),
        )

    def _persist_entity(self, raw_id: int, result: ExtractionResult) -> Optional[int]:
        if result.entity_type == "shift_note":
            return self._insert_shift_note(raw_id, result)
        if result.entity_type == "incident_report":
            return self._insert_incident_report(raw_id, result)
        if result.entity_type == "incident_investigation":
            return self._insert_incident_investigation(raw_id, result)
        return None

    def _insert_shift_note(self, raw_id: int, result: ExtractionResult) -> int:
        canonical = result.canonical
        cur = self.conn.execute(
            """
            INSERT INTO shift_notes (
                raw_message_id, note_date, day_of_week, shift_window,
                author_name, participant_name, activities_summary, mood_summary,
                hydration_intake, meals_consumed, kilometres_walked, bm_occurred,
                bm_rating, sleep_start_time, sleep_disturbance, personal_care_provided,
                resident_wellness, emotional_support_required, transition_difficulty,
                change_management_difficulty, shift_duties_completed,
                house_jobs_participation, incidents_occurred, near_misses,
                hazards_identified, visitors_present, issues_or_successes,
                follow_up_requests, staff_emotions, additional_fields
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                raw_id,
                canonical.get("note_date"),
                canonical.get("day_of_week"),
                canonical.get("shift_window"),
                canonical.get("author_name"),
                canonical.get("participant_name"),
                canonical.get("activities_summary"),
                canonical.get("mood_summary"),
                canonical.get("hydration_intake"),
                _to_json(canonical.get("meals_consumed")),
                canonical.get("kilometres_walked"),
                _to_bool_int(canonical.get("bm_occurred")),
                canonical.get("bm_rating"),
                canonical.get("sleep_start_time"),
                _to_bool_int(canonical.get("sleep_disturbance")),
                _to_bool_int(canonical.get("personal_care_provided")),
                canonical.get("resident_wellness"),
                canonical.get("emotional_support_required"),
                canonical.get("transition_difficulty"),
                canonical.get("change_management_difficulty"),
                _to_json(canonical.get("shift_duties_completed")),
                canonical.get("house_jobs_participation"),
                _to_bool_int(canonical.get("incidents_occurred")),
                canonical.get("near_misses"),
                canonical.get("hazards_identified"),
                canonical.get("visitors_present"),
                canonical.get("issues_or_successes"),
                canonical.get("follow_up_requests"),
                _to_json(canonical.get("staff_emotions")),
                _to_json(result.additional_fields),
            ),
        )
        return cur.lastrowid

    def _insert_incident_report(self, raw_id: int, result: ExtractionResult) -> int:
        c = result.canonical
        cur = self.conn.execute(
            """
            INSERT INTO incident_reports (
                raw_message_id, participant_name, incident_stage, awareness_timestamp,
                staff_present_count, impacted_role, impacted_person_name,
                pre_incident_context, incident_description, immediate_actions,
                bsp_guidance, strategy_effectiveness, training_request,
                training_rationale, preventative_actions, incident_types,
                restraint_used, prn_name, prn_dosage, prn_admin_person,
                prn_admin_time, prn_authorised, prn_recurrence, prn_location,
                prn_primary_behaviour, prn_time_period, prn_time_window,
                prn_baseline_duration, subject_of_allegation, witnesses_present,
                reporter_name, reporter_role, reporter_email, additional_fields
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                raw_id,
                c.get("participant_name"),
                c.get("incident_stage"),
                c.get("awareness_timestamp"),
                c.get("staff_present_count"),
                c.get("impacted_role"),
                c.get("impacted_person_name"),
                c.get("pre_incident_context"),
                c.get("incident_description"),
                _to_json(c.get("immediate_actions")),
                c.get("bsp_guidance"),
                c.get("strategy_effectiveness"),
                _to_bool_int(c.get("training_request")),
                c.get("training_rationale"),
                c.get("preventative_actions"),
                _to_json(c.get("incident_types")),
                _to_bool_int(c.get("restraint_used")),
                c.get("prn_name"),
                c.get("prn_dosage"),
                c.get("prn_admin_person"),
                c.get("prn_admin_time"),
                _to_bool_int(c.get("prn_authorised")),
                c.get("prn_recurrence"),
                c.get("prn_location"),
                c.get("prn_primary_behaviour"),
                c.get("prn_time_period"),
                c.get("prn_time_window"),
                c.get("prn_baseline_duration"),
                _to_bool_int(c.get("subject_of_allegation")),
                c.get("witnesses_present"),
                c.get("reporter_name"),
                c.get("reporter_role"),
                c.get("reporter_email"),
                _to_json(result.additional_fields),
            ),
        )
        return cur.lastrowid

    def _insert_incident_investigation(self, raw_id: int, result: ExtractionResult) -> int:
        c = result.canonical
        columns = [
            "raw_message_id",
            "participant_name",
            "incident_stage",
            "awareness_timestamp",
            "staff_present_count",
            "impacted_role",
            "impacted_person_name",
            "pre_incident_context",
            "incident_description",
            "immediate_actions",
            "bsp_guidance",
            "strategy_effectiveness",
            "training_request",
            "training_rationale",
            "preventative_actions",
            "incident_types",
            "restraint_used",
            "prn_name",
            "prn_dosage",
            "prn_admin_person",
            "prn_admin_time",
            "prn_authorised",
            "prn_recurrence",
            "prn_location",
            "prn_primary_behaviour",
            "prn_time_period",
            "prn_time_window",
            "prn_baseline_duration",
            "subject_of_allegation",
            "witnesses_present",
            "reporter_name",
            "reporter_role",
            "reporter_email",
            "ndis_reporting_status",
            "incident_classification",
            "brief_description",
            "additional_context",
            "system_factor_list",
            "investigator_confirmation",
            "investigation_status",
            "investigation_lead_name",
            "additional_fields",
        ]
        values = [
            raw_id,
            c.get("participant_name"),
            c.get("incident_stage"),
            c.get("awareness_timestamp"),
            c.get("staff_present_count"),
            c.get("impacted_role"),
            c.get("impacted_person_name"),
            c.get("pre_incident_context"),
            c.get("incident_description"),
            _to_json(c.get("immediate_actions")),
            c.get("bsp_guidance"),
            c.get("strategy_effectiveness"),
            _to_bool_int(c.get("training_request")),
            c.get("training_rationale"),
            c.get("preventative_actions"),
            _to_json(c.get("incident_types")),
            _to_bool_int(c.get("restraint_used")),
            c.get("prn_name"),
            c.get("prn_dosage"),
            c.get("prn_admin_person"),
            c.get("prn_admin_time"),
            _to_bool_int(c.get("prn_authorised")),
            c.get("prn_recurrence"),
            c.get("prn_location"),
            c.get("prn_primary_behaviour"),
            c.get("prn_time_period"),
            c.get("prn_time_window"),
            c.get("prn_baseline_duration"),
            _to_bool_int(c.get("subject_of_allegation")),
            c.get("witnesses_present"),
            c.get("reporter_name"),
            c.get("reporter_role"),
            c.get("reporter_email"),
            c.get("ndis_reporting_status"),
            c.get("incident_classification"),
            c.get("brief_description"),
            c.get("additional_context"),
            _to_json(c.get("system_factor_list")),
            c.get("investigator_confirmation"),
            c.get("investigation_status"),
            c.get("investigation_lead_name"),
            _to_json(result.additional_fields),
        ]
        placeholder = ", ".join(["?"] * len(columns))
        cur = self.conn.execute(
            f"INSERT INTO incident_investigations ({', '.join(columns)}) VALUES ({placeholder})",
            values,
        )
        return cur.lastrowid


def run_extraction(db_path: Path, limit: Optional[int] = None) -> int:
    pipeline = ExtractionPipeline(db_path)
    try:
        return pipeline.process_all(limit=limit)
    finally:
        pipeline.close()
