# Phase 0 — Discovery Summary

## 1. Template Catalogue & Sample Index

| Template ID | Description & Structural Cues | Samples (24 total) | Notes for Future Formats |
| --- | --- | --- | --- |
| `automated_daily_shift_note` | Plain-text Outlook email authored by individual staff, multipurpose sections (“Date:”, “Written by;”, “Description of activities”, “Description of mood”, hydration, kilometres, meals array, BM questions) plus optional JPEG attachments. | `Will's automated daily shift note- 2024-03-26.eml`, `Will's automated daily shift note- 2024-04-09.eml`, `Will's automated daily shift note- 2024-08-09.eml` | Parser must tolerate inconsistent punctuation (“Written by;”), missing fields, and attachments that should be stored and referenced even if not parsed. |
| `jotform_shift_note` | HTML table from Jotform with repeated `<tr class="questionRow">` elements, labels held in `.questionColumn`, values as pills or text in `.valueColumn`. Subject prefix `Re- Will White - The Hive SILC Shift Notes - …`. | Six files authored by David Baraza, Diana East, Graeme Kolomalu (x2), Ina Maraeara (`samples/Re- …`). | Mapping file should key off visible question text, not numeric IDs (e.g., `question_104`), to survive Jotform reordering. Multi-select pills need flattening into arrays. |
| `jotform_incident_notification` | HTML table like above, subject `Incident Report Notification - Will White …`, includes large narrative fields (“circumstances”, “describe incident”), PRN/chemical restraint metadata, and reporter identity. | Nine numbered notifications (`samples/Incident Report Notification - Will White ###.eml`). | Template already exhibits chips plus long paragraphs; parser must preserve newline-separated bulleting and gracefully ingest extra checkboxes (e.g., future hazard lists). |
| `incident_investigation_update` | Either text/plain block or HTML table summarising post-incident investigation with added governance fields (NDIS status, classification, action plans). Subject starts with `EDIT- Incident Investigation Completed - Incident Dated …`. | Six dated investigation updates (`samples/EDIT- Incident Investigation Completed - Incident Dated YYYY-MM-DD …`). | Content may arrive as plain text or HTML depending on sender; extractor should auto-detect part type and rely on delimiter phrases (e.g., “Immediate action taken”, “Contributing Factor”) to remain resilient. |

## 2. Canonical Data Dictionaries

### 2.1 `shift_notes` Entity

| Field | Type | Mandatory | Populated By | Notes |
| --- | --- | --- | --- | --- |
| `note_date` | `date` | Yes | All templates | Plain text uses “Date:”; Jotform uses “Shift date (date your shift ended)”. |
| `shift_window` | `enum` (`morning`, `day`, `overnight`, `overnight+morning`, etc.) | Jotform | Jotform “Which shift are you reporting on?” pill; default to `unknown` for plain-text emails. |
| `day_of_week` | `enum` | Jotform | Derived from explicit field or from `note_date` if absent. |
| `author_name` | `string` | Yes | “Written by;” or “This report was prepared by”. |
| `activities_summary` | `text` | Plain-text automated | Copied verbatim (“Description of activities”). |
| `mood_summary` | `text` | Optional | “Description of mood” or Jotform field “Did Will require support to manage his emotions…”. |
| `hydration_intake` | `string` | Optional | “What did the participant drink today” (plain) or “Did Will drink water today?” style fields in future forms. |
| `meals_consumed` | `string[]` | Optional | JSON-like list in plain template or future table selections. |
| `kilometres_walked` | `float` | Optional | Plain template field. |
| `bm_occurred` | `bool` | Yes | “Did Will have a Bowel Movement…” / “Did will have a bowel movement?” |
| `bm_rating` | `enum` | Optional | Plain template “What rating on the Bristol Stool Chart…” |
| `sleep_start_time` | `time_range` | Jotform overnight shift | “What time did the resident go to bed?” etc. |
| `sleep_disturbance` | `bool` | Jotform | “Was the resident's sleep disturbed…” |
| `personal_care_provided` | `bool` | Jotform | “Did you provide personal care to Will?” |
| `resident_wellness` | `enum` | Jotform | “Did the resident seem well/unwell…” |
| `emotional_support_required` | `text`/`enum` | Jotform | “Did Will require support to manage his emotions…” |
| `transition_difficulty` | `enum` | Jotform | Fields about transitions / managing change. |
| `house_jobs_participation` | `enum` | Jotform | “Was Will involved in jobs…” |
| `incidents_occurred` | `bool` | Jotform | “Did any injuries or incidents occur…” |
| `near_misses` | `text` | Jotform | “Were any near misses identified…” |
| `hazards_identified` | `text` | Jotform | “Were any hazards identified…” |
| `visitors_present` | `text/bool` | Jotform | Visitor tracking. |
| `issues_or_successes` | `text` | Jotform | Free-form reflection. |
| `follow_up_requests` | `text` | Jotform | “Is there anything you need your colleague to follow up…” |
| `staff_emotions` | `string[]` | Jotform | “Which of the following did you feel due to your shift?” multi-select pills. |
| `attachments` | `attachment[]` | Optional | Captured from MIME part metadata (e.g., resident photos). |
| `additional_fields` | `json` | Always | Any unmapped label/value pairs for forward compatibility. |

### 2.2 `incident_reports` Entity

| Field | Type | Mandatory | Source Field |
| --- | --- | --- | --- |
| `incident_subject` | `string` | Yes | Email subject. |
| `participant_name` | `string` | Yes | “Who is this incident report about?” |
| `incident_stage` | `enum` | Yes | “Incident Management Stage”. |
| `awareness_timestamp` | `datetime` | Yes | “Date & time you became aware of the incident”. |
| `staff_present_count` | `integer` | Optional | “How many staff were present…”. |
| `impacted_role` | `enum` | Yes | “Who was, or potentially was impacted…”. |
| `impacted_person_name` | `string` | Optional | “Name of person impacted…”. |
| `pre_incident_context` | `text` | Optional | “What were the circumstances leading up…”. |
| `incident_description` | `text` | Yes | “Describe the incident/ allegation…”. |
| `immediate_actions` | `string[]` | Optional | Bullet list parsed from “Immediate action taken…”. |
| `bsp_guidance` | `text` | Optional | “What does the behaviour support plan say to do?”. |
| `strategy_effectiveness` | `enum` | Optional | “Were these strategies effective?”. |
| `training_request` | `bool` | Optional | “Would you like more training…”. |
| `training_rationale` | `text` | Optional | “If not, why not?”. |
| `preventative_actions` | `text` | Optional | “What could have been differently…”. |
| `incident_types` | `string[]` | Optional | “Type of incident (Tick all that apply)”. |
| `restraint_used` | `bool` | Optional | “Was a restraint used…”. |
| `restraint_type` | `enum` | Optional | Derived from selection (chemical/physical/etc.). |
| `prn_name` | `string` | Optional | “Name of PRN/ Chemical restraint”. |
| `prn_dosage` | `string` | Optional | “Dosage administered”. |
| `prn_admin_time` | `time` | Optional | “What time was PRN administered”. |
| `prn_admin_person` | `string` | Optional | “Name of person who administered…”. |
| `prn_authorised` | `bool` | Optional | “Is this restrictive practice authorised?”. |
| `prn_likely_recur` | `bool` | Optional | “Was it a one-off emergency use…”. |
| `subject_of_allegation` | `bool` | Optional | “Is there a subject of allegation?”. |
| `witnesses_present` | `string` | Optional | “Was there any Witnesses”. |
| `reporter_name` | `string` | Yes | “Name of person completing the form”. |
| `reporter_role` | `string` | Optional | “Role”. |
| `reporter_email` | `email` | Optional | “Email”. |
| `attachments` | `attachment[]` | Optional | Binary artifacts referenced in the message. |
| `additional_fields` | `json` | Always | Captures unseen fields. |

### 2.3 `incident_investigations` Entity

| Field | Type | Mandatory | Source/Notes |
| --- | --- | --- | --- |
| `incident_reference` | `string` | Yes | Derived from subject (“Incident Dated …”). |
| `participant_name` | `string` | Yes | Usually repeated at top of form. |
| `incident_stage` | `enum` | Yes | Stage such as “Assess an incident”, “Investigation Completed”. |
| `awareness_timestamp` | `datetime` | Optional | Reused from initial report if present. |
| `staff_present_count` | `integer` | Optional | Same as notification. |
| `impacted_role` | `enum` | Optional | Resident/staff/community. |
| `narrative_summary` | `text` | Yes | Combined sections “Circumstances leading up”, “Describe the incident”. |
| `immediate_actions` | `string[]` | Optional | Bullet list of actions. |
| `bsp_guidance` | `text` | Optional | “Given the behaviour support plan says to do…”. |
| `strategy_effectiveness` | `enum` | Optional | “Were these strategies effective?”. |
| `training_request` | `bool` | Optional | Follow-on field. |
| `prn_details` | `json` | Optional | Consolidates PRN name, dosage, location, primary behaviour, time window, baseline duration, admin person. |
| `restraint_authorised` | `bool` | Optional | “Is this restrictive practice authorised?”. |
| `ndis_reporting_status` | `enum` | Optional | “NDIS Quality and Safeguard Reporting Status” (e.g., Monthly Reporting). |
| `incident_classification` | `enum` | Optional | “Incident Classification” (e.g., Priority 3). |
| `brief_description` | `text` | Optional | “Brief Incident Description”. |
| `additional_context` | `text` | Optional | “Additional information not included in the initial incident report”. |
| `contributing_factors_people` | `text` | Optional | Narrative under “People: …”. |
| `contributing_factors_process` | `text` | Optional | “Process: …”. |
| `contributing_factors_system` | `text` | Optional | “System: …”. |
| `system_factor_list` | `string[]` | Optional | Checkbox values from “What factors System Factors contributed…”. |
| `action_items` | `array<object>` | Optional | Parsed from “Contributing Factor / Action / Person Responsible” tables. |
| `investigator_confirmation` | `bool` | Optional | “I confirm that I have assessed this incident…” acknowledgement. |
| `investigator_name` | `string` | Yes | Usually same as reporter (“Name of person completing the form”). |
| `investigator_role` | `string` | Optional | Provided role. |
| `attachments` | `attachment[]` | Optional | Supporting documents or embedded images. |
| `additional_fields` | `json` | Always | Safety net for unforeseen data. |

## 3. Storage & Analytics Expectations (Phase 0 Exit Criteria)

1. **Offline-first constraint** — reaffirmed from `docs/REQUIREMENTS.md` and validated with sample formats; all tooling must operate without network calls.
2. **Data flow agreement** — MVP will persist raw `.eml` + attachment assets on disk, register metadata in SQLite tables (`raw_messages`, `attachments`, `shift_notes`, `incident_reports`, `incident_investigations`, `field_values`), and export curated Parquet snapshots via DuckDB for analytics/AI consumers. This mirrors the “practical path” discussed with the user.
3. **Extensibility hooks** — template catalogue + data dictionaries define how new email layouts will integrate: add classifier rule, mapping file, and extractor plugin while defaulting to raw storage + `additional_fields` to prevent data loss.
4. **Analytics targets** — initial KPIs confirmed: hydration tracking, BM streaks, PRN utilisation, incident frequency/severity, and investigation follow-up compliance, all derivable from canonical fields listed above.
5. **Phase 0 exit** — with sample inventory, structured field definitions, and agreed storage/analytics approach documented, engineering can proceed to Phase 1 (ingestion + MIME infrastructure) using this document as the signed-off reference.

