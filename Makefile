PYTHON ?= python
DB_PATH ?= data/shift_notes.db
RAW_DIR ?= data/raw
ATTACHMENT_DIR ?= data/attachments
REPORT_DIR ?= reports
SAMPLES_DIR ?= samples

.PHONY: pipeline ingest extract report reports clean clean-reports clean-raw

ingest:
	$(PYTHON) src/ingest_emails.py \
		--source-dir $(SAMPLES_DIR) \
		--db-path $(DB_PATH) \
		--raw-dir $(RAW_DIR) \
		--attachment-dir $(ATTACHMENT_DIR)

extract:
	$(PYTHON) src/run_extraction.py --db-path $(DB_PATH)

report:
	$(PYTHON) src/run_reports.py --db-path $(DB_PATH) --output-dir $(REPORT_DIR)

reports: report

pipeline: ingest extract report

clean:
	rm -rf $(DB_PATH) $(RAW_DIR) $(ATTACHMENT_DIR) $(REPORT_DIR)

clean-reports:
	rm -rf $(REPORT_DIR)

clean-raw:
	rm -rf $(RAW_DIR) $(ATTACHMENT_DIR)
