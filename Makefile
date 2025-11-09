PYTHON ?= python
DB_PATH ?= data/shift_notes.db
RAW_DIR ?= data/raw
ATTACHMENT_DIR ?= data/attachments
REPORT_DIR ?= reports
SAMPLES_DIR ?= samples

.PHONY: pipeline ingest extract reports clean

pipeline: ingest extract reports

ingest:
	$(PYTHON) src/ingest_emails.py \
		--source-dir $(SAMPLES_DIR) \
		--db-path $(DB_PATH) \
		--raw-dir $(RAW_DIR) \
		--attachment-dir $(ATTACHMENT_DIR)

extract:
	$(PYTHON) src/run_extraction.py --db-path $(DB_PATH)

reports:
	$(PYTHON) src/run_reports.py --db-path $(DB_PATH) --output-dir $(REPORT_DIR)

clean:
	rm -rf $(DB_PATH) $(RAW_DIR) $(ATTACHMENT_DIR) $(REPORT_DIR)
