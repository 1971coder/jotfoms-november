from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import List

from parsers.message_loader import MessageEnvelope, load_message
from parsers.template_detection import detect_template

LOGGER = logging.getLogger(__name__)
PARSER_VERSION = "phase1-ingest-v1"


class EmailIngestionPipeline:
    """
    Handles copying raw .eml files/attachments to storage and recording metadata in SQLite.
    """

    def __init__(
        self,
        db_path: Path,
        raw_storage_dir: Path,
        attachment_storage_dir: Path,
    ) -> None:
        self.db_path = db_path
        self.raw_storage_dir = raw_storage_dir.resolve()
        self.attachment_storage_dir = attachment_storage_dir.resolve()

        self.raw_storage_dir.mkdir(parents=True, exist_ok=True)
        self.attachment_storage_dir.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self._ensure_schema()

    def close(self) -> None:
        self.conn.close()

    def _ensure_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS raw_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT,
                subject TEXT,
                sender TEXT,
                recipients TEXT,
                cc TEXT,
                bcc TEXT,
                sent_at TEXT,
                sha256 TEXT UNIQUE,
                size_bytes INTEGER,
                stored_path TEXT,
                parser_version TEXT,
                template_id TEXT,
                detection_confidence REAL,
                source_filename TEXT,
                ingest_ts TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_message_id INTEGER NOT NULL,
                filename TEXT,
                content_type TEXT,
                size_bytes INTEGER,
                sha256 TEXT,
                stored_path TEXT,
                content_id TEXT,
                FOREIGN KEY(raw_message_id) REFERENCES raw_messages(id) ON DELETE CASCADE
            );
            """
        )
        self.conn.commit()

    def ingest_directory(self, source_dir: Path) -> int:
        """
        Ingest all .eml files under the provided directory (non-recursive).
        Returns number of new messages stored.
        """
        processed = 0
        for eml_path in sorted(source_dir.glob("*.eml")):
            result = self.ingest_file(eml_path)
            processed += int(result)
        return processed

    def ingest_file(self, eml_path: Path) -> bool:
        envelope = load_message(eml_path)

        if self._raw_message_exists(envelope.sha256):
            LOGGER.info("Skipping %s (already ingested)", eml_path.name)
            return False

        stored_path = self._store_raw_bytes(envelope)
        template_id, confidence = detect_template(envelope)

        recipients_json = json.dumps(envelope.recipients)
        cc_json = json.dumps(envelope.cc)
        bcc_json = json.dumps(envelope.bcc)

        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO raw_messages (
                message_id, subject, sender, recipients, cc, bcc,
                sent_at, sha256, size_bytes, stored_path,
                parser_version, template_id, detection_confidence, source_filename
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                envelope.message_id,
                envelope.subject,
                envelope.sender,
                recipients_json,
                cc_json,
                bcc_json,
                envelope.sent_at.isoformat() if envelope.sent_at else None,
                envelope.sha256,
                len(envelope.raw_bytes),
                str(stored_path),
                PARSER_VERSION,
                template_id,
                confidence,
                eml_path.name,
            ),
        )
        raw_message_id = cur.lastrowid

        attachment_records = self._store_attachments(raw_message_id, envelope)
        LOGGER.info(
            "Ingested %s as SHA=%s (%d attachments)",
            eml_path.name,
            envelope.sha256,
            len(attachment_records),
        )

        self.conn.commit()
        return True

    def _raw_message_exists(self, sha256: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM raw_messages WHERE sha256 = ? LIMIT 1", (sha256,)
        )
        return cur.fetchone() is not None

    def _store_raw_bytes(self, envelope: MessageEnvelope) -> Path:
        destination = (self.raw_storage_dir / f"{envelope.sha256}.eml").resolve()
        if not destination.exists():
            destination.write_bytes(envelope.raw_bytes)
        return destination

    def _store_attachments(
        self, raw_message_id: int, envelope: MessageEnvelope
    ) -> List[int]:
        if not envelope.attachments:
            return []

        attachment_dir = (self.attachment_storage_dir / envelope.sha256).resolve()
        attachment_dir.mkdir(parents=True, exist_ok=True)

        created_ids = []
        for attachment in envelope.attachments:
            stored_path = (attachment_dir / attachment.filename).resolve()
            if not stored_path.exists():
                stored_path.write_bytes(attachment.payload)

            cur = self.conn.execute(
                """
                INSERT INTO attachments (
                    raw_message_id, filename, content_type, size_bytes,
                    sha256, stored_path, content_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    raw_message_id,
                    attachment.filename,
                    attachment.content_type,
                    len(attachment.payload),
                    attachment.sha256,
                    str(stored_path),
                    attachment.content_id,
                ),
            )
            created_ids.append(cur.lastrowid)

        return created_ids


def ingest_command(
    source_dir: Path,
    db_path: Path,
    raw_storage_dir: Path,
    attachment_storage_dir: Path,
) -> int:
    pipeline = EmailIngestionPipeline(
        db_path=db_path,
        raw_storage_dir=raw_storage_dir,
        attachment_storage_dir=attachment_storage_dir,
    )
    try:
        return pipeline.ingest_directory(source_dir)
    finally:
        pipeline.close()
