from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class Attachment:
    filename: str
    content_type: str
    payload: bytes
    content_id: Optional[str]
    sha256: str


@dataclass
class MessageEnvelope:
    source_path: Path
    message_id: Optional[str]
    subject: str
    sender: str
    recipients: List[str]
    cc: List[str]
    bcc: List[str]
    sent_at: Optional[datetime]
    raw_bytes: bytes
    text_body: Optional[str]
    html_body: Optional[str]
    attachments: List[Attachment]
    headers: Dict[str, str]

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw_bytes).hexdigest()


def _decode_text_part(part) -> str:
    """
    Safely decode a text/* MIME part, defaulting to UTF-8 if the charset is missing.
    """
    payload = part.get_payload(decode=True) or b""
    charset = part.get_content_charset() or "utf-8"
    try:
        return payload.decode(charset, errors="replace")
    except LookupError:
        return payload.decode("utf-8", errors="replace")


def _parse_address_list(header_value: Optional[str]) -> List[str]:
    if not header_value:
        return []
    return [addr for _, addr in getaddresses([header_value]) if addr]


def _parse_date(header_value: Optional[str]) -> Optional[datetime]:
    if not header_value:
        return None
    try:
        return parsedate_to_datetime(header_value)
    except (TypeError, ValueError):
        return None


def load_message(path: Path) -> MessageEnvelope:
    """
    Parse an .eml file into a MessageEnvelope that downstream components can use.
    """
    raw_bytes = path.read_bytes()
    msg = BytesParser(policy=policy.default).parsebytes(raw_bytes)

    text_body: Optional[str] = None
    html_body: Optional[str] = None
    attachments: List[Attachment] = []

    for part in msg.walk():
        # Skip multipart containers
        if part.get_content_maintype() == "multipart":
            continue

        disposition = part.get_content_disposition()
        filename = part.get_filename()

        if disposition in {"attachment", "inline"} and filename:
            payload = part.get_payload(decode=True) or b""
            attachments.append(
                Attachment(
                    filename=filename,
                    content_type=part.get_content_type(),
                    payload=payload,
                    content_id=part.get("Content-ID"),
                    sha256=hashlib.sha256(payload).hexdigest(),
                )
            )
            continue

        if part.get_content_type() == "text/plain" and text_body is None:
            text_body = _decode_text_part(part)
            continue

        if part.get_content_type() == "text/html" and html_body is None:
            html_body = _decode_text_part(part)
            continue

    headers = {k: v for k, v in msg.items()}

    return MessageEnvelope(
        source_path=path,
        message_id=msg.get("Message-ID"),
        subject=msg.get("Subject", "").strip(),
        sender=msg.get("From", "").strip(),
        recipients=_parse_address_list(msg.get("To")),
        cc=_parse_address_list(msg.get("Cc")),
        bcc=_parse_address_list(msg.get("Bcc")),
        sent_at=_parse_date(msg.get("Date")),
        raw_bytes=raw_bytes,
        text_body=text_body,
        html_body=html_body,
        attachments=attachments,
        headers=headers,
    )

