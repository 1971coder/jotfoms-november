from __future__ import annotations

import json
import re
from datetime import datetime
from typing import List, Optional


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def parse_bool(value: str | None) -> Optional[bool]:
    if value is None:
        return None
    lowered = value.strip().lower()
    if lowered in {"yes", "y", "true", "1"} or lowered.startswith("yes"):
        return True
    if lowered in {"no", "n", "false", "0"} or lowered.startswith("no"):
        return False
    return None


def parse_date(value: str | None) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_datetime(value: str | None) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    formats = [
        "%Y-%m-%d %I:%M %p",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).isoformat()
        except ValueError:
            continue
    return None


def parse_time(value: str | None) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%H:%M", "%I:%M %p"):
        try:
            return datetime.strptime(value, fmt).time().isoformat(timespec="minutes")
        except ValueError:
            continue
    return None


def parse_json_list(value: str | None) -> Optional[List[str]]:
    if not value:
        return None
    try:
        data = json.loads(value)
        if isinstance(data, list):
            return [str(item).strip() for item in data if str(item).strip()]
    except json.JSONDecodeError:
        pass
    return None


def split_multivalue(value: str | None) -> Optional[List[str]]:
    if not value:
        return None
    tokens = []
    candidate = value.replace(";", "\n")
    for chunk in re.split(r"[\n,]+", candidate):
        text = chunk.strip(" -")
        if text:
            tokens.append(text)
    return tokens or None


def parse_int(value: str | None) -> Optional[int]:
    if not value:
        return None
    match = re.search(r"\d+", value.replace(",", ""))
    if match:
        try:
            return int(match.group(0))
        except ValueError:
            return None
    return None


def parse_bullet_list(value: str | None) -> Optional[List[str]]:
    if not value:
        return None
    lines = [line.strip() for line in value.replace("\r", "\n").split("\n")]
    items = []
    for line in lines:
        if not line:
            continue
        if line.startswith("-"):
            cleaned = line.lstrip("- ").strip()
            if cleaned:
                items.append(cleaned)
        else:
            items.append(line)
    return items or None
