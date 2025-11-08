from __future__ import annotations

import re
from typing import Dict, Iterable, List


class SectionParser:
    """
    Utility for extracting labelled sections from plain-text emails.
    """

    def __init__(self, labels: Iterable[str]) -> None:
        self.patterns = {
            label: re.compile(rf"^{re.escape(label)}[\s:;-]*?(.*)$", re.IGNORECASE)
            for label in labels
        }

    def parse(self, text: str) -> Dict[str, str]:
        sections: Dict[str, List[str]] = {}
        current_label: str | None = None
        current_lines: List[str] = []

        for raw_line in text.replace("\r\n", "\n").split("\n"):
            line = raw_line.strip()
            if not line:
                if current_label and current_lines:
                    current_lines.append("")
                continue

            matched_label = None
            remainder = ""
            for label, pattern in self.patterns.items():
                match = pattern.match(line)
                if match:
                    matched_label = label
                    remainder = match.group(1).strip()
                    break

            if matched_label:
                if current_label and current_lines:
                    sections[current_label] = "\n".join(
                        [chunk for chunk in current_lines if chunk.strip()]
                    ).strip()
                current_label = matched_label
                current_lines = []
                if remainder:
                    current_lines.append(remainder)
                continue

            if current_label:
                current_lines.append(line)

        if current_label and current_lines:
            sections[current_label] = "\n".join(
                [chunk for chunk in current_lines if chunk.strip()]
            ).strip()

        return sections

