from __future__ import annotations

from html.parser import HTMLParser
from typing import Dict, List, Tuple


class QuestionRowHTMLParser(HTMLParser):
    """
    Minimal HTML parser that extracts label/value pairs from Jotform-style tables.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: List[Tuple[str, str]] = []
        self._reset_row_state()

    def _reset_row_state(self) -> None:
        self._row_active = False
        self._row_depth = 0
        self._capture_target: str | None = None
        self._question_chunks: List[str] = []
        self._value_chunks: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)
        classes = (attrs_dict.get("class") or "").split()

        if tag == "tr":
            if "questionRow" in classes:
                self._row_active = True
                self._row_depth = 1
                self._capture_target = None
                self._question_chunks = []
                self._value_chunks = []
                return
            if self._row_active:
                self._row_depth += 1

        if not self._row_active:
            return

        if tag == "td":
            if "questionColumn" in classes:
                self._capture_target = "question"
            elif "valueColumn" in classes:
                self._capture_target = "value"
        elif tag == "br":
            self._append_text("\n")
        elif tag == "table" and self._capture_target == "value":
            # Chips are wrapped in nested tables; inject newline so values separate.
            self._value_chunks.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if not self._row_active:
            return

        if tag == "td":
            self._capture_target = None
        elif tag == "tr":
            self._row_depth -= 1
            if self._row_depth <= 0:
                question = self._clean_text(self._question_chunks)
                value = self._clean_text(self._value_chunks)
                if question:
                    self.rows.append((question, value))
                self._reset_row_state()
        elif tag == "table" and self._capture_target == "value":
            self._value_chunks.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._row_active or not self._capture_target:
            return
        text = data.replace("\xa0", " ")
        self._append_text(text)

    def _append_text(self, text: str) -> None:
        if self._capture_target == "question":
            self._question_chunks.append(text)
        elif self._capture_target == "value":
            self._value_chunks.append(text)

    @staticmethod
    def _clean_text(chunks: List[str]) -> str:
        raw = "".join(chunks)
        raw = raw.replace("\r", "\n")
        # Collapse >2 newlines to single newline, and condense internal spaces.
        lines = [line.strip() for line in raw.splitlines()]
        lines = [line for line in lines if line]
        if not lines:
            return ""
        return "\n".join(lines)


def extract_question_rows(html: str) -> Dict[str, str]:
    parser = QuestionRowHTMLParser()
    parser.feed(html)
    rows: Dict[str, List[str]] = {}
    for label, value in parser.rows:
        if not label:
            continue
        canonical_label = " ".join(label.split())
        rows.setdefault(canonical_label, []).append(value)
    return rows
