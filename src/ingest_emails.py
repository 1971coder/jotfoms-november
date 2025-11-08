from __future__ import annotations

import argparse
import logging
from pathlib import Path

from ingestion.pipeline import ingest_command


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest .eml files and store metadata/attachments locally."
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("samples"),
        help="Directory containing .eml files (default: ./samples)",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/shift_notes.db"),
        help="SQLite database file (default: data/shift_notes.db)",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory to store raw .eml copies (default: data/raw)",
    )
    parser.add_argument(
        "--attachment-dir",
        type=Path,
        default=Path("data/attachments"),
        help="Directory to store extracted attachments (default: data/attachments)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

    ingested = ingest_command(
        source_dir=args.source_dir,
        db_path=args.db_path,
        raw_storage_dir=args.raw_dir,
        attachment_storage_dir=args.attachment_dir,
    )
    print(f"Ingested {ingested} new message(s) from {args.source_dir}")


if __name__ == "__main__":
    main()
