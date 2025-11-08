from __future__ import annotations

import argparse
import logging
from pathlib import Path

from extraction.pipeline import run_extraction


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract structured entities from ingested raw messages."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/shift_notes.db"),
        help="SQLite database file produced by ingestion (default: data/shift_notes.db)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of raw messages to process",
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
    processed = run_extraction(db_path=args.db_path, limit=args.limit)
    print(f"Processed {processed} message(s)")


if __name__ == "__main__":
    main()

