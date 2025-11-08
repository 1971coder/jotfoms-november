from __future__ import annotations

import argparse
import logging
from pathlib import Path

from analytics.reporting import generate_reports


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Parquet exports and analytics views from the SQLite store."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/shift_notes.db"),
        help="SQLite database file to read from (default: data/shift_notes.db)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports"),
        help="Directory to write Parquet files into (default: reports/)",
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
    outputs = generate_reports(sqlite_path=args.db_path, output_dir=args.output_dir)
    print(f"Wrote {len(outputs)} report(s) to {args.output_dir}")


if __name__ == "__main__":
    main()

