"""Download only Kalshi series metadata into SQLite."""

from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3

from kalshi_daily_research.client import KalshiMetadataClient
from kalshi_daily_research.ingest import RawMetadataIngestor, RawSeriesRunConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("db/kalshi_daily_probability_dataset.sqlite"),
        help="SQLite database path.",
    )
    parser.add_argument("--base-url", default=None, help="Kalshi API base URL.")
    parser.add_argument("--page-limit", type=int, default=200, help="Series API page size.")
    parser.add_argument("--max-pages", type=int, default=None, help="Optional cursor page cap.")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bar.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.db_path.parent.mkdir(parents=True, exist_ok=True)
    client = KalshiMetadataClient(base_url=args.base_url)
    with sqlite3.connect(args.db_path) as conn:
        result = RawMetadataIngestor(conn, client).run_series_only(
            RawSeriesRunConfig(
                page_limit=args.page_limit,
                max_pages=args.max_pages,
                show_progress=not args.no_progress,
            )
        )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
