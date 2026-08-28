"""Download native daily candles for a filtered frozen market universe."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from pathlib import Path
import sqlite3

from kalshi_daily_research.candles import (
    DailyCandleIngestor,
    DailyCandleRunConfig,
    FILTER_MODES,
    RETRY_STATUSES,
    SOURCE_MODES,
)
from kalshi_daily_research.client import KalshiMetadataClient
from kalshi_daily_research.series_manifest import MANIFEST_GROUPS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("db/kalshi_daily_probability_dataset.sqlite"),
        help="SQLite database containing the frozen series selection and market metadata.",
    )
    parser.add_argument(
        "--selection-id",
        required=True,
        help="Frozen series selection ID from build_series_manifest.",
    )
    parser.add_argument(
        "--group",
        choices=MANIFEST_GROUPS,
        default="non_sport_crypto",
        help="Frozen manifest group; default: non_sport_crypto.",
    )
    parser.add_argument(
        "--source-mode",
        choices=sorted(SOURCE_MODES),
        default="live",
        help="Candle source tier: live batch endpoint or historical single-market endpoint; default: live.",
    )
    parser.add_argument(
        "--filter-mode",
        choices=sorted(FILTER_MODES),
        default="union",
        help="Markets passing volume, lifetime, both, or the union of both filters; default: union.",
    )
    parser.add_argument(
        "--min-volume-fp",
        type=float,
        default=10_000.0,
        help="Inclusive cumulative volume_fp threshold; default: 10000.",
    )
    parser.add_argument(
        "--min-lifetime-days",
        type=float,
        default=5.0,
        help="Inclusive open-to-close lifetime threshold; default: 5 days.",
    )
    parser.add_argument("--start-date", default=None, metavar="YYYY-MM-DD", help="Inclusive UTC history start date.")
    parser.add_argument("--end-date", default=None, metavar="YYYY-MM-DD", help="Inclusive UTC history end date.")
    parser.add_argument(
        "--max-tickers-per-batch",
        type=int,
        default=100,
        help="Maximum tickers per live batch request; default: 100.",
    )
    parser.add_argument(
        "--max-candles-per-batch",
        type=int,
        default=10_000,
        help="Estimated candle budget per live batch request; default: 10000.",
    )
    parser.add_argument("--max-batches", type=int, default=None, help="Optional batch cap for smoke runs.")
    parser.add_argument(
        "--max-markets",
        type=int,
        default=None,
        help="Optional market cap for smoke runs; applied before requests in either source mode.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Historical request concurrency; default: 4.",
    )
    parser.add_argument(
        "--include-latest-before-start",
        action="store_true",
        help="Request Kalshi's synthetic continuity candle before the start; disabled by default.",
    )
    parser.add_argument(
        "--retry-status",
        choices=sorted(RETRY_STATUSES),
        default=None,
        help=(
            "Retry unresolved markets with this historical status from the database for the "
            "same selection, group, and source mode. Live request size uses --max-tickers-per-batch; "
            "historical concurrency uses --max-workers."
        ),
    )
    parser.add_argument("--no-progress", action="store_true", help="Disable the progress bar.")
    return parser


def _parse_start_date(value: str) -> int:
    return int(datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC).timestamp())


def _parse_end_date(value: str) -> int:
    next_day = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC) + timedelta(days=1)
    return int(next_day.timestamp()) - 1


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if bool(args.start_date) != bool(args.end_date):
        raise ValueError("--start-date and --end-date must be supplied together")
    start_ts = _parse_start_date(args.start_date) if args.start_date else None
    end_ts = _parse_end_date(args.end_date) if args.end_date else None
    if start_ts is not None and end_ts <= start_ts:
        raise ValueError("--end-date must be on or after --start-date")

    client = KalshiMetadataClient()
    config = DailyCandleRunConfig(
        selection_id=args.selection_id,
        selection_group=args.group,
        source_mode=args.source_mode,
        filter_mode=args.filter_mode,
        min_volume_fp=args.min_volume_fp,
        min_lifetime_days=args.min_lifetime_days,
        start_ts=start_ts,
        end_ts=end_ts,
        max_tickers_per_batch=args.max_tickers_per_batch,
        max_candles_per_batch=args.max_candles_per_batch,
        max_batches=args.max_batches,
        max_markets=args.max_markets,
        max_workers=args.max_workers,
        include_latest_before_start=args.include_latest_before_start,
        retry_status=args.retry_status,
        show_progress=not args.no_progress,
    )
    with sqlite3.connect(args.db_path) as conn:
        result = DailyCandleIngestor(conn, client).run(config)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
