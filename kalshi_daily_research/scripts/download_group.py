"""Download Kalshi markets/events for one frozen series-selection group."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import sqlite3

from kalshi_daily_research.client import KalshiMetadataClient
from kalshi_daily_research.ingest import ManifestGroupRunConfig, RawMetadataIngestor, _unix_timestamp
from kalshi_daily_research.series_manifest import MANIFEST_GROUPS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("db/kalshi_daily_probability_dataset.sqlite"),
        help="SQLite database containing the selection manifest.",
    )
    parser.add_argument("--selection-id", required=True, help="Frozen series selection ID from build_series_manifest.")
    parser.add_argument("--group", dest="selection_group", choices=MANIFEST_GROUPS, required=True)
    parser.add_argument("--base-url", default=None, help="Kalshi API base URL.")
    parser.add_argument("--page-limit", type=int, default=200, help="Page size for market endpoints.")
    parser.add_argument("--max-pages", type=int, default=None, help="Optional cursor page cap for smoke runs.")
    parser.add_argument("--source-mode", choices=("live", "historical", "both"), default="live")
    parser.add_argument(
        "--completed-month",
        default=None,
        metavar="YYYY-MM",
        help="Only settled markets in this UTC calendar month (live endpoint).",
    )
    parser.add_argument("--completed-from", default=None, metavar="YYYY-MM-DD", help="Inclusive UTC settlement date.")
    parser.add_argument("--completed-to", default=None, metavar="YYYY-MM-DD", help="Exclusive UTC settlement date.")
    parser.add_argument("--skip-event-details", action="store_true", help="Do not fetch event detail records linked to markets.")
    parser.add_argument("--refresh-event-details", action="store_true", help="Refetch existing event detail records.")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bars.")
    return parser


def _date_start_timestamp(value: str) -> int:
    parsed = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    return int(parsed.timestamp())


def _resolve_completion_window(args: argparse.Namespace) -> tuple[int | None, int | None]:
    if args.completed_month and (args.completed_from or args.completed_to):
        raise ValueError("Use either --completed-month or --completed-from/--completed-to, not both.")
    if args.completed_month:
        try:
            year, month = (int(part) for part in args.completed_month.split("-", 1))
            if month < 1 or month > 12:
                raise ValueError
        except ValueError as exc:
            raise ValueError("--completed-month must use YYYY-MM format") from exc
        start = datetime(year, month, 1, tzinfo=UTC)
        end = datetime(year + 1, 1, 1, tzinfo=UTC) if month == 12 else datetime(year, month + 1, 1, tzinfo=UTC)
        return int(start.timestamp()), int(end.timestamp())
    if bool(args.completed_from) != bool(args.completed_to):
        raise ValueError("--completed-from and --completed-to must be supplied together")
    if args.completed_from is None:
        return None, None
    start_ts = _date_start_timestamp(args.completed_from)
    end_ts = _date_start_timestamp(args.completed_to)
    if end_ts <= start_ts:
        raise ValueError("--completed-to must be after --completed-from")
    return start_ts, end_ts


def _resolve_historical_cutoff(
    client: KalshiMetadataClient,
    *,
    completed_from_ts: int | None,
    completed_to_ts: int | None,
    source_mode: str,
) -> str | None:
    if source_mode not in {"historical", "both"}:
        return None
    cutoff = client.get_historical_cutoff()
    cutoff_utc = cutoff.get("market_settled_ts")
    cutoff_ts = _unix_timestamp(cutoff_utc)
    if cutoff_ts is None:
        raise ValueError("Historical cutoff response did not contain market_settled_ts")
    if source_mode == "historical" and completed_to_ts is not None and completed_to_ts > cutoff_ts:
        raise ValueError(
            "The requested settlement window crosses the live/historical cutoff "
            f"({cutoff_utc}). Split the window or use the appropriate source mode."
        )
    return str(cutoff_utc)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    completed_from_ts, completed_to_ts = _resolve_completion_window(args)
    client = KalshiMetadataClient(base_url=args.base_url)
    historical_cutoff_utc = _resolve_historical_cutoff(
        client,
        completed_from_ts=completed_from_ts,
        completed_to_ts=completed_to_ts,
        source_mode=args.source_mode,
    )
    with sqlite3.connect(args.db_path) as conn:
        result = RawMetadataIngestor(conn, client).run_manifest_group(
            ManifestGroupRunConfig(
                selection_id=args.selection_id,
                selection_group=args.selection_group,
                page_limit=args.page_limit,
                max_pages=args.max_pages,
                source_mode=args.source_mode,
                fetch_event_details=not args.skip_event_details,
                refresh_event_details=args.refresh_event_details,
                show_progress=not args.no_progress,
                completed_from_ts=completed_from_ts,
                completed_to_ts=completed_to_ts,
                historical_cutoff_utc=historical_cutoff_utc,
            )
        )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
