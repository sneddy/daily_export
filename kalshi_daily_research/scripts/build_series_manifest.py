"""Build a reproducible series-selection manifest from a saved series run."""

from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3

from kalshi_daily_research.series_manifest import (
    DEFAULT_CRYPTO_CATEGORY,
    DEFAULT_EXCLUDED_FREQUENCY_GROUPS,
    DEFAULT_MIN_VOLUME,
    DEFAULT_SPORT_CATEGORY,
    build_series_manifest,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("db/kalshi_daily_probability_dataset.sqlite"),
        help="SQLite database containing raw_series and raw_payloads.",
    )
    parser.add_argument(
        "--series-run-id",
        default=None,
        help="Source series run ID. Defaults to the latest successful series run.",
    )
    parser.add_argument(
        "--min-volume",
        type=float,
        default=DEFAULT_MIN_VOLUME,
        help=f"Inclusive series volume floor in USD; default: {DEFAULT_MIN_VOLUME:,.0f}.",
    )
    parser.add_argument(
        "--exclude-frequency-group",
        dest="excluded_frequency_groups",
        action="append",
        default=None,
        help="Frequency group to exclude; may be repeated. Defaults to short_recc.",
    )
    parser.add_argument(
        "--sport-category",
        default=DEFAULT_SPORT_CATEGORY,
        help=f"Raw category assigned to sport; default: {DEFAULT_SPORT_CATEGORY}.",
    )
    parser.add_argument(
        "--crypto-category",
        default=DEFAULT_CRYPTO_CATEGORY,
        help=f"Raw category assigned to crypto; default: {DEFAULT_CRYPTO_CATEGORY}.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=None,
        help="Optional CSV path. Defaults to manifests/series_selection_<selection_id>.csv.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    excluded_groups = (
        tuple(args.excluded_frequency_groups)
        if args.excluded_frequency_groups is not None
        else DEFAULT_EXCLUDED_FREQUENCY_GROUPS
    )
    with sqlite3.connect(args.db_path) as conn:
        result = build_series_manifest(
            conn,
            series_run_id=args.series_run_id,
            min_volume=args.min_volume,
            excluded_frequency_groups=excluded_groups,
            sport_category=args.sport_category,
            crypto_category=args.crypto_category,
            output_path=args.output_path,
        )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

