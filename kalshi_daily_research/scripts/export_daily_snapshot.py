"""Export one daily-candle run and its market context to plain CSV files."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

import pandas as pd

from kalshi_daily_research.series_manifest import MANIFEST_GROUPS


SOURCE_MODES = ("historical", "live")
MAIN_GROUP = "non_sport_crypto"

METADATA_COLUMNS = [
    "market_id", "market_ticker", "event_ticker", "series_ticker",
    "market_question", "market_subtitle", "yes_subtitle", "no_subtitle",
    "market_rules", "event_question", "event_subtitle", "series_title",
    "series_category", "series_frequency", "market_status", "market_result",
    "open_time", "close_time", "passes_volume_filter",
    "passes_lifetime_filter", "passes_both_filters", "volume_fp",
    "lifetime_days", "history_start_ts", "history_end_ts",
    "expected_daily_rows", "received_daily_rows", "first_observation_ts",
    "last_observation_ts", "download_status", "actual_candles_exported",
]

CANDLE_COLUMNS = [
    "market_id", "market_ticker", "series_ticker", "end_period_ts",
    "date_utc", "price_open", "price_low", "price_high", "price_close",
    "price_mean", "price_previous", "yes_bid_close", "yes_ask_close",
    "volume", "open_interest",
]

NUMERIC_METADATA_COLUMNS = [
    "passes_volume_filter", "passes_lifetime_filter", "passes_both_filters",
    "volume_fp", "lifetime_days", "history_start_ts", "history_end_ts",
    "expected_daily_rows", "received_daily_rows", "first_observation_ts",
    "last_observation_ts",
]

NUMERIC_CANDLE_COLUMNS = [
    "end_period_ts", "price_open", "price_low", "price_high", "price_close",
    "price_mean", "price_previous", "yes_bid_close", "yes_ask_close",
    "volume", "open_interest",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, required=True, help="SQLite database containing the selected candle run.")
    parser.add_argument("--selection-id", required=True, help="Frozen series selection ID.")
    parser.add_argument("--group", choices=MANIFEST_GROUPS, required=True, help="Manifest group to export.")
    parser.add_argument("--source-mode", choices=SOURCE_MODES, required=True, help="Single source tier to export.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory for the full CSV/JSON snapshot.")
    parser.add_argument(
        "--candle-run-id",
        default=None,
        help="Specific daily-candle run ID; defaults to the latest matching run.",
    )
    return parser


def _parse_json(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(value) if value else {}
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _timestamp_to_utc(value: Any) -> str | None:
    if value is None or value == "":
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError, OSError):
        return None


def _read_sql(conn: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    return pd.read_sql_query(query, conn, params=params)


def _select_candle_run(
    conn: sqlite3.Connection,
    *,
    selection_id: str,
    selection_group: str,
    source_mode: str,
    candle_run_id: str | None,
) -> pd.Series:
    runs = _read_sql(
        conn,
        """
        SELECT run_id, started_at_utc, finished_at_utc, status, source_mode, base_url,
               config_json, stats_json, error_text
        FROM metadata_runs
        WHERE json_extract(config_json, '$.stage') = 'daily_candles'
          AND json_extract(config_json, '$.selection_id') = ?
          AND json_extract(config_json, '$.selection_group') = ?
          AND source_mode = ?
        ORDER BY started_at_utc DESC, rowid DESC
        """,
        (selection_id, selection_group, source_mode),
    )
    if candle_run_id is None:
        if runs.empty:
            raise ValueError("No matching daily-candle run exists for the selected group and source mode")
        return runs.iloc[0]
    selected = runs.loc[runs["run_id"].eq(candle_run_id)]
    if selected.empty:
        raise ValueError(f"Candle run not found for the selected group and source mode: {candle_run_id}")
    return selected.iloc[0]


def _select_metadata_run(
    conn: sqlite3.Connection,
    *,
    selection_id: str,
    selection_group: str,
    source_mode: str,
) -> pd.Series | None:
    runs = _read_sql(
        conn,
        """
        SELECT run_id, status, source_mode, config_json
        FROM metadata_runs
        WHERE json_extract(config_json, '$.stage') = 'manifest_group'
          AND json_extract(config_json, '$.selection_id') = ?
          AND json_extract(config_json, '$.selection_group') = ?
          AND source_mode = ?
        ORDER BY started_at_utc DESC, rowid DESC
        """,
        (selection_id, selection_group, source_mode),
    )
    return None if runs.empty else runs.iloc[0]


def _load_market_metadata(
    conn: sqlite3.Connection,
    *,
    candle_run_id: str,
    selection_id: str,
    selection_group: str,
) -> pd.DataFrame:
    metadata = _read_sql(
        conn,
        """
        SELECT
            mh.market_id,
            mh.market_ticker,
            mh.event_ticker,
            mh.series_ticker,
            m.title AS market_question,
            m.subtitle AS market_subtitle,
            m.yes_sub_title AS yes_subtitle,
            m.no_sub_title AS no_subtitle,
            m.rules_primary AS market_rules,
            e.title AS event_question,
            e.sub_title AS event_subtitle,
            s.title AS series_title,
            s.category AS series_category,
            s.frequency AS series_frequency,
            m.status AS market_status,
            m.result AS market_result,
            m.open_time,
            m.close_time,
            mh.passes_volume_filter,
            mh.passes_lifetime_filter,
            mh.passes_both_filters,
            mh.volume_fp,
            mh.lifetime_days,
            mh.history_start_ts,
            mh.history_end_ts,
            mh.expected_daily_rows,
            mh.received_daily_rows,
            mh.first_observation_ts,
            mh.last_observation_ts,
            mh.status AS download_status
        FROM market_history_manifest mh
        LEFT JOIN raw_markets m ON m.market_id = mh.market_id
        LEFT JOIN raw_events e ON e.event_ticker = mh.event_ticker
        LEFT JOIN raw_series s
          ON s.series_ticker = COALESCE(NULLIF(m.series_ticker, ''), e.series_ticker)
        WHERE mh.run_id = ? AND mh.selection_id = ? AND mh.selection_group = ?
        ORDER BY mh.market_ticker
        """,
        (candle_run_id, selection_id, selection_group),
    )
    if metadata.empty:
        raise ValueError("The selected candle run has no market metadata")
    if not metadata["market_id"].is_unique:
        raise ValueError("Market metadata must contain one row per market_id")
    for column in NUMERIC_METADATA_COLUMNS:
        metadata[column] = pd.to_numeric(metadata[column], errors="coerce")
    for column in ["open_time", "close_time"]:
        metadata[column] = pd.to_datetime(metadata[column], utc=True, errors="coerce")
    return metadata


def _load_candles(
    conn: sqlite3.Connection,
    *,
    candle_run_id: str,
    selection_id: str,
    selection_group: str,
    source_mode: str,
) -> pd.DataFrame:
    candles = _read_sql(
        conn,
        """
        WITH RECURSIVE source_runs(run_id) AS (
            SELECT ?
            UNION
            SELECT json_extract(m.config_json, '$.retry_parent_run_id')
            FROM metadata_runs m
            JOIN source_runs sr ON sr.run_id = m.run_id
            WHERE m.source_mode = ?
              AND json_extract(m.config_json, '$.retry_parent_run_id') IS NOT NULL
        )
        SELECT
            c.market_id,
            c.market_ticker,
            c.series_ticker,
            c.end_period_ts,
            date(c.end_period_ts, 'unixepoch') AS date_utc,
            c.price_open,
            c.price_low,
            c.price_high,
            c.price_close,
            c.price_mean,
            c.price_previous,
            c.yes_bid_close,
            c.yes_ask_close,
            c.volume,
            c.open_interest
        FROM raw_daily_candles c
        JOIN market_history_manifest mh
          ON mh.market_id = c.market_id
         AND mh.run_id = ?
         AND mh.selection_id = ?
         AND mh.selection_group = ?
        WHERE c.source_mode = ?
          AND c.run_id IN (SELECT run_id FROM source_runs)
        ORDER BY c.market_ticker, c.end_period_ts
        """,
        (candle_run_id, source_mode, candle_run_id, selection_id, selection_group, source_mode),
    )
    for column in NUMERIC_CANDLE_COLUMNS:
        candles[column] = pd.to_numeric(candles[column], errors="coerce")
    candles["date_utc"] = pd.to_datetime(candles["date_utc"], utc=True, errors="coerce")
    return candles


def _build_summary(
    *,
    metadata: pd.DataFrame,
    candles: pd.DataFrame,
    selected_run: pd.Series,
    metadata_run: pd.Series | None,
    selection_id: str,
    selection_group: str,
    source_mode: str,
) -> tuple[dict[str, Any], pd.DataFrame]:
    actual_counts = candles.groupby("market_id").size()
    metadata["actual_candles_exported"] = (
        metadata["market_id"].map(actual_counts).fillna(0).astype("int64")
    )

    first_date = candles["date_utc"].min().isoformat() if not candles.empty else None
    last_date = candles["date_utc"].max().isoformat() if not candles.empty else None

    price_close = candles["price_close"].dropna()
    bid_close = candles["yes_bid_close"].dropna()
    ask_close = candles["yes_ask_close"].dropna()
    quality = {
        "metadata_market_id_unique": bool(metadata["market_id"].is_unique),
        "candle_market_day_unique": bool(not candles.duplicated(["market_id", "end_period_ts"]).any()),
        "duplicate_market_day_rows": int(candles.duplicated(["market_id", "end_period_ts"]).sum()),
        "missing_candle_market_id_rows": int(candles["market_id"].isna().sum()),
        "candle_markets_missing_metadata": int(
            (~candles["market_id"].isin(metadata["market_id"])).sum()
        ),
        "price_close_below_zero_rows": int(price_close.lt(0).sum()),
        "price_close_above_one_rows": int(price_close.gt(1).sum()),
        "bid_close_below_zero_rows": int(bid_close.lt(0).sum()),
        "bid_close_above_one_rows": int(bid_close.gt(1).sum()),
        "ask_close_below_zero_rows": int(ask_close.lt(0).sum()),
        "ask_close_above_one_rows": int(ask_close.gt(1).sum()),
        "negative_volume_rows": int(candles["volume"].lt(0).sum()),
        "negative_open_interest_rows": int(candles["open_interest"].lt(0).sum()),
        "market_metadata_rows": int(len(metadata)),
        "exported_markets": int(candles["market_id"].nunique()),
        "market_rows_with_zero_exported_candles": int(
            metadata["actual_candles_exported"].eq(0).sum()
        ),
    }
    filter_counts = (
        metadata.groupby(
            ["passes_volume_filter", "passes_lifetime_filter", "passes_both_filters"],
            dropna=False,
        )
        .size()
        .reset_index(name="markets")
    )
    run_config = _parse_json(selected_run["config_json"])
    metadata_config = _parse_json(metadata_run["config_json"]) if metadata_run is not None else {}
    run_parameters = metadata_config.get("config", run_config.get("config", {}))
    candle_parameters = run_config.get("config", run_config)
    settlement_window = {
        "from": _timestamp_to_utc(run_parameters.get("completed_from_ts")),
        "to": _timestamp_to_utc(run_parameters.get("completed_to_ts")),
    }
    summary = {
        "export_created_at_utc": datetime.now(timezone.utc).isoformat(),
        "selection_id": selection_id,
        "selection_group": selection_group,
        "source_mode": source_mode,
        "filter_mode": candle_parameters.get("filter_mode"),
        "min_volume_fp": candle_parameters.get("min_volume_fp"),
        "min_lifetime_days": candle_parameters.get("min_lifetime_days"),
        "api_base_url": selected_run["base_url"],
        "metadata_run_id": metadata_run["run_id"] if metadata_run is not None else None,
        "metadata_run_status": metadata_run["status"] if metadata_run is not None else None,
        "candle_endpoint": (
            "/markets/candlesticks"
            if source_mode == "live"
            else "/historical/markets/{ticker}/candlesticks"
        ),
        "settlement_window_utc": settlement_window,
        "historical_cutoff_utc": metadata_config.get("historical_cutoff_utc")
        or run_config.get("historical_cutoff_utc"),
        "candle_run_id": selected_run["run_id"],
        "retry_parent_run_id": run_config.get("retry_parent_run_id"),
        "run_status": selected_run["status"],
        "run_started_at_utc": selected_run["started_at_utc"],
        "run_finished_at_utc": selected_run["finished_at_utc"],
        "market_metadata_rows": int(len(metadata)),
        "candle_rows": int(len(candles)),
        "markets_with_candles": int(candles["market_id"].nunique()),
        "first_candle_date_utc": first_date,
        "last_candle_date_utc": last_date,
        "filter_counts": filter_counts.to_dict(orient="records"),
        "quality": quality,
        "run_stats": _parse_json(selected_run["stats_json"]),
    }
    return summary, filter_counts


def _write_outputs(
    *,
    metadata: pd.DataFrame,
    candles: pd.DataFrame,
    summary: dict[str, Any],
    output_dir: Path,
    output_prefix: str,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = output_dir / f"{output_prefix}_market_metadata.csv"
    candles_path = output_dir / f"{output_prefix}_daily_candles.csv"
    summary_path = output_dir / "export_summary.json"

    metadata_output = metadata.copy()
    for column in ["open_time", "close_time"]:
        metadata_output[column] = metadata_output[column].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata_output = metadata_output[METADATA_COLUMNS]
    candles_output = candles[CANDLE_COLUMNS].copy()
    metadata_output.to_csv(metadata_path, index=False)
    candles_output.to_csv(candles_path, index=False)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, default=str)

    filtered_dir = output_dir.parent / f"{output_dir.name}_filtered"
    filtered_dir.mkdir(parents=True, exist_ok=True)
    filtered_metadata_path = filtered_dir / f"{output_prefix}_market_metadata.csv"
    filtered_candles_path = filtered_dir / f"{output_prefix}_daily_candles.csv"
    filtered_summary_path = filtered_dir / "export_summary.json"
    filtered_ids = set(metadata.loc[metadata["passes_both_filters"].eq(1), "market_id"])
    filtered_metadata = metadata_output.loc[metadata_output["market_id"].isin(filtered_ids)].copy()
    filtered_candles = candles_output.loc[candles_output["market_id"].isin(filtered_ids)].copy()
    filtered_summary = dict(summary)
    filtered_summary.update({
        "source_export": str(output_dir),
        "source_filter_counts": summary["filter_counts"],
        "filter_mode": "both",
        "filter_definition": "passes_both_filters == 1",
        "filter_counts": [{
            "passes_volume_filter": 1,
            "passes_lifetime_filter": 1,
            "passes_both_filters": 1,
            "markets": int(len(filtered_metadata)),
        }],
        "min_volume_fp": summary.get("min_volume_fp"),
        "min_lifetime_days": summary.get("min_lifetime_days"),
        "market_metadata_rows": int(len(filtered_metadata)),
        "candle_rows": int(len(filtered_candles)),
        "markets_with_candles": int(filtered_candles["market_id"].nunique()),
        "quality": {
            "metadata_market_id_unique": bool(filtered_metadata["market_id"].is_unique),
            "candle_market_day_unique": bool(
                not filtered_candles.duplicated(["market_id", "end_period_ts"]).any()
            ),
            "duplicate_market_day_rows": int(
                filtered_candles.duplicated(["market_id", "end_period_ts"]).sum()
            ),
            "candle_markets_missing_metadata": int(
                (~filtered_candles["market_id"].isin(filtered_metadata["market_id"])).sum()
            ),
        },
    })
    filtered_metadata.to_csv(filtered_metadata_path, index=False)
    filtered_candles.to_csv(filtered_candles_path, index=False)
    with filtered_summary_path.open("w", encoding="utf-8") as handle:
        json.dump(filtered_summary, handle, indent=2, default=str)

    return {
        "metadata_path": str(metadata_path),
        "candles_path": str(candles_path),
        "summary_path": str(summary_path),
        "filtered_metadata_path": str(filtered_metadata_path),
        "filtered_candles_path": str(filtered_candles_path),
        "filtered_summary_path": str(filtered_summary_path),
        "metadata_rows": len(metadata_output),
        "candle_rows": len(candles_output),
        "filtered_metadata_rows": len(filtered_metadata),
        "filtered_candle_rows": len(filtered_candles),
    }


def export_daily_snapshot(
    *,
    db_path: Path,
    selection_id: str,
    selection_group: str,
    source_mode: str,
    output_dir: Path,
    candle_run_id: str | None = None,
) -> dict[str, Any]:
    if source_mode not in SOURCE_MODES:
        raise ValueError(f"source_mode must be one of: {', '.join(SOURCE_MODES)}")
    if selection_group not in MANIFEST_GROUPS:
        raise ValueError(f"selection_group must be one of: {', '.join(MANIFEST_GROUPS)}")
    db_path = db_path.expanduser().resolve()
    if not db_path.exists():
        raise FileNotFoundError(db_path)
    output_dir = output_dir.expanduser().resolve()
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=60) as conn:
        selected_run = _select_candle_run(
            conn,
            selection_id=selection_id,
            selection_group=selection_group,
            source_mode=source_mode,
            candle_run_id=candle_run_id,
        )
        selected_id = str(selected_run["run_id"])
        metadata_run = _select_metadata_run(
            conn,
            selection_id=selection_id,
            selection_group=selection_group,
            source_mode=source_mode,
        )
        metadata = _load_market_metadata(
            conn,
            candle_run_id=selected_id,
            selection_id=selection_id,
            selection_group=selection_group,
        )
        candles = _load_candles(
            conn,
            candle_run_id=selected_id,
            selection_id=selection_id,
            selection_group=selection_group,
            source_mode=source_mode,
        )
    summary, _ = _build_summary(
        metadata=metadata,
        candles=candles,
        selected_run=selected_run,
        metadata_run=metadata_run,
        selection_id=selection_id,
        selection_group=selection_group,
        source_mode=source_mode,
    )
    output_prefix = "main" if selection_group == MAIN_GROUP else selection_group
    result = _write_outputs(
        metadata=metadata,
        candles=candles,
        summary=summary,
        output_dir=output_dir,
        output_prefix=output_prefix,
    )
    return {
        "selection_id": selection_id,
        "selection_group": selection_group,
        "source_mode": source_mode,
        "candle_run_id": selected_id,
        "run_status": selected_run["status"],
        **result,
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = export_daily_snapshot(
        db_path=args.db_path,
        selection_id=args.selection_id,
        selection_group=args.group,
        source_mode=args.source_mode,
        output_dir=args.output_dir,
        candle_run_id=args.candle_run_id,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
