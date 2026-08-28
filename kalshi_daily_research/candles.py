"""Native daily candlestick ingestion for frozen market universes."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import hashlib
import json
import math
import sqlite3
import threading
from typing import Any, Iterable
import uuid

try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover - tqdm is optional for library use
    tqdm = None  # type: ignore[assignment]

from .client import KalshiMetadataClient
from .ingest import _float, _json_text, _now_utc, _unix_timestamp
from .schema import ensure_schema


FILTER_MODES = frozenset({"volume", "lifetime", "both", "union"})
RETRY_STATUSES = frozenset({"api_error"})
SOURCE_MODES = frozenset({"live", "historical"})


@dataclass(frozen=True)
class DailyCandleRunConfig:
    """Configuration for one native daily candle backfill."""

    selection_id: str
    selection_group: str = "non_sport_crypto"
    filter_mode: str = "union"
    min_volume_fp: float = 10_000.0
    min_lifetime_days: float = 5.0
    start_ts: int | None = None
    end_ts: int | None = None
    period_interval: int = 1440
    max_tickers_per_batch: int = 100
    max_candles_per_batch: int = 10_000
    max_batches: int | None = None
    include_latest_before_start: bool = False
    retry_status: str | None = None
    show_progress: bool = True
    source_mode: str = "live"
    max_markets: int | None = None
    max_workers: int = 4


def _run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]


def _ceil_rows(start_ts: int, end_ts: int, period_interval: int) -> int:
    if end_ts < start_ts:
        return 0
    return max(1, math.ceil((end_ts - start_ts) / (period_interval * 60)) + 1)


def _candle_value(section: Any, field: str) -> float | None:
    if not isinstance(section, dict):
        return None
    value = section.get(f"{field}_dollars")
    if value is None:
        value = section.get(field)
    return _float(value)


def _normalize_candle(
    candle: dict[str, Any],
    *,
    market_id: str,
    market_ticker: str,
    series_ticker: str | None,
    source_mode: str,
    source_endpoint: str,
    request_start_ts: int,
    request_end_ts: int,
    retrieved_at: str,
    run_id: str,
    period_interval: int,
) -> dict[str, Any] | None:
    end_period_ts = _unix_timestamp(candle.get("end_period_ts"))
    if end_period_ts is None:
        return None
    price = candle.get("price")
    yes_bid = candle.get("yes_bid")
    yes_ask = candle.get("yes_ask")
    return {
        "market_id": market_id,
        "market_ticker": market_ticker,
        "series_ticker": series_ticker,
        "end_period_ts": end_period_ts,
        "period_interval": period_interval,
        "source_mode": source_mode,
        "source_endpoint": source_endpoint,
        "price_open": _candle_value(price, "open"),
        "price_low": _candle_value(price, "low"),
        "price_high": _candle_value(price, "high"),
        "price_close": _candle_value(price, "close"),
        "price_mean": _candle_value(price, "mean"),
        "price_previous": _candle_value(price, "previous"),
        "price_min": _candle_value(price, "min"),
        "price_max": _candle_value(price, "max"),
        "yes_bid_open": _candle_value(yes_bid, "open"),
        "yes_bid_low": _candle_value(yes_bid, "low"),
        "yes_bid_high": _candle_value(yes_bid, "high"),
        "yes_bid_close": _candle_value(yes_bid, "close"),
        "yes_ask_open": _candle_value(yes_ask, "open"),
        "yes_ask_low": _candle_value(yes_ask, "low"),
        "yes_ask_high": _candle_value(yes_ask, "high"),
        "yes_ask_close": _candle_value(yes_ask, "close"),
        "volume": _float(candle.get("volume_fp", candle.get("volume"))),
        "open_interest": _float(candle.get("open_interest_fp", candle.get("open_interest"))),
        "request_start_ts": request_start_ts,
        "request_end_ts": request_end_ts,
        "retrieved_at_utc": retrieved_at,
        "run_id": run_id,
        "raw_payload_json": _json_text(candle),
    }


class DailyCandleIngestor:
    """Build a market filter manifest and ingest live native daily candles."""

    def __init__(self, conn: sqlite3.Connection, client: Any) -> None:
        self.conn = conn
        self.client = client
        ensure_schema(conn)

    def _latest_daily_candle_run_id(self, config: DailyCandleRunConfig) -> str:
        row = self.conn.execute(
            """
            SELECT run_id
            FROM metadata_runs
            WHERE json_extract(config_json, '$.stage') = 'daily_candles'
              AND json_extract(config_json, '$.selection_id') = ?
              AND json_extract(config_json, '$.selection_group') = ?
              AND source_mode = ?
            ORDER BY started_at_utc DESC, rowid DESC
            LIMIT 1
            """,
            (config.selection_id, config.selection_group, config.source_mode),
        ).fetchone()
        if row is None:
            raise ValueError(
                "No previous daily-candle run exists for the selected selection and group."
            )
        return str(row[0])

    def _load_retry_candidates(
        self,
        config: DailyCandleRunConfig,
    ) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            WITH ranked_manifest AS (
                SELECT
                    mh.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY mh.market_id
                        ORDER BY mr.started_at_utc DESC, mr.rowid DESC, mh.run_id DESC
                    ) AS latest_row,
                    MAX(CASE WHEN mh.status = 'api_error' THEN 1 ELSE 0 END)
                        OVER (PARTITION BY mh.market_id) AS had_api_error
                FROM market_history_manifest mh
                JOIN metadata_runs mr ON mr.run_id = mh.run_id
                WHERE mh.selection_id = ?
                  AND mh.selection_group = ?
                  AND mr.source_mode = ?
            )
            SELECT
                market_id,
                market_ticker,
                event_ticker,
                series_ticker,
                selection_id,
                selection_group,
                filter_mode,
                passes_volume_filter,
                passes_lifetime_filter,
                passes_both_filters,
                volume_fp,
                lifetime_days,
                history_start_ts,
                history_end_ts,
                expected_daily_rows
            FROM ranked_manifest
            WHERE latest_row = 1
              AND had_api_error = 1
              AND status IN ('api_error', 'queued')
            ORDER BY market_ticker
            """,
            (
                config.selection_id,
                config.selection_group,
                config.source_mode,
            ),
        ).fetchall()

        return [
            {
                "market_id": row[0],
                "market_ticker": row[1],
                "event_ticker": row[2],
                "series_ticker": row[3],
                "selection_id": row[4],
                "selection_group": row[5],
                "filter_mode": row[6],
                "passes_volume_filter": row[7],
                "passes_lifetime_filter": row[8],
                "passes_both_filters": row[9],
                "volume_fp": row[10],
                "lifetime_days": row[11],
                "history_start_ts": row[12],
                "history_end_ts": row[13],
                "expected_daily_rows": row[14],
                "status": "queued",
            }
            for row in rows
        ]

    def _load_candidates(
        self,
        config: DailyCandleRunConfig,
        *,
        retry_parent_run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        if config.retry_status is not None:
            if retry_parent_run_id is None:
                raise ValueError("retry_parent_run_id is required for retry candidates")
            candidates = self._load_retry_candidates(config)
            if not candidates:
                raise ValueError(
                    f"No unresolved markets with historical status {config.retry_status!r} found "
                    "for the selected selection and group."
                )
            return candidates

        rows = self.conn.execute(
            """
            SELECT
                m.market_id,
                m.ticker AS market_ticker,
                m.event_ticker,
                COALESCE(m.series_ticker, e.series_ticker) AS series_ticker,
                m.volume_fp,
                m.open_time,
                m.close_time,
                COALESCE(sm.selection_group, json_extract(r.config_json, '$.selection_group'))
                    AS resolved_group
            FROM raw_markets m
            LEFT JOIN raw_events e ON e.event_ticker = m.event_ticker
            LEFT JOIN series_selection_members sm
              ON sm.series_ticker = COALESCE(m.series_ticker, e.series_ticker)
             AND sm.selection_id = ?
             AND sm.eligible = 1
            JOIN metadata_runs r
              ON r.run_id = m.run_id
             AND r.source_mode = ?
            WHERE COALESCE(sm.selection_group, json_extract(r.config_json, '$.selection_group')) = ?
              AND (
                    sm.series_ticker IS NOT NULL
                    OR json_extract(r.config_json, '$.selection_id') = ?
              )
              AND m.ticker IS NOT NULL
            ORDER BY m.ticker
            """,
            (config.selection_id, config.source_mode, config.selection_group, config.selection_id),
        ).fetchall()

        now_ts = int(datetime.now(tz=UTC).timestamp())
        candidates: list[dict[str, Any]] = []
        for row in rows:
            (
                market_id,
                market_ticker,
                event_ticker,
                series_ticker,
                volume_fp,
                open_time,
                close_time,
                resolved_group,
            ) = row
            volume = _float(volume_fp)
            open_ts = _unix_timestamp(open_time)
            close_ts = _unix_timestamp(close_time)
            lifetime_days = None
            if open_ts is not None and close_ts is not None:
                lifetime_days = (close_ts - open_ts) / 86400.0
            passes_volume = volume is not None and volume >= config.min_volume_fp
            passes_lifetime = lifetime_days is not None and lifetime_days >= config.min_lifetime_days
            passes_both = passes_volume and passes_lifetime
            keep = {
                "volume": passes_volume,
                "lifetime": passes_lifetime,
                "both": passes_both,
                "union": passes_volume or passes_lifetime,
            }[config.filter_mode]
            if not keep:
                continue

            history_start_ts = max(config.start_ts or 0, open_ts or config.start_ts or 0)
            history_end_ts = min(config.end_ts or now_ts, close_ts or config.end_ts or now_ts)
            expected_daily_rows = _ceil_rows(history_start_ts, history_end_ts, config.period_interval)
            if history_end_ts < history_start_ts:
                expected_daily_rows = 0
            candidates.append(
                {
                    "market_id": market_id or f"kalshi:{market_ticker}",
                    "market_ticker": market_ticker,
                    "event_ticker": event_ticker,
                    "series_ticker": series_ticker,
                    "selection_id": config.selection_id,
                    "selection_group": resolved_group or config.selection_group,
                    "filter_mode": config.filter_mode,
                    "passes_volume_filter": int(passes_volume),
                    "passes_lifetime_filter": int(passes_lifetime),
                    "passes_both_filters": int(passes_both),
                    "volume_fp": volume,
                    "lifetime_days": lifetime_days,
                    "history_start_ts": history_start_ts,
                    "history_end_ts": history_end_ts,
                    "expected_daily_rows": expected_daily_rows,
                    "status": "queued" if expected_daily_rows else "skipped_no_window",
                }
            )
        return candidates

    @staticmethod
    def _batches(candidates: list[dict[str, Any]], config: DailyCandleRunConfig) -> Iterable[list[dict[str, Any]]]:
        batch: list[dict[str, Any]] = []
        estimated_rows = 0
        max_tickers_per_batch = config.max_tickers_per_batch
        for candidate in candidates:
            if candidate["expected_daily_rows"] <= 0:
                continue
            expected = candidate["expected_daily_rows"]
            would_exceed = batch and (
                len(batch) >= max_tickers_per_batch
                or estimated_rows + expected > config.max_candles_per_batch
            )
            if would_exceed:
                yield batch
                batch = []
                estimated_rows = 0
            batch.append(candidate)
            estimated_rows += expected
        if batch:
            yield batch

    def _insert_run(
        self,
        run_id: str,
        started_at: str,
        config: DailyCandleRunConfig,
        *,
        retry_parent_run_id: str | None = None,
    ) -> None:
        run_config = {
            "stage": "daily_candles",
            "selection_id": config.selection_id,
            "selection_group": config.selection_group,
            "filter_mode": config.filter_mode,
            "retry_status": config.retry_status,
            "retry_parent_run_id": retry_parent_run_id,
            "config": asdict(config),
        }
        self.conn.execute(
            """
            INSERT INTO metadata_runs
                (run_id, started_at_utc, status, base_url, source_mode, config_json)
            VALUES (?, ?, 'running', ?, ?, ?)
            """,
            (
                run_id,
                started_at,
                getattr(self.client, "base_url", ""),
                config.source_mode,
                _json_text(run_config),
            ),
        )

    def _copy_manifest_rows(self, run_id: str, parent_run_id: str) -> None:
        self.conn.execute(
            """
            INSERT INTO market_history_manifest (
                run_id, market_id, market_ticker, event_ticker, series_ticker,
                selection_id, selection_group, filter_mode,
                passes_volume_filter, passes_lifetime_filter, passes_both_filters,
                volume_fp, lifetime_days, history_start_ts, history_end_ts,
                expected_daily_rows, received_daily_rows, first_observation_ts,
                last_observation_ts, status, error_text
            )
            SELECT
                ?, market_id, market_ticker, event_ticker, series_ticker,
                selection_id, selection_group, filter_mode,
                passes_volume_filter, passes_lifetime_filter, passes_both_filters,
                volume_fp, lifetime_days, history_start_ts, history_end_ts,
                expected_daily_rows, received_daily_rows, first_observation_ts,
                last_observation_ts, status, error_text
            FROM market_history_manifest
            WHERE run_id = ?
            """,
            (run_id, parent_run_id),
        )

    def _reset_retry_rows(self, run_id: str, candidates: list[dict[str, Any]]) -> None:
        self.conn.executemany(
            """
            UPDATE market_history_manifest
            SET received_daily_rows=0,
                first_observation_ts=NULL,
                last_observation_ts=NULL,
                status='queued',
                error_text=NULL
            WHERE run_id=? AND market_id=?
            """,
            [(run_id, candidate["market_id"]) for candidate in candidates],
        )

    def _insert_manifest_rows(self, run_id: str, candidates: list[dict[str, Any]]) -> None:
        values = [
            (
                run_id,
                candidate["market_id"],
                candidate["market_ticker"],
                candidate["event_ticker"],
                candidate["series_ticker"],
                candidate["selection_id"],
                candidate["selection_group"],
                candidate["filter_mode"],
                candidate["passes_volume_filter"],
                candidate["passes_lifetime_filter"],
                candidate["passes_both_filters"],
                candidate["volume_fp"],
                candidate["lifetime_days"],
                candidate["history_start_ts"],
                candidate["history_end_ts"],
                candidate["expected_daily_rows"],
                candidate["status"],
            )
            for candidate in candidates
        ]
        self.conn.executemany(
            """
            INSERT INTO market_history_manifest (
                run_id, market_id, market_ticker, event_ticker, series_ticker,
                selection_id, selection_group, filter_mode,
                passes_volume_filter, passes_lifetime_filter, passes_both_filters,
                volume_fp, lifetime_days, history_start_ts, history_end_ts,
                expected_daily_rows, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )

    def _store_batch_payload(self, run_id: str, batch_number: int, payload: dict[str, Any], retrieved_at: str) -> None:
        payload_json = _json_text(payload)
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        self.conn.execute(
            """
            INSERT INTO raw_payloads (
                run_id, entity_type, entity_key, source_endpoint,
                retrieved_at_utc, payload_sha256, payload_json
            ) VALUES (?, 'daily_candles_batch', ?, '/markets/candlesticks', ?, ?, ?)
            """,
            (run_id, f"batch:{batch_number}", retrieved_at, payload_hash, payload_json),
        )

    def _upsert_candle(self, row: dict[str, Any]) -> None:
        columns = [
            "market_id", "market_ticker", "series_ticker", "end_period_ts", "period_interval",
            "source_mode", "source_endpoint", "price_open", "price_low", "price_high",
            "price_close", "price_mean", "price_previous", "price_min", "price_max",
            "yes_bid_open", "yes_bid_low", "yes_bid_high", "yes_bid_close",
            "yes_ask_open", "yes_ask_low", "yes_ask_high", "yes_ask_close",
            "volume", "open_interest", "request_start_ts", "request_end_ts",
            "retrieved_at_utc", "run_id", "raw_payload_json",
        ]
        placeholders = ", ".join("?" for _ in columns)
        updates = ", ".join(f"{column}=excluded.{column}" for column in columns if column not in {
            "market_ticker", "end_period_ts", "period_interval", "source_mode"
        })
        self.conn.execute(
            f"""
            INSERT INTO raw_daily_candles ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (market_ticker, end_period_ts, period_interval, source_mode)
            DO UPDATE SET {updates}
            """,
            tuple(row[column] for column in columns),
        )

    def _update_manifest(self, run_id: str, ticker: str, received: int, first_ts: int | None, last_ts: int | None, status: str, error_text: str | None = None) -> None:
        self.conn.execute(
            """
            UPDATE market_history_manifest
            SET received_daily_rows=?, first_observation_ts=?, last_observation_ts=?,
                status=?, error_text=?
            WHERE run_id=? AND market_ticker=?
            """,
            (received, first_ts, last_ts, status, error_text, run_id, ticker),
        )

    def _process_market_payload(
        self,
        *,
        run_id: str,
        candidate: dict[str, Any],
        payload: dict[str, Any],
        request_start_ts: int,
        request_end_ts: int,
        retrieved_at: str,
        config: DailyCandleRunConfig,
    ) -> tuple[str, int, int]:
        """Normalize one market payload and update its manifest row."""

        ticker = payload.get("market_ticker") or payload.get("ticker")
        if not ticker or ticker != candidate["market_ticker"]:
            self._update_manifest(
                run_id,
                candidate["market_ticker"],
                0,
                None,
                None,
                "empty",
                "historical payload ticker did not match the requested market",
            )
            return "empty", 0, 1

        candles = payload.get("candlesticks")
        if not isinstance(candles, list):
            candles = []
        received = 0
        invalid_records = 0
        observation_ts: list[int] = []
        for candle in candles:
            if not isinstance(candle, dict):
                invalid_records += 1
                continue
            end_period_ts = _unix_timestamp(candle.get("end_period_ts"))
            if end_period_ts is None or not (
                candidate["history_start_ts"] <= end_period_ts <= candidate["history_end_ts"]
            ):
                continue
            normalized = _normalize_candle(
                candle,
                market_id=candidate["market_id"],
                market_ticker=ticker,
                series_ticker=candidate["series_ticker"],
                source_mode=config.source_mode,
                source_endpoint=(
                    "/markets/candlesticks"
                    if config.source_mode == "live"
                    else f"/historical/markets/{candidate['market_ticker']}/candlesticks"
                ),
                request_start_ts=request_start_ts,
                request_end_ts=request_end_ts,
                retrieved_at=retrieved_at,
                run_id=run_id,
                period_interval=config.period_interval,
            )
            if normalized is None:
                invalid_records += 1
                continue
            self._upsert_candle(normalized)
            received += 1
            observation_ts.append(end_period_ts)

        status = "success" if received else "empty"
        self._update_manifest(
            run_id,
            ticker,
            received,
            min(observation_ts) if observation_ts else None,
            max(observation_ts) if observation_ts else None,
            status,
        )
        return status, received, invalid_records

    def _store_single_payload(
        self,
        run_id: str,
        market_ticker: str,
        payload: dict[str, Any],
        retrieved_at: str,
        source_endpoint: str,
    ) -> None:
        payload_json = _json_text(payload)
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        self.conn.execute(
            """
            INSERT INTO raw_payloads (
                run_id, entity_type, entity_key, source_endpoint,
                retrieved_at_utc, payload_sha256, payload_json
            ) VALUES (?, 'daily_candles', ?, ?, ?, ?, ?)
            """,
            (run_id, market_ticker, source_endpoint, retrieved_at, payload_hash, payload_json),
        )

    def _run_historical_requests(
        self,
        *,
        run_id: str,
        candidates: list[dict[str, Any]],
        config: DailyCandleRunConfig,
        stats: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch archived market candles through the single-market endpoint."""

        queued_candidates = [candidate for candidate in candidates if candidate["expected_daily_rows"] > 0]
        status_counts = {
            "success": 0,
            "empty": 0,
            "api_error": 0,
            "api_error_batches": 0,
            "skipped_no_window": stats["skipped_no_window"],
        }
        stats["status_counts"] = status_counts
        stats["requests"] = 0
        stats["max_workers"] = config.max_workers
        progress = (
            tqdm(
                total=len(queued_candidates),
                desc="Kalshi historical daily candles",
                unit="market",
                disable=not config.show_progress,
            )
            if tqdm
            else None
        )
        thread_local = threading.local()

        def fetch(candidate: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None, str | None, str]:
            client = getattr(thread_local, "client", None)
            if client is None:
                if isinstance(self.client, KalshiMetadataClient):
                    client = KalshiMetadataClient(
                        base_url=self.client.base_url,
                        config=self.client.config,
                    )
                else:
                    client = self.client
                thread_local.client = client
            retrieved_at = _now_utc()
            try:
                payload = client.get_historical_market_candlesticks(
                    market_ticker=candidate["market_ticker"],
                    start_ts=candidate["history_start_ts"],
                    end_ts=candidate["history_end_ts"],
                    period_interval=config.period_interval,
                )
                return candidate, payload, None, retrieved_at
            except Exception as exc:  # noqa: BLE001
                return candidate, None, repr(exc), retrieved_at

        def consume(result: tuple[dict[str, Any], dict[str, Any] | None, str | None, str]) -> None:
            candidate, payload, error_text, retrieved_at = result
            stats["requests"] += 1
            stats["batches"] += 1
            if error_text is not None:
                stats["api_errors"] += 1
                status_counts["api_error"] += 1
                status_counts["api_error_batches"] += 1
                with self.conn:
                    self._update_manifest(
                        run_id,
                        candidate["market_ticker"],
                        0,
                        None,
                        None,
                        "api_error",
                        error_text,
                    )
            elif not isinstance(payload, dict):
                stats["invalid_records"] += 1
                status_counts["empty"] += 1
                stats["empty_markets"] += 1
                with self.conn:
                    self._update_manifest(
                        run_id,
                        candidate["market_ticker"],
                        0,
                        None,
                        None,
                        "empty",
                    )
            else:
                stats["markets_returned"] += 1
                with self.conn:
                    self._store_single_payload(
                        run_id,
                        candidate["market_ticker"],
                        payload,
                        retrieved_at,
                        f"/historical/markets/{candidate['market_ticker']}/candlesticks",
                    )
                    status, received, invalid_records = self._process_market_payload(
                        run_id=run_id,
                        candidate=candidate,
                        payload=payload,
                        request_start_ts=candidate["history_start_ts"],
                        request_end_ts=candidate["history_end_ts"],
                        retrieved_at=retrieved_at,
                        config=config,
                    )
                status_counts[status] += 1
                stats["invalid_records"] += invalid_records
                stats["candles_seen"] += received
                stats["candles_written"] += received
                if status == "empty":
                    stats["empty_markets"] += 1
            if progress is not None:
                progress.set_postfix(status_counts, refresh=False)
                progress.update(1)

        try:
            with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
                candidate_iter = iter(queued_candidates)
                pending = {
                    executor.submit(fetch, next(candidate_iter)): True
                    for _ in range(min(len(queued_candidates), config.max_workers * 4))
                }
                while pending:
                    completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for future in completed:
                        pending.pop(future)
                        consume(future.result())
                        try:
                            next_candidate = next(candidate_iter)
                        except StopIteration:
                            continue
                        pending[executor.submit(fetch, next_candidate)] = True
        except KeyboardInterrupt:
            with self.conn:
                self.conn.execute(
                    "UPDATE metadata_runs SET finished_at_utc=?, status='partial', stats_json=? WHERE run_id=?",
                    (_now_utc(), _json_text(stats), run_id),
                )
            raise
        finally:
            if progress is not None:
                progress.close()

        final_status = "success" if stats["api_errors"] == 0 else "partial"
        with self.conn:
            self.conn.execute(
                "UPDATE metadata_runs SET finished_at_utc=?, status=?, stats_json=? WHERE run_id=?",
                (_now_utc(), final_status, _json_text(stats), run_id),
            )
        return stats

    def run(self, config: DailyCandleRunConfig) -> dict[str, Any]:
        if config.source_mode not in SOURCE_MODES:
            raise ValueError(f"source_mode must be one of: {', '.join(sorted(SOURCE_MODES))}")
        if config.filter_mode not in FILTER_MODES:
            raise ValueError(f"filter_mode must be one of: {', '.join(sorted(FILTER_MODES))}")
        if config.min_volume_fp < 0 or config.min_lifetime_days < 0:
            raise ValueError("filter thresholds must be non-negative")
        if config.period_interval != 1440:
            raise ValueError("This command currently supports daily candles only: period_interval=1440")
        if config.retry_status is not None and config.retry_status not in RETRY_STATUSES:
            raise ValueError(f"retry_status must be one of: {', '.join(sorted(RETRY_STATUSES))}")
        if not 1 <= config.max_tickers_per_batch <= 100:
            raise ValueError("max_tickers_per_batch must be between 1 and 100")
        if not 1 <= config.max_candles_per_batch <= 10_000:
            raise ValueError("max_candles_per_batch must be between 1 and 10000")
        if config.max_markets is not None and config.max_markets < 1:
            raise ValueError("max_markets must be positive")
        if not 1 <= config.max_workers <= 32:
            raise ValueError("max_workers must be between 1 and 32")
        if config.source_mode == "historical" and config.include_latest_before_start:
            raise ValueError("include_latest_before_start is supported only for live candles")
        if config.source_mode == "historical" and config.max_batches is not None:
            raise ValueError("max_batches is supported only for live candles; use max_markets for historical smoke tests")
        if config.start_ts is not None and config.end_ts is not None and config.end_ts <= config.start_ts:
            raise ValueError("end_ts must be after start_ts")

        run_id = _run_id()
        started_at = _now_utc()
        retry_parent_run_id = (
            self._latest_daily_candle_run_id(config)
            if config.retry_status is not None
            else None
        )
        candidates = self._load_candidates(
            config,
            retry_parent_run_id=retry_parent_run_id,
        )
        if config.max_markets is not None:
            candidates = candidates[: config.max_markets]
        stats: dict[str, Any] = {
            "run_id": run_id,
            "selection_id": config.selection_id,
            "selection_group": config.selection_group,
            "source_mode": config.source_mode,
            "filter_mode": config.filter_mode,
            "candidate_markets": len(candidates),
            "volume_filter_markets": sum(c["passes_volume_filter"] for c in candidates),
            "lifetime_filter_markets": sum(c["passes_lifetime_filter"] for c in candidates),
            "both_filter_markets": sum(c["passes_both_filters"] for c in candidates),
            "batches": 0,
            "markets_returned": 0,
            "candles_seen": 0,
            "candles_written": 0,
            "invalid_records": 0,
            "api_errors": 0,
            "empty_markets": 0,
            "skipped_no_window": sum(c["expected_daily_rows"] == 0 for c in candidates),
            "retry_status": config.retry_status,
            "retry_parent_run_id": retry_parent_run_id,
        }

        with self.conn:
            self._insert_run(
                run_id,
                started_at,
                config,
                retry_parent_run_id=retry_parent_run_id,
            )
            if retry_parent_run_id is None:
                self._insert_manifest_rows(run_id, candidates)
            else:
                self._copy_manifest_rows(run_id, retry_parent_run_id)
                self._reset_retry_rows(run_id, candidates)

        if config.source_mode == "historical":
            return self._run_historical_requests(
                run_id=run_id,
                candidates=candidates,
                config=config,
                stats=stats,
            )

        batches = list(self._batches(candidates, config))
        if config.max_batches is not None:
            batches = batches[:config.max_batches]
        status_counts = {
            "success": 0,
            "empty": 0,
            "api_error": 0,
            "api_error_batches": 0,
            "skipped_no_window": stats["skipped_no_window"],
        }
        stats["status_counts"] = status_counts
        progress = (
            tqdm(
                total=len(batches),
                desc="Kalshi daily candle batches",
                unit="batch",
                disable=not config.show_progress,
            )
            if tqdm
            else None
        )

        def advance_progress() -> None:
            if progress is not None:
                progress.set_postfix(status_counts, refresh=False)
                progress.update(1)

        try:
            for batch_number, batch in enumerate(batches, start=1):
                stats["batches"] += 1
                request_start = min(candidate["history_start_ts"] for candidate in batch)
                request_end = max(candidate["history_end_ts"] for candidate in batch)
                tickers = [candidate["market_ticker"] for candidate in batch]
                candidate_by_ticker = {candidate["market_ticker"]: candidate for candidate in batch}
                retrieved_at = _now_utc()
                try:
                    payload = self.client.get_batch_market_candlesticks(
                        market_tickers=tickers,
                        start_ts=request_start,
                        end_ts=request_end,
                        period_interval=config.period_interval,
                        include_latest_before_start=config.include_latest_before_start,
                    )
                    with self.conn:
                        self._store_batch_payload(run_id, batch_number, payload, retrieved_at)
                except Exception as exc:  # noqa: BLE001
                    stats["api_errors"] += 1
                    status_counts["api_error"] += len(batch)
                    status_counts["api_error_batches"] += 1
                    with self.conn:
                        for candidate in batch:
                            self._update_manifest(
                                run_id,
                                candidate["market_ticker"],
                                0,
                                None,
                                None,
                                "api_error",
                                repr(exc),
                            )
                    advance_progress()
                    continue

                returned_markets = payload.get("markets") if isinstance(payload, dict) else None
                if not isinstance(returned_markets, list):
                    returned_markets = []
                stats["markets_returned"] += len(returned_markets)
                seen_tickers: set[str] = set()
                with self.conn:
                    for market_payload in returned_markets:
                        if not isinstance(market_payload, dict):
                            stats["invalid_records"] += 1
                            continue
                        ticker = market_payload.get("market_ticker") or market_payload.get("ticker")
                        if not ticker or ticker not in candidate_by_ticker:
                            stats["invalid_records"] += 1
                            continue
                        candidate = candidate_by_ticker[ticker]
                        seen_tickers.add(ticker)
                        candles = market_payload.get("candlesticks")
                        if not isinstance(candles, list):
                            candles = []
                        received = 0
                        observation_ts: list[int] = []
                        for candle in candles:
                            if not isinstance(candle, dict):
                                stats["invalid_records"] += 1
                                continue
                            end_period_ts = _unix_timestamp(candle.get("end_period_ts"))
                            if end_period_ts is None or not (
                                candidate["history_start_ts"] <= end_period_ts <= candidate["history_end_ts"]
                            ):
                                continue
                            normalized = _normalize_candle(
                                candle,
                                market_id=candidate["market_id"],
                                market_ticker=ticker,
                                series_ticker=candidate["series_ticker"],
                                source_mode=config.source_mode,
                                source_endpoint="/markets/candlesticks",
                                request_start_ts=request_start,
                                request_end_ts=request_end,
                                retrieved_at=retrieved_at,
                                run_id=run_id,
                                period_interval=config.period_interval,
                            )
                            if normalized is None:
                                stats["invalid_records"] += 1
                                continue
                            self._upsert_candle(normalized)
                            received += 1
                            observation_ts.append(end_period_ts)
                            stats["candles_seen"] += 1
                            stats["candles_written"] += 1
                        market_status = "success" if received else "empty"
                        status_counts[market_status] += 1
                        if received == 0:
                            stats["empty_markets"] += 1
                        self._update_manifest(
                            run_id,
                            ticker,
                            received,
                            min(observation_ts) if observation_ts else None,
                            max(observation_ts) if observation_ts else None,
                            market_status,
                        )

                    for candidate in batch:
                        if candidate["market_ticker"] not in seen_tickers:
                            status_counts["empty"] += 1
                            stats["empty_markets"] += 1
                            self._update_manifest(
                                run_id,
                                candidate["market_ticker"],
                                0,
                                None,
                                None,
                                "empty",
                            )
                advance_progress()
        except Exception as exc:
            with self.conn:
                self.conn.execute(
                    "UPDATE metadata_runs SET finished_at_utc=?, status='failed', stats_json=?, error_text=? WHERE run_id=?",
                    (_now_utc(), _json_text(stats), repr(exc), run_id),
                )
            raise
        finally:
            if progress is not None:
                progress.close()

        final_status = "success" if stats["api_errors"] == 0 else "partial"
        with self.conn:
            self.conn.execute(
                "UPDATE metadata_runs SET finished_at_utc=?, status=?, stats_json=? WHERE run_id=?",
                (_now_utc(), final_status, _json_text(stats), run_id),
            )
        return stats
