"""Source-preserving ingestion of Kalshi raw metadata into SQLite."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import hashlib
import json
import logging
import sqlite3
from typing import Any, Iterable
import uuid

try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover - tqdm is optional for library use
    tqdm = None  # type: ignore[assignment]

from .client import KalshiMetadataClient
from .schema import ensure_schema
from .series_manifest import MANIFEST_GROUPS


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RawMetadataRunConfig:
    """Configuration for one raw metadata snapshot run."""

    page_limit: int = 200
    max_pages: int | None = None
    source_mode: str = "both"
    fetch_event_details: bool = True
    refresh_event_details: bool = False
    show_progress: bool = True
    completed_from_ts: int | None = None
    completed_to_ts: int | None = None
    min_series_volume: float = 20_000.0


@dataclass(frozen=True)
class RawSeriesRunConfig:
    """Configuration for the series-only extraction stage."""

    page_limit: int = 200
    max_pages: int | None = None
    show_progress: bool = True


@dataclass(frozen=True)
class ManifestGroupRunConfig:
    """Configuration for downloading markets/events for one frozen manifest group."""

    selection_id: str
    selection_group: str
    page_limit: int = 200
    max_pages: int | None = None
    source_mode: str = "live"
    fetch_event_details: bool = True
    refresh_event_details: bool = False
    show_progress: bool = True
    completed_from_ts: int | None = None
    completed_to_ts: int | None = None


def _now_utc() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _text(value: Any) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool_flag(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"false", "0", "no", "off"}:
            return 0
        if normalized in {"true", "1", "yes", "on"}:
            return 1
    return int(bool(value))


def _nested_json(value: Any) -> str | None:
    return None if value is None else _json_text(value)


def _payload_key(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()[:32]


def _entity_key(payload: dict[str, Any], preferred: str | None) -> str:
    return _text(preferred) or f"payload:{_payload_key(payload)}"


def _market_id(ticker: str | None) -> str | None:
    return f"kalshi:{ticker}" if ticker else None


def _event_id(ticker: str | None) -> str | None:
    return f"kalshi:event:{ticker}" if ticker else None


def _unix_timestamp(value: Any) -> int | None:
    """Parse Kalshi's ISO or Unix timestamp representations."""

    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp())


def _is_in_completion_window(
    payload: dict[str, Any],
    *,
    from_ts: int | None,
    to_ts: int | None,
) -> bool:
    """Return whether a market settled in the half-open UTC interval."""

    if from_ts is None:
        return True
    settled_ts = _unix_timestamp(payload.get("settlement_ts"))
    return settled_ts is not None and from_ts <= settled_ts < int(to_ts)


def _normalize_series(payload: dict[str, Any], *, endpoint: str, run_id: str, retrieved_at: str) -> dict[str, Any] | None:
    ticker = _text(payload.get("ticker"))
    if ticker is None:
        return None
    return {
        "series_ticker": ticker,
        "source": "kalshi",
        "title": payload.get("title"),
        "subtitle": payload.get("subtitle") or payload.get("sub_title"),
        "category": payload.get("category"),
        "tags_json": _nested_json(payload.get("tags")),
        "frequency": payload.get("frequency"),
        "volume_fp": _float(payload.get("volume_fp")),
        "status": payload.get("status"),
        "created_time": payload.get("created_time"),
        "updated_time": payload.get("updated_time") or payload.get("last_updated_ts"),
        "close_time": payload.get("close_time"),
        "settlement_time": payload.get("settlement_time"),
        "source_endpoint": endpoint,
        "raw_payload_json": _json_text(payload),
        "retrieved_at_utc": retrieved_at,
        "run_id": run_id,
    }


def _is_non_mve_market(payload: dict[str, Any]) -> bool:
    """Return whether a market is not part of a multivariate collection."""

    collection = _text(payload.get("mve_collection_ticker"))
    selected_legs = payload.get("mve_selected_legs")
    return not collection and selected_legs in (None, "", "[]", "null", [], {})


def _normalize_event(payload: dict[str, Any], *, endpoint: str, run_id: str, retrieved_at: str) -> dict[str, Any] | None:
    ticker = _text(payload.get("event_ticker") or payload.get("ticker"))
    event_id = _event_id(ticker)
    if event_id is None:
        return None
    return {
        "event_id": event_id,
        "source": "kalshi",
        "event_ticker": ticker,
        "series_ticker": _text(payload.get("series_ticker")),
        "title": payload.get("title"),
        "sub_title": payload.get("sub_title"),
        "category": payload.get("category"),
        "tags_json": _nested_json(payload.get("tags")),
        "mutually_exclusive": _bool_flag(payload.get("mutually_exclusive")),
        "strike_period": payload.get("strike_period"),
        "status": payload.get("status"),
        "created_time": payload.get("created_time"),
        "close_time": payload.get("close_time"),
        "last_updated_ts": payload.get("last_updated_ts"),
        "event_url": payload.get("event_url"),
        "rules_primary": payload.get("rules_primary"),
        "source_endpoint": endpoint,
        "raw_payload_json": _json_text(payload),
        "retrieved_at_utc": retrieved_at,
        "run_id": run_id,
    }


def _normalize_market(payload: dict[str, Any], *, endpoint: str, run_id: str, retrieved_at: str) -> dict[str, Any] | None:
    ticker = _text(payload.get("ticker"))
    market_id = _market_id(ticker)
    if market_id is None:
        return None
    return {
        "market_id": market_id,
        "source": "kalshi",
        "ticker": ticker,
        "event_ticker": _text(payload.get("event_ticker")),
        "series_ticker": _text(payload.get("series_ticker")),
        "title": payload.get("title"),
        "subtitle": payload.get("subtitle") or payload.get("sub_title"),
        "yes_sub_title": payload.get("yes_sub_title"),
        "no_sub_title": payload.get("no_sub_title"),
        "market_type": payload.get("market_type"),
        "status": payload.get("status"),
        "created_time": payload.get("created_time"),
        "updated_time": payload.get("updated_time") or payload.get("last_updated_ts"),
        "open_time": payload.get("open_time"),
        "close_time": payload.get("close_time"),
        "expected_expiration_time": payload.get("expected_expiration_time"),
        "expiration_time": payload.get("expiration_time"),
        "latest_expiration_time": payload.get("latest_expiration_time"),
        "settlement_ts": payload.get("settlement_ts"),
        "last_price_dollars": _float(payload.get("last_price_dollars")),
        "previous_price_dollars": _float(payload.get("previous_price_dollars")),
        "yes_bid_dollars": _float(payload.get("yes_bid_dollars")),
        "yes_ask_dollars": _float(payload.get("yes_ask_dollars")),
        "no_bid_dollars": _float(payload.get("no_bid_dollars")),
        "no_ask_dollars": _float(payload.get("no_ask_dollars")),
        "yes_bid_size_fp": _float(payload.get("yes_bid_size_fp")),
        "yes_ask_size_fp": _float(payload.get("yes_ask_size_fp")),
        "volume_fp": _float(payload.get("volume_fp")),
        "volume_24h_fp": _float(payload.get("volume_24h_fp")),
        "open_interest_fp": _float(payload.get("open_interest_fp")),
        "liquidity_dollars": _float(payload.get("liquidity_dollars")),
        "notional_value_dollars": _float(payload.get("notional_value_dollars")),
        "response_price_units": payload.get("response_price_units"),
        "price_level_structure": payload.get("price_level_structure"),
        "tick_size": _int(payload.get("tick_size")),
        "strike_type": payload.get("strike_type"),
        "floor_strike": _float(payload.get("floor_strike")),
        "cap_strike": _float(payload.get("cap_strike")),
        "functional_strike": payload.get("functional_strike"),
        "custom_strike_json": _nested_json(payload.get("custom_strike")),
        "mve_collection_ticker": payload.get("mve_collection_ticker"),
        "mve_selected_legs_json": _nested_json(payload.get("mve_selected_legs")),
        "rules_primary": payload.get("rules_primary"),
        "rules_secondary": payload.get("rules_secondary"),
        "can_close_early": _bool_flag(payload.get("can_close_early")),
        "early_close_condition": payload.get("early_close_condition"),
        "is_provisional": _bool_flag(payload.get("is_provisional")),
        "result": payload.get("result"),
        "settlement_value_dollars": _float(payload.get("settlement_value_dollars")),
        "source_endpoint": endpoint,
        "raw_payload_json": _json_text(payload),
        "retrieved_at_utc": retrieved_at,
        "run_id": run_id,
    }


class RawMetadataIngestor:
    """Ingest raw series, event, and market metadata into a daily SQLite database."""

    def __init__(self, conn: sqlite3.Connection, client: Any) -> None:
        self.conn = conn
        self.client = client
        ensure_schema(conn)

    def run_series_only(self, config: RawSeriesRunConfig | None = None) -> dict[str, Any]:
        """Download only series metadata; never request events or markets."""

        config = config or RawSeriesRunConfig()
        run_id = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        started_at = _now_utc()
        stats: dict[str, Any] = {
            "series_seen": 0,
            "series_written": 0,
            "series_selected": 0,
            "series_filtered": 0,
            "series_volume_unknown": 0,
            "invalid_records": 0,
        }
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO metadata_runs
                    (run_id, started_at_utc, status, base_url, source_mode, config_json)
                VALUES (?, ?, 'running', ?, 'series', ?)
                """,
                (run_id, started_at, getattr(self.client, "base_url", ""), _json_text(asdict(config))),
            )

        try:
            self._ingest_series_list(
                config=RawMetadataRunConfig(
                    page_limit=config.page_limit,
                    max_pages=config.max_pages,
                    source_mode="live",
                    show_progress=config.show_progress,
                    min_series_volume=0.0,
                ),
                run_id=run_id,
                stats=stats,
            )
            with self.conn:
                self.conn.execute(
                    "UPDATE metadata_runs SET finished_at_utc=?, status='success', stats_json=? WHERE run_id=?",
                    (_now_utc(), _json_text(stats), run_id),
                )
            return {"run_id": run_id, **stats}
        except Exception as exc:
            with self.conn:
                self.conn.execute(
                    "UPDATE metadata_runs SET finished_at_utc=?, status='failed', stats_json=?, error_text=? WHERE run_id=?",
                    (_now_utc(), _json_text(stats), repr(exc), run_id),
                )
            raise

    def run(self, config: RawMetadataRunConfig | None = None) -> dict[str, Any]:
        config = config or RawMetadataRunConfig()
        if config.source_mode not in {"live", "historical", "both"}:
            raise ValueError("source_mode must be one of: live, historical, both")
        if (config.completed_from_ts is None) != (config.completed_to_ts is None):
            raise ValueError("completed_from_ts and completed_to_ts must be supplied together")
        if config.completed_from_ts is not None and config.completed_to_ts <= config.completed_from_ts:
            raise ValueError("completed_to_ts must be after completed_from_ts")
        if config.min_series_volume < 0:
            raise ValueError("min_series_volume must be non-negative")

        run_id = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        started_at = _now_utc()
        stats: dict[str, Any] = {
            "series_seen": 0,
            "series_written": 0,
            "series_selected": 0,
            "series_filtered": 0,
            "series_volume_unknown": 0,
            "events_seen": 0,
            "events_written": 0,
            "event_details_fetched": 0,
            "markets_seen": 0,
            "markets_written": 0,
            "markets_filtered": 0,
            "markets_mve_filtered": 0,
            "invalid_records": 0,
            "event_errors": 0,
        }
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO metadata_runs
                    (run_id, started_at_utc, status, base_url, source_mode, config_json)
                VALUES (?, ?, 'running', ?, ?, ?)
                """,
                (run_id, started_at, getattr(self.client, "base_url", ""), config.source_mode, _json_text(asdict(config))),
            )

        event_tickers: set[str] = set()
        series_tickers: set[str] = set()
        try:
            selected_series_tickers = self._ingest_series_list(
                config=config,
                run_id=run_id,
                stats=stats,
            )
            if config.completed_from_ts is None:
                self._ingest_event_list(config=config, run_id=run_id, stats=stats, event_tickers=event_tickers)

            market_endpoints: list[str] = []
            if config.source_mode in {"live", "both"}:
                market_endpoints.append("/markets")
            if config.source_mode in {"historical", "both"}:
                market_endpoints.append("/historical/markets")

            for endpoint in market_endpoints:
                market_kind = "live" if endpoint == "/markets" else "historical"
                market_bar = _make_progress(
                    desc=f"Kalshi {market_kind} markets by series",
                    unit="market",
                    enabled=config.show_progress,
                )
                try:
                    for series_ticker in sorted(selected_series_tickers):
                        market_params: dict[str, Any] = {"series_ticker": series_ticker}
                        if endpoint == "/markets":
                            market_params["mve_filter"] = "exclude"
                            if config.completed_from_ts is not None:
                                market_params.update(
                                    {
                                        "status": "settled",
                                        "min_settled_ts": config.completed_from_ts,
                                        "max_settled_ts": config.completed_to_ts - 1,
                                    }
                                )
                            payloads: Iterable[dict[str, Any]] = self.client.iter_markets(
                                limit=config.page_limit,
                                max_pages=config.max_pages,
                                **market_params,
                            )
                        else:
                            payloads = self.client.iter_historical_markets(
                                limit=config.page_limit,
                                max_pages=config.max_pages,
                                **market_params,
                            )

                        for payload in payloads:
                            stats["markets_seen"] += 1
                            if endpoint == "/historical/markets" and not _is_non_mve_market(payload):
                                stats["markets_mve_filtered"] += 1
                                _update_progress(
                                    market_bar,
                                    stats["markets_seen"],
                                    stats["markets_written"],
                                    stats["invalid_records"],
                                    filtered=stats["markets_filtered"] + stats["markets_mve_filtered"],
                                )
                                continue
                            if not _is_in_completion_window(
                                payload,
                                from_ts=config.completed_from_ts,
                                to_ts=config.completed_to_ts,
                            ):
                                if config.completed_from_ts is not None:
                                    stats["markets_filtered"] += 1
                                _update_progress(
                                    market_bar,
                                    stats["markets_seen"],
                                    stats["markets_written"],
                                    stats["invalid_records"],
                                    filtered=stats["markets_filtered"] + stats["markets_mve_filtered"],
                                )
                                continue
                            ticker = _text(payload.get("ticker"))
                            event_ticker = _text(payload.get("event_ticker"))
                            market_series_ticker = _text(payload.get("series_ticker")) or series_ticker
                            if event_ticker:
                                event_tickers.add(event_ticker)
                            if market_series_ticker:
                                series_tickers.add(market_series_ticker)
                            wrote = self._write_payload_and_row(
                                entity_type="market",
                                entity_key=_entity_key(payload, ticker),
                                endpoint=endpoint,
                                payload=payload,
                                normalized=_normalize_market(payload, endpoint=endpoint, run_id=run_id, retrieved_at=_now_utc()),
                                table="raw_markets",
                                key_column="market_id",
                                run_id=run_id,
                            )
                            if wrote:
                                stats["markets_written"] += 1
                            else:
                                stats["invalid_records"] += 1
                            _update_progress(
                                market_bar,
                                stats["markets_seen"],
                                stats["markets_written"],
                                stats["invalid_records"],
                                filtered=stats["markets_filtered"] + stats["markets_mve_filtered"],
                            )
                finally:
                    _close_progress(market_bar)

            if config.fetch_event_details:
                stats["event_details_fetched"] += self._fetch_missing_event_details(
                    event_tickers=event_tickers,
                    run_id=run_id,
                    refresh_existing=config.refresh_event_details,
                    stats=stats,
                    show_progress=config.show_progress,
                )
            if config.completed_from_ts is not None:
                stats["series_details_fetched"] = self._fetch_missing_series_details(
                    series_tickers=series_tickers,
                    run_id=run_id,
                    refresh_existing=config.refresh_event_details,
                    stats=stats,
                    show_progress=config.show_progress,
                )

            finished_at = _now_utc()
            with self.conn:
                self.conn.execute(
                    "UPDATE metadata_runs SET finished_at_utc=?, status='success', stats_json=? WHERE run_id=?",
                    (finished_at, _json_text(stats), run_id),
                )
            return {"run_id": run_id, **stats}
        except Exception as exc:
            with self.conn:
                self.conn.execute(
                    "UPDATE metadata_runs SET finished_at_utc=?, status='failed', stats_json=?, error_text=? WHERE run_id=?",
                    (_now_utc(), _json_text(stats), repr(exc), run_id),
                )
            raise

    def run_manifest_group(self, config: ManifestGroupRunConfig) -> dict[str, Any]:
        """Download only markets/events whose series are in one frozen manifest group."""

        if config.selection_group not in MANIFEST_GROUPS:
            raise ValueError(f"selection_group must be one of: {', '.join(MANIFEST_GROUPS)}")
        if config.source_mode not in {"live", "historical", "both"}:
            raise ValueError("source_mode must be one of: live, historical, both")
        if (config.completed_from_ts is None) != (config.completed_to_ts is None):
            raise ValueError("completed_from_ts and completed_to_ts must be supplied together")
        if config.completed_from_ts is not None and config.completed_to_ts <= config.completed_from_ts:
            raise ValueError("completed_to_ts must be after completed_from_ts")

        selection = self.conn.execute(
            """
            SELECT selection_id, series_run_id, rule_sha256, membership_sha256, status
            FROM series_selection_runs
            WHERE selection_id = ?
            """,
            (config.selection_id,),
        ).fetchone()
        if selection is None:
            raise ValueError(f"Unknown series selection: {config.selection_id}")
        if selection[4] != "success":
            raise ValueError(f"Series selection {config.selection_id} is not successful: {selection[4]}")

        selected_series_tickers = {
            str(row[0])
            for row in self.conn.execute(
                """
                SELECT series_ticker
                FROM series_selection_members
                WHERE selection_id = ? AND eligible = 1 AND selection_group = ?
                ORDER BY series_ticker
                """,
                (config.selection_id, config.selection_group),
            )
        }
        if not selected_series_tickers:
            raise ValueError(
                f"Selection {config.selection_id} contains no eligible series in group {config.selection_group}"
            )

        run_id = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        started_at = _now_utc()
        stats: dict[str, Any] = {
            "selection_id": config.selection_id,
            "selection_group": config.selection_group,
            "series_run_id": selection[1],
            "series_targets": len(selected_series_tickers),
            "events_seen": 0,
            "events_written": 0,
            "event_details_fetched": 0,
            "markets_seen": 0,
            "markets_written": 0,
            "markets_filtered": 0,
            "markets_mve_filtered": 0,
            "invalid_records": 0,
            "event_errors": 0,
        }
        run_config = {
            "stage": "manifest_group",
            "selection_id": config.selection_id,
            "selection_group": config.selection_group,
            "series_run_id": selection[1],
            "selection_rule_sha256": selection[2],
            "selection_membership_sha256": selection[3],
            "config": asdict(config),
        }
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO metadata_runs
                    (run_id, started_at_utc, status, base_url, source_mode, config_json)
                VALUES (?, ?, 'running', ?, ?, ?)
                """,
                (run_id, started_at, getattr(self.client, "base_url", ""), config.source_mode, _json_text(run_config)),
            )

        event_tickers: set[str] = set()
        series_tickers: set[str] = set()
        market_config = RawMetadataRunConfig(
            page_limit=config.page_limit,
            max_pages=config.max_pages,
            source_mode=config.source_mode,
            fetch_event_details=config.fetch_event_details,
            refresh_event_details=config.refresh_event_details,
            show_progress=config.show_progress,
            completed_from_ts=config.completed_from_ts,
            completed_to_ts=config.completed_to_ts,
            min_series_volume=0.0,
        )
        try:
            self._ingest_markets_for_series(
                config=market_config,
                run_id=run_id,
                stats=stats,
                selected_series_tickers=selected_series_tickers,
                event_tickers=event_tickers,
                series_tickers=series_tickers,
            )
            if config.fetch_event_details:
                stats["event_details_fetched"] += self._fetch_missing_event_details(
                    event_tickers=event_tickers,
                    run_id=run_id,
                    refresh_existing=config.refresh_event_details,
                    stats=stats,
                    show_progress=config.show_progress,
                )

            with self.conn:
                self.conn.execute(
                    "UPDATE metadata_runs SET finished_at_utc=?, status='success', stats_json=? WHERE run_id=?",
                    (_now_utc(), _json_text(stats), run_id),
                )
            return {"run_id": run_id, **stats}
        except Exception as exc:
            with self.conn:
                self.conn.execute(
                    "UPDATE metadata_runs SET finished_at_utc=?, status='failed', stats_json=?, error_text=? WHERE run_id=?",
                    (_now_utc(), _json_text(stats), repr(exc), run_id),
                )
            raise

    def _ingest_markets_for_series(
        self,
        *,
        config: RawMetadataRunConfig,
        run_id: str,
        stats: dict[str, Any],
        selected_series_tickers: set[str],
        event_tickers: set[str],
        series_tickers: set[str],
    ) -> None:
        """Fetch market rows for an explicit set of series tickers."""

        market_endpoints: list[str] = []
        if config.source_mode in {"live", "both"}:
            market_endpoints.append("/markets")
        if config.source_mode in {"historical", "both"}:
            market_endpoints.append("/historical/markets")

        for endpoint in market_endpoints:
            market_kind = "live" if endpoint == "/markets" else "historical"
            market_bar = _make_progress(
                desc=f"Kalshi {market_kind} markets by series",
                unit="market",
                enabled=config.show_progress,
            )
            try:
                for series_ticker in sorted(selected_series_tickers):
                    market_params: dict[str, Any] = {"series_ticker": series_ticker}
                    if endpoint == "/markets":
                        market_params["mve_filter"] = "exclude"
                        if config.completed_from_ts is not None:
                            market_params.update(
                                {
                                    "status": "settled",
                                    "min_settled_ts": config.completed_from_ts,
                                    "max_settled_ts": config.completed_to_ts - 1,
                                }
                            )
                        payloads: Iterable[dict[str, Any]] = self.client.iter_markets(
                            limit=config.page_limit,
                            max_pages=config.max_pages,
                            **market_params,
                        )
                    else:
                        payloads = self.client.iter_historical_markets(
                            limit=config.page_limit,
                            max_pages=config.max_pages,
                            **market_params,
                        )

                    for payload in payloads:
                        stats["markets_seen"] += 1
                        if endpoint == "/historical/markets" and not _is_non_mve_market(payload):
                            stats["markets_mve_filtered"] += 1
                            _update_progress(
                                market_bar,
                                stats["markets_seen"],
                                stats["markets_written"],
                                stats["invalid_records"],
                                filtered=stats["markets_filtered"] + stats["markets_mve_filtered"],
                            )
                            continue
                        if not _is_in_completion_window(
                            payload,
                            from_ts=config.completed_from_ts,
                            to_ts=config.completed_to_ts,
                        ):
                            if config.completed_from_ts is not None:
                                stats["markets_filtered"] += 1
                            _update_progress(
                                market_bar,
                                stats["markets_seen"],
                                stats["markets_written"],
                                stats["invalid_records"],
                                filtered=stats["markets_filtered"] + stats["markets_mve_filtered"],
                            )
                            continue
                        ticker = _text(payload.get("ticker"))
                        event_ticker = _text(payload.get("event_ticker"))
                        market_series_ticker = _text(payload.get("series_ticker")) or series_ticker
                        if event_ticker:
                            event_tickers.add(event_ticker)
                        if market_series_ticker:
                            series_tickers.add(market_series_ticker)
                        wrote = self._write_payload_and_row(
                            entity_type="market",
                            entity_key=_entity_key(payload, ticker),
                            endpoint=endpoint,
                            payload=payload,
                            normalized=_normalize_market(
                                payload,
                                endpoint=endpoint,
                                run_id=run_id,
                                retrieved_at=_now_utc(),
                            ),
                            table="raw_markets",
                            key_column="market_id",
                            run_id=run_id,
                        )
                        if wrote:
                            stats["markets_written"] += 1
                        else:
                            stats["invalid_records"] += 1
                        _update_progress(
                            market_bar,
                            stats["markets_seen"],
                            stats["markets_written"],
                            stats["invalid_records"],
                            filtered=stats["markets_filtered"] + stats["markets_mve_filtered"],
                        )
            finally:
                _close_progress(market_bar)

    def _ingest_series_list(
        self,
        *,
        config: RawMetadataRunConfig,
        run_id: str,
        stats: dict[str, Any],
    ) -> set[str]:
        selected_series_tickers: set[str] = set()
        series_bar = _make_progress(desc="Kalshi series", unit="series", enabled=config.show_progress)
        try:
            for payload in self.client.iter_series(
                limit=config.page_limit,
                max_pages=config.max_pages,
                include_volume=True,
            ):
                stats["series_seen"] += 1
                ticker = _text(payload.get("ticker"))
                wrote = self._write_payload_and_row(
                    entity_type="series",
                    entity_key=_entity_key(payload, ticker),
                    endpoint="/series",
                    payload=payload,
                    normalized=_normalize_series(payload, endpoint="/series", run_id=run_id, retrieved_at=_now_utc()),
                    table="raw_series",
                    key_column="series_ticker",
                    run_id=run_id,
                )
                if wrote:
                    stats["series_written"] += 1
                else:
                    stats["invalid_records"] += 1
                volume = _float(payload.get("volume_fp"))
                if volume is None:
                    stats["series_volume_unknown"] += 1
                if ticker and (volume is None or volume >= config.min_series_volume):
                    selected_series_tickers.add(ticker)
                    stats["series_selected"] = len(selected_series_tickers)
                elif ticker:
                    stats["series_filtered"] += 1
                _update_progress(
                    series_bar,
                    stats["series_seen"],
                    stats["series_written"],
                    stats["invalid_records"],
                    filtered=stats["series_filtered"],
                    selected=stats["series_selected"],
                )
        finally:
            _close_progress(series_bar)
        return selected_series_tickers

    def _ingest_event_list(
        self,
        *,
        config: RawMetadataRunConfig,
        run_id: str,
        stats: dict[str, Any],
        event_tickers: set[str],
    ) -> None:
        events_bar = _make_progress(desc="Kalshi events", unit="event", enabled=config.show_progress)
        try:
            for payload in self.client.iter_events(limit=config.page_limit, max_pages=config.max_pages, with_nested_markets=False):
                stats["events_seen"] += 1
                ticker = _text(payload.get("event_ticker") or payload.get("ticker"))
                if ticker:
                    event_tickers.add(ticker)
                wrote = self._write_payload_and_row(
                    entity_type="event",
                    entity_key=_entity_key(payload, ticker),
                    endpoint="/events",
                    payload=payload,
                    normalized=_normalize_event(payload, endpoint="/events", run_id=run_id, retrieved_at=_now_utc()),
                    table="raw_events",
                    key_column="event_id",
                    run_id=run_id,
                )
                if wrote:
                    stats["events_written"] += 1
                else:
                    stats["invalid_records"] += 1
                _update_progress(events_bar, stats["events_seen"], stats["events_written"], stats["invalid_records"])
        finally:
            _close_progress(events_bar)

    def _fetch_missing_event_details(
        self,
        *,
        event_tickers: set[str],
        run_id: str,
        refresh_existing: bool,
        stats: dict[str, Any],
        show_progress: bool,
    ) -> int:
        existing = set()
        if not refresh_existing:
            existing = {
                str(row[0])
                for row in self.conn.execute("SELECT event_ticker FROM raw_events WHERE event_ticker IS NOT NULL")
                if row[0]
            }
        fetched = 0
        processed = 0
        missing_tickers = sorted(event_tickers - existing)
        detail_bar = _make_progress(
            desc="Kalshi event details",
            unit="event",
            total=len(missing_tickers),
            enabled=show_progress,
        )
        try:
            for ticker in missing_tickers:
                try:
                    payload_wrapper = self.client.get_event(ticker)
                    payload = payload_wrapper.get("event") if isinstance(payload_wrapper, dict) else None
                    if not isinstance(payload, dict):
                        processed += 1
                        _update_progress(detail_bar, processed, stats["events_written"], stats["event_errors"])
                        continue
                    stats["events_seen"] += 1
                    wrote = self._write_payload_and_row(
                        entity_type="event",
                        entity_key=_entity_key(payload, ticker),
                        endpoint=f"/events/{ticker}",
                        payload=payload,
                        normalized=_normalize_event(payload, endpoint=f"/events/{ticker}", run_id=run_id, retrieved_at=_now_utc()),
                        table="raw_events",
                        key_column="event_id",
                        run_id=run_id,
                    )
                    if wrote:
                        stats["events_written"] += 1
                    else:
                        stats["invalid_records"] += 1
                    fetched += 1
                except Exception:  # noqa: BLE001
                    stats["event_errors"] += 1
                    logger.exception("Failed to fetch Kalshi event metadata | event_ticker=%s", ticker)
                processed += 1
                _update_progress(detail_bar, processed, stats["events_written"], stats["event_errors"])
        finally:
            _close_progress(detail_bar)
        return fetched

    def _fetch_missing_series_details(
        self,
        *,
        series_tickers: set[str],
        run_id: str,
        refresh_existing: bool,
        stats: dict[str, Any],
        show_progress: bool,
    ) -> int:
        existing = set()
        if not refresh_existing:
            existing = {
                str(row[0])
                for row in self.conn.execute("SELECT series_ticker FROM raw_series WHERE series_ticker IS NOT NULL")
                if row[0]
            }
        missing_tickers = sorted(series_tickers - existing)
        if not missing_tickers:
            return 0
        fetched = 0
        series_bar = _make_progress(
            desc="Kalshi series details",
            unit="series",
            total=len(missing_tickers),
            enabled=show_progress,
        )
        try:
            for ticker in missing_tickers:
                try:
                    payload_wrapper = self.client.get_series(ticker)
                    payload = payload_wrapper.get("series") if isinstance(payload_wrapper, dict) else None
                    if isinstance(payload, dict):
                        stats["series_seen"] += 1
                        wrote = self._write_payload_and_row(
                            entity_type="series",
                            entity_key=_entity_key(payload, ticker),
                            endpoint=f"/series/{ticker}",
                            payload=payload,
                            normalized=_normalize_series(payload, endpoint=f"/series/{ticker}", run_id=run_id, retrieved_at=_now_utc()),
                            table="raw_series",
                            key_column="series_ticker",
                            run_id=run_id,
                        )
                        if wrote:
                            stats["series_written"] += 1
                        else:
                            stats["invalid_records"] += 1
                        fetched += 1
                except Exception:  # noqa: BLE001
                    stats["series_errors"] = stats.get("series_errors", 0) + 1
                    logger.exception("Failed to fetch Kalshi series metadata | series_ticker=%s", ticker)
                _update_progress(series_bar, fetched, stats["series_written"], stats.get("series_errors", 0))
        finally:
            _close_progress(series_bar)
        return fetched

    def _write_payload_and_row(
        self,
        *,
        entity_type: str,
        entity_key: str,
        endpoint: str,
        payload: dict[str, Any],
        normalized: dict[str, Any] | None,
        table: str,
        key_column: str,
        run_id: str,
    ) -> bool:
        payload_json = _json_text(payload)
        retrieved_at = _now_utc()
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        with self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO raw_payloads
                    (run_id, entity_type, entity_key, source_endpoint, retrieved_at_utc, payload_sha256, payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, entity_type, entity_key, endpoint, retrieved_at, payload_hash, payload_json),
            )
            if normalized is None:
                return False
            columns = list(normalized)
            placeholders = ", ".join("?" for _ in columns)
            updates = ", ".join(
                f"{column}=excluded.{column}"
                for column in columns
                if column != key_column
            )
            self.conn.execute(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
                f"ON CONFLICT({key_column}) DO UPDATE SET {updates}",
                tuple(normalized[column] for column in columns),
            )
        return True


def _make_progress(*, desc: str, unit: str, enabled: bool, total: int | None = None) -> Any:
    if not enabled or tqdm is None or (total is not None and total <= 0):
        return None
    return tqdm(total=total, desc=desc, unit=unit, dynamic_ncols=True, leave=True)


def _update_progress(
    bar: Any,
    seen: int,
    written: int,
    errors: int,
    *,
    filtered: int = 0,
    selected: int = 0,
) -> None:
    if bar is None:
        return
    bar.n = int(seen)
    postfix = {"written": int(written), "errors": int(errors)}
    if filtered:
        postfix["filtered"] = int(filtered)
    if selected:
        postfix["selected"] = int(selected)
    bar.set_postfix(**postfix, refresh=False)
    bar.refresh()


def _close_progress(bar: Any) -> None:
    if bar is not None:
        bar.close()
