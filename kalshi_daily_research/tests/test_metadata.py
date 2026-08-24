from __future__ import annotations

import json
import sqlite3

from kalshi_daily_research.ingest import RawMetadataIngestor, RawMetadataRunConfig, RawSeriesRunConfig
from kalshi_daily_research.schema import ensure_schema


class FakeClient:
    base_url = "https://example.test/trade-api/v2/"

    def iter_series(self, *, limit: int, max_pages: int | None, include_volume: bool):
        yield {
            "ticker": "SERIES-1",
            "title": "Example series",
            "category": "Science",
            "frequency": "daily",
            "volume_fp": "50000.00",
            "tags": ["example"],
        }

    def iter_events(self, *, limit: int, max_pages: int, with_nested_markets: bool):
        yield {
            "event_ticker": "EVENT-1",
            "series_ticker": "SERIES-1",
            "title": "Example event",
            "category": "Science",
        }

    def iter_markets(self, *, limit: int, max_pages: int | None, **params):
        yield {
            "ticker": "MARKET-1",
            "event_ticker": "EVENT-1",
            "series_ticker": "SERIES-1",
            "title": "Will the event happen?",
            "market_type": "binary",
            "volume_fp": "123.45",
            "result": "yes",
        }

    def iter_historical_markets(self, *, limit: int, max_pages: int | None, **params):
        if False:
            yield {}

    def get_event(self, event_ticker: str):
        raise AssertionError("Existing event should not require a detail request")

    def get_series(self, series_ticker: str):
        return {"series": {"ticker": series_ticker, "title": "Example series detail"}}


def test_raw_metadata_ingestion_is_idempotent_for_current_tables() -> None:
    conn = sqlite3.connect(":memory:")
    ensure_schema(conn)

    first = RawMetadataIngestor(conn, FakeClient()).run(
        RawMetadataRunConfig(source_mode="live", fetch_event_details=True, show_progress=False)
    )
    second = RawMetadataIngestor(conn, FakeClient()).run(
        RawMetadataRunConfig(source_mode="live", fetch_event_details=True, show_progress=False)
    )

    assert first["series_written"] == 1
    assert first["events_written"] == 1
    assert first["markets_written"] == 1
    assert second["markets_written"] == 1
    assert conn.execute("SELECT COUNT(*) FROM raw_series").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM raw_markets").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == 6

    market_row = conn.execute(
        "SELECT market_id, ticker, volume_fp, raw_payload_json FROM raw_markets"
    ).fetchone()
    assert market_row[:3] == ("kalshi:MARKET-1", "MARKET-1", 123.45)
    assert json.loads(market_row[3])["market_type"] == "binary"


class CompletedFakeClient(FakeClient):
    def iter_markets(self, *, limit: int, max_pages: int | None, **params):
        yield {
            "ticker": "MARKET-IN-MONTH",
            "event_ticker": "EVENT-IN-MONTH",
            "series_ticker": "SERIES-IN-MONTH",
            "title": "Settled this month",
            "market_type": "binary",
            "settlement_ts": "2026-08-10T12:00:00Z",
        }

    def iter_historical_markets(self, *, limit: int, max_pages: int | None, **params):
        yield {
            "ticker": "MARKET-OUTSIDE-MONTH",
            "event_ticker": "EVENT-OUTSIDE-MONTH",
            "series_ticker": "SERIES-OUTSIDE-MONTH",
            "title": "Settled outside this month",
            "market_type": "binary",
            "settlement_ts": "2026-07-31T12:00:00Z",
        }


def test_completed_month_filter_is_applied_to_market_rows() -> None:
    conn = sqlite3.connect(":memory:")
    result = RawMetadataIngestor(conn, CompletedFakeClient()).run(
        RawMetadataRunConfig(
            source_mode="both",
            fetch_event_details=False,
            show_progress=False,
            completed_from_ts=1785592086,
            completed_to_ts=1788270486,
        )
    )
    assert result["markets_seen"] == 2
    assert result["markets_written"] == 1
    assert result["markets_filtered"] == 1
    assert conn.execute("SELECT COUNT(*) FROM raw_markets").fetchone()[0] == 1
    assert conn.execute("SELECT ticker FROM raw_markets").fetchone()[0] == "MARKET-IN-MONTH"
    assert conn.execute(
        "SELECT series_ticker FROM raw_series WHERE series_ticker = 'SERIES-IN-MONTH'"
    ).fetchone()[0] == "SERIES-IN-MONTH"


class SeriesVolumeClient(FakeClient):
    def __init__(self) -> None:
        self.market_series: list[str] = []

    def iter_series(self, *, limit: int, max_pages: int | None, include_volume: bool):
        yield {"ticker": "SERIES-HIGH", "title": "High volume", "volume_fp": "50000"}
        yield {"ticker": "SERIES-LOW", "title": "Low volume", "volume_fp": "10"}

    def iter_markets(self, *, limit: int, max_pages: int | None, **params):
        self.market_series.append(str(params["series_ticker"]))
        yield {
            "ticker": f"MARKET-{params['series_ticker']}",
            "series_ticker": params["series_ticker"],
            "market_type": "binary",
            "volume_fp": "50000",
        }


def test_series_volume_filter_limits_market_scans() -> None:
    conn = sqlite3.connect(":memory:")
    client = SeriesVolumeClient()
    result = RawMetadataIngestor(conn, client).run(
        RawMetadataRunConfig(source_mode="live", fetch_event_details=False, show_progress=False)
    )
    assert result["series_seen"] == 2
    assert result["series_selected"] == 1
    assert result["series_filtered"] == 1
    assert client.market_series == ["SERIES-HIGH"]
    assert conn.execute("SELECT COUNT(*) FROM raw_series").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM raw_markets").fetchone()[0] == 1


def test_series_only_ingestion_does_not_download_events_or_markets() -> None:
    conn = sqlite3.connect(":memory:")
    result = RawMetadataIngestor(conn, FakeClient()).run_series_only(
        RawSeriesRunConfig(show_progress=False)
    )
    assert result["series_seen"] == 1
    assert result["series_written"] == 1
    assert conn.execute("SELECT COUNT(*) FROM raw_series").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM raw_markets").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM raw_payloads WHERE entity_type='event'").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM raw_payloads WHERE entity_type='market'").fetchone()[0] == 0
