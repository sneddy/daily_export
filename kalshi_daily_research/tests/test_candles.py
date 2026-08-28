from __future__ import annotations

import json
import sqlite3

from kalshi_daily_research.candles import DailyCandleIngestor, DailyCandleRunConfig
from kalshi_daily_research.schema import ensure_schema


SELECTION_ID = "selection-1"
SERIES_RUN_ID = "series-run"
METADATA_RUN_ID = "metadata-run"


class FakeCandleClient:
    base_url = "https://example.test/trade-api/v2/"

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def get_batch_market_candlesticks(self, **params):
        self.calls.append(params)
        return {
            "markets": [
                {
                    "market_ticker": ticker,
                    "candlesticks": [
                        {
                            "end_period_ts": 1_785_888_000,
                            "price": {
                                "open_dollars": "0.40",
                                "low_dollars": "0.30",
                                "high_dollars": "0.50",
                                "close_dollars": "0.45",
                                "mean_dollars": "0.41",
                                "previous_dollars": "0.39",
                                "min_dollars": "0.30",
                                "max_dollars": "0.50",
                            },
                            "volume_fp": "12.00",
                            "open_interest_fp": "8.00",
                        }
                    ],
                }
                for ticker in params["market_tickers"]
            ]
        }


class FailingCandleClient(FakeCandleClient):
    def get_batch_market_candlesticks(self, **params):
        self.calls.append(params)
        raise RuntimeError("HTTPError('400 Client Error: Bad Request')")


class FakeHistoricalCandleClient(FakeCandleClient):
    def get_historical_market_candlesticks(self, **params):
        self.calls.append(params)
        ticker = params["market_ticker"]
        return {
            "ticker": ticker,
            "candlesticks": [
                {
                    "end_period_ts": 1_785_888_000,
                    "price": {
                        "open": "0.40",
                        "low": "0.30",
                        "high": "0.50",
                        "close": "0.45",
                        "mean": "0.41",
                        "previous": "0.39",
                    },
                    "yes_bid": {"open": "0.30", "low": "0.30", "high": "0.30", "close": "0.30"},
                    "yes_ask": {"open": "0.50", "low": "0.50", "high": "0.50", "close": "0.50"},
                    "volume": "12.00",
                    "open_interest": "8.00",
                }
            ],
        }


def _seed_database(conn: sqlite3.Connection) -> None:
    ensure_schema(conn)
    conn.executemany(
        """
        INSERT INTO metadata_runs
            (run_id, started_at_utc, status, base_url, source_mode, config_json)
        VALUES (?, '2026-08-20T00:00:00Z', 'success', ?, ?, ?)
        """,
        [
            (SERIES_RUN_ID, "https://example.test", "series", "{}"),
            (
                METADATA_RUN_ID,
                "https://example.test",
                "live",
                json.dumps({"selection_group": "non_sport_crypto"}),
            ),
        ],
    )
    conn.execute(
        """
        INSERT INTO series_selection_runs
            (selection_id, created_at_utc, series_run_id, series_started_at_utc,
             rule_json, rule_sha256, membership_sha256, status, stats_json)
        VALUES (?, '2026-08-20T00:00:00Z', ?, '2026-08-20T00:00:00Z', '{}', 'r', 'm', 'success', '{}')
        """,
        (SELECTION_ID, SERIES_RUN_ID),
    )
    conn.execute(
        """
        INSERT INTO series_selection_members
            (selection_id, series_ticker, title, category, category_norm,
             frequency_group, volume_fp, volume_known, eligible, selection_group)
        VALUES (?, 'SERIES-1', 'Example', 'Other', 'other', 'other', 50000, 1, 1, 'non_sport_crypto')
        """,
        (SELECTION_ID,),
    )
    rows = [
        ("MARKET-BOTH", 20000, "2026-08-01T00:00:00Z", "2026-08-11T00:00:00Z"),
        ("MARKET-LIFETIME", 100, "2026-08-01T00:00:00Z", "2026-08-11T00:00:00Z"),
        ("MARKET-VOLUME", 20000, "2026-08-03T00:00:00Z", "2026-08-07T00:00:00Z"),
    ]
    conn.executemany(
        """
        INSERT INTO raw_markets
            (market_id, source, ticker, event_ticker, series_ticker, volume_fp,
             open_time, close_time, source_endpoint, raw_payload_json,
             retrieved_at_utc, run_id)
        VALUES (?, 'kalshi', ?, ?, 'SERIES-1', ?, ?, ?, '/markets', '{}',
                '2026-08-20T00:00:00Z', ?)
        """,
        [
            (f"kalshi:{ticker}", ticker, f"EVENT-{ticker}", volume, opened, closed, METADATA_RUN_ID)
            for ticker, volume, opened, closed in rows
        ],
    )


def test_union_filter_records_both_filter_flags_and_writes_daily_rows() -> None:
    conn = sqlite3.connect(":memory:")
    _seed_database(conn)
    client = FakeCandleClient()

    result = DailyCandleIngestor(conn, client).run(
        DailyCandleRunConfig(
            selection_id=SELECTION_ID,
            selection_group="non_sport_crypto",
            filter_mode="union",
            min_volume_fp=10_000,
            min_lifetime_days=5,
            show_progress=False,
        )
    )

    assert result["candidate_markets"] == 3
    assert result["volume_filter_markets"] == 2
    assert result["lifetime_filter_markets"] == 2
    assert result["both_filter_markets"] == 1
    assert result["candles_written"] == 3
    assert len(client.calls) == 1
    assert conn.execute("SELECT COUNT(*) FROM raw_daily_candles").fetchone()[0] == 3
    flags = conn.execute(
        """
        SELECT market_ticker, passes_volume_filter, passes_lifetime_filter, passes_both_filters
        FROM market_history_manifest
        ORDER BY market_ticker
        """
    ).fetchall()
    assert flags == [
        ("MARKET-BOTH", 1, 1, 1),
        ("MARKET-LIFETIME", 0, 1, 0),
        ("MARKET-VOLUME", 1, 0, 0),
    ]


def test_both_filter_mode_downloads_only_intersection() -> None:
    conn = sqlite3.connect(":memory:")
    _seed_database(conn)
    client = FakeCandleClient()

    result = DailyCandleIngestor(conn, client).run(
        DailyCandleRunConfig(
            selection_id=SELECTION_ID,
            selection_group="non_sport_crypto",
            filter_mode="both",
            show_progress=False,
        )
    )

    assert result["candidate_markets"] == 1
    assert client.calls[0]["market_tickers"] == ["MARKET-BOTH"]


def test_historical_mode_uses_single_market_legacy_payload_and_source_provenance() -> None:
    conn = sqlite3.connect(":memory:")
    _seed_database(conn)
    conn.execute(
        """
        INSERT INTO metadata_runs
            (run_id, started_at_utc, status, base_url, source_mode, config_json)
        VALUES ('historical-metadata-run', '2026-08-20T01:00:00Z', 'success',
                'https://example.test', 'historical', '{}')
        """
    )
    conn.execute(
        "UPDATE raw_markets SET run_id='historical-metadata-run'"
    )
    client = FakeHistoricalCandleClient()

    result = DailyCandleIngestor(conn, client).run(
        DailyCandleRunConfig(
            selection_id=SELECTION_ID,
            selection_group="non_sport_crypto",
            source_mode="historical",
            filter_mode="union",
            max_markets=2,
            max_workers=2,
            show_progress=False,
        )
    )

    assert result["candidate_markets"] == 2
    assert result["requests"] == 2
    assert result["candles_written"] == 2
    assert {call["market_ticker"] for call in client.calls} == {
        "MARKET-BOTH",
        "MARKET-LIFETIME",
    }
    candle = conn.execute(
        """
        SELECT source_mode, source_endpoint, price_mean, volume, open_interest
        FROM raw_daily_candles
        ORDER BY market_ticker
        LIMIT 1
        """
    ).fetchone()
    assert candle == (
        "historical",
        "/historical/markets/MARKET-BOTH/candlesticks",
        0.41,
        12.0,
        8.0,
    )
    run_source = conn.execute(
        "SELECT source_mode FROM metadata_runs WHERE run_id=?",
        (result["run_id"],),
    ).fetchone()[0]
    assert run_source == "historical"
    assert conn.execute(
        "SELECT COUNT(*) FROM raw_payloads WHERE run_id=? AND entity_type='daily_candles'",
        (result["run_id"],),
    ).fetchone()[0] == 2


def test_retry_status_uses_latest_run_and_isolates_tickers() -> None:
    conn = sqlite3.connect(":memory:")
    _seed_database(conn)

    failed_client = FailingCandleClient()
    failed_result = DailyCandleIngestor(conn, failed_client).run(
        DailyCandleRunConfig(
            selection_id=SELECTION_ID,
            selection_group="non_sport_crypto",
            filter_mode="union",
            show_progress=False,
        )
    )

    parent_run_id = failed_result["run_id"]
    assert failed_result["api_errors"] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM market_history_manifest WHERE run_id=? AND status='api_error'",
        (parent_run_id,),
    ).fetchone()[0] == 3

    retry_client = FakeCandleClient()
    retry_result = DailyCandleIngestor(conn, retry_client).run(
        DailyCandleRunConfig(
            selection_id=SELECTION_ID,
            selection_group="non_sport_crypto",
            filter_mode="union",
            retry_status="api_error",
            max_tickers_per_batch=1,
            show_progress=False,
        )
    )

    assert retry_result["retry_parent_run_id"] == parent_run_id
    assert retry_result["candidate_markets"] == 3
    assert retry_result["api_errors"] == 0
    assert len(retry_client.calls) == 3
    assert all(len(call["market_tickers"]) == 1 for call in retry_client.calls)

    retry_run_id = retry_result["run_id"]
    assert conn.execute(
        "SELECT COUNT(*) FROM market_history_manifest WHERE run_id=?",
        (retry_run_id,),
    ).fetchone()[0] == 3
    assert conn.execute(
        "SELECT COUNT(*) FROM market_history_manifest WHERE run_id=? AND status='success'",
        (retry_run_id,),
    ).fetchone()[0] == 3
    config_json = conn.execute(
        "SELECT config_json FROM metadata_runs WHERE run_id=?",
        (retry_run_id,),
    ).fetchone()[0]
    assert json.loads(config_json)["retry_parent_run_id"] == parent_run_id


def test_retry_status_continues_from_unprocessed_rows_in_latest_retry_run() -> None:
    conn = sqlite3.connect(":memory:")
    _seed_database(conn)

    failed_client = FailingCandleClient()
    DailyCandleIngestor(conn, failed_client).run(
        DailyCandleRunConfig(
            selection_id=SELECTION_ID,
            selection_group="non_sport_crypto",
            filter_mode="union",
            show_progress=False,
        )
    )

    partial_retry = DailyCandleIngestor(conn, FakeCandleClient()).run(
        DailyCandleRunConfig(
            selection_id=SELECTION_ID,
            selection_group="non_sport_crypto",
            filter_mode="union",
            retry_status="api_error",
            max_tickers_per_batch=1,
            max_batches=1,
            show_progress=False,
        )
    )
    assert partial_retry["candidate_markets"] == 3

    resumed_client = FakeCandleClient()
    resumed = DailyCandleIngestor(conn, resumed_client).run(
        DailyCandleRunConfig(
            selection_id=SELECTION_ID,
            selection_group="non_sport_crypto",
            filter_mode="union",
            retry_status="api_error",
            max_tickers_per_batch=1,
            show_progress=False,
        )
    )

    assert resumed["candidate_markets"] == 2
    assert len(resumed_client.calls) == 2
    assert all(len(call["market_tickers"]) == 1 for call in resumed_client.calls)
