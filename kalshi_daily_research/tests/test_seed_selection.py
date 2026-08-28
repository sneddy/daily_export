from __future__ import annotations

import json
import sqlite3

from kalshi_daily_research.schema import ensure_schema
from kalshi_daily_research.scripts.seed_selection import seed_selection


def test_seed_selection_copies_only_frozen_series_inputs(tmp_path) -> None:
    source_path = tmp_path / "source.sqlite"
    target_path = tmp_path / "historical.sqlite"
    selection_id = "selection-1"
    series_run_id = "series-run"

    with sqlite3.connect(source_path) as conn:
        ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO metadata_runs
                (run_id, started_at_utc, status, base_url, source_mode, config_json)
            VALUES (?, '2026-08-20T00:00:00Z', 'success', 'https://example.test', 'series', '{}')
            """,
            (series_run_id,),
        )
        conn.execute(
            """
            INSERT INTO raw_payloads
                (run_id, entity_type, entity_key, source_endpoint,
                 retrieved_at_utc, payload_sha256, payload_json)
            VALUES (?, 'series', 'SERIES-1', '/series',
                    '2026-08-20T00:00:00Z', 'hash', ?)
            """,
            (series_run_id, json.dumps({"ticker": "SERIES-1"})),
        )
        conn.execute(
            """
            INSERT INTO raw_series
                (series_ticker, source, title, category, frequency,
                 source_endpoint, raw_payload_json, retrieved_at_utc, run_id)
            VALUES ('SERIES-1', 'kalshi', 'Example', 'Other', 'custom',
                    '/series', '{}', '2026-08-20T00:00:00Z', ?)
            """,
            (series_run_id,),
        )
        conn.execute(
            """
            INSERT INTO series_selection_runs
                (selection_id, created_at_utc, series_run_id, series_started_at_utc,
                 rule_json, rule_sha256, membership_sha256, status, stats_json)
            VALUES (?, '2026-08-20T00:00:00Z', ?, '2026-08-20T00:00:00Z',
                    '{}', 'rule', 'membership', 'success', '{}')
            """,
            (selection_id, series_run_id),
        )
        conn.execute(
            """
            INSERT INTO series_selection_members
                (selection_id, series_ticker, title, category, category_norm,
                 frequency_group, volume_known, eligible, selection_group)
            VALUES (?, 'SERIES-1', 'Example', 'Other', 'other',
                    'rest', 1, 1, 'non_sport_crypto')
            """,
            (selection_id,),
        )
        conn.execute(
            """
            INSERT INTO raw_markets
                (market_id, source, ticker, source_endpoint, raw_payload_json,
                 retrieved_at_utc, run_id)
            VALUES ('kalshi:MARKET-1', 'kalshi', 'MARKET-1', '/markets', '{}',
                    '2026-08-20T00:00:00Z', ?)
            """,
            (series_run_id,),
        )

    result = seed_selection(
        source_db_path=source_path,
        target_db_path=target_path,
        selection_id=selection_id,
    )

    assert result["already_seeded"] is False
    with sqlite3.connect(target_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM raw_series").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM series_selection_members").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM raw_markets").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM raw_daily_candles").fetchone()[0] == 0

    second = seed_selection(
        source_db_path=source_path,
        target_db_path=target_path,
        selection_id=selection_id,
    )
    assert second["already_seeded"] is True
