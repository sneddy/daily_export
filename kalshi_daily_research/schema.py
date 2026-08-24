"""SQLite schema for the source-preserving Kalshi daily metadata layer."""

from __future__ import annotations

import sqlite3


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create the daily metadata tables without touching the legacy database."""

    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata_runs (
            run_id TEXT PRIMARY KEY,
            started_at_utc TEXT NOT NULL,
            finished_at_utc TEXT,
            status TEXT NOT NULL,
            base_url TEXT NOT NULL,
            source_mode TEXT NOT NULL,
            config_json TEXT NOT NULL,
            stats_json TEXT,
            error_text TEXT
        );

        CREATE TABLE IF NOT EXISTS raw_payloads (
            payload_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL REFERENCES metadata_runs(run_id),
            entity_type TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            source_endpoint TEXT NOT NULL,
            retrieved_at_utc TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            UNIQUE(run_id, entity_type, entity_key, source_endpoint)
        );

        CREATE TABLE IF NOT EXISTS raw_series (
            series_ticker TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            title TEXT,
            subtitle TEXT,
            category TEXT,
            tags_json TEXT,
            frequency TEXT,
            status TEXT,
            created_time TEXT,
            updated_time TEXT,
            close_time TEXT,
            settlement_time TEXT,
            source_endpoint TEXT NOT NULL,
            raw_payload_json TEXT NOT NULL,
            retrieved_at_utc TEXT NOT NULL,
            run_id TEXT NOT NULL REFERENCES metadata_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS raw_events (
            event_id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            event_ticker TEXT,
            series_ticker TEXT,
            title TEXT,
            sub_title TEXT,
            category TEXT,
            tags_json TEXT,
            mutually_exclusive INTEGER,
            strike_period TEXT,
            status TEXT,
            created_time TEXT,
            close_time TEXT,
            last_updated_ts TEXT,
            event_url TEXT,
            rules_primary TEXT,
            source_endpoint TEXT NOT NULL,
            raw_payload_json TEXT NOT NULL,
            retrieved_at_utc TEXT NOT NULL,
            run_id TEXT NOT NULL REFERENCES metadata_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS raw_markets (
            market_id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            ticker TEXT,
            event_ticker TEXT,
            series_ticker TEXT,
            title TEXT,
            subtitle TEXT,
            yes_sub_title TEXT,
            no_sub_title TEXT,
            market_type TEXT,
            status TEXT,
            created_time TEXT,
            updated_time TEXT,
            open_time TEXT,
            close_time TEXT,
            expected_expiration_time TEXT,
            expiration_time TEXT,
            latest_expiration_time TEXT,
            settlement_ts TEXT,
            last_price_dollars REAL,
            previous_price_dollars REAL,
            yes_bid_dollars REAL,
            yes_ask_dollars REAL,
            no_bid_dollars REAL,
            no_ask_dollars REAL,
            yes_bid_size_fp REAL,
            yes_ask_size_fp REAL,
            volume_fp REAL,
            volume_24h_fp REAL,
            open_interest_fp REAL,
            liquidity_dollars REAL,
            notional_value_dollars REAL,
            response_price_units TEXT,
            price_level_structure TEXT,
            tick_size INTEGER,
            strike_type TEXT,
            floor_strike REAL,
            cap_strike REAL,
            functional_strike TEXT,
            custom_strike_json TEXT,
            mve_collection_ticker TEXT,
            mve_selected_legs_json TEXT,
            rules_primary TEXT,
            rules_secondary TEXT,
            can_close_early INTEGER,
            early_close_condition TEXT,
            is_provisional INTEGER,
            result TEXT,
            settlement_value_dollars REAL,
            source_endpoint TEXT NOT NULL,
            raw_payload_json TEXT NOT NULL,
            retrieved_at_utc TEXT NOT NULL,
            run_id TEXT NOT NULL REFERENCES metadata_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS series_selection_runs (
            selection_id TEXT PRIMARY KEY,
            created_at_utc TEXT NOT NULL,
            series_run_id TEXT NOT NULL REFERENCES metadata_runs(run_id),
            series_started_at_utc TEXT NOT NULL,
            series_finished_at_utc TEXT,
            rule_json TEXT NOT NULL,
            rule_sha256 TEXT NOT NULL,
            membership_sha256 TEXT NOT NULL,
            status TEXT NOT NULL,
            stats_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS series_selection_members (
            selection_id TEXT NOT NULL REFERENCES series_selection_runs(selection_id),
            series_ticker TEXT NOT NULL,
            title TEXT,
            category TEXT,
            category_norm TEXT NOT NULL,
            frequency TEXT,
            frequency_group TEXT NOT NULL,
            volume_fp REAL,
            volume_known INTEGER NOT NULL,
            eligible INTEGER NOT NULL,
            selection_group TEXT,
            exclusion_reason TEXT,
            PRIMARY KEY (selection_id, series_ticker)
        );

        CREATE INDEX IF NOT EXISTS idx_raw_payloads_entity
            ON raw_payloads(entity_type, entity_key);
        CREATE INDEX IF NOT EXISTS idx_raw_events_event_ticker
            ON raw_events(event_ticker);
        CREATE INDEX IF NOT EXISTS idx_raw_markets_event_ticker
            ON raw_markets(event_ticker);
        CREATE INDEX IF NOT EXISTS idx_raw_markets_series_ticker
            ON raw_markets(series_ticker);
        CREATE INDEX IF NOT EXISTS idx_raw_markets_status
            ON raw_markets(status);
        CREATE INDEX IF NOT EXISTS idx_raw_markets_run_id
            ON raw_markets(run_id);
        CREATE INDEX IF NOT EXISTS idx_series_selection_members_group
            ON series_selection_members(selection_id, selection_group);
        CREATE INDEX IF NOT EXISTS idx_series_selection_members_ticker
            ON series_selection_members(series_ticker);

        CREATE TABLE IF NOT EXISTS market_history_manifest (
            run_id TEXT NOT NULL REFERENCES metadata_runs(run_id),
            market_id TEXT NOT NULL,
            market_ticker TEXT NOT NULL,
            event_ticker TEXT,
            series_ticker TEXT,
            selection_id TEXT NOT NULL,
            selection_group TEXT NOT NULL,
            filter_mode TEXT NOT NULL,
            passes_volume_filter INTEGER NOT NULL,
            passes_lifetime_filter INTEGER NOT NULL,
            passes_both_filters INTEGER NOT NULL,
            volume_fp REAL,
            lifetime_days REAL,
            history_start_ts INTEGER,
            history_end_ts INTEGER,
            expected_daily_rows INTEGER,
            received_daily_rows INTEGER NOT NULL DEFAULT 0,
            first_observation_ts INTEGER,
            last_observation_ts INTEGER,
            status TEXT NOT NULL,
            error_text TEXT,
            PRIMARY KEY (run_id, market_id)
        );

        CREATE INDEX IF NOT EXISTS idx_market_history_manifest_market
            ON market_history_manifest(market_ticker, selection_group);
        CREATE INDEX IF NOT EXISTS idx_market_history_manifest_filters
            ON market_history_manifest(selection_group, filter_mode, passes_both_filters);

        CREATE TABLE IF NOT EXISTS raw_daily_candles (
            candle_id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id TEXT NOT NULL,
            market_ticker TEXT NOT NULL,
            series_ticker TEXT,
            end_period_ts INTEGER NOT NULL,
            period_interval INTEGER NOT NULL,
            source_mode TEXT NOT NULL,
            source_endpoint TEXT NOT NULL,
            price_open REAL,
            price_low REAL,
            price_high REAL,
            price_close REAL,
            price_mean REAL,
            price_previous REAL,
            price_min REAL,
            price_max REAL,
            yes_bid_open REAL,
            yes_bid_low REAL,
            yes_bid_high REAL,
            yes_bid_close REAL,
            yes_ask_open REAL,
            yes_ask_low REAL,
            yes_ask_high REAL,
            yes_ask_close REAL,
            volume REAL,
            open_interest REAL,
            request_start_ts INTEGER NOT NULL,
            request_end_ts INTEGER NOT NULL,
            retrieved_at_utc TEXT NOT NULL,
            run_id TEXT NOT NULL REFERENCES metadata_runs(run_id),
            raw_payload_json TEXT NOT NULL,
            UNIQUE (market_ticker, end_period_ts, period_interval, source_mode)
        );

        CREATE INDEX IF NOT EXISTS idx_raw_daily_candles_market_day
            ON raw_daily_candles(market_ticker, end_period_ts);
        CREATE INDEX IF NOT EXISTS idx_raw_daily_candles_run
            ON raw_daily_candles(run_id);
        """
    )

    # Keep databases created by the first metadata version forward-compatible.
    existing_series_columns = {
        str(row[1]) for row in conn.execute("PRAGMA table_info(raw_series)")
    }
    if "volume_fp" not in existing_series_columns:
        conn.execute("ALTER TABLE raw_series ADD COLUMN volume_fp REAL")

    existing_candle_columns = {
        str(row[1]) for row in conn.execute("PRAGMA table_info(raw_daily_candles)")
    }
    if "market_id" not in existing_candle_columns:
        conn.execute("ALTER TABLE raw_daily_candles ADD COLUMN market_id TEXT")
