# Historical Kalshi export: 2020–2024

This document describes a separate historical stream that extends the live export with markets settled during 2020–2024.

The historical stream contains only two groups:

- `non_sport_crypto` — `main`;
- `crypto`.

`sport` is not included in this run.

Historical data is stored separately from live data:

```text
db/kalshi_historical_2020_2024.sqlite

data/historical_2020_2024/main/
data/historical_2020_2024/crypto/
```

Trade records are not downloaded. The research dataset uses native daily candles with `period_interval=1440`.

## 1. Fixed selection

Use the same frozen series selection as the live pipeline. It is important to distinguish two filtering levels:

- `get_kashi_series` previously saved all available series;
- `build_series_manifest` applied the series-level filter: `series.volume_fp >= 10,000`, and excluded `short_recc`;
- the historical stream uses only eligible series from this frozen manifest;
- a separate market-level filter is applied later, before downloading daily candles.

First, copy the frozen selection and the original series metadata into a separate historical database.

```bash
cd daily_export
python -m kalshi_daily_research.scripts.seed_selection \
  --source-db-path db/kalshi_daily_probability_dataset.sqlite \
  --target-db-path db/kalshi_historical_2020_2024.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3
```

The command does not download new historical data and does not copy markets, events, or candles. It copies the already-frozen series manifest. Re-running it is safe when the selection has already been copied with the same membership hash.

## 2. Historical cutoff and date window

The period is defined by `settlement_ts`:

```text
[2020-01-01, 2025-01-01)
```

`--completed-to` is an exclusive boundary. Before a full download, the metadata command requests the Kalshi historical cutoff. If the requested period overlaps the live tier, the command exits with a clear error instead of silently skipping part of the market set.

## 3. Historical market and event metadata

### Main (`non_sport_crypto`)

```bash
cd daily_export
python -m kalshi_daily_research.scripts.download_group \
  --db-path db/kalshi_historical_2020_2024.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group non_sport_crypto \
  --source-mode historical \
  --completed-from 2020-01-01 \
  --completed-to 2025-01-01
```

### Crypto

```bash
cd daily_export
python -m kalshi_daily_research.scripts.download_group \
  --db-path db/kalshi_historical_2020_2024.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group crypto \
  --source-mode historical \
  --completed-from 2020-01-01 \
  --completed-to 2025-01-01
```

This stage downloads market and event metadata only for series that passed the frozen series-level filter, and saves records in the specified settlement window. Market-level volume/lifetime filters are not applied yet.

Therefore, in the metadata run result:

- `series_targets` is the number of eligible series from the frozen manifest;
- `markets_filtered` is the number of markets outside the settlement window, not markets with low volume;
- `markets_written` is the number of markets saved after the settlement filter and before market-level filters.

After these commands, the historical DB should contain manifest-group runs only for main and crypto.

## 4. Full historical daily candles

At this stage, market-level flags are computed from the saved market metadata:

```text
market.volume_fp >= 10000
market lifetime >= 5 days
```

The command then creates `market_history_manifest` and requests candles only for markets matching `--filter-mode`. Historical candles are requested one market ticker at a time.

### Main (`non_sport_crypto`)

```bash
cd daily_export
python -m kalshi_daily_research.scripts.download_daily_candles \
  --db-path db/kalshi_historical_2020_2024.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group non_sport_crypto \
  --source-mode historical \
  --filter-mode union \
  --min-volume-fp 10000 \
  --min-lifetime-days 5 \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --max-workers 4
```

### Crypto

```bash
cd daily_export
python -m kalshi_daily_research.scripts.download_daily_candles \
  --db-path db/kalshi_historical_2020_2024.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group crypto \
  --source-mode historical \
  --filter-mode union \
  --min-volume-fp 10000 \
  --min-lifetime-days 5 \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --max-workers 4
```

`union` retains markets that pass at least one of the two market-level filters. The metadata keeps separate flags for volume, lifetime, and their intersection.

This does not repeat the series-level filter. The series-level filter was already applied when the frozen selection was created; this step filters individual markets within the selected series.

## 5. Retry and resume

If a run stops or receives an `api_error`, it can be continued from the same database:

```bash
cd daily_export
python -m kalshi_daily_research.scripts.download_daily_candles \
  --db-path db/kalshi_historical_2020_2024.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group non_sport_crypto \
  --source-mode historical \
  --filter-mode union \
  --min-volume-fp 10000 \
  --min-lifetime-days 5 \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --max-workers 4 \
  --retry-status api_error
```

Successful market responses are written to SQLite as they are processed. It is therefore safe to stop the process and repeat the retry command.

## 6. Export to CSV

Run the export script after the candle runs are complete. It reads SQLite in read-only mode and writes the full snapshot, the both-filter view, and `export_summary.json`.

### Main (`non_sport_crypto`)

```bash
cd daily_export
python -m kalshi_daily_research.scripts.export_daily_snapshot \
  --db-path db/kalshi_historical_2020_2024.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group non_sport_crypto \
  --source-mode historical \
  --output-dir data/historical_2020_2024/main
```

### Crypto

```bash
cd daily_export
python -m kalshi_daily_research.scripts.export_daily_snapshot \
  --db-path db/kalshi_historical_2020_2024.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group crypto \
  --source-mode historical \
  --output-dir data/historical_2020_2024/crypto
```

By default, the script selects the latest matching daily-candle run. Use `--candle-run-id` to freeze a specific run for a reproducible export. If the selected run is a retry, candles from its parent chain are included.

The optional `notebooks/daily_data_diagnostic.ipynb` can still be used for interactive SQL diagnostics, but it is no longer required to create the export files.

When using the optional diagnostic notebook, set:

```python
SOURCE_MODE = "historical"
SELECTION_GROUP = "non_sport_crypto"
```

For crypto, change only the group:

```python
SELECTION_GROUP = "crypto"
```

The export script automatically selects the historical DB from `--db-path` and creates:

```text
data/historical_2020_2024/main/
├── main_market_metadata.csv
├── main_daily_candles.csv
└── export_summary.json

data/historical_2020_2024/crypto/
├── crypto_market_metadata.csv
├── crypto_daily_candles.csv
└── export_summary.json
```

It also creates a filtered view in the neighboring `main_filtered/` or `crypto_filtered/` directory.

`export_summary.json` records the source mode, selection group, run IDs, settlement window, API endpoint, coverage, row counts, and quality checks.

For a combined historical comparison, open `notebooks/daily_data_eda_historical.ipynb`. It loads the `main` and `crypto` snapshots together. Set `USE_FILTERED = True` in the first code cell to use the both-filter views instead of the full union snapshots.

## 7. Separation checks

Before using the data, check:

```bash
sqlite3 db/kalshi_historical_2020_2024.sqlite <<'SQL'
SELECT source_mode, COUNT(*)
FROM metadata_runs
GROUP BY source_mode;

SELECT source_mode, COUNT(*)
FROM raw_daily_candles
GROUP BY source_mode;
SQL
```

The historical DB must not contain live metadata/candle runs; apart from historical runs, it contains only the copied original `series` run. The live DB and existing live CSV files are not modified.

Do not use `--source-mode both` in this workflow: main and crypto are run with separate historical commands, while the live stream remains a separate dataset.
