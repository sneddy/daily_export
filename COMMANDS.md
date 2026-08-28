# get_kashi_series

Instructions for the separate 2020–2024 historical stream are in [historical_commands.md](historical_commands.md). This document describes the live pipeline.

Downloads series metadata only; it does not download events or markets.

```bash
cd daily_export
python -m kalshi_daily_research.scripts.get_kashi_series \
  --db-path db/kalshi_daily_probability_dataset.sqlite
```

Args:

| Argument | Purpose; default |
|---|---|
| `--db-path` | SQLite; `db/kalshi_daily_probability_dataset.sqlite` |
| `--base-url` | API URL; public |
| `--page-limit` | page size; `200` |
| `--max-pages` | test page cap; unset |
| `--no-progress` | disable progress |

Output:

`raw_series` — normalized fields; `raw_payloads` — original JSON; `metadata_runs` — run stats.
The run ID and its start/finish timestamps are the frozen source snapshot for the next stage.

A series is a group of related Kalshi markets, such as weather, elections, or sports. Metadata includes ticker, category, tags, frequency, settlement source, and `volume_fp`.

All series are saved without a volume filter. The next step builds a reproducible manifest.

# build_series_manifest

Creates the series universe for subsequent market/event downloads. Defaults: `volume_fp >= 10,000`, exclude `short_recc`, and assign the groups `non_sport_crypto` (main), `sport`, and `crypto`.

```bash
cd daily_export
python -m kalshi_daily_research.scripts.build_series_manifest \
  --db-path db/kalshi_daily_probability_dataset.sqlite
```

Args:

| Argument | Purpose; default |
|---|---|
| `--db-path` | SQLite; `db/kalshi_daily_probability_dataset.sqlite` |
| `--series-run-id` | specific successful series run; latest automatically |
| `--min-volume` | inclusive USD floor; `10000` |
| `--exclude-frequency-group` | repeatable filter; `short_recc` |
| `--sport-category` | raw category for sport; `Sports` |
| `--crypto-category` | raw category for crypto; `Crypto` |
| `--output-path` | CSV manifest; automatic path under `manifests/` |

Output: CSV `manifests/series_selection_<selection_id>.csv` and the `series_selection_runs` and `series_selection_members` SQLite tables. The manifest preserves the source `series_run_id`, series retrieval timestamps, rules, and hashes.

# download_group

Downloads markets/events for one group from the prepared manifest. `/series` and the global `/events` endpoint are not scanned again. Run it separately for each group; the database remains shared.

Current `selection_id`:

```text
20260817T124811Z-3b15c8a0-637b555156e3
```

Three separate live-only runs:

```bash
cd daily_export
python -m kalshi_daily_research.scripts.download_group \
  --db-path db/kalshi_daily_probability_dataset.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group non_sport_crypto \
  --source-mode live

python -m kalshi_daily_research.scripts.download_group \
  --db-path db/kalshi_daily_probability_dataset.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group sport \
  --source-mode live

python -m kalshi_daily_research.scripts.download_group \
  --db-path db/kalshi_daily_probability_dataset.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group crypto \
  --source-mode live
```

Use `--source-mode live` for the live pipeline. Run the historical stream separately using [historical_commands.md](historical_commands.md); do not use `--source-mode both` in the current workflow.

Args:

| Argument | Purpose; default |
|---|---|
| `--selection-id` | frozen manifest; required |
| `--group` | `non_sport_crypto`, `sport`, or `crypto`; required |
| `--source-mode` | `live`; also `historical`, `both` |
| `--completed-month` | UTC month for settled markets; unset |
| `--skip-event-details` | do not download related event details |
| `--refresh-event-details` | download event details again |
| `--max-pages` | test cap; unset |

Each run creates a separate `metadata_runs` record with the group, selection hashes, and statistics; raw JSON is saved in `raw_payloads`.

# download_daily_candles

Downloads native daily candles at a 1440-minute interval for a market universe filtered by cumulative `volume_fp` and market lifetime. Each market is requested only once within a run; membership in each filter is preserved in `market_history_manifest`. Retries use a separate run.

Main arguments:

| Argument | Purpose; default |
|---|---|
| `--source-mode` | candle source tier: `live` or `historical`; `live` |
| `--max-tickers-per-batch` | live batch request size; `100` |
| `--max-markets` | market cap for smoke tests; unset |
| `--max-workers` | historical request concurrency; `4` |
| `--retry-status` | retry unresolved status from the same selection/group/source mode; unset |

Main run for `non_sport_crypto`:

```bash
cd daily_export
python -m kalshi_daily_research.scripts.download_daily_candles \
  --db-path db/kalshi_daily_probability_dataset.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group non_sport_crypto \
  --filter-mode union \
  --min-volume-fp 10000 \
  --min-lifetime-days 5
```

Equivalent runs for the other frozen manifest groups:

```bash
# sport
python -m kalshi_daily_research.scripts.download_daily_candles \
  --db-path db/kalshi_daily_probability_dataset.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group sport \
  --filter-mode union \
  --min-volume-fp 10000 \
  --min-lifetime-days 5

# crypto
python -m kalshi_daily_research.scripts.download_daily_candles \
  --db-path db/kalshi_daily_probability_dataset.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group crypto \
  --filter-mode union \
  --min-volume-fp 10000 \
  --min-lifetime-days 5
```

`union` downloads markets that pass at least one filter, allowing both universes to be compared without another download. Use the following modes for separate universes:

```bash
# volume_fp >= 10,000
python -m kalshi_daily_research.scripts.download_daily_candles \
  --db-path db/kalshi_daily_probability_dataset.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group non_sport_crypto \
  --filter-mode volume

# lifetime >= 5 days
python -m kalshi_daily_research.scripts.download_daily_candles \
  --db-path db/kalshi_daily_probability_dataset.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group non_sport_crypto \
  --filter-mode lifetime

# both filters simultaneously
python -m kalshi_daily_research.scripts.download_daily_candles \
  --db-path db/kalshi_daily_probability_dataset.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group non_sport_crypto \
  --filter-mode both
```

Output:

- `raw_daily_candles` — source-preserving daily candle rows;
- `market_history_manifest` — filter flags, expected/received rows, coverage and errors;
- `metadata_runs` — run configuration and batch statistics;
- `raw_payloads` — original batch JSON responses.

The default command uses the live batch candlestick endpoint. Historical candles use the single-market endpoint and are documented separately in [historical_commands.md](historical_commands.md). `--start-date` and `--end-date` can restrict the UTC date window. `--max-batches` is available for live smoke tests; `--max-markets` is available for either source mode, and `--max-workers` controls historical request concurrency.

To retry unresolved markets that have a historical `api_error` for the same selection and group:

```bash
python -m kalshi_daily_research.scripts.download_daily_candles \
  --db-path db/kalshi_daily_probability_dataset.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group non_sport_crypto \
  --filter-mode union \
  --min-volume-fp 10000 \
  --min-lifetime-days 5 \
  --max-tickers-per-batch 20 \
  --retry-status api_error
```

The retry mode searches the full manifest history, excludes markets resolved by a later `success` or `empty` status, copies the latest full market manifest as its baseline, and uses the requested `--max-tickers-per-batch` value. Use `--max-tickers-per-batch 1` to isolate individual failures. The new run keeps `retry_parent_run_id` in its provenance; rerun `export_daily_snapshot` afterwards so the exported candles combine the parent and retry run history.

# export_daily_snapshot

Creates the supervisor-facing CSV/JSON snapshot from one daily-candle run. It reads SQLite in read-only mode, includes the parent chain for retry runs, and writes both the full export and the `passes_both_filters` view. Use `--source-mode` explicitly so live and historical exports cannot be mixed.

```bash
python -m kalshi_daily_research.scripts.export_daily_snapshot \
  --db-path db/kalshi_daily_probability_dataset.sqlite \
  --selection-id 20260817T124811Z-3b15c8a0-637b555156e3 \
  --group non_sport_crypto \
  --source-mode live \
  --output-dir data/demo
```

For the historical `main` and `crypto` commands, use [historical_commands.md](historical_commands.md).

Args:

| Argument | Purpose; default |
|---|---|
| `--db-path` | SQLite database; required |
| `--selection-id` | frozen series selection; required |
| `--group` | `non_sport_crypto`, `sport`, or `crypto`; required |
| `--source-mode` | `live` or `historical`; required |
| `--output-dir` | full snapshot directory; required |
| `--candle-run-id` | specific run; latest matching run automatically |

# notebooks

The supervisor-facing workflow uses the export script and two optional notebooks:

1. `export_daily_snapshot` reads SQLite and creates the prepared files;
2. `notebooks/daily_data_diagnostic.ipynb` optionally reads SQLite for interactive diagnostics;
3. `notebooks/daily_data_eda.ipynb` reads the live exported files and contains no SQL or SQLite dependency;
4. `notebooks/daily_data_eda_historical.ipynb` reads the historical main and crypto exports together and contains no SQL or SQLite dependency.

The live export writes:

```text
data/demo/main_market_metadata.csv
data/demo/main_daily_candles.csv
data/demo/export_summary.json
```

`main_market_metadata.csv` is the one-row-per-market lookup keyed by `market_id`; it contains the question, descriptions, dates, selection filters, and candle coverage. Join it to `main_daily_candles.csv` by `market_id`.

The export script automatically selects the latest matching `daily_candles` run unless `--candle-run-id` is supplied. The exported summary preserves whether that source run was `success` or `partial`.
