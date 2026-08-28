# Notebook guide

These notebooks support the Kalshi live and historical-data research workflows. The presentation label is **main** (`non_sport_crypto` in technical identifiers).

## Recommended order

1. `explore_series.ipynb`
2. `analyze_metadata.ipynb`
3. `daily_data_diagnostic.ipynb`
4. `daily_data_eda.ipynb`
5. `daily_data_eda_historical.ipynb`

The export script prepares the daily snapshot. The diagnostic notebook is optional and supports interactive SQLite checks, while the EDA notebook demonstrates the exported data. Outcome-based research notebooks live in the separate `context_research/` project.

## Notebook overview

| Notebook | Purpose | Main inputs and outputs | Key insights | Interpretation |
|---|---|---|---|---|
| `explore_series.ipynb` | Explore the full Kalshi series universe and define an initial series-level universe before downloading market metadata. | **Input:** `raw_series` from SQLite.<br><br>**Outputs:** volume-threshold tables, frequency and category distributions, operational groups, and top-volume series.<br><br>Does not download candles or create the final daily dataset. | 13,029 series are present. A cumulative `volume_fp >= 10,000` floor leaves 7,351 series. After that floor: `weekly` 106, `daily` 93, `hourly` 27, and `fifteen_min` 12. Sports, Entertainment, Politics, and Elections are the largest categories. | This is a discovery notebook. Series-level counts must not be confused with market-level counts because one series can contain many markets. |
| `analyze_metadata.ipynb` | Decide whether to retain markets with cumulative `volume_fp >= 10,000`, lifecycle of at least five days, or both. Also inspect metadata completeness and lifecycle coverage. | **Input:** raw market, event, series, and run metadata from SQLite.<br><br>**Outputs:** volume/lifetime sensitivity tables, filter overlap, retrieval dates, and lifecycle coverage plots.<br><br>Read-only; it does not create the daily export. | The analyzed universe contains 71,425 markets, 6,813 events, and 2,096 mapped series. No missing volume, open time, close time, or series mapping was found; 152 markets have non-positive lifetimes. Volume filter: 13,144 markets (18.4%). Lifetime filter: 42,101 (58.9%). Both: 7,545 (10.6%). | This notebook evaluates market-selection trade-offs. Lifecycle coverage does not prove that daily candles exist for every date. |
| `daily_data_diagnostic.ipynb` | Inspect a selected daily-candle run interactively using SQLite. The reproducible CSV/JSON export is handled by `export_daily_snapshot`. | **Inputs:** selected `daily_candles` run and raw metadata in SQLite.<br><br>**Outputs:** diagnostic tables and checks; the notebook can still rebuild a snapshot when run interactively, but this is not required for the standard workflow.<br><br>Checks duplicate market-days, probability bounds, volume, open interest, identifiers, and coverage. Includes candles from the parent chain when the selected run is a retry. | The export script gives the same full and both-filter outputs for live or historical runs through explicit CLI arguments. | This notebook is for interactive diagnosis; use the export script for repeatable artifact generation. |
| `daily_data_eda.ipynb` | Demonstrate what was actually exported in a supervisor-facing notebook without SQL or SQLite. | **Input:** one selected CSV/JSON snapshot from `data/demo/` or `data/demo_filtered/`, chosen through `DATA_DIR`; `METADATA_PATH`, `CANDLES_PATH`, and `SUMMARY_PATH` are derived from that directory.<br><br>**Outputs:** snapshot and field-availability tables, filter overlap, date-coverage plots, market/question context, probability paths, and quality checks.<br><br>Joins metadata only for selected markets or small tables by `market_id`. | The same EDA can inspect either the full union snapshot or the both-filter subset without changing its analysis cells. | This is descriptive only. It does not align forecasts to resolved outcomes or calculate Brier Score. The next research layer must define forecast timing, outcome alignment, and resolution handling. |
| `daily_data_eda_historical.ipynb` | Compare the historical `main` and `crypto` snapshots in one notebook without SQL or SQLite. | **Input:** `data/historical_2020_2024/main/` and `data/historical_2020_2024/crypto/`, or their `_filtered` siblings; `USE_FILTERED` selects the view.<br><br>**Outputs:** combined provenance and overview tables, group-level calendar coverage, question context, probability paths, group comparison, and quality checks.<br><br>Adds a presentation-only `group` column after loading the two snapshots. | Makes the difference in sample size, time coverage, activity, and market outcomes visible before outcome-based evaluation. | This is descriptive only. Group differences should not be interpreted as predictability differences until forecast timing and outcome alignment are defined. |
## Important interpretation

The metadata decision notebook reports the intersection when it shows “both filters” (7,545 markets). The daily download uses the union cohort, so its manifest is larger (47,700 markets). These are different research choices, not inconsistent counts.

The numbers in the table describe the current snapshot and can change when the database or selected run is refreshed.
