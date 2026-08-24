# Notebook guide

These notebooks support the Kalshi live-data research workflow. The presentation label is **main** (`non_sport_crypto` in technical identifiers).

## Recommended order

1. `explore_series.ipynb`
2. `analyze_metadata.ipynb`
3. `daily_data_diagnostic.ipynb`
4. `daily_data_eda.ipynb`

The first two notebooks support research decisions. The last two prepare and demonstrate the daily dataset.

## Notebook overview

| Notebook | Purpose | Main inputs and outputs | Key insights | Interpretation |
|---|---|---|---|---|
| `explore_series.ipynb` | Explore the full Kalshi series universe and define an initial series-level universe before downloading market metadata. | **Input:** `raw_series` from SQLite.<br><br>**Outputs:** volume-threshold tables, frequency and category distributions, operational groups, and top-volume series.<br><br>Does not download candles or create the final daily dataset. | 13,029 series are present. A cumulative `volume_fp >= 10,000` floor leaves 7,351 series. After that floor: `weekly` 106, `daily` 93, `hourly` 27, and `fifteen_min` 12. Sports, Entertainment, Politics, and Elections are the largest categories. | This is a discovery notebook. Series-level counts must not be confused with market-level counts because one series can contain many markets. |
| `analyze_metadata.ipynb` | Decide whether to retain markets with cumulative `volume_fp >= 10,000`, lifecycle of at least five days, or both. Also inspect metadata completeness and lifecycle coverage. | **Input:** raw market, event, series, and run metadata from SQLite.<br><br>**Outputs:** volume/lifetime sensitivity tables, filter overlap, retrieval dates, and lifecycle coverage plots.<br><br>Read-only; it does not create the daily export. | The analyzed universe contains 71,425 markets, 6,813 events, and 2,096 mapped series. No missing volume, open time, close time, or series mapping was found; 152 markets have non-positive lifetimes. Volume filter: 13,144 markets (18.4%). Lifetime filter: 42,101 (58.9%). Both: 7,545 (10.6%). | This notebook evaluates market-selection trade-offs. Lifecycle coverage does not prove that daily candles exist for every date. |
| `daily_data_diagnostic.ipynb` | Prepare the supervisor-facing snapshot from a frozen live daily-candle run. This is the only notebook that reads SQLite and executes SQL. | **Inputs:** selected `daily_candles` run and raw metadata in SQLite.<br><br>**Outputs:** `main_market_manifest.csv`, `main_market_metadata.csv`, `main_daily_candles.csv.gz`, and `export_summary.json` under `data/demo/`.<br><br>Checks duplicate market-days, intervals, probability bounds, volume, and coverage. | The current manifest has 47,700 candidate markets and 506,173 daily candle rows for 15,851 markets, covering 2024-04-09 to 2026-08-19 UTC. The run is explicitly `partial`; API errors, empty markets, and skipped windows remain visible in the summary. Current structural checks show no duplicate market-days, non-daily intervals, out-of-range probabilities, or negative volume. | This is the preparation layer. The active download uses the union of the volume and lifetime filters, so its market count is intentionally larger than the “both filters” count in `analyze_metadata.ipynb`. |
| `daily_data_eda.ipynb` | Demonstrate what was actually exported in a supervisor-facing notebook without SQL or SQLite. | **Inputs:** the four prepared files in `data/demo/`.<br><br>**Outputs:** inventory tables, filter overlap, date-coverage tables and plots, sample daily probability paths, metadata displays, and quality checks.<br><br>Joins market metadata to manifest and candles by `market_id`. | The current metadata lookup has 47,700 unique `market_id` rows. Three markets lack a market-level question but retain event-level context. The notebook makes the difference between the selected manifest and markets with returned candles visible. | This is descriptive only. It does not align forecasts to resolved outcomes or calculate Brier Score. The next research layer must define forecast timing, outcome alignment, and resolution handling. |

## Important interpretation

The metadata decision notebook reports the intersection when it shows “both filters” (7,545 markets). The daily download uses the union cohort, so its manifest is larger (47,700 markets). These are different research choices, not inconsistent counts.

The numbers in the table describe the current snapshot and can change when the database or selected run is refreshed.

