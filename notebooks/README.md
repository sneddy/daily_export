# Notebook guide

These notebooks support the Kalshi live-data research workflow. The presentation label is **main** (`non_sport_crypto` in technical identifiers).

## Recommended order

1. `explore_series.ipynb`
2. `analyze_metadata.ipynb`
3. `daily_data_diagnostic.ipynb`
4. `daily_data_eda.ipynb`
5. `research_script.ipynb`

The first two notebooks support research decisions. The last two prepare and demonstrate the daily dataset.

## Notebook overview

| Notebook | Purpose | Main inputs and outputs | Key insights | Interpretation |
|---|---|---|---|---|
| `explore_series.ipynb` | Explore the full Kalshi series universe and define an initial series-level universe before downloading market metadata. | **Input:** `raw_series` from SQLite.<br><br>**Outputs:** volume-threshold tables, frequency and category distributions, operational groups, and top-volume series.<br><br>Does not download candles or create the final daily dataset. | 13,029 series are present. A cumulative `volume_fp >= 10,000` floor leaves 7,351 series. After that floor: `weekly` 106, `daily` 93, `hourly` 27, and `fifteen_min` 12. Sports, Entertainment, Politics, and Elections are the largest categories. | This is a discovery notebook. Series-level counts must not be confused with market-level counts because one series can contain many markets. |
| `analyze_metadata.ipynb` | Decide whether to retain markets with cumulative `volume_fp >= 10,000`, lifecycle of at least five days, or both. Also inspect metadata completeness and lifecycle coverage. | **Input:** raw market, event, series, and run metadata from SQLite.<br><br>**Outputs:** volume/lifetime sensitivity tables, filter overlap, retrieval dates, and lifecycle coverage plots.<br><br>Read-only; it does not create the daily export. | The analyzed universe contains 71,425 markets, 6,813 events, and 2,096 mapped series. No missing volume, open time, close time, or series mapping was found; 152 markets have non-positive lifetimes. Volume filter: 13,144 markets (18.4%). Lifetime filter: 42,101 (58.9%). Both: 7,545 (10.6%). | This notebook evaluates market-selection trade-offs. Lifecycle coverage does not prove that daily candles exist for every date. |
| `daily_data_diagnostic.ipynb` | Prepare the supervisor-facing snapshot from a frozen live daily-candle run. This is the only notebook that reads SQLite and executes SQL. | **Inputs:** selected `daily_candles` run and raw metadata in SQLite.<br><br>**Outputs:** full snapshot files under `data/demo/` plus a `data/demo_filtered/` view containing only markets that pass both the volume and lifetime filters.<br><br>Checks duplicate market-days, probability bounds, volume, open interest, identifiers, and coverage. Includes candles from the parent chain when the selected run is a retry. | The full export preserves the union cohort and the filtered view provides a reproducible high-quality subset without another API download. | This is the preparation layer. The active download uses the union of the volume and lifetime filters; the filtered view applies their intersection for downstream inspection. |
| `daily_data_eda.ipynb` | Demonstrate what was actually exported in a supervisor-facing notebook without SQL or SQLite. | **Input:** one selected CSV/JSON snapshot from `data/demo/` or `data/demo_filtered/`, chosen through `DATA_DIR`; `METADATA_PATH`, `CANDLES_PATH`, and `SUMMARY_PATH` are derived from that directory.<br><br>**Outputs:** snapshot and field-availability tables, filter overlap, date-coverage plots, market/question context, probability paths, and quality checks.<br><br>Joins metadata only for selected markets or small tables by `market_id`. | The same EDA can inspect either the full union snapshot or the both-filter subset without changing its analysis cells. | This is descriptive only. It does not align forecasts to resolved outcomes or calculate Brier Score. The next research layer must define forecast timing, outcome alignment, and resolution handling. |
| `research_script.ipynb` | Run the focused first empirical test on the filtered snapshot: whether daily market probabilities are informative about resolved outcomes. | **Input:** `data/demo_filtered/main_market_metadata.csv` and `main_daily_candles.csv.gz`.<br><br>**Outputs:** top-50 resolved sample, forecast rows at 1/3/7 days before close, Brier Score table, calibration table/plot, category summary, and a small case-study table.<br><br>Does not read SQLite, call the API, or write asset files. | Provides the first compact comparison of market probabilities with a 50% reference and the observed yes-rate. | This is a pilot research analysis, not a population-level estimate. The top-50 sample is selected by volume and should later be expanded with uncertainty intervals and a broader resolved-market sample. |

## Important interpretation

The metadata decision notebook reports the intersection when it shows “both filters” (7,545 markets). The daily download uses the union cohort, so its manifest is larger (47,700 markets). These are different research choices, not inconsistent counts.

The numbers in the table describe the current snapshot and can change when the database or selected run is refreshed.

