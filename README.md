# Kalshi Daily Research Dataset

This project builds a reproducible daily-resolution dataset for research on Kalshi prediction markets.

Operational commands and decisions are recorded in [COMMANDS.md](COMMANDS.md).

## Purpose

Build a reproducible daily dataset that can support the following research question:

> How informative and well-calibrated are prediction-market probabilities across different topics and levels of market activity?

The first platform is Kalshi. The first implementation uses native daily candles obtained through the live API endpoints. It does not require downloading all individual trades or constructing minute-level panels.

The intended research outputs are:

- Brier Score;
- Log Loss;
- reliability and calibration tables;
- Brier decomposition where sample size permits;
- results by topic, volume, liquidity, and time to resolution;
- confidence intervals and complete sample and coverage diagnostics.

The current repository contains the data-extraction and daily-data demonstration layers. Outcome alignment and forecast evaluation are downstream research layers that are not yet finalized.

## Current first iteration

The first iteration is intentionally narrow:

- Kalshi only;
- live API endpoints only;
- native daily candles with period_interval = 1440;
- no raw trade-level download;
- no historical endpoint;
- no recurrent high-frequency panels;
- three research groups:
  - sport;
  - crypto;
  - non_sport_crypto (main).

The live source mode refers to the API endpoint used for retrieval. It does not imply real-time streaming, and it does not guarantee a fixed six-month window. The actual date coverage is determined from the returned candle timestamps and is reported in the run metadata and exported summary.

## Selection rules

Selection happens at two different levels.

### Series-level selection

All series metadata is retained in raw_series. A frozen series manifest is then built using:

- cumulative series volume_fp >= 10,000;
- exclusion of the short_recc frequency group.

The short_recc group is explicitly defined as:

- weekly;
- daily;
- hourly;
- fifteen_min.

After eligibility is established, series are assigned to:

- sport when the raw category is Sports;
- crypto when the raw category is Crypto;
- main for all remaining eligible categories.

The selected series membership is stored in series_selection_runs and series_selection_members and exported to manifests/.

### Market-level selection

After event and market metadata is downloaded, the daily-candle planner applies a separate market-level filter.

The current default is the union of:

- cumulative market volume_fp >= 10,000;
- market lifetime >= 5 days.

A market enters the daily-candle download if it passes at least one of these filters. The manifest retains the individual volume, lifetime, and intersection flags so the cohorts can be compared later without downloading candles again.

## Current architecture

~~~mermaid
flowchart LR
    A[Kalshi live API] --> B[Raw series metadata]
    B --> C[Frozen series manifest]
    C --> D[Sport / Crypto / Main]
    D --> E[Event and market metadata]
    E --> F[Market history manifest]
    F --> G[Market-level filters]
    G --> H[Native daily candles]
    H --> I[(Raw SQLite database)]
    I --> J[Diagnostic export]
    J --> K[CSV demo snapshot]
    K --> L[CSV-only EDA notebook]
    L --> M[Future outcome and calibration research]
~~~

The pipeline does not resample trades. It requests Kalshi's native daily candle representation in batches and preserves source fields and provenance.

## Data layers

### 1. Raw metadata

The raw metadata layer contains:

~~~text
raw_series
raw_events
raw_markets
metadata_runs
raw_payloads
~~~

raw_series stores the full series snapshot. Events and markets are downloaded only for series selected by the frozen manifest.

The normalized tables provide queryable fields, while raw_payloads retains original API responses for recoverability.

Stable identifiers are:

~~~text
market_id = kalshi:{ticker}
event_id  = kalshi:event:{event_ticker}
~~~

### 2. Frozen selection

The selection layer contains:

~~~text
series_selection_runs
series_selection_members
manifests/series_selection_<selection_id>.csv
~~~

This layer records the source series run, selection rules, frequency group, volume, eligibility, exclusion reason, and research group.

### 3. Raw daily history

The daily history layer contains:

~~~text
raw_daily_candles
market_history_manifest
~~~

raw_daily_candles is a thin, source-preserving representation of native daily candles. Important fields include:

~~~text
market_id
market_ticker
series_ticker
end_period_ts
period_interval
source_mode
price_open
price_low
price_high
price_close
price_mean
price_previous
price_min
price_max
yes_bid_*
yes_ask_*
volume
open_interest
request_start_ts
request_end_ts
retrieved_at_utc
run_id
raw_payload_json
~~~

market_history_manifest stores the extraction decision and coverage information for each market:

~~~text
selection_id
selection_group
filter_mode
passes_volume_filter
passes_lifetime_filter
passes_both_filters
volume_fp
lifetime_days
history_start_ts
history_end_ts
expected_daily_rows
received_daily_rows
status
error_text
run_id
~~~

The raw layer does not contain research-derived features such as returns, volatility, spreads, momentum, volume tiers, forward-filled values, category aggregates, Brier Score, or calibration outputs.

The primary probability used for the first downstream analysis is expected to be price_close, but the final forecast-timestamp convention remains a research decision.

### 4. Prepared demo snapshot

The diagnostic notebook creates a lightweight presentation snapshot under data/demo/:

~~~text
data/demo/
├── main_market_manifest.csv
├── main_market_metadata.csv
├── main_daily_candles.csv.gz
└── export_summary.json
~~~

The market manifest has one row per selected market. The market metadata file is a one-row-per-market lookup keyed by `market_id`; it contains the market question, descriptions, event context, series context, and rules. The daily-candle file contains market-by-day observations needed for the demonstration notebook. The full source payloads remain in SQLite.

## Notebook workflow

The notebooks are intentionally separated into preparation and presentation layers.

The purpose, execution order, and current key insights for every notebook are documented in [notebooks/README.md](notebooks/README.md).

### Diagnostic notebook

notebooks/daily_data_diagnostic.ipynb:

- reads SQLite;
- selects a frozen daily-candle run;
- extracts the market manifest, market metadata lookup, and daily candles;
- checks duplicates, intervals, probabilities, missingness, and coverage;
- preserves the source run status;
- writes data/demo/ files.

This is the only notebook in the demo workflow that uses SQL.

### EDA demonstration notebook

notebooks/daily_data_eda.ipynb:

- reads only the prepared CSV and JSON files;
- contains no SQL queries;
- does not depend on SQLite;
- joins question and description fields by `market_id`;
- shows the selected universe, filters, date coverage, sample probability paths, and basic data quality.

This notebook is intended to be easy to share with a supervisor and to demonstrate what the daily export contains before outcome-based evaluation is added.

## Operational workflow

Run the stages in this order:

1. Download all series metadata.
2. Build the frozen series manifest.
3. Download event and market metadata separately for sport, crypto, and main.
4. Download native daily candles for each group.
5. Run daily_data_diagnostic.ipynb to create the demo snapshot.
6. Run daily_data_eda.ipynb for visualization and inspection.

The exact commands and current selection ID are maintained in [COMMANDS.md](COMMANDS.md).

## Design principles

1. Preserve source provenance and reproducibility.
2. Use native daily candles instead of downloading all trades.
3. Keep raw source fields separate from research-derived features.
4. Separate series eligibility, market filtering, research cohorts, and reporting slices.
5. Never hide missing observations with untracked forward-filling.
6. Keep the legacy 5-minute pipeline unchanged.
7. Preserve raw API payloads so that schema changes do not destroy information.
8. Make each snapshot reconstructible from a database, configuration, run ID, and manifest.
9. Make partial retrieval status visible rather than presenting incomplete coverage as complete.

## Research contract

The planned downstream evaluation table will represent one probability forecast for one resolved binary market:

~~~text
market_id
event_id
forecast_timestamp_utc
horizon_days
forecast_probability
outcome
platform_category
research_category
volume_tier
liquidity_tier
days_to_resolution
observation_age_days
~~~

The as-of rule must be explicit. For a forecast cutoff, the analysis should use the latest valid daily close at or before that cutoff and retain the age of the observation. Information after the cutoff must not be used.

Planned metrics include:

- Brier Score;
- Log Loss;
- calibration and reliability;
- Brier decomposition;
- base-rate comparison;
- stratified results by topic, volume, liquidity, and horizon.

Uncertainty should be estimated with market- or event-level resampling so that repeated markets from the same event or series are not treated as fully independent observations.

## Future extensions

The following are intentionally deferred:

- historical Kalshi endpoints;
- incremental daily refresh;
- gap repair and settlement repair;
- higher-frequency robustness panels;
- canonical Parquet releases;
- as-of forecast snapshots;
- external survey, macroeconomic, or other covariate integration;
- complex repricing and decisiveness benchmarks;
- Polymarket integration.

These extensions should be added without changing the raw daily source contract.

## Project layout

~~~text
daily_export/
├── README.md
├── COMMANDS.md
├── pyproject.toml
├── requirements.txt
├── kalshi_daily_research/
│   ├── client.py
│   ├── ingest.py
│   ├── schema.py
│   ├── candles.py
│   ├── series_selection.py
│   ├── series_manifest.py
│   ├── scripts/
│   └── tests/
├── manifests/
├── notebooks/
│   ├── analyze_metadata.ipynb
│   ├── daily_data_diagnostic.ipynb
│   ├── daily_data_eda.ipynb
│   └── explore_series.ipynb
├── data/
│   └── demo/
└── db/
~~~

The daily project uses its own package and database namespace. It does not modify the legacy Kalshi export or its existing 5-minute artifacts.

## First-iteration completion criteria

The first iteration is considered operationally complete when:

- the series selection rule is frozen and auditable;
- sport, crypto, and main groups can be downloaded separately;
- event and market metadata are available for the selected groups;
- native daily candles are stored with source provenance;
- market-level volume and lifetime decisions are recorded;
- partial retrieval and coverage diagnostics are visible;
- the diagnostic notebook can rebuild the demo CSV snapshot;
- the EDA notebook can run without SQL or SQLite;
- the resulting snapshot can support the next outcome-alignment and Brier Score stage.
