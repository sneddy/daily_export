# get_kashi_series

Только metadata series; events/markets не скачивает.

```bash
cd daily_export
python -m kalshi_daily_research.scripts.get_kashi_series \
  --db-path db/kalshi_daily_probability_dataset.sqlite
```

Args:

| Аргумент | Назначение; default |
|---|---|
| `--db-path` | SQLite; `db/kalshi_daily_probability_dataset.sqlite` |
| `--base-url` | API URL; public |
| `--page-limit` | page size; `200` |
| `--max-pages` | test page cap; unset |
| `--no-progress` | disable progress |

Output:

`raw_series` — normalized fields; `raw_payloads` — original JSON; `metadata_runs` — run stats.
The run ID and its start/finish timestamps are the frozen source snapshot for the next stage.

Series — группа однотипных рынков Kalshi (погода, выборы, спорт). Metadata: ticker, category, tags, frequency, settlement source, `volume_fp`.

Все series сохраняются без volume-фильтра. Следующим шагом строится воспроизводимый manifest.

# build_series_manifest

Фиксирует выборку series для дальнейшего скачивания markets/events. По умолчанию: `volume_fp >= 10,000`, исключение `short_recc`, группы `non_sport_crypto` (main), `sport`, `crypto`.

```bash
cd daily_export
python -m kalshi_daily_research.scripts.build_series_manifest \
  --db-path db/kalshi_daily_probability_dataset.sqlite
```

Args:

| Аргумент | Назначение; default |
|---|---|
| `--db-path` | SQLite; `db/kalshi_daily_probability_dataset.sqlite` |
| `--series-run-id` | конкретный successful series run; latest автоматически |
| `--min-volume` | inclusive USD floor; `10000` |
| `--exclude-frequency-group` | повторяемый filter; `short_recc` |
| `--sport-category` | raw category для sport; `Sports` |
| `--crypto-category` | raw category для crypto; `Crypto` |
| `--output-path` | CSV manifest; auto в `manifests/` |

Output: CSV `manifests/series_selection_<selection_id>.csv` и таблицы `series_selection_runs`, `series_selection_members` в SQLite. Manifest сохраняет source `series_run_id`, даты скачивания series, rules и hashes.

# download_group

Скачивает markets/events только для одной группы из готового manifest. `/series` и глобальный `/events` повторно не сканируются. Запускать отдельно для каждой группы; база остаётся общей.

Текущий `selection_id`:

```text
20260817T124811Z-3b15c8a0-637b555156e3
```

Три отдельных live-only запуска:

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

Для текущего этапа использовать только `--source-mode live`; `historical` и `both` пока не запускать.

Args:

| Аргумент | Назначение; default |
|---|---|
| `--selection-id` | frozen manifest; required |
| `--group` | `non_sport_crypto`, `sport` или `crypto`; required |
| `--source-mode` | `live`; также `historical`, `both` |
| `--completed-month` | UTC month для settled markets; unset |
| `--skip-event-details` | не скачивать связанные event details |
| `--refresh-event-details` | повторно скачать event details |
| `--max-pages` | test cap; unset |

Каждый запуск создаёт отдельный `metadata_runs` с group, selection hashes и статистикой; raw JSON сохраняется в `raw_payloads`.

# download_daily_candles

Скачивает native daily candles (`period_interval=1440`) для market universe, отфильтрованного по cumulative `volume_fp` и lifetime market. Один market скачивается только один раз; принадлежность к каждому фильтру сохраняется в `market_history_manifest`.

Основной запуск для `non_sport_crypto`:

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

Аналогичные запуски для остальных frozen manifest groups:

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

`union` загружает markets, прошедшие хотя бы один фильтр, и позволяет затем сравнить обе выборки без повторной загрузки. Для отдельных universe используются:

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

The command uses the live batch candlestick endpoint and does not request historical candles. `--start-date` and `--end-date` can restrict the UTC date window. `--max-batches` is available for smoke tests.

# notebooks

The supervisor-facing workflow is split into two notebooks:

1. `notebooks/daily_data_diagnostic.ipynb` reads SQLite, checks the selected candle run, and writes the prepared demo files;
2. `notebooks/daily_data_eda.ipynb` reads only those files and contains no SQL or SQLite dependency.

Run all cells in the diagnostic notebook first. It writes:

```text
data/demo/main_market_manifest.csv
data/demo/main_market_metadata.csv
data/demo/main_daily_candles.csv.gz
data/demo/export_summary.json
```

`main_market_metadata.csv` is the one-row-per-market lookup keyed by `market_id`; join it to either the manifest or daily candles to recover the market question, descriptions, event context, series context, and rules.

The diagnostic notebook automatically selects the latest `daily_candles` run for the configured `selection_id` and `non_sport_crypto` group. The exported summary preserves whether that source run was `success` or `partial`.
