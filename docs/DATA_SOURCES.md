# Data Sources & Attribution

All raw inputs are public and free to access. This project ingests three
upstream datasets; each is governed by its own license terms (summarised
below). The `LICENSE` file at repo root covers the project code only —
the upstream data is subject to the providers' terms.

---

## 1. AEMO MMSDM — Dispatch (`raw.region_5min`)

| | |
|---|---|
| **Provider** | Australian Energy Market Operator (AEMO) |
| **Dataset** | DISPATCHREGIONSUM + DISPATCHPRICE (joined) |
| **Format** | Monthly zip archives on the MMSDM Historical Data website |
| **Grain** | 5-minute regional dispatch, 5 NEM regions |
| **Access** | Public HTTP, no authentication; fetched via the `nemosis` package which mirrors AEMO's archive layout |
| **Window used** | 2022-08-01 → 2026-03-31 (44 months, ~1.93M rows) |
| **License** | AEMO publishes MMSDM as public data, free to use with attribution. See AEMO's data terms at <https://www.aemo.com.au/Privacy-and-legal-notices/Copyright-permissions>. |
| **Attribution** | "Contains AEMO MMSDM data. © AEMO." |

---

## 2. AEMO MMSDM — Rooftop PV (`raw.rooftop_pv_30min`)

| | |
|---|---|
| **Provider** | Australian Energy Market Operator (AEMO) |
| **Dataset** | ROOFTOP_PV_ACTUAL, filtered to `TYPE='MEASUREMENT'` |
| **Format** | Same MMSDM monthly archive as dispatch |
| **Grain** | 30-minute behind-the-meter rooftop PV generation, 5 NEM regions |
| **Access** | Public HTTP, no authentication; fetched via `nemosis` |
| **Window used** | 2022-08-01 → 2026-03-31 (~320K rows after region + TYPE filter) |
| **License** | Same as dispatch — AEMO public data with attribution |
| **Attribution** | "Contains AEMO MMSDM ROOFTOP_PV_ACTUAL data. © AEMO." |

Note: AEMO publishes both `MEASUREMENT` and `SATELLITE` series. We keep only
`MEASUREMENT` because both series have identical row coverage in the project
window with no additional analytical value from the satellite backup.

---

## 3. Open-Meteo Archive API — Daily weather (`raw.weather_daily`)

| | |
|---|---|
| **Provider** | Open-Meteo (open-meteo.com) |
| **Dataset** | Historical Weather API (ERA5 reanalysis backend) |
| **Format** | JSON over HTTPS, no authentication required for non-commercial use |
| **Grain** | Daily aggregates per NEM region capital city (5 rows/day) |
| **Variables** | `temperature_2m_max`, `temperature_2m_min`, `shortwave_radiation_sum`, `sunshine_duration`, `precipitation_sum` |
| **Capitals used** | NSW1=Sydney, VIC1=Melbourne, QLD1=Brisbane, SA1=Adelaide, TAS1=Hobart |
| **Timezone** | `Australia/Brisbane` (AEST, no DST) for **all** regions, to align with NEM trading_day |
| **Window used** | 2022-08-01 → 2026-03-31 (~6,695 rows = 1,339 days × 5 regions) |
| **License** | Open-Meteo data is licensed under **CC BY-NC 4.0** (non-commercial). Free for non-commercial research and portfolio use. Commercial use requires a paid API plan with Open-Meteo. |
| **Attribution** | "Weather data by Open-Meteo.com" |

References:
- Open-Meteo terms: <https://open-meteo.com/en/terms>
- ERA5 (the underlying reanalysis): Hersbach et al. (2020), *The ERA5 global reanalysis*, Q.J.R. Meteorol. Soc. 146: 1999–2049

---

## ERA5 publication latency caveat

Open-Meteo's archive endpoint backs onto ECMWF ERA5, which is typically
published with a 5-day rolling latency. `pipeline/fetch_open_meteo.py`
enforces a 99 %-coverage gate on the fetched window, so if a request
includes the last few days it will fail loudly rather than silently
ingest partial rows. For automated incremental pulls, call the fetcher
with `end_date ≤ today − 7 days`. The dashboard window ends on
2026-03-31, so this latency does not affect any analytical results
in this repo.

---

## What is *not* in this repo

- **Raw archive bytes** — `data/raw/` and `data/parquet/` are gitignored.
  They are regenerable end-to-end from `pipeline/fetch_*.py` against the
  public upstream sources.
- **Personally-identifying information** — none of the three sources
  contain PII. AEMO publishes aggregate regional metrics; Open-Meteo
  publishes gridded weather.
- **Sensitive market positions** — AEMO MMSDM is published with a delay
  long enough that no traded positions could be reconstructed from it.

---

## Reproducing the data layer

```bash
# 1. Set credentials
cp .env.example .env  # then fill DB_PWD

# 2. Build schemas
psql -U postgres -d nem -f db/01_raw_schema.sql

# 3. Fetch + load each source (idempotent, monthly chunked)
python pipeline/fetch_aemo.py
python pipeline/load_to_postgres.py

python pipeline/fetch_rooftop.py
python pipeline/load_rooftop.py

python pipeline/fetch_open_meteo.py
python pipeline/load_weather.py

# 4. Build analytics views on top
psql -U postgres -d nem -f db/02_analytics_views.sql
```

All three fetchers are idempotent (skip already-cached months / days) and
chunked, so a re-run after an interrupted load resumes cleanly.
