# NEMWeb CURRENT vs MMSDM Historical — Schema Diff (Workstream A1)

**Status:** finalised 2026-05-14. Captured from live NEMWeb CURRENT (DispatchIS report 15:40 AEST, MEASUREMENT rooftop 15:00 interval) using `pipeline/probe_nemweb_current.py`.

**Purpose:** confirm that NEMWeb CURRENT publishes the same MMS schema we already ingest from MMSDM, so the daily inference cron can pull D-1 data from CURRENT without changing `db/01_raw_schema.sql`, `db/02_analytics_views.sql`, or any downstream model code.

---

## TL;DR — verdicts

| Concern | Verdict |
|---|---|
| Column names + types for the 11 dispatch + 4 rooftop fields we use | **identical** to MMSDM |
| Timestamp semantics (SETTLEMENTDATE / INTERVAL_DATETIME) | **interval-END, same as MMSDM** |
| Time zone (AEST, no DST) | **same as MMSDM** |
| Inner CSV format | **same MMS multi-table format (I / D / C rows)** |
| `db/01_raw_schema.sql` changes needed | **none** |
| `pipeline/load_*.py` changes needed | **none** — parquet schema unchanged |

---

## 1. nemosis 3.8.1 does not route these tables through CURRENT

`nemosis.dynamic_data_compiler` for `DISPATCHREGIONSUM`, `DISPATCHPRICE`, and `ROOFTOP_PV_ACTUAL` is hard-wired to `aemo_mms_url` (`Data_Archive/.../MMSDM_Historical_Data_SQLLoader/...`). CURRENT routing in nemosis exists only for 4 unrelated tables (`BIDDING`, `DAILY_REGION_SUMMARY`, `NEXT_DAY_DISPATCHLOAD`, `INTERMITTENT_GEN_SCADA`). The Phase 2 plan's "same nemosis package, different endpoints" assumption was wrong; CURRENT ingestion needs a custom scraper.

Evidence:
- `nemosis/defaults.py:55,62,81` → `table_types[<our tables>] = "MMS"`
- `nemosis/processing_info_maps.py:260` → `downloader["MMS"] = downloader.run`
- `nemosis/downloader.py:26` → `run()` writes `defaults.aemo_mms_url`

---

## 2. DISPATCH (5-min cadence) — `DispatchIS_Reports`

### Endpoint
```
https://nemweb.com.au/Reports/Current/DispatchIS_Reports/
```

Directory listing returns 578 zip files at the time of probing (~2 days of 5-min intervals at 288/day, so retention is roughly 2 days). One zip per 5-min interval, named:
```
PUBLIC_DISPATCHIS_{YYYYMMDDHHMM}_{run_id}.zip
```
Filename timestamp = `SETTLEMENTDATE` = interval-END (verified: file `202605141540` contains `SETTLEMENTDATE = 2026/05/14 15:40:00`).

### Inner CSV — 7 tables per zip
A single `PUBLIC_DISPATCHIS_*.csv` contains 7 MMS tables, identified by their `I` rows:

| Table name (CURRENT) | MMSDM equivalent | Relevant? |
|---|---|---|
| `DISPATCH_CASE_SOLUTION` | DISPATCHCASESOLUTION | no |
| `DISPATCH_LOCAL_PRICE` | — | no |
| **`DISPATCH_PRICE`** | **DISPATCHPRICE** | **yes** |
| **`DISPATCH_REGIONSUM`** | **DISPATCHREGIONSUM** | **yes** |
| `DISPATCH_INTERCONNECTORRES` | DISPATCHINTERCONNECTORRES | no |
| `DISPATCH_CONSTRAINT` | DISPATCHCONSTRAINT | no |
| `DISPATCH_INTERCONNECTION` | — | no |

Naming difference: MMSDM uses unspaced compound names (`DISPATCHREGIONSUM`), CURRENT inserts an underscore (`DISPATCH_REGIONSUM`). The fetcher matches on the underscore form.

### Column diff for `DISPATCH_REGIONSUM`

CURRENT publishes 126 columns vs MMSDM's full schema. The 10 columns we KEEP in `pipeline/_common.py::KEEP_COLS` (minus `RRP`, which lives in `DISPATCH_PRICE`) are **all present with identical names**:

`SETTLEMENTDATE, REGIONID, INTERVENTION, TOTALDEMAND, AVAILABLEGENERATION, TOTALINTERMITTENTGENERATION, UIGF, SEMISCHEDULE_CLEAREDMW, DEMAND_AND_NONSCHEDGEN, NETINTERCHANGE`

Sample row (NSW1, 2026-05-14 15:40):
```
SETTLEMENTDATE=2026/05/14 15:40:00, REGIONID=NSW1, INTERVENTION=0, TOTALDEMAND=7835.31
```

The 116 extra columns are FCAS / raise+lower / dispatchable load / battery storage fields that we already drop via `df[KEEP_COLS]` narrowing in `fetch_aemo.py:134`. No code change needed.

### Column diff for `DISPATCH_PRICE`

CURRENT publishes 66 columns. The 4 columns we keep (`SETTLEMENTDATE, REGIONID, INTERVENTION, RRP`) are all present with identical names. The 62 extras are FCAS RRP / ROP / APCFLAG variants that we already drop.

Sample row: `SETTLEMENTDATE=2026/05/14 15:40:00, REGIONID=NSW1, INTERVENTION=0, RRP=100.05`

### Bonus over MMSDM

CURRENT bundles `DISPATCH_REGIONSUM` + `DISPATCH_PRICE` in **one zip** per 5-min interval. The MMSDM pipeline downloads two separate monthly zips and merges in `fetch_aemo.py:121`; the CURRENT fetcher can skip the second HTTP round-trip.

---

## 3. ROOFTOP_PV_ACTUAL (30-min cadence) — `ROOFTOP_PV/ACTUAL`

### Endpoint
```
https://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/
```

1,346 zip files at probe time. Two file types are interleaved — 673 each:
```
PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_{YYYYMMDDHHMMSS}_{run_id}.zip
PUBLIC_ROOFTOP_PV_ACTUAL_SATELLITE_{YYYYMMDDHHMMSS}_{run_id}.zip
```

Pipeline already filters to `TYPE='MEASUREMENT'` (`db/01_raw_schema.sql:101`). For CURRENT, filter by filename instead of by row — each CURRENT zip is a single TYPE, so filename matching avoids reading the SATELLITE half. Retention is ~14 days × 48 intervals = 672 ≈ observed 673.

### Filename time ≠ `INTERVAL_DATETIME`

Important gotcha: the filename timestamp is the **publication batch label**, not the interval-END.

Empirically (probed 2026-05-14 15:30 file):
- Filename time = `153000`
- `INTERVAL_DATETIME` in data = `2026/05/14 15:00:00`
- `LASTCHANGED` (actual publish timestamp) = `2026/05/14 15:19:00`

Interval covered = `[14:30, 15:00]` (END semantics, same as MMSDM). The `15:30` in the filename is AEMO's scheduled batch label, ~30 minutes after the interval closes.

**Fetcher implication:** to fetch the rooftop interval ending at 15:00, request the file labelled `153000`, NOT `150000`. Parse `INTERVAL_DATETIME` from the CSV body for the join, never from the filename.

### Inner CSV — single table per zip

```
I,ROOFTOP,ACTUAL,2,INTERVAL_DATETIME,REGIONID,POWER,QI,TYPE,LASTCHANGED
D,ROOFTOP,ACTUAL,2,"2026/05/14 15:00:00",NSW1,1668.625,1,MEASUREMENT,"2026/05/14 15:19:00"
```

Table name in CSV: `ROOFTOP_ACTUAL` (MMSDM equivalent: `ROOFTOP_PV_ACTUAL`).

5 D-rows per zip (one per main region: NSW1, QLD1, SA1, TAS1, VIC1). Sub-regions (QLDN/QLDC/QLDS/TASN/TASS) **not present** in CURRENT — the pipeline drops them at ingestion anyway, so this is a small win (no rows to filter out).

### Column diff

CURRENT publishes 6 columns vs MMSDM's same 6. Cols we keep (`INTERVAL_DATETIME, REGIONID, POWER, QI`) are present. Extras (`TYPE, LASTCHANGED`) we drop via `df[ROOFTOP_COLS]`.

Per-region sample (15:00 interval):
```
NSW1: 1668.625 MW
QLD1: 1429.899 MW
SA1:   833.080 MW
TAS1:   58.838 MW
VIC1: 1502.095 MW
```

---

## 4. Implications for Workstream A2 (production fetcher)

| Decision | Choice |
|---|---|
| New file: `pipeline/fetch_aemo_current.py` | one HTTPS GET per 5-min interval, parse 2 tables out of 7, narrow to KEEP_COLS, write per-day parquet |
| New file: `pipeline/fetch_rooftop_current.py` | filter filename to `_MEASUREMENT_`, one zip per 30-min interval, narrow to ROOFTOP_COLS, write per-day parquet |
| Reuse `pipeline/load_to_postgres.py` / `load_rooftop.py` | yes — parquet schema unchanged |
| Reuse `db/01_raw_schema.sql` | yes — no migrations needed |
| Reuse `db/02_analytics_views.sql` | yes |
| `pipeline/_common.py` | extend with `parse_mms_multi_table()` helper; share between MMSDM + CURRENT |
| Idempotency strategy | skip 5-min / 30-min zip if its parquet day-shard already covers that interval — same pattern as `fetch_aemo.py` |
| Backfill behaviour | CURRENT keeps ~2 days of dispatch + ~14 days of rooftop. Anything older must come from MMSDM (`fetch_aemo.py`). The two fetchers cover non-overlapping windows. |

---

## 5. Risks + mitigations

| Risk | Mitigation |
|---|---|
| AEMO renames table from `DISPATCH_REGIONSUM` back to `DISPATCHREGIONSUM` mid-cron | Match on regex `DISPATCH_?REGIONSUM` in the parser |
| AEMO retention drops below D-1 window (probed 2 days for dispatch is tight) | Cron runs daily at AEST 21:00 (after NEM close) so D-1 dispatch is always within retention |
| MEASUREMENT publication lag > 30 min | LASTCHANGED was 19 min after interval close on the probe; gives a 11-min cushion. If exceeded, fall back to SATELLITE filename (lower-quality but always-present) and tag rows with `data_quality` flag — Phase 3 if it becomes an issue. |
| Filename batch-time labelling changes | Parser reads `INTERVAL_DATETIME` from the CSV body, not the filename, so this is robust to AEMO relabelling. |
