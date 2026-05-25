# Phase 2 — Operationalisation: Status

**As of 2026-05-14.** Phase 2 is paused after workstreams A + B completed
end-to-end. Remaining workstreams (cloud cron / DW / public dashboard) are
deferred while I prioritise PL-300 certification + active Junior DA job
applications. The code already on this branch is production-grade and
self-contained — none of it requires the remaining workstreams to be useful.

---

## What Phase 2 turned Phase 1 into

Phase 1 ended as a frozen retrospective analysis: 8 statistical findings on
a 44-month window, locked notebooks, a 2-page Power BI report. Static.

Phase 2 added a **forward-running inference loop** that consumes near-realtime
NEMWeb data and emits `P(D reversal)` for each NEM region. The end-to-end
chain is wired and verified — two daily predictions land in
`analytics.predictions` for 2026-05-13 and 2026-05-14 (both correctly flagged
reversal=1; `hit=t` in `analytics.v_prediction_vs_actual`).

---

## Delivered (workstreams A + B)

### Workstream A — real-time NEMWeb CURRENT ingestion

| Artefact | Role |
|---|---|
| [`docs/NEMWEB_CURRENT_SCHEMA_DIFF.md`](NEMWEB_CURRENT_SCHEMA_DIFF.md) | Full schema diff vs MMSDM. Documents that nemosis 3.8.1 has no CURRENT routing for DISPATCHREGIONSUM / DISPATCHPRICE / ROOFTOP_PV_ACTUAL, with column-by-column proof that NEMWeb CURRENT publishes the same MMS schema. |
| `pipeline/_common.py` (extended) | `parse_mms_zip()` + `download_nemweb_zip()` + `df_to_insert_rows()` helpers — single source of truth for MMS-format parsing + NaN→NULL coercion. |
| `pipeline/probe_nemweb_current.py` | Read-only HTTP probe that validates URL structure + schema. Reproducible dry-run; the artefact that justifies the schema diff doc. |
| `pipeline/fetch_aemo_current.py` | 5-min DispatchIS scraper → `raw.region_5min` with `ON CONFLICT DO NOTHING`. Idempotent daily cron entry point. |
| `pipeline/fetch_rooftop_current.py` | 30-min ROOFTOP_PV_ACTUAL MEASUREMENT scraper → `raw.rooftop_pv_30min`. Same idempotency contract. |

Verified on 2026-05-14 with a full window pull: **2885 dispatch rows + 2080
rooftop rows, zero failures**, total ~3 minutes wall time.

### Workstream B — inference plumbing

| Artefact | Role |
|---|---|
| `db/03_predictions_schema.sql` | `analytics.predictions` log table + `v_prediction_vs_actual` reconciliation view. PK `(predict_for_date, regionid, model_version)` lets multiple model versions coexist for A/B. |
| `notebooks/02_reversal_classifier.ipynb` (extended) | Final cell dumps the fitted leak-free LR pipeline + feature contract (`features_leakfree` column order, `first_trading_day` anchor for `time_idx`, train end date, recorded test AUC) to `models/leak_free_lr.joblib`. |
| `pipeline/predict.py` | CLI `--date YYYY-MM-DD` / `--backfill START END` / `--dry-run`. Loads the joblib artefact, replicates the notebook's feature derivation (holiday flag, `time_idx`, `lag7_rooftop_p95_mw`, `semisched_share_yesterday`), runs `predict_proba`, upserts to `analytics.predictions`. |
| `pipeline/smoke_test_predict.py` | Replays NB02's full test window through `predict.py`, computes AUC against actual `is_reversal`, asserts it matches the artefact's pickled `test_auc` to within 1e-6. **Currently PASSING with bit-identical AUC 0.754907** — proves the feature builder didn't drift during the refactor. |

---

## Deferred (workstreams C + D + E)

| Workstream | What it would have added | Why deferred |
|---|---|---|
| **C — GitHub Actions cron + AWS S3 backup** | Daily AEST 01:00 cron running fetch_*_current → predict.py; raw parquet backed up to S3 for retention beyond NEMWeb's ~2-day CURRENT window. | High ROI but ~1-2 days work plus AWS account + secrets setup. Postponed pending job-hunt cadence; the local pipeline already works. |
| **D — Snowflake + dbt port** | Move analytics layer to Snowflake DW; rewrite the 8 Postgres views as dbt models with `not_null` / `unique` / `accepted_values` tests; `dbt docs serve` for lineage. | Snowflake free trial is 30 days; doesn't fit a 6-month job-hunt portfolio link timeline without a credit card. Architecture would need a "trial-resilient" design (S3 as source of truth, Snowflake as removable analytical layer) which is non-trivial. |
| **E — Streamlit live dashboard + README v2 + architecture diagram** | Public URL on Streamlit Community Cloud reading daily snapshot from S3; expanded README narrating the Phase 2 system. | Depends on C being live to have a snapshot to render. Architecture diagram waits until C/D are concrete. |

---

## Why pause now

The marginal cost-benefit analysis after workstreams A + B:

- **Phase 1 already clears most Junior DA bar requirements** — 8 findings, pre-registered methodology, Power BI dashboard, SQL view layer, statistical rigour. The biggest gap left at the *Junior DA resume-screening stage* is a formal certification (PL-300 / DP-203), not more engineering depth.
- **Phase 2 A + B closed the most technically substantive gap** — predict.py + CURRENT ingestion is real production code that demonstrates "I built a forecast pipeline, not just analysed retrospectively". This is the differentiator that holds in *interviews*, not in initial screens.
- **Phase 2 C + D + E target a different audience** — Senior DA / DA-DE hybrid / Analytics Engineer roles. Useful eventually but not the immediate bottleneck.
- **PL-300 has higher resume-screening throughput per hour invested** — Microsoft cert is an explicit ATS keyword filter, ~3-4 weeks part-time prep, ~70% pass rate. For a Junior DA targeting Melbourne, this is likely the larger unlock.

If interviews come back asking for "deeper system / cloud / production" evidence,
C + E in particular are well-scoped restarts (~2-3 days combined).

---

## Restart checklist

When picking Phase 2 back up:

1. Re-read [`docs/PHASE2_PLAN.md`](PHASE2_PLAN.md) (private; gitignored) — full
   workstream definitions, time estimates, risk register.
2. Re-read [`docs/NEMWEB_CURRENT_SCHEMA_DIFF.md`](NEMWEB_CURRENT_SCHEMA_DIFF.md)
   — gotchas on table naming + interval-END semantics + 2-day retention.
3. Verify `pipeline/smoke_test_predict.py` still PASSES — confirms model
   artefact didn't go stale.
4. Check `analytics.predictions` table state — anything since last run?
5. Decide on workstream order:
   - C1 (local cron via Task Scheduler) — fastest validation (~2h)
   - C2 (GitHub Actions + Neon or Snowflake) — public-facing cloud (~1d)
   - E (Streamlit dashboard) — only after C is producing daily snapshots
   - D (Snowflake/dbt) — only if Snowflake trial timing aligns with portfolio
     timeline; needs the "trial-resilient" architecture in mind

The fetcher + predict.py + smoke test are battle-tested. Anything built on
top of them inherits a green baseline.

---

## Known issues / nice-to-haves

Documented from the [Workstream A review](#):

1. **Cron timing constraint** — `predict.py` for target date D needs full-day
   D-1 data (288 dispatch + 48 rooftop intervals). Run after AEMO finishes
   publishing D-1 — recommended slot is AEST 01:00 the next day. Already
   documented in the `predict.py` module docstring.
2. **NEMWeb CURRENT retention is ~2 days** for dispatch. If cron misses 48 h
   consecutively, those intervals are unrecoverable from CURRENT (would
   require either `Reports/Archive/` scraping or waiting for the next MMSDM
   monthly publication ~10 days later). Failure alerting is a workstream-C
   responsibility.
3. **Open-Meteo Archive empirical lag is ~1 day**, not the 5 days noted in
   the original CLAUDE.md. This means Open-Meteo is usable for next-day
   prediction; the original Phase 2 plan's BoM swap (workstream A3) is
   downgraded from "blocker" to "nice to have".
4. **MMSDM / CURRENT data gap** — MMSDM has ~10-day publication lag; CURRENT
   has ~2-day retention. There is a multi-day window in the middle where
   neither source has data. Plugging this gap (NEMWeb `Reports/Archive/`)
   was not in workstream A scope. Acceptable for daily inference (only D-1
   is needed), would matter for historical backfill of a missed run.
