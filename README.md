# Operational Demand Reversal in the Australian National Electricity Market (NEM), 2022–2026

![Python](https://img.shields.io/badge/Python-3.11-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-336791)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811)
![Status](https://img.shields.io/badge/Phase%201-Complete-brightgreen)
![Status](https://img.shields.io/badge/Phase%202-Partial%20(A%20%2B%20B)-yellow)

Quantifying how rooftop solar has flipped the NEM from max-demand-constrained to **min-demand-constrained** across all 5 regions over 44 months.

Pipeline: Australian Energy Market Operator (AEMO) MMSDM + Open-Meteo Archive → PostgreSQL → analytics views → Python statistical notebooks + Power BI dashboard.

---

## Why this matters

Australia's grid was historically sized around the evening peak. The 6–8 pm winter maximum drove capacity planning, operating reserves, ramping reserves, and inter-regional flow design.

Behind-the-meter rooftop PV, now installed on roughly one in three Australian homes, has shifted that constraint to the other end of the daily load curve. In South Australia, **92 % of trading days** now have their minimum operational demand land between 10 am and 3 pm rather than pre-dawn.

Curtailment of grid-scale renewables, negative spot prices, and AEMO's minimum-demand security limits are symptoms of the same regime shift. Measuring how fast and how unevenly it is propagating across the five NEM regions matters for any planning question still calibrated against the old peak-constrained system.

---

## Problem Statement

Three pre-registered questions about NEM demand behaviour from August 2022 to March 2026:

- Is the daily-minimum operational demand systematically landing at midday rather than pre-dawn, and where is that pattern most established?
- Is the pattern consistent with a behind-the-meter rooftop PV mechanism, or are rooftop generation and demand reversal merely both trending upward over time?
- Are weather drivers of midday demand decoupled from drivers of peak demand, and can next-day reversal be forecast from end-of-today information?

---

## Executive Summary

- South Australia is already in a minimum-demand regime: **92 %** of trading days have their daily-minimum operational demand between 10 am and 3 pm.
- Rooftop-PV flow is the strongest explanatory variable for reversal magnitude in all four behind-the-meter-solar mainland regions, after trend and seasonality are removed (Newey-West HAC OLS, p ≤ 0.007).
- Tasmania's reversal frequency climbed similarly, but three independent tests isolate hydro dispatch — not rooftop PV — as the underlying mechanism.

---

## Key Findings

Pre-registered hypothesis verdicts are in [`docs/METHODOLOGY.md`](docs/METHODOLOGY.md). Eight findings below; numbers and figures are from the production run.

**1. South Australia has fully saturated on frequency, and is still deepening on magnitude.**
SA reversal frequency = **92 % of trading days** (std 7 pp across 44 months — already a regime, not isolated events). The frequency leg has hit its ceiling, but the deepest December monthly minimum keeps dropping: +120 MW (Dec 2022) → **−311 MW (Dec 2025)**. The two halves of the story have decoupled.

<img src="figures/01_sa_freq_vs_magnitude.png" alt="Finding 1 — SA frequency saturated × magnitude deepening" width="700">

**2. The pre-dawn / midday demand gap is widening in every region.**
Trend-controlled OLS on monthly `gap = h03_mean − h13_mean` per region, all five regions p < 10⁻⁴. SA = **+90 MW / year**, VIC = +186, NSW = +237, QLD = +151, TAS = +19. Caveat: SA's hour-03 baseline drifted +7 % over the same window, so some of the widening reflects a higher baseline rather than purely a deeper midday trough — reported as a methodological caveat, not a contradiction of the finding.

<img src="figures/02_sa_h03_vs_h13.png" alt="Finding 2 — h03 vs h13 demand gap" width="700">

**3. Cross-region order tracks BTM-solar penetration, with Tasmania flagged as a hydro outlier from the start.**
Reversal frequency: SA 92 % / QLD 71 % / VIC 58 % / NSW 56 % — monotone in the expected penetration ordering. TAS climbed from 16 % to 74 % across the window but on a different mechanism (see Cross-validating findings).

<img src="figures/03_cross_region_trajectory.png" alt="Finding 3 — Cross-region trajectory" width="700">

**4. Rooftop-PV flow explains reversal magnitude in every BTM-solar region, after trend and seasonality are controlled for.**
Per-region OLS `deepest_min_demand ~ p95_rooftop_mw + time_idx + C(calendar_month)` with Newey-West HAC standard errors (maxlags = 6) absorbs trend and autocorrelation so the rooftop coefficient is identified by month-to-month flow variation. SA β = **−0.34** (p = 0.007), VIC −1.20 (3×10⁻⁵), NSW −1.00 (1×10⁻⁴), QLD −0.63 (0.003); Tasmania sign-flips to **β = +0.77 (p = 0.48)** — first independent flag that TAS reversal is not consistent with a BTM-PV mechanism.

<img src="figures/04_rooftop_partial_residual.png" alt="Finding 4 — Rooftop-PV partial-residual scatter" width="700">

**5. Weekend amplifies reversal frequency in VIC and NSW.**
Weekend reversal-rate lift of **+19 pp in VIC, +19 pp in NSW** (chi-square p < 10⁻¹⁰). The behavioural channel (industrial / commercial midday load missing on weekends) shows up cleanly in mainland mid-penetration regions. SA shows no lift because weekday frequency is already saturated at 92 %; QLD is non-significant.

<img src="figures/05_weekend_uplift.png" alt="Finding 5 — Weekend uplift" width="700">

**6. Weather drivers of min and max demand are *shared*, not decoupled — but weather explains min demand more reliably.**
Pre-registered H4b (driver decoupling) rejected in 0 of 5 regions: solar radiation is the strongest negatively-signed predictor of both daily extremes. Fallback H4' confirms instead — R²_min > R²_max in 4 of 5 regions, with material gaps (SA Δ = +0.30, VIC +0.17, NSW +0.19, QLD +0.28), consistent with `weather → BTM-PV → min-demand` being a tighter physical chain than `weather → air-conditioning behaviour → max-demand`. Tasmania is the only flip — second independent TAS flag.

<img src="figures/06_weather_coef_compare.png" alt="Finding 6 — Weather coefficient comparison" width="700">

**7. Reversal days and negative-pricing days co-occur tightly in BTM-solar regions.**
Per-region Fisher conditional odds ratio for `is_reversal_day × had_negative_RRP_today`. VIC OR = **4.78**, NSW 5.26, QLD 5.64 (all p < 10⁻²⁵). SA OR = 2.75 is muted by saturation (73 % of SA's non-reversal days already carry negative pricing — no headroom). Tasmania OR = 0.99 with a CI straddling 1.0 — third independent TAS flag.

<img src="figures/07_neg_pricing_or_forest.png" alt="Finding 7 — Odds ratio forest plot" width="700">

**8. Tomorrow's reversal is predictable from today's information, but only modestly.**
VIC 24h-ahead classifier across three tracks: persistence baseline AUC = **0.631** → leak-free LR AUC = **0.755** (lift +0.124 ✓) → forecast-proxy RF with same-day weather AUC = **0.923** (ceiling, not deployable). Lift over persistence passes the pre-registered bar; the absolute AUC ≥ 0.80 bar misses, so the result is reported as a modest production signal rather than publication-grade.

<table>
<tr>
<td><img src="figures/08_ml_roc.png" alt="Finding 8 — ROC curve, all model tracks" width="380" height="380"></td>
<td><img src="figures/08_ml_rf_importance.png" alt="Finding 8 — Random Forest permutation importance" width="575" height="380"></td>
</tr>
</table>

---

## Cross-validating findings

Three independent tests, three convergent conclusions. The strongest conclusions here are triangulated across independent specifications rather than resting on a single model.

- **Tasmania's reversal is hydro dispatch, not rooftop PV.** Finding 4 (rooftop coefficient sign-flips positive, p = 0.48), Finding 6 (R²_max > R²_min — opposite asymmetry to every other region), and Finding 7 (OR ≈ 1, CI straddles 1) all isolate TAS through entirely different statistical machinery. The convergence is what makes the conclusion defensible.
- **South Australia's saturation regime shows up as muted signal everywhere.** Finding 5 (no weekend lift — already saturated weekdays), Finding 7 (OR muted at 2.75 — no headroom), and the H1 / H5' split between frequency and magnitude all reflect the same underlying ceiling effect.
- **Shared weather drivers (H4b reject) support Findings 1, 2, and 4.** Solar-related weather variables are associated with lower demand at both daily extremes, not just the midday trough — so a single rooftop-PV channel is consistent with Findings 1, 2, and 4 without needing two separate mechanisms.

---

## Dashboard (Power BI)

Two-page interactive report. Page 1 has a region slicer that cross-filters its five hero charts; Page 2 is methodology + ML summary, not slicer-driven.

| Page | Focus |
|---|---|
| 1. Findings | 5 hero charts: SA dual-axis (Finding 1), 5-region trajectory (Finding 3), h03/h13 fan (Finding 2), rooftop magnitude scatter (Finding 4), odds-ratio forest (Finding 7) |
| 2. Methodology & ML | Partial-residual scatter (Finding 4 magnitude), 3 AUC KPI cards, ROC + RF permutation importance (Finding 8) |

<img src="dashboard/page1_findings.png" alt="Page 1 — Findings" width="850">

Page 2 is a methodology + ML walkthrough — the Finding 4 partial-residual scatter and the Finding 8 model benchmark consolidated into a single Power BI view. Open [`dashboard/nem_reversal.pbix`](dashboard/nem_reversal.pbix) to explore it interactively.

---

## Phase 2 — Operationalisation (in progress)

Phase 1 above is the frozen retrospective analysis. Phase 2 turns it into a forward-running daily inference loop — same model, same features, but consuming near-realtime NEMWeb data instead of MMSDM monthly archives.

**Delivered (workstreams A + B):**

- **`pipeline/predict.py`** — CLI inference (`python pipeline/predict.py --date YYYY-MM-DD`) that loads the Phase 1 leak-free LR pipeline from a joblib artefact and writes `P(D reversal)` to a Postgres `analytics.predictions` log. Replicates the notebook feature derivation (holiday flag, `time_idx`, lag-7 rooftop P95, semi-scheduled share) so the production output is bit-identical to the notebook's `predict_proba`.
- **`pipeline/smoke_test_predict.py`** — replays the full NB02 test window through `predict.py` and asserts the realised AUC matches the artefact's pickled `test_auc` to within 1e-6. Currently passing with `gap = 0.00e+00`.
- **`pipeline/fetch_aemo_current.py` + `pipeline/fetch_rooftop_current.py`** — NEMWeb CURRENT scrapers for daily incremental ingestion. The original Phase 2 plan assumed nemosis would route CURRENT URLs transparently for these tables — [it doesn't](docs/NEMWEB_CURRENT_SCHEMA_DIFF.md), so a custom scraper was needed. Both are idempotent via `ON CONFLICT DO NOTHING` on the existing primary keys; no schema migrations required.
- **`db/03_predictions_schema.sql`** — `analytics.predictions` table (PK lets multiple `model_version`s coexist for A/B) + `v_prediction_vs_actual` reconciliation view.
- **End-to-end verification** — predictions for 2026-05-13 and 2026-05-14 (the first dates produced from CURRENT data alone) both correctly flagged reversal=1 with high probability (P=0.89, 0.77); `hit=t` in the reconciliation view.

**Deferred (workstreams C + D + E):**

GitHub Actions cron, AWS S3 raw backup, Snowflake + dbt port, and the public Streamlit dashboard are scoped but not yet executed. Decision context, restart checklist, and known issues are documented in [`docs/PHASE2_STATUS.md`](docs/PHASE2_STATUS.md). The pause is deliberate — Phase 2 A + B is the technical substance most relevant to a Junior DA portfolio; C + D + E target an Analytics Engineer / Senior DA audience and can resume when needed.

---

## Methodology

Full pre-registered hypotheses, thresholds, and verdicts in [`docs/METHODOLOGY.md`](docs/METHODOLOGY.md).

| Technique | Detail |
|---|---|
| Trend-controlled OLS (Finding 4) | `target ~ rooftop_proxy + time_idx + C(calendar_month)` with Newey-West HAC SE (maxlags = 6). `time_idx` absorbs secular trend so the rooftop coefficient is identified by month-to-month flow variation rather than time-correlation. |
| Partial-residual plot (Finding 4) | Frisch-Waugh-Lovell: both axes residualised on `time_idx + C(calendar_month)` before plotting, so the visible slope equals the trend-controlled partial coefficient of the full HAC model. |
| Fisher conditional odds ratio (Finding 7) | 2×2 contingency `is_reversal_day × had_negative_RRP_today`; Wald log-OR 95 % CI, chi-square cross-check. |
| Weekend chi-square (Finding 5) | 2×2 contingency `is_weekend × is_reversal_day`. i.i.d. assumption violated by autocorrelation; reported p-values optimistic but verdicts robust at p ~ 10⁻¹⁰. |
| ML benchmark (Finding 8) | Three tracks: persistence baseline, leak-free LR/RF (only end-of-D-1 features), forecast-proxy LR/RF (adds same-day weather). Feature selection by explicit whitelist; chronological train / test split at 2025-01-01. |
| Feature importance (Finding 8) | Permutation importance on held-out test set (20 repeats, ΔAUC scoring) — robust to multicollinearity, unbiased to feature cardinality. Gini importance kept as appendix. |

---

## Caveats and limitations

Pulled in full from [`docs/METHODOLOGY.md § Caveats`](docs/METHODOLOGY.md#caveats-and-assumptions); the most important:

- **Forecast-proxy AUC of 0.923 uses same-day actual weather** as a stand-in for a 24 h NWP forecast. Deployed AUC would land between leak-free 0.755 and the forecast-proxy ceiling depending on real NWP solar 24 h skill, not at 0.923.
- **`time_idx` is a linear time index, not a measure of installed BTM-PV capacity.** It absorbs anything that drifts monotonically over 44 months — cumulative rooftop stock is the most plausible single contributor but battery uptake, retail tariff change, and behavioural change all also contribute.
- **i.i.d. assumption** in chi-square / Fisher tests is technically violated by daily autocorrelation. Reported p-values are optimistic. Passing-region p-values span 10⁻³ to 10⁻⁴⁰, so even aggressive autocorrelation correction would not invalidate the verdicts; a clustered-SE logistic regression would be the principled robustness check and is left as a future improvement.
- **One AEMO upstream gap** — 2024-09-05, two 30-min rooftop intervals missing across all five regions. NULL-guarded in the view layer; ML training drops two VIC rows.
- All findings are **descriptive associations, not causal**. Inter-regional flow effects, retail-tariff dynamics, and behavioural responses are explicitly out of scope.

---

## Dataset

| | |
|---|---|
| **Window** | 2022-08-01 → 2026-03-31 (44 months, 1,339 trading days) |
| **Regions** | NSW1, VIC1, SA1, QLD1, TAS1 (four mainland regions plus Tasmania) |
| **Dispatch** | AEMO MMSDM `DISPATCHREGIONSUM` ⨝ `DISPATCHPRICE`, INTERVENTION = 0 — ~1.93 M rows at 5-min |
| **Rooftop PV** | AEMO MMSDM `ROOFTOP_PV_ACTUAL`, TYPE = MEASUREMENT — ~321 K rows at 30-min |
| **Weather** | Open-Meteo Archive API (ERA5 reanalysis), 5 regional capitals, Australia/Brisbane tz — ~6.7 K rows daily |
| **Storage** | PostgreSQL 14 (`raw.*` + `analytics.*` schemas) |

Source license + attribution detail in [`docs/DATA_SOURCES.md`](docs/DATA_SOURCES.md).

> **Note:** Raw parquet / nemosis cache is not included in this repo (regenerable end-to-end from the pipeline scripts against public upstream sources).

---

## Tech Stack

- **Pipeline:** Python, pandas, nemosis (AEMO MMSDM archive), requests + BeautifulSoup (NEMWeb CURRENT scrape), SQLAlchemy
- **Database:** PostgreSQL 14
- **Analysis:** numpy, pandas, statsmodels (OLS + Newey-West HAC), scipy (Fisher, chi-square), scikit-learn (LR / RF / permutation importance), holidays
- **Inference (Phase 2):** joblib model artefact + CLI inference; `psycopg2.extras.execute_values` for `ON CONFLICT DO NOTHING` upserts
- **Dashboard:** Power BI Desktop, custom theme JSON
- **Sources:** [AEMO MMSDM](https://aemo.com.au/) (historical) + [NEMWeb CURRENT](https://nemweb.com.au/Reports/Current/) (Phase 2 realtime), [Open-Meteo Archive](https://open-meteo.com/), [`holidays`](https://pypi.org/project/holidays/) Python package

---

## Project Structure

```
nem_negative_pricing/
├── pipeline/
│   ├── _common.py              # KEEP_COLS / REVERSAL_HOURS + MMS parser / NaN→NULL helpers
│   ├── fetch_aemo.py           # Dispatch MMSDM fetcher (Phase 1; idempotent, monthly chunked)
│   ├── load_to_postgres.py     # Dispatch parquet → Postgres COPY
│   ├── fetch_rooftop.py        # ROOFTOP_PV_ACTUAL MMSDM fetcher
│   ├── load_rooftop.py         # Rooftop parquet → Postgres
│   ├── fetch_open_meteo.py     # Open-Meteo Archive fetcher
│   ├── load_weather.py         # Weather parquet → Postgres
│   ├── export_for_powerbi.py   # Precompute FWL residuals + Fisher OR for PB
│   ├── fetch_aemo_current.py   # Phase 2 — NEMWeb CURRENT 5-min dispatch scraper
│   ├── fetch_rooftop_current.py# Phase 2 — NEMWeb CURRENT 30-min rooftop scraper
│   ├── probe_nemweb_current.py # Phase 2 — read-only schema probe (dry-run)
│   ├── predict.py              # Phase 2 — CLI inference (writes analytics.predictions)
│   └── smoke_test_predict.py   # Phase 2 — regression test vs NB02 pickled test AUC
├── db/
│   ├── 01_raw_schema.sql       # 3 raw tables (dispatch / rooftop / weather)
│   ├── 02_analytics_views.sql  # 8 analytics views
│   └── 03_predictions_schema.sql # Phase 2 — analytics.predictions + reconciliation view
├── notebooks/
│   ├── 01_descriptive.ipynb    # Findings 1–3 + sub-findings
│   └── 02_reversal_classifier.ipynb  # Findings 4–8
├── dashboard/
│   ├── nem_reversal.pbix       # 2-page Power BI source
│   ├── page1_findings.png
│   ├── page2_methodology.png
│   ├── theme.json              # Colour palette
│   └── data/                   # Precomputed CSVs for PB
├── figures/                    # 14 notebook-generated PNG outputs (10 embedded in this README)
├── docs/
│   ├── METHODOLOGY.md          # Pre-registered hypotheses + verdicts
│   ├── DATA_SOURCES.md         # AEMO / Open-Meteo attribution
│   ├── PHASE2_STATUS.md        # Phase 2 — delivered / deferred / restart checklist
│   └── NEMWEB_CURRENT_SCHEMA_DIFF.md # Phase 2 — schema diff CURRENT vs MMSDM
├── models/                     # Phase 2 — joblib artefacts (gitignored; regenerable from NB02)
├── environment.yml             # conda env spec
├── .env.example                # Postgres connection template
└── README.md
```

---

## Reproducing Locally

```bash
# 1. Environment
conda env create -f environment.yml
conda activate nem_demand
cp .env.example .env                                # then fill DB_PWD

# 2. Database schemas
createdb -U postgres nem
psql -U postgres -d nem -f db/01_raw_schema.sql
psql -U postgres -d nem -f db/02_analytics_views.sql

# 3. Ingest (idempotent — safe to re-run; ~30 min cold cache)
python pipeline/fetch_aemo.py        && python pipeline/load_to_postgres.py
python pipeline/fetch_rooftop.py     && python pipeline/load_rooftop.py
python pipeline/fetch_open_meteo.py  && python pipeline/load_weather.py

# 4. Precompute Power BI artefacts
python pipeline/export_for_powerbi.py

# 5. Run notebooks
jupyter lab notebooks/01_descriptive.ipynb
jupyter lab notebooks/02_reversal_classifier.ipynb
```

---

## Documentation

- [`docs/METHODOLOGY.md`](docs/METHODOLOGY.md) — pre-registered hypotheses, thresholds, verdicts, methodology audit trail
- [`docs/DATA_SOURCES.md`](docs/DATA_SOURCES.md) — AEMO + Open-Meteo licenses, attribution, ERA5 publication latency

---

## Source Files

The Power BI source file [`dashboard/nem_reversal.pbix`](dashboard/nem_reversal.pbix) is included in this repository. GitHub does not render `.pbix` inline — download and open with [Power BI Desktop](https://powerbi.microsoft.com/desktop/) to inspect measures, visuals, and the colour theme.
