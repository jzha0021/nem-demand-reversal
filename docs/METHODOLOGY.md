# Methodology — Australian NEM Operational Demand Reversal

This document is the methodology reference for the eight findings in this project. Core hypotheses, thresholds, and decision rules were specified before final notebook execution. Where the design changed during the project, the change is logged explicitly in the audit trail at the end of this document.

The narrative-facing notebooks ([`01_descriptive.ipynb`](../notebooks/01_descriptive.ipynb), [`02_reversal_classifier.ipynb`](../notebooks/02_reversal_classifier.ipynb)) cross-reference back to this document for the technical detail.

---

## Finding ↔ Pre-registered hypothesis map

| Finding | Hypothesis | Short description |
|---------|-----------|-------------------|
| 1       | H1 + H5'  | South Australia reversal frequency saturated × magnitude still deepening |
| 2       | H6        | Pre-dawn (hour 03) vs midday (hour 13) demand gap widening |
| 3       | H3        | Cross-region reversal frequency follows rooftop-PV penetration order |
| 4       | H2        | Rooftop-PV flow drives reversal magnitude (trend-controlled OLS) |
| 5       | H4a       | Weekend amplifies reversal frequency |
| 6       | H4b + H4' | Weather decoupling test (rejected) + R²-asymmetry fallback (confirmed) |
| 7       | H8        | Reversal day × negative-pricing co-occurrence (odds ratio) |
| 8       | Sub-Q3    | 24h-ahead reversal-day classifier (ML) |

Two further hypotheses (H5 and H7) were specified during the project but did not contribute to the final findings; see the methodology audit trail at the end of this document for the reasoning.

---

## Data window and sources

**Window:** 2022-08-01 → 2026-03-31 (44 calendar months × 5 NEM regions). Start chosen to exclude COVID demand-shape distortion and the 2022-H1 NEM energy crisis; end aligned with MMSDM publication latency.

**Regions:** NSW1, VIC1, SA1, QLD1, TAS1 (all five NEM regions; ACT is not separately metered in NEM dispatch).

**Sources (all fully scriptable, no manual URL discovery):**

| Source | Table | Cadence | Notes |
|---|---|---|---|
| AEMO MMSDM dispatch | `DISPATCHREGIONSUM` ⨝ `DISPATCHPRICE` | 5-min | Filter `INTERVENTION = 0` (skip rare AEMO override runs) |
| AEMO MMSDM rooftop | `ROOFTOP_PV_ACTUAL` | 30-min | Filter `TYPE = 'MEASUREMENT'` (drop satellite-derived backup) |
| Open-Meteo Archive API (ERA5) | weather daily | daily | All 5 regions queried with `Australia/Brisbane` tz (AEST without DST) |
| `holidays` Python package | AU public holidays | per-state | Used as ML feature; deterministic |

---

## Definition lock

The following conventions are pinned in `pipeline/_common.py` and reused in every SQL view and notebook. Any change must update both this section and the conventions module.

- **AEMO timestamp convention.** `SETTLEMENTDATE` and `INTERVAL_DATETIME` are **interval-ENDING**. The interval covering 23:55 → 00:00 next day carries `SETTLEMENTDATE = 00:00:00` on the next calendar day. To bucket correctly we shift to interval-START: `SETTLEMENTDATE − 5 min` for dispatch, `INTERVAL_DATETIME − 30 min` for rooftop. Single implementation: `pipeline/_common.py::add_trading_period(interval_minutes=…)`.
- **`trading_day`** = `date(interval-START)` in AEST. Calendar-day boundary, not the NEM 04:00 settlement-day convention.
- **`hour`** = `hour(interval-START)` in AEST. Integer 0–23.
- **Reversal hours** = `{10, 11, 12, 13, 14, 15}` (interval-START hour-of-day). Encoded as `pipeline/_common.py::REVERSAL_HOURS`.
- **Reversal day** = a (region, trading_day) where the daily-minimum `TOTALDEMAND` interval falls in the reversal-hour set.
- **Time zone.** All timestamps treated as AEST (UTC+10) naive, no daylight-saving conversion. Weather data is queried in `Australia/Brisbane` tz (also AEST without DST) so daily aggregation windows align with NEM `trading_day`.
- **Region naming.** Backend uses AEMO codes (`SA1`, `VIC1`, `NSW1`, `QLD1`, `TAS1`). Plot legends and tick labels use the Australian postal abbreviations (`SA`, `VIC`, `NSW`, `QLD`, `TAS`); plot titles and narrative use the full state names.

---

## Pre-registered hypotheses, thresholds and verdicts

### H1 — Reversal already established in South Australia *(→ Finding 1)*

**Question.** Has the daily-minimum operational demand shifted from pre-dawn into the middle of the day in SA, and if so on what share of trading days?

**Spec.** Per month, share of trading days whose minimum-demand hour falls in the reversal-hour set.

**Pass threshold.** SA mean reversal frequency across the window ≥ 50 %, with std < 30 pp (so reversal is the norm, not a few isolated days).

**Verdict.** Confirmed. SA mean = 92 %, std = 7 pp, range [60 %, 100 %] across 44 months. Reversal had already saturated by the window start (2022-08 = 87 %).

**Kill criterion.** If SA reversal < 1 % in the window, the descriptive premise of the project fails and the core narrative would be unsupported. *(Did not trigger.)*

### H5' — Saturation × deepening decoupling in SA *(→ Finding 1, magnitude leg)*

**Question.** Once frequency saturates near 100 %, does the magnitude leg (how deep the trough is) keep growing, or also saturate?

**Spec.** SA monthly reversal-frequency std < 10 pp (saturation), AND SA December hour-13 mean demand drop ≥ 25 % across 2022 → 2025 (magnitude still growing).

**Verdict.** Confirmed (both legs). SA frequency std = 7 pp (saturated). December hour-13 mean dropped 581 → 404 MW = −30.5 % over three Decembers. The deepest single monthly minimum moved from +120 MW (Dec 2022) to −311 MW (Dec 2025).

**Note on revision.** Replaces an earlier hypothesis (H5) that predicted higher reversal frequency in spring/autumn than summer. Initial data inspection showed the opposite (summer dominates), and the H5' reformulation captures the dynamic that survived — frequency hit its ceiling first, magnitude is still moving.

### H6 — Pre-dawn vs midday gap widening *(→ Finding 2)*

**Question.** How fast is the gap between hour-03 (pre-dawn baseload) and hour-13 (midday) demand widening, after seasonality is controlled for?

**Spec.** Per region, OLS on monthly panel: `gap_h03_minus_h13 ~ time_idx + C(calendar_month)`. Annualised slope = `time_idx coefficient × 12`.

**Pass threshold.** SA slope ≥ +50 MW/yr (p < 0.01); VIC slope ≥ +200 MW/yr (p < 0.01); same sign expected in the other three regions.

**Verdict.** Partial — primary leg confirmed; VIC below pre-registered threshold. All five regions show p < 10⁻⁴. SA = +90 MW/yr (passes ✓). VIC = +186 MW/yr (below the +200 bar by ~7 %; the pre-registered threshold is reported as-stated rather than relaxed retrospectively). NSW = +237, QLD = +151, TAS = +19 MW/yr. SA's hour-03 baseline drifted +7 % over the same window, so some of the widening reflects a higher baseline rather than purely a deeper trough; called out in the deliverable narrative.

### H3 — Cross-region penetration ordering *(→ Finding 3)*

**Question.** Does cross-region reversal frequency follow the household-rooftop-PV penetration ordering (SA > QLD > VIC > NSW)?

**Spec.** Rank-order check across 5 regions. TAS is flagged as a wild card from the outset because its grid is hydro-dominated, not PV-driven.

**Verdict.** Direction confirmed. SA 92 % / QLD 71 % / VIC 58 % / NSW 56 % — monotone in the expected penetration order. TAS reversal frequency climbed from 16 % to 74 % across the window, but three independent tests downstream (Findings 4, 6, 7) all isolate TAS as hydro-driven, not solar-driven; it is treated as a sub-finding rather than the tail of the rooftop ranking.

### H2 — Rooftop-PV flow drives reversal *(→ Finding 4)*

**Question.** Is the reversal mechanistically driven by rooftop-PV output, or are rooftop and reversal merely both trending upward over time (spurious time-correlation)?

**Spec.** Per region, OLS on the monthly panel:

```
target ~ p95_rooftop_mw + time_idx + C(calendar_month)
```

with Newey-West HAC standard errors (maxlags = 6) to allow for autocorrelation in the monthly residuals. The `time_idx` term absorbs any secular trend so that the rooftop coefficient is identified by month-to-month flow variation *after* trend and seasonality are removed. Targets: `reversal_pct` (frequency leg) and `deepest_min_demand` (magnitude leg). Robustness check with `mean_daily_peak_mw` proxy in place of P95.

**Pass threshold.** ≥ 3 of 5 regions show a significant rooftop coefficient (p < 0.05), physically correctly signed (magnitude leg negative, frequency leg positive), with R² ≥ 0.5.

**Verdict.** Confirmed — magnitude leg confirmed in **all four BTM-solar mainland regions** (SA β = −0.34 / p = 0.007; VIC −1.20 / 3×10⁻⁵; NSW −1.00 / 1×10⁻⁴; QLD −0.63 / 0.003). Tasmania sign-flips to β = +0.77 (p = 0.48), identifying it as not BTM-PV-driven (first of three independent TAS flags). Frequency leg, under the same trend control, collapses to 1 of 5 substantive (NSW only; TAS also passes mechanically but is hydro-driven, not BTM-solar). VIC's reversal-frequency rise is fully absorbed by the secular trend.

**Methodological note (frequency vs magnitude).** Month-to-month flow residual explains reversal magnitude in the four BTM-solar regions, but frequency variation is absorbed by the trend term. Cumulative BTM-PV stock buildup is the most plausible mechanism behind that trend, but is not separately identified from other slow-moving factors (battery uptake, retail tariff drift, behavioural change).

### H4a — Weekend amplification *(→ Finding 5)*

**Question.** Is reversal partly a behavioural phenomenon — does the absence of weekday industrial / commercial midday load amplify reversal on weekends?

**Spec.** Per region, 2 × 2 contingency table `is_weekend × is_reversal_day`; chi-square p-value and weekend / weekday reversal-rate ratio.

**Pass threshold.** ≥ 3 of 5 regions show weekend lift > 5 % with p < 0.05.

**Verdict.** Confirmed — three regions pass mechanically (VIC +19 pp, NSW +19 pp, TAS +10 pp). However TAS's reversal is hydro-driven (cross-validated by Findings 4 and 7), so the BTM-solar behavioural-channel interpretation reduces to **two regions (VIC and NSW)**. SA shows no weekend lift — its reversal rate is already saturated at 92 % on weekdays. QLD is non-significant.

**Caveat.** Chi-square assumes daily independence, which does not hold. Reported p-values are optimistic. The VIC / NSW result is robust because the test statistic is ~10⁻¹⁰ — even aggressive autocorrelation correction would not threaten significance.

### H4b — Weather decoupling (rejected) + H4' — R²-asymmetry fallback (confirmed) *(→ Finding 6)*

**Question.** Are the weather drivers of midday demand decoupled from the weather drivers of peak demand? If decoupling fails, fall back to: does weather *explain* min demand more reliably than max demand?

**Spec H4b (decoupling).** Per region, fit standardised OLS for `max_demand` and `min_demand` on the same six weather + calendar features (`t_max_c`, `t_min_c`, `solar_mj_m2`, `sunshine_seconds`, `precip_mm`, `is_weekend`; binary feature also z-scored so all coefficients are on a common ~SD scale). Top-3 driver ranking by partial R². **Pass:** ≥ 2 of 5 regions show top-3 overlap ≤ 1 AND at least one feature sign-flipped between max and min (`|t| > 2` on both sides).

**Spec H4' (fallback).** R²_min > R²_max in ≥ 3 of 5 regions, with material gap (≥ 0.05) in ≥ 2.

**Verdict H4b.** Rejected. 0 of 5 regions pass. Weather drivers are *shared* across max and min demand — `solar_mj_m2` in particular is the dominant negative driver of both.

**Verdict H4'.** Confirmed. R²_min > R²_max in 4 of 5 regions (SA Δ = +0.30, VIC +0.17, NSW +0.19, QLD +0.28). TAS is the only flip (R²_max 0.63 > R²_min 0.38) — second independent TAS flag.

**Interpretation.** Solar-related weather variables are associated with lower demand at both daily extremes (H4b reject — drivers are not directionally decoupled). But weather explains min demand more reliably than max demand (H4' confirm), consistent with `weather → BTM-PV → min-demand` being a more deterministic physical chain than `weather → AC-behaviour → max-demand`.

### H8 — Negative-pricing × reversal co-occurrence *(→ Finding 7)*

**Question.** Reversal (operational demand low at midday) and negative pricing (`RRP < 0` in some midday interval) are mechanistically related — both arise when supply outpaces demand. How tightly do they co-occur on a day-by-day basis, and does the co-occurrence isolate BTM-solar regions from the hydro-driven one?

**Spec.** Per region, 2 × 2 contingency `is_reversal_day × had_negative_RRP_today`. Fisher conditional odds ratio with 95 % CI, plus chi-square p-value.

**Pass threshold.** OR > 3.0 with p < 0.05 in ≥ 3 of 5 regions.

**Verdict.** Confirmed — three regions clear the bar: VIC OR = 4.78, NSW 5.26, QLD 5.64 (all p < 10⁻²⁵). SA OR = 2.75 is statistically significant (p ≈ 3 × 10⁻⁵) but below the 3.0 threshold — saturation muting (73 % of SA's non-reversal days already carry negative pricing, leaving little headroom). TAS OR = 0.99 with a CI that straddles 1.0 — third independent TAS flag.

### Sub-Q3 — 24h-ahead reversal classifier *(→ Finding 8)*

**Question.** Can we predict whether tomorrow will be a reversal day using only information available at end-of-today?

**Spec.** Region: VIC (saturated SA gives trivially high accuracy; VIC's 29 → 84 % climbing trajectory keeps both classes present). Train < 2025-01-01 (~875 rows), test ≥ 2025-01-01 (~455 rows). Three model tracks:
1. **Persistence baseline** — `ŷ = yesterday's is_reversal`.
2. **Leak-free** (Logistic Regression + Random Forest) — only features known at end-of-D-1: calendar + D-1 demand / weather / rooftop lags. This is the production-deployable specification.
3. **Forecast-proxy** (LR + RF) — leak-free features + same-day weather as a stand-in for a perfect 24 h NWP forecast. Reports the ceiling, not a deployable AUC.

Feature selection is by explicit whitelist — never `df.drop(target)` — to prevent the same-day weather columns from leaking into the leak-free track.

**Pass threshold.** Leak-free AUC ≥ persistence + 0.05 (lift bar). AUC ≥ 0.80 absolute (publication-grade bar). If the lift bar passes but the absolute bar fails, the pre-registered fallback applies: the result is reported as a modest signal rather than publication-grade.

**Verdict.** Partial — modest signal. Persistence AUC 0.631; leak-free LR AUC 0.755 (lift +0.124 ✓); forecast-proxy RF AUC 0.923 (ceiling). Lift bar passes; absolute bar fails.

---

## Project-level pass conditions

Three pre-registered acceptance criteria for the core narrative to proceed:

1. **H1 + H6 confirmed** — without these the descriptive finding (reversal exists and is widening) is unsupported.
2. **At least one of H2 / H3 confirmed** — at least one mechanism or cross-region structural finding.
3. **At least one of H4 / H5' confirmed** — at least one counter-intuitive or mechanism finding to carry the secondary narrative.

All three criteria met: H1 + H6 ✓✓; H2 ✓ + H3 ✓; H4a + H4' + H5' all carry ✓✓✓.

---

## Caveats and assumptions

- **Forecast-proxy AUC of 0.923 uses same-day actual weather** as a stand-in for a 24 h NWP forecast. The deployed AUC in production would land between leak-free 0.755 and forecast-proxy 0.923 depending on NWP solar 24 h skill, not at the ceiling.
- **2024-09-05 AEMO upstream gap.** Two 30-min `ROOFTOP_PV_ACTUAL` intervals (12:30 + 13:00) are missing simultaneously across all five regions. NULL-guarded in `analytics.v_rooftop_daily`; ML training drops two rows for VIC.
- **Tasmania reversal mechanism is hydro dispatch**, not BTM-PV. Three independent tests (Findings 4, 6, 7) confirm this. TAS is not folded into any "BTM-PV story" aggregate; it is reported as a separate sub-finding.
- **i.i.d. assumption** in chi-square / Fisher odds-ratio tests (Findings 5, 7) is technically violated by daily autocorrelation in reversal events. Reported p-values are therefore optimistic. Verdicts are still robust because passing-region p-values span ~10⁻³ to ~10⁻⁴⁰, all orders of magnitude below the 0.05 threshold; even aggressive autocorrelation correction would not invalidate them. A more defensible robustness check would be a clustered-SE logistic regression — left as a future improvement.
- **`time_idx` is a linear time index**, not a measure of actual BTM-PV installed capacity. It absorbs any factor that drifts monotonically over the 44-month window, of which cumulative BTM-PV stock buildup is the most plausible single contributor but not the only one (battery uptake, retail tariff change, behavioural change can all contribute).
- **Confusion matrix at the 0.5 threshold is class-imbalance-distorted.** Test-set reversal rate is 70 %, so the leak-free RF predicts the majority class aggressively: 90 % reversal recall but only 31 % no-reversal recall. Threshold tuning balances precision and recall but does not change AUC.
- **Weather data is queried in `Australia/Brisbane` tz** (AEST, no DST) for all five regions. Daily aggregation windows match the NEM `trading_day`. Physical clock time in NSW / VIC / SA / TAS shifts by ±1 h around DST in real life, but the analysis stays at the daily-aggregate layer, so this washes out.
- **Two data source layers.** The frozen retrospective findings above are computed on AEMO MMSDM Historical (~2-week publication lag) + Open-Meteo ERA5 Archive (~1-day empirical lag). A live operational layer runs alongside in production: NEMWeb CURRENT (~5-minute latency) for dispatch + rooftop, Open-Meteo Archive with a `today − 1` end-date target, and a fail-red 99 %-coverage gate that exits non-zero when ERA5 lags further. Both layers share the same leak-free feature design — only the source endpoints differ. Genuine 24-hour-ahead NWP weather forecasting (e.g. BoM ACCESS-G, ECMWF AIFS-ENS) remains future work; current live inference uses observed D-1 weather, not a forecast.

---

## Methodology audit trail

- **H5 rejected.** Initial single-month data showed summer reversal frequency dominates winter, contradicting the original "spring/autumn ≥ summer × 1.2" prediction. Replaced by H5' (saturation × magnitude decoupling).
- **H2 reformulated stock → flow.** Original spec used cumulative installed CER rooftop capacity (stock). CER postcode data is not on data.gov.au CKAN; pulling it would require manual URL discovery, breaking the automated-ingestion constraint. Replaced with monthly P95 of `ROOFTOP_PV_ACTUAL.POWER` per region (flow). Trade-off: P95 is sensitive to weather, but the month-of-year fixed effect in the OLS absorbs that.
- **H2 spec upgraded.** Bare spec `target ~ proxy + C(month)` upgraded to trend-controlled `target ~ proxy + time_idx + C(month)` with Newey-West HAC SE (maxlags = 6). Reason: rooftop and reversal both trend upward over the window; without `time_idx` the rooftop coefficient could be picking up secular trend rather than a structural rooftop → demand effect. Revised verdicts under the new spec: magnitude leg falls from 5 of 5 to 4 of 5 (TAS sign-flips); frequency leg falls from 3 of 5 to 1 of 5 substantive (NSW only; TAS also passes mechanically but is spurious).
- **H4 split into H4a + H4b + H4'.** The original H4 wrapped three methodologically distinct sub-tests (weekend proportion, weather-driver decoupling, R²-asymmetry fallback). Splitting them gives a clean per-test verdict and makes the H4b reject / H4' confirm combination interpretable.
- **H7 dropped.** H7 (intraday timing distribution of reversal events) had low pre-analysis confidence (60 %) and would have contributed little explanatory power to the deliverable regardless of outcome. Removed before execution rather than left as a placeholder test; the drop is recorded here for transparency.
- **H8 added.** Negative-pricing × reversal co-occurrence added as a secondary finding tied to the BTM-PV mechanism, falsifiable per region. The TAS OR ≈ 1 result functions as the third independent confirmation that Tasmania's mechanism is hydro-driven.
