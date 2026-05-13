-- =====================================================================
-- 02_analytics_views.sql — Analytics layer: 8 views over raw tables
-- =====================================================================
-- Run AFTER 01_raw_schema.sql + pipeline/load_*.py:
--   psql -d nem -f db/02_analytics_views.sql
--
-- All views are CREATE OR REPLACE — re-running is safe, no data lost.
--
-- Hierarchy:
--   v_region_5min                       (base; raw + derived hour/day/flags)
--     ├── v_daily_demand_summary        (1 row per region × trading_day)
--     │     ├── v_reversal_events
--     │     ├── v_monthly_reversal_rate
--     │     └── v_ml_features           (joined with weather + rooftop)
--     └── v_demand_h03_h13_panel        (Finding 2 input)
--
--   v_rooftop_daily                     (1 row per region × trading_day)
--     └── v_h2_panel                    (Finding 4 OLS input, joins
--                                        v_monthly_reversal_rate)
--
-- ⚠ Definition lock (must match docs/METHODOLOGY.md + pipeline/_common.py):
--   dispatch trading_day  = (settlementdate    - interval '5 minutes')::date
--   dispatch hour         = EXTRACT(HOUR FROM   settlementdate    - interval '5 minutes')
--   rooftop  trading_day  = (interval_datetime - interval '30 minutes')::date
--   rooftop  hour         = EXTRACT(HOUR FROM   interval_datetime - interval '30 minutes')
--   reversal_hours        = hour ∈ {10..15}            (BETWEEN 10 AND 15)
--   weekend               = dow  ∈ {0, 6}              (Sun=0, Sat=6 in PG)
--   weather.date already aligns with trading_day (Australia/Brisbane tz)
--
-- If you change any of these, change them everywhere AND log in
-- docs/METHODOLOGY.md changelog.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS analytics;


-- =====================================================================
-- Dispatch view layer
-- =====================================================================


-- ---------------------------------------------------------------------
-- v_region_5min — base view: raw + derived calendar / classification cols
-- ---------------------------------------------------------------------
-- Centralises the interval-START shift so no downstream view ever
-- touches `settlementdate` directly. Adding a new flag (e.g. is_holiday)
-- means adding one column here, not refactoring 5 places.
CREATE OR REPLACE VIEW analytics.v_region_5min AS
WITH shifted AS (
    SELECT
        settlementdate,
        settlementdate - interval '5 minutes'                            AS interval_start,
        regionid,
        intervention,
        totaldemand,
        availablegeneration,
        totalintermittentgeneration,
        uigf,
        semischedule_clearedmw,
        demand_and_nonschedgen,
        netinterchange,
        rrp
    FROM raw.region_5min
)
SELECT
    settlementdate,
    interval_start,
    interval_start::date                                            AS trading_day,
    EXTRACT(HOUR FROM interval_start)::smallint                     AS hour,
    EXTRACT(DOW  FROM interval_start)::smallint                     AS dow,
    (EXTRACT(HOUR FROM interval_start)::int BETWEEN 10 AND 15)      AS is_reversal_interval,
    (EXTRACT(DOW  FROM interval_start)::int IN (0, 6))              AS is_weekend,
    regionid,
    intervention,
    totaldemand,
    (totaldemand < 0)                                               AS is_negative_demand,
    availablegeneration,
    totalintermittentgeneration,
    uigf,
    semischedule_clearedmw,
    demand_and_nonschedgen,
    netinterchange,
    rrp,
    (rrp < 0)                                                       AS is_negative_rrp
FROM shifted;

COMMENT ON VIEW analytics.v_region_5min IS
    'Base analytical view. raw.region_5min + interval-START shift + derived '
    'calendar fields (trading_day, hour, dow, is_weekend) + classification '
    'flags (is_reversal_interval, is_negative_demand, is_negative_rrp). '
    'All downstream views must build on this, not on raw.region_5min.';


-- ---------------------------------------------------------------------
-- v_daily_demand_summary — 1 row per (regionid, trading_day)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_daily_demand_summary AS
SELECT
    regionid,
    trading_day,
    COUNT(*)                                                        AS n_intervals,
    MAX(totaldemand)                                                AS max_demand,
    MIN(totaldemand)                                                AS min_demand,
    AVG(totaldemand)                                                AS mean_demand,
    STDDEV_SAMP(totaldemand)                                        AS std_demand,
    -- Hour-of-day at which the daily MIN demand occurred.
    -- array_agg ordered by totaldemand ASC, then settlementdate ASC for
    -- deterministic tie-breaking; [1] picks the argmin hour.
    (array_agg(hour ORDER BY totaldemand ASC,  settlementdate ASC))[1]  AS min_demand_hour,
    (array_agg(hour ORDER BY totaldemand DESC, settlementdate ASC))[1]  AS max_demand_hour,
    AVG(rrp)                                                        AS mean_rrp,
    BOOL_OR(is_negative_demand)                                     AS had_negative_demand,
    SUM(CASE WHEN is_negative_demand THEN 1 ELSE 0 END)             AS n_neg_demand_intervals,
    MAX(dow)                                                        AS dow,
    MAX(is_weekend::int)::boolean                                   AS is_weekend
FROM analytics.v_region_5min
GROUP BY 1, 2;

COMMENT ON VIEW analytics.v_daily_demand_summary IS
    'Daily summary per (region, trading_day): max/min/mean/std demand, '
    'argmin/argmax hour-of-day, neg-demand interval count, dow flag.';


-- ---------------------------------------------------------------------
-- v_reversal_events — one row per (region, trading_day) where reversal occurred
-- ---------------------------------------------------------------------
-- "Reversal day" definition (docs/METHODOLOGY.md):
--   the trading_day's MIN totaldemand interval has hour ∈ {10..15}.
CREATE OR REPLACE VIEW analytics.v_reversal_events AS
SELECT
    regionid,
    trading_day,
    dow,
    is_weekend,
    min_demand,
    min_demand_hour,
    max_demand,
    max_demand_hour,
    mean_demand,
    n_neg_demand_intervals,
    had_negative_demand
FROM analytics.v_daily_demand_summary
WHERE min_demand_hour BETWEEN 10 AND 15;

COMMENT ON VIEW analytics.v_reversal_events IS
    'Trading days that satisfy the reversal definition (daily-min hour in {10..15}). '
    'One row per (regionid, trading_day).';


-- ---------------------------------------------------------------------
-- v_monthly_reversal_rate — 1 row per (region, year-month)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_monthly_reversal_rate AS
SELECT
    regionid,
    date_trunc('month', trading_day)::date                          AS month,
    COUNT(*)                                                        AS n_trading_days,
    COUNT(*) FILTER (WHERE min_demand_hour BETWEEN 10 AND 15)       AS n_reversal_days,
    ROUND(100.0 * COUNT(*) FILTER (WHERE min_demand_hour BETWEEN 10 AND 15)
                / COUNT(*), 2)                                      AS reversal_pct,
    -- Magnitude of the deepest min that month (lower = more PV oversupply)
    MIN(min_demand)                                                 AS deepest_min_demand,
    AVG(min_demand) FILTER (WHERE min_demand_hour BETWEEN 10 AND 15) AS mean_min_demand_on_reversal_days,
    SUM(n_neg_demand_intervals)                                     AS n_neg_demand_intervals,
    COUNT(*) FILTER (WHERE had_negative_demand)                     AS n_neg_demand_days
FROM analytics.v_daily_demand_summary
GROUP BY 1, 2;

COMMENT ON VIEW analytics.v_monthly_reversal_rate IS
    'Monthly reversal panel per (region, month). Headline metric is reversal_pct. '
    'Use for Findings 1 (frequency saturation) and 3 (cross-region trajectory).';


-- ---------------------------------------------------------------------
-- v_demand_h03_h13_panel — Finding 2 monthly panel (h03 vs h13 mean demand)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_demand_h03_h13_panel AS
SELECT
    regionid,
    date_trunc('month', trading_day)::date                          AS month,
    AVG(totaldemand) FILTER (WHERE hour = 3)                        AS h03_mean,
    AVG(totaldemand) FILTER (WHERE hour = 13)                       AS h13_mean,
    AVG(totaldemand) FILTER (WHERE hour = 3)
        - AVG(totaldemand) FILTER (WHERE hour = 13)                 AS gap_h03_minus_h13,
    COUNT(*) FILTER (WHERE hour = 3)                                AS n_h03_intervals,
    COUNT(*) FILTER (WHERE hour = 13)                               AS n_h13_intervals,
    CASE WHEN AVG(totaldemand) FILTER (WHERE hour = 3) > 0
         THEN AVG(totaldemand) FILTER (WHERE hour = 13)
              / AVG(totaldemand) FILTER (WHERE hour = 3)
    END                                                             AS ratio_h13_over_h03
FROM analytics.v_region_5min
GROUP BY 1, 2;

COMMENT ON VIEW analytics.v_demand_h03_h13_panel IS
    'Finding 2 monthly panel: mean TOTALDEMAND at hour 03 vs hour 13 per (region, month). '
    'Run OLS on gap_h03_minus_h13 ~ time + C(month). '
    '03 is baseload, 13 is BTM PV peak; gap quantifies PV impact.';


-- =====================================================================
-- Rooftop view layer
-- =====================================================================


-- ---------------------------------------------------------------------
-- v_rooftop_daily — 1 row per (regionid, trading_day)
-- ---------------------------------------------------------------------
-- Daily summary of BTM rooftop generation:
--   peak_mw         — daily MAX(power)
--   total_mwh       — integral over the day. Each interval is 30 min = 0.5 h,
--                     so SUM(power_mw × 0.5) = MWh.
--   midday_mean_mw  — average power during reversal hours {10..15}
--                     (interval-START hour). The single best driver of
--                     operational-demand reversal.
--   p95_mw          — 95th percentile of the day's 48 intervals.
--
-- Completeness gate: every aggregate is wrapped in
-- CASE WHEN n_intervals = 48 THEN ... END. A day missing any 30-min
-- interval gets NULL on all four metrics, because partial-day aggregates
-- are biased (total_mwh underestimates, peak_mw misses noon peak, etc.).
-- n_intervals stays exposed so downstream can audit without re-grouping.
-- Known gap: AEMO did not publish 2 intervals on 2024-09-05 across all
-- regions — daily aggregates for that day are NULL on all 5 regions.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_rooftop_daily AS
WITH shifted AS (
    SELECT
        interval_datetime,
        interval_datetime - interval '30 minutes'                       AS interval_start,
        regionid,
        power
    FROM raw.rooftop_pv_30min
)
SELECT
    regionid,
    interval_start::date                                                AS trading_day,
    COUNT(*)                                                            AS n_intervals,
    CASE WHEN COUNT(*) = 48 THEN MAX(power)         END                 AS peak_mw,
    CASE WHEN COUNT(*) = 48 THEN SUM(power) * 0.5   END                 AS total_mwh,
    CASE WHEN COUNT(*) = 48 THEN
        AVG(power) FILTER (
            WHERE EXTRACT(HOUR FROM interval_start)::int BETWEEN 10 AND 15
        )
    END                                                                 AS midday_mean_mw,
    CASE WHEN COUNT(*) = 48 THEN
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY power)
    END                                                                 AS p95_mw
FROM shifted
GROUP BY 1, 2;

COMMENT ON VIEW analytics.v_rooftop_daily IS
    'Daily rooftop PV summary per (region, trading_day). '
    'midday_mean_mw averages over reversal hours {10..15} (interval-START). '
    'total_mwh = SUM(power) * 0.5 because each 30-min interval is 0.5 h. '
    'All four metrics are NULL on partial-interval days (n_intervals < 48).';


-- ---------------------------------------------------------------------
-- v_h2_panel — 1 row per (regionid, month) — Finding 4 OLS input
-- ---------------------------------------------------------------------
-- Capacity proxy choices:
--   p95_rooftop_mw       = monthly P95 of daily peaks. "Top-5% peak"; robust
--                          to cloudy days; grows monotonically with installed
--                          capacity. Primary independent variable.
--   mean_daily_peak_mw   = monthly mean of daily peaks. Sensitivity check.
--   mean_midday_mw       = monthly mean of daily midday-mean. More weather-
--                          sensitive — useful for Finding 6 weather coupling.
--   mean_daily_mwh       = monthly mean of daily total energy.
--
-- Statsmodels OLS (per region):
--   target ~ p95_rooftop_mw + time_idx + C(calendar_month)
-- with Newey-West HAC standard errors (maxlags = 6). `time_idx` absorbs the
-- secular trend so the rooftop coefficient is identified by month-to-month
-- flow variation. Targets used: reversal_pct (frequency leg) and
-- deepest_min_demand (magnitude leg).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_h2_panel AS
WITH rooftop_monthly AS (
    SELECT
        regionid,
        date_trunc('month', trading_day)::date                          AS month,
        -- Aggregates from v_rooftop_daily are NULL on partial-interval days.
        -- AVG / PERCENTILE_CONT skip NULLs, so monthly metrics are over
        -- fully-observed days only.
        AVG(peak_mw)                                                    AS mean_daily_peak_mw,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY peak_mw)           AS p95_rooftop_mw,
        AVG(midday_mean_mw)                                             AS mean_midday_mw,
        AVG(total_mwh)                                                  AS mean_daily_mwh,
        COUNT(*)                                                        AS n_rooftop_days,
        COUNT(peak_mw)                                                  AS n_valid_rooftop_days
    FROM analytics.v_rooftop_daily
    GROUP BY 1, 2
)
-- LEFT JOIN keeps every (region, month) from the reversal-rate panel.
-- statsmodels OLS drops NULL-feature rows automatically.
SELECT
    r.regionid,
    r.month,
    EXTRACT(YEAR  FROM r.month)::smallint                               AS year,
    EXTRACT(MONTH FROM r.month)::smallint                               AS calendar_month,
    r.reversal_pct,
    r.n_trading_days,
    r.n_reversal_days,
    r.deepest_min_demand,
    r.mean_min_demand_on_reversal_days,
    rt.p95_rooftop_mw,
    rt.mean_daily_peak_mw,
    rt.mean_midday_mw,
    rt.mean_daily_mwh,
    rt.n_rooftop_days,
    rt.n_valid_rooftop_days
FROM analytics.v_monthly_reversal_rate r
LEFT JOIN rooftop_monthly rt
    USING (regionid, month);

COMMENT ON VIEW analytics.v_h2_panel IS
    'Finding 4 monthly panel per (region, month). '
    'Per-region OLS: target ~ p95_rooftop_mw + time_idx + C(calendar_month), '
    'with Newey-West HAC standard errors (maxlags = 6). time_idx absorbs the '
    'secular trend so the rooftop coefficient is identified by month-to-month '
    'flow variation. p95_rooftop_mw is the monthly P95 of daily peak rooftop '
    'output, used as installed-capacity proxy.';


-- =====================================================================
-- Cross-source ML feature warehouse
-- =====================================================================


-- ---------------------------------------------------------------------
-- v_ml_features — Finding 8 (24h-ahead reversal classifier) features
-- ---------------------------------------------------------------------
-- Target:    is_reversal (= 1 if D's min-demand hour ∈ {10..15})
-- Grain:     1 row per (regionid, trading_day)
--
-- ⚠ Feature whitelist discipline:
-- This view is a feature WAREHOUSE — it intentionally exposes both
-- leak-free and perfect-foresight features. The notebook MUST select
-- features explicitly by name; do NOT do `SELECT * EXCEPT target`.
--
-- Canonical Finding 8 feature set is defined in docs/METHODOLOGY.md. The
-- mapping below shows what this view provides vs. what the notebook
-- must derive separately:
--
--   METHODOLOGY.md feature               | source
--   -------------------------------------+--------------------------------
--   weather 5 vars (D)                   | THIS view (t_max_c, t_min_c,
--                                        |   solar_mj_m2, sunshine_seconds,
--                                        |   precip_mm)
--   dow, is_weekend, month               | THIS view
--   is_public_holiday                    | NOTEBOOK — `holidays` package
--   time_idx                             | NOTEBOOK — row_number / date diff
--   prev_p95_rooftop_mw (LAG-7)          | NOTEBOOK — df.shift(7) on
--                                        |   rooftop_p95_mw
--   semisched_share_yesterday            | NOTEBOOK — daily aggregate of
--                                        |   raw.region_5min
--                                        |   semischedule_clearedmw /
--                                        |   totaldemand, then LAG 1
--
-- Three model tracks per docs/METHODOLOGY.md:
--   1. persistence:        prev_is_reversal only (baseline floor)
--   2. leak-free:          calendar + D-1 demand/weather/rooftop lags +
--                          (notebook-derived) holiday + time_idx + LAG-7
--                          rooftop + semisched_share_yesterday
--   3. forecast-proxy:     leak-free + same-day weather columns (proxies
--                          a real NWP forecast — caveat the report)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_ml_features AS
WITH daily_join AS (
    SELECT
        s.regionid,
        s.trading_day,
        s.dow,
        s.is_weekend,
        EXTRACT(MONTH FROM s.trading_day)::smallint                     AS month,
        s.max_demand,
        s.min_demand,
        s.min_demand_hour,
        (s.min_demand_hour BETWEEN 10 AND 15)                           AS is_reversal,
        w.t_max_c,
        w.t_min_c,
        w.solar_mj_m2,
        w.sunshine_seconds,
        w.precip_mm,
        rt.peak_mw                                                      AS rooftop_peak_mw,
        rt.midday_mean_mw                                               AS rooftop_midday_mean_mw,
        rt.total_mwh                                                    AS rooftop_total_mwh,
        rt.p95_mw                                                       AS rooftop_p95_mw
    FROM analytics.v_daily_demand_summary s
    LEFT JOIN raw.weather_daily w
        ON w.regionid = s.regionid AND w.date = s.trading_day
    LEFT JOIN analytics.v_rooftop_daily rt
        ON rt.regionid = s.regionid AND rt.trading_day = s.trading_day
)
SELECT
    regionid,
    trading_day,
    is_reversal,
    -- target-related (exclude from features when training)
    min_demand,
    max_demand,
    min_demand_hour,
    -- calendar features for D (known at D-1 close — safe)
    month,
    dow,
    is_weekend,
    -- D-1 lagged features (safe for leak-free baseline)
    LAG(max_demand)             OVER w  AS prev_max_demand,
    LAG(min_demand)             OVER w  AS prev_min_demand,
    LAG(is_reversal::int)       OVER w  AS prev_is_reversal,
    LAG(t_max_c)                OVER w  AS prev_t_max_c,
    LAG(solar_mj_m2)            OVER w  AS prev_solar_mj_m2,
    LAG(sunshine_seconds)       OVER w  AS prev_sunshine_seconds,
    LAG(precip_mm)              OVER w  AS prev_precip_mm,
    LAG(rooftop_peak_mw)        OVER w  AS prev_rooftop_peak_mw,
    LAG(rooftop_midday_mean_mw) OVER w  AS prev_rooftop_midday_mean_mw,
    LAG(rooftop_total_mwh)      OVER w  AS prev_rooftop_total_mwh,
    -- target-day perfect-foresight features (caveat: NWP-forecast in prod)
    t_max_c,
    t_min_c,
    solar_mj_m2,
    sunshine_seconds,
    precip_mm,
    rooftop_peak_mw,
    rooftop_midday_mean_mw,
    rooftop_total_mwh,
    rooftop_p95_mw
FROM daily_join
WINDOW w AS (PARTITION BY regionid ORDER BY trading_day);

COMMENT ON VIEW analytics.v_ml_features IS
    'Finding 8 feature warehouse, 1 row per (region, trading_day). '
    'Target = is_reversal (D''s min-demand hour ∈ {10..15}). '
    'prev_* columns are LAG-1 (D-1) — leak-free for 24h-ahead forecast. '
    'Same-day weather/rooftop columns are perfect-foresight; in production '
    'they would be NWP forecast + AEMO rooftop forecast.';


-- =====================================================================
-- Sanity / smoke queries (run after creating views)
-- =====================================================================
-- 1. Row counts on each view (window 2022-08 → 2026-03, 1339 days)
--    SELECT 'v_region_5min'           AS view, COUNT(*) FROM analytics.v_region_5min
--    UNION ALL SELECT 'v_daily_demand_summary',  COUNT(*) FROM analytics.v_daily_demand_summary
--    UNION ALL SELECT 'v_reversal_events',       COUNT(*) FROM analytics.v_reversal_events
--    UNION ALL SELECT 'v_monthly_reversal_rate', COUNT(*) FROM analytics.v_monthly_reversal_rate
--    UNION ALL SELECT 'v_demand_h03_h13_panel',  COUNT(*) FROM analytics.v_demand_h03_h13_panel
--    UNION ALL SELECT 'v_rooftop_daily',         COUNT(*) FROM analytics.v_rooftop_daily
--    UNION ALL SELECT 'v_h2_panel',              COUNT(*) FROM analytics.v_h2_panel
--    UNION ALL SELECT 'v_ml_features',           COUNT(*) FROM analytics.v_ml_features;
--
--    Expected:
--      v_region_5min            = 1,928,160  (5 × 385,632 5-min intervals)
--      v_daily_demand_summary   =     6,695  (5 × 1,339)
--      v_monthly_reversal_rate  =       220  (5 × 44)
--      v_demand_h03_h13_panel   =       220
--      v_rooftop_daily          =     6,695
--      v_h2_panel               =       220
--      v_ml_features            =     6,695
--
-- 2. SA1 reversal_pct trend (project headline)
--    SELECT month, reversal_pct, n_reversal_days, n_trading_days, deepest_min_demand
--    FROM analytics.v_monthly_reversal_rate
--    WHERE regionid = 'SA1' ORDER BY month;
--
-- 3. Finding 4 hero — SA1 monthly reversal_pct vs p95_rooftop_mw
--    SELECT month, reversal_pct, p95_rooftop_mw, mean_midday_mw
--    FROM analytics.v_h2_panel WHERE regionid = 'SA1' ORDER BY month;
--
-- 4. ML feature audit — VIC1 NULL count
--    SELECT
--      COUNT(*) AS n,
--      SUM((is_reversal IS NULL)::int)     AS n_null_target,
--      SUM((prev_max_demand IS NULL)::int) AS n_null_prev_max,
--      SUM((solar_mj_m2 IS NULL)::int)     AS n_null_solar,
--      SUM((rooftop_peak_mw IS NULL)::int) AS n_null_rooftop_peak
--    FROM analytics.v_ml_features WHERE regionid = 'VIC1';
--
--    Expected: n_null_target = 0, n_null_prev_max = 1 (first day has no D-1),
--    n_null_solar = 0, n_null_rooftop_peak = 5 (2024-09-05 across 5 regions).
--
-- 5. Rooftop gap audit — days with n_intervals != 48
--    SELECT regionid, trading_day, n_intervals
--    FROM analytics.v_rooftop_daily
--    WHERE n_intervals <> 48
--    ORDER BY trading_day, regionid;
