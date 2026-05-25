{{ config(materialized='view') }}

-- Finding 4 monthly panel per (region, month). Per-region OLS:
--   target ~ p95_rooftop_mw + time_idx + C(calendar_month)
-- with Newey-West HAC standard errors (maxlags = 6).

WITH rooftop_monthly AS (
    SELECT
        regionid,
        date_trunc('month', trading_day)::date                          AS month,
        AVG(peak_mw)                                                    AS mean_daily_peak_mw,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY peak_mw)           AS p95_rooftop_mw,
        AVG(midday_mean_mw)                                             AS mean_midday_mw,
        AVG(total_mwh)                                                  AS mean_daily_mwh,
        COUNT(*)                                                        AS n_rooftop_days,
        COUNT(peak_mw)                                                  AS n_valid_rooftop_days
    FROM {{ ref('v_rooftop_daily') }}
    GROUP BY 1, 2
)
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
FROM {{ ref('v_monthly_reversal_rate') }} r
LEFT JOIN rooftop_monthly rt
    USING (regionid, month)
