{{ config(materialized='view') }}

-- Daily BTM rooftop summary. Every aggregate gated on n_intervals = 48
-- because partial-day aggregates are biased (e.g. 2024-09-05 across all
-- regions is missing 2 intervals — those days yield NULL on all metrics
-- while n_intervals stays exposed for audit).

SELECT
    regionid,
    trading_day,
    COUNT(*)                                                            AS n_intervals,
    CASE WHEN COUNT(*) = 48 THEN MAX(power)         END                 AS peak_mw,
    CASE WHEN COUNT(*) = 48 THEN SUM(power) * 0.5   END                 AS total_mwh,
    CASE WHEN COUNT(*) = 48 THEN
        {{ avg_if('power', 'hour BETWEEN 10 AND 15') }}
    END                                                                 AS midday_mean_mw,
    CASE WHEN COUNT(*) = 48 THEN
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY power)
    END                                                                 AS p95_mw
FROM {{ ref('stg_rooftop_pv_30min') }}
GROUP BY 1, 2
