{{ config(materialized='view') }}

-- Finding 2 monthly panel: mean TOTALDEMAND at hour 03 vs hour 13.
-- 03 is baseload, 13 is BTM PV peak; gap quantifies PV impact over time.

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
FROM {{ ref('stg_region_5min') }}
GROUP BY 1, 2
