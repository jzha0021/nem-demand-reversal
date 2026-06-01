{{ config(materialized='view') }}

-- Finding 2 monthly panel: mean TOTALDEMAND at hour 03 vs hour 13.
-- 03 is baseload, 13 is BTM PV peak; gap quantifies PV impact over time.

SELECT
    regionid,
    {{ to_date(dbt.date_trunc('month', 'trading_day')) }}               AS month,
    {{ avg_if('totaldemand', 'hour = 3') }}                             AS h03_mean,
    {{ avg_if('totaldemand', 'hour = 13') }}                            AS h13_mean,
    {{ avg_if('totaldemand', 'hour = 3') }}
        - {{ avg_if('totaldemand', 'hour = 13') }}                      AS gap_h03_minus_h13,
    {{ count_if('hour = 3') }}                                          AS n_h03_intervals,
    {{ count_if('hour = 13') }}                                         AS n_h13_intervals,
    CASE WHEN {{ avg_if('totaldemand', 'hour = 3') }} > 0
         THEN {{ avg_if('totaldemand', 'hour = 13') }}
              / {{ avg_if('totaldemand', 'hour = 3') }}
    END                                                                 AS ratio_h13_over_h03
FROM {{ ref('stg_region_5min') }}
GROUP BY 1, 2
