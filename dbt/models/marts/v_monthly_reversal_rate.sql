{{ config(materialized='view') }}

SELECT
    regionid,
    {{ to_date(dbt.date_trunc('month', 'trading_day')) }}               AS month,
    COUNT(*)                                                            AS n_trading_days,
    {{ count_if('min_demand_hour BETWEEN 10 AND 15') }}                 AS n_reversal_days,
    ROUND(100.0 * {{ count_if('min_demand_hour BETWEEN 10 AND 15') }}
                / COUNT(*), 2)                                          AS reversal_pct,
    MIN(min_demand)                                                     AS deepest_min_demand,
    {{ avg_if('min_demand', 'min_demand_hour BETWEEN 10 AND 15') }}     AS mean_min_demand_on_reversal_days,
    SUM(n_neg_demand_intervals)                                         AS n_neg_demand_intervals,
    {{ count_if('had_negative_demand') }}                               AS n_neg_demand_days
FROM {{ ref('v_daily_demand_summary') }}
GROUP BY 1, 2
