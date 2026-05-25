{{ config(materialized='view') }}

-- "Reversal day" definition (docs/METHODOLOGY.md):
-- the trading_day's MIN totaldemand interval has hour ∈ {10..15}.

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
FROM {{ ref('v_daily_demand_summary') }}
WHERE min_demand_hour BETWEEN 10 AND 15
