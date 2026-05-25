{{ config(materialized='view') }}

SELECT
    regionid,
    trading_day,
    COUNT(*)                                                        AS n_intervals,
    MAX(totaldemand)                                                AS max_demand,
    MIN(totaldemand)                                                AS min_demand,
    AVG(totaldemand)                                                AS mean_demand,
    STDDEV_SAMP(totaldemand)                                        AS std_demand,
    (array_agg(hour ORDER BY totaldemand ASC,  settlementdate ASC))[1]  AS min_demand_hour,
    (array_agg(hour ORDER BY totaldemand DESC, settlementdate ASC))[1]  AS max_demand_hour,
    AVG(rrp)                                                        AS mean_rrp,
    BOOL_OR(is_negative_demand)                                     AS had_negative_demand,
    SUM(CASE WHEN is_negative_demand THEN 1 ELSE 0 END)             AS n_neg_demand_intervals,
    MAX(dow)                                                        AS dow,
    MAX(is_weekend::int)::boolean                                   AS is_weekend
FROM {{ ref('stg_region_5min') }}
GROUP BY 1, 2
