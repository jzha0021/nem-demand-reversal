{{ config(materialized='view') }}

SELECT
    regionid,
    trading_day,
    COUNT(*)                                                            AS n_intervals,
    MAX(totaldemand)                                                    AS max_demand,
    MIN(totaldemand)                                                    AS min_demand,
    AVG(totaldemand)                                                    AS mean_demand,
    STDDEV_SAMP(totaldemand)                                            AS std_demand,
    {{ array_first_ordered('hour', 'totaldemand ASC,  settlementdate ASC') }} AS min_demand_hour,
    {{ array_first_ordered('hour', 'totaldemand DESC, settlementdate ASC') }} AS max_demand_hour,
    AVG(rrp)                                                            AS mean_rrp,
    {{ bool_or_agg('is_negative_demand') }}                             AS had_negative_demand,
    {{ sum_if(1, 'is_negative_demand') }}                               AS n_neg_demand_intervals,
    MAX(dow)                                                            AS dow,
    {{ bool_or_agg('is_weekend') }}                                     AS is_weekend
FROM {{ ref('stg_region_5min') }}
GROUP BY 1, 2
