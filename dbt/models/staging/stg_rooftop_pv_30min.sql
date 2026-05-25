{{ config(materialized='view') }}

-- 30-min interval-END → interval-START shift. Daily aggregation lives in
-- marts/v_rooftop_daily; this layer is pure passthrough + time shift so
-- ad-hoc 30-min queries can use the same trading_day semantics as dispatch.

SELECT
    interval_datetime,
    interval_datetime - interval '30 minutes'                       AS interval_start,
    (interval_datetime - interval '30 minutes')::date               AS trading_day,
    EXTRACT(HOUR FROM interval_datetime - interval '30 minutes')::smallint AS hour,
    regionid,
    power
FROM {{ source('raw', 'rooftop_pv_30min') }}
