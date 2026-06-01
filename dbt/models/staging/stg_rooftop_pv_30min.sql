{{ config(materialized='view') }}

-- 30-min interval-END → interval-START shift. Daily aggregation lives in
-- marts/v_rooftop_daily; this layer is dedupe + time shift so ad-hoc
-- 30-min queries can use the same trading_day semantics as dispatch.

WITH deduped AS (
    {{ dedupe_latest(source('raw', 'rooftop_pv_30min'),
                     ['interval_datetime', 'regionid']) }}
), shifted AS (
    SELECT
        interval_datetime,
        {{ dbt.dateadd('minute', -30, 'interval_datetime') }}            AS interval_start,
        regionid,
        power
    FROM deduped
)
SELECT
    interval_datetime,
    interval_start,
    CAST(interval_start AS DATE)                                    AS trading_day,
    EXTRACT(HOUR FROM interval_start)::smallint                     AS hour,
    regionid,
    power
FROM shifted
