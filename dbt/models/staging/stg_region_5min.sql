{{ config(materialized='view') }}

-- Centralises the interval-END → interval-START shift. No downstream
-- model touches `settlementdate` directly; everything uses `trading_day`
-- / `hour` derived here. Mirrors the contract in pipeline/_common.py
-- (add_trading_period) and docs/METHODOLOGY.md.
--
-- Snowflake-side dedup: see the `dedupe_latest` macro. On Postgres
-- the macro renders as a plain passthrough since the raw layer
-- already enforces PK via ON CONFLICT DO NOTHING.

WITH deduped AS (
    {{ dedupe_latest(source('raw', 'region_5min'),
                     ['settlementdate', 'regionid']) }}
), shifted AS (
    SELECT
        settlementdate,
        {{ dbt.dateadd('minute', -5, 'settlementdate') }}                AS interval_start,
        regionid,
        intervention,
        totaldemand,
        availablegeneration,
        totalintermittentgeneration,
        uigf,
        semischedule_clearedmw,
        demand_and_nonschedgen,
        netinterchange,
        rrp
    FROM deduped
)
SELECT
    settlementdate,
    interval_start,
    CAST(interval_start AS DATE)                                    AS trading_day,
    EXTRACT(HOUR FROM interval_start)::smallint                     AS hour,
    EXTRACT(DOW  FROM interval_start)::smallint                     AS dow,
    (EXTRACT(HOUR FROM interval_start) BETWEEN 10 AND 15)           AS is_reversal_interval,
    (EXTRACT(DOW  FROM interval_start) IN (0, 6))                   AS is_weekend,
    regionid,
    intervention,
    totaldemand,
    (totaldemand < 0)                                               AS is_negative_demand,
    availablegeneration,
    totalintermittentgeneration,
    uigf,
    semischedule_clearedmw,
    demand_and_nonschedgen,
    netinterchange,
    rrp,
    (rrp < 0)                                                       AS is_negative_rrp
FROM shifted
