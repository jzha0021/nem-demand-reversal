{{ config(materialized='view') }}

-- Open-Meteo daily aggregates, aggregated in Australia/Brisbane (AEST,
-- no DST) so `date` already aligns with NEM trading_day. Rename to
-- `trading_day` so joins to downstream are unambiguous.

WITH deduped AS (
    {{ dedupe_latest(source('raw', 'weather_daily'),
                     ['date', 'regionid']) }}
)
SELECT
    date                                                            AS trading_day,
    regionid,
    t_max_c,
    t_min_c,
    solar_mj_m2,
    sunshine_seconds,
    precip_mm
FROM deduped
