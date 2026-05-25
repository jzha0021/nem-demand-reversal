{{ config(materialized='view') }}

-- Open-Meteo daily aggregates, aggregated in Australia/Brisbane (AEST,
-- no DST) so `date` already aligns with NEM trading_day. Rename to
-- `trading_day` so joins to downstream are unambiguous.

SELECT
    date                                                            AS trading_day,
    regionid,
    t_max_c,
    t_min_c,
    solar_mj_m2,
    sunshine_seconds,
    precip_mm
FROM {{ source('raw', 'weather_daily') }}
