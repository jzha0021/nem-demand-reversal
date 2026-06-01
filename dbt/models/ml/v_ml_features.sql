{{ config(materialized='view') }}

-- Finding 8 feature warehouse, 1 row per (region, trading_day).
-- Exposes both leak-free (prev_*) and perfect-foresight (same-day) columns;
-- notebook + predict.py select features by explicit whitelist, NEVER `SELECT *`.

{# Snowflake doesn't support the `WINDOW name AS (...)` clause, so the
   window spec is inlined on every LAG via Jinja substitution. #}
{% set w = '(PARTITION BY regionid ORDER BY trading_day)' %}

WITH daily_join AS (
    SELECT
        s.regionid,
        s.trading_day,
        s.dow,
        s.is_weekend,
        EXTRACT(MONTH FROM s.trading_day)::smallint                     AS month,
        s.n_intervals,
        s.max_demand,
        s.min_demand,
        s.min_demand_hour,
        (s.min_demand_hour BETWEEN 10 AND 15)                           AS is_reversal,
        w.t_max_c,
        w.t_min_c,
        w.solar_mj_m2,
        w.sunshine_seconds,
        w.precip_mm,
        rt.peak_mw                                                      AS rooftop_peak_mw,
        rt.midday_mean_mw                                               AS rooftop_midday_mean_mw,
        rt.total_mwh                                                    AS rooftop_total_mwh,
        rt.p95_mw                                                       AS rooftop_p95_mw
    FROM {{ ref('v_daily_demand_summary') }} s
    LEFT JOIN {{ ref('stg_weather_daily') }} w
        ON w.regionid = s.regionid AND w.trading_day = s.trading_day
    LEFT JOIN {{ ref('v_rooftop_daily') }} rt
        ON rt.regionid = s.regionid AND rt.trading_day = s.trading_day
)
SELECT
    regionid,
    trading_day,
    n_intervals,
    is_reversal,
    min_demand,
    max_demand,
    min_demand_hour,
    month,
    dow,
    is_weekend,
    LAG(max_demand)                       OVER {{ w }}  AS prev_max_demand,
    LAG(min_demand)                       OVER {{ w }}  AS prev_min_demand,
    LAG({{ bool_to_int('is_reversal') }}) OVER {{ w }}  AS prev_is_reversal,
    LAG(t_max_c)                          OVER {{ w }}  AS prev_t_max_c,
    LAG(solar_mj_m2)                      OVER {{ w }}  AS prev_solar_mj_m2,
    LAG(sunshine_seconds)                 OVER {{ w }}  AS prev_sunshine_seconds,
    LAG(precip_mm)                        OVER {{ w }}  AS prev_precip_mm,
    LAG(rooftop_peak_mw)                  OVER {{ w }}  AS prev_rooftop_peak_mw,
    LAG(rooftop_midday_mean_mw)           OVER {{ w }}  AS prev_rooftop_midday_mean_mw,
    LAG(rooftop_total_mwh)                OVER {{ w }}  AS prev_rooftop_total_mwh,
    t_max_c,
    t_min_c,
    solar_mj_m2,
    sunshine_seconds,
    precip_mm,
    rooftop_peak_mw,
    rooftop_midday_mean_mw,
    rooftop_total_mwh,
    rooftop_p95_mw
FROM daily_join
