-- =====================================================================
-- 01_raw_schema.sql — Raw layer: 3 source-aligned tables
-- =====================================================================
-- Run:  psql -d nem -f db/01_raw_schema.sql
--
-- Tables (all in schema `raw`):
--   raw.region_5min        — dispatch 5-min × 5 NEM regions  (~1.93M rows)
--   raw.rooftop_pv_30min   — ROOFTOP_PV_ACTUAL 30-min × 5    (~320K rows)
--   raw.weather_daily      — daily weather × 5 region cities (~6.7K rows)
--
-- ⚠ This script DROPs and re-creates each raw table. Raw data is fully
--    regenerable from the pipeline scripts (nemosis cache + Open-Meteo
--    Archive API), so DROP is safe. DO NOT run in any environment where
--    the raw tables hold data that isn't reproducible from
--    pipeline/fetch_*.py + pipeline/load_*.py.
--
-- Time-zone convention applied throughout:
--   All AEMO timestamps are AEST (UTC+10) naive, NO DST shift.
--   Open-Meteo daily aggregates use Australia/Brisbane (AEST, no DST)
--   for ALL 5 regions so weather.date aligns 1:1 with NEM trading_day.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS raw;


-- ---------------------------------------------------------------------
-- raw.region_5min — DISPATCHREGIONSUM ⨝ DISPATCHPRICE (INTERVENTION=0)
-- ---------------------------------------------------------------------
-- Grain:     (settlementdate, regionid) — 5-min × 5 NEM regions
-- Volume:    ~1.93M rows for window 2022-08 → 2026-03
-- Source:    nemosis MMSDM monthly archive
-- Filtering: INTERVENTION = 0 only (applied in pipeline/fetch_aemo.py)
-- At this scale we do NOT partition; revisit if volume crosses ~50M
-- (e.g. when DISPATCH_UNIT_SCADA is later added).
-- ---------------------------------------------------------------------

DROP TABLE IF EXISTS raw.region_5min;

CREATE TABLE raw.region_5min (
    settlementdate                 timestamp       NOT NULL,
    regionid                       text            NOT NULL,
    intervention                   smallint        NOT NULL,

    -- demand & generation context (DISPATCHREGIONSUM)
    totaldemand                    numeric(12, 4),
    availablegeneration            numeric(12, 4),
    totalintermittentgeneration    numeric(12, 4),  -- wind + utility solar dispatched
    uigf                           numeric(12, 4),  -- unconstrained intermittent gen forecast
    semischedule_clearedmw         numeric(12, 4),
    demand_and_nonschedgen         numeric(12, 4),
    netinterchange                 numeric(12, 4),

    -- price (DISPATCHPRICE)
    rrp                            numeric(12, 4),

    PRIMARY KEY (settlementdate, regionid)
);

CREATE INDEX idx_region_5min_region_time
    ON raw.region_5min (regionid, settlementdate);

COMMENT ON TABLE  raw.region_5min IS
    'NEM 5-min regional dispatch + price. '
    'Source: nemosis DISPATCHREGIONSUM ⨝ DISPATCHPRICE, INTERVENTION=0 only. '
    'Time zone: AEST (UTC+10) naive, no DST. '
    'See docs/METHODOLOGY.md (Definition lock) for trading_day / hour bucketing.';

COMMENT ON COLUMN raw.region_5min.settlementdate IS
    'AEMO interval-ENDING timestamp (AEST naive). Interval covers '
    '[settlementdate − 5 min, settlementdate). For day/hour bucketing '
    'use (settlementdate - interval ''5 minutes'') to avoid the off-by-one '
    'where 23:55→00:00 would otherwise group into the next day.';

COMMENT ON COLUMN raw.region_5min.intervention IS
    '0 = normal dispatch run, 1 = AEMO override run. Filtered at ingestion to 0.';

COMMENT ON COLUMN raw.region_5min.totaldemand IS
    'Operational demand in MW. = underlying demand − BTM rooftop PV. '
    'Headline metric for reversal analysis.';

COMMENT ON COLUMN raw.region_5min.uigf IS
    'Unconstrained Intermittent Generation Forecast (MW). '
    '"What would utility wind+solar produce without curtailment".';

COMMENT ON COLUMN raw.region_5min.totalintermittentgeneration IS
    'Actual cleared output from semi-scheduled wind + utility solar (MW). '
    'Compare to UIGF for curtailment estimate.';

COMMENT ON COLUMN raw.region_5min.rrp IS
    'Regional Reference Price ($/MWh). Can be negative; '
    'floor is -$1000/MWh, cap is $17500/MWh (FY26).';


-- ---------------------------------------------------------------------
-- raw.rooftop_pv_30min — AEMO ROOFTOP_PV_ACTUAL (BTM rooftop solar)
-- ---------------------------------------------------------------------
-- Grain:     (interval_datetime, regionid) — 30-min × 5 NEM regions
-- Volume:    ~320K rows when full
-- Source:    nemosis MMSDM monthly archive
-- Filtering: TYPE='MEASUREMENT' AND regionid ∈ {NSW1,QLD1,SA1,TAS1,VIC1}
--            (applied in pipeline/fetch_rooftop.py)
--            SATELLITE is AEMO's backup type; same row count, no extra value.
--            Sub-regions QLDC/QLDN/QLDS/TASN/TASS appear in source but
--            no analysis uses them — dropped at ingestion.
-- ---------------------------------------------------------------------

DROP TABLE IF EXISTS raw.rooftop_pv_30min;

CREATE TABLE raw.rooftop_pv_30min (
    interval_datetime              timestamp       NOT NULL,
    regionid                       text            NOT NULL,
    power                          numeric(12, 4),  -- MW (interval-mean)
    qi                             numeric(8, 4),   -- quality indicator [0,1]

    PRIMARY KEY (interval_datetime, regionid)
);

CREATE INDEX idx_rooftop_pv_30min_region_time
    ON raw.rooftop_pv_30min (regionid, interval_datetime);

COMMENT ON TABLE  raw.rooftop_pv_30min IS
    'AEMO ROOFTOP_PV_ACTUAL — estimated BTM rooftop PV generation in MW per region. '
    'Source: nemosis ROOFTOP_PV_ACTUAL, TYPE=MEASUREMENT, 5 main NEM regions. '
    'Time zone: AEST (UTC+10) naive, no DST. '
    'Primary BTM PV flow proxy for Finding 4.';

COMMENT ON COLUMN raw.rooftop_pv_30min.interval_datetime IS
    'AEMO interval-ENDING timestamp (AEST naive). Interval covers '
    '(interval_datetime − 30 min, interval_datetime]. For day/hour bucketing '
    'use (interval_datetime - interval ''30 minutes'') to align with '
    'NEM trading_day. Convention verified empirically: Dec 22 2024 SA1 '
    'peak lands at 12:30 INTERVAL_DATETIME, consistent with END semantics.';

COMMENT ON COLUMN raw.rooftop_pv_30min.regionid IS
    'Filtered at ingestion to {NSW1, QLD1, SA1, TAS1, VIC1}. '
    'AEMO source also publishes sub-regions (QLDC/QLDN/QLDS/TASN/TASS) which we drop.';

COMMENT ON COLUMN raw.rooftop_pv_30min.power IS
    'Interval-mean rooftop PV output in MW. From AEMO ROOFTOP_PV_ACTUAL.POWER, '
    'TYPE=MEASUREMENT only.';

COMMENT ON COLUMN raw.rooftop_pv_30min.qi IS
    'AEMO quality indicator (typically 0–1). Mostly redundant after TYPE filter.';


-- ---------------------------------------------------------------------
-- raw.weather_daily — Open-Meteo Archive (ERA5 reanalysis backend)
-- ---------------------------------------------------------------------
-- Grain:     (date, regionid) — daily × 5 NEM region capital cities
-- Volume:    ~6,695 rows when full (1,339 days × 5 regions)
-- Source:    Open-Meteo Archive API, no auth
-- Capitals:  NSW1=Sydney, VIC1=Melbourne, QLD1=Brisbane,
--            SA1=Adelaide, TAS1=Hobart
-- Time:      ALL 5 regions use Australia/Brisbane (AEST, UTC+10, NO DST)
--            for the daily aggregation window — NOT each city's local tz.
--            This aligns Open-Meteo's "day" with the NEM trading_day used
--            everywhere else. Local tz would shift Adelaide/Melbourne/
--            Sydney/Hobart day boundaries by ±30 min / ±1 h around DST
--            transitions and year-round (Adelaide is UTC+9:30 baseline),
--            polluting Finding 6 weather joins and ML feature alignment.
-- ---------------------------------------------------------------------

DROP TABLE IF EXISTS raw.weather_daily;

CREATE TABLE raw.weather_daily (
    date              date            NOT NULL,
    regionid          text            NOT NULL,
    t_max_c           numeric(5, 2),  -- temperature_2m_max,      °C
    t_min_c           numeric(5, 2),  -- temperature_2m_min,      °C
    solar_mj_m2       numeric(6, 2),  -- shortwave_radiation_sum, MJ/m^2/day
    sunshine_seconds  integer,        -- sunshine_duration,       seconds (0..86400)
    precip_mm         numeric(6, 2),  -- precipitation_sum,       mm

    PRIMARY KEY (date, regionid)
);

CREATE INDEX idx_weather_daily_region_date
    ON raw.weather_daily (regionid, date);

COMMENT ON TABLE  raw.weather_daily IS
    'Daily weather aggregates per NEM region capital city. '
    'Source: Open-Meteo Archive API (ERA5 reanalysis), TZ=Australia/Brisbane '
    '(AEST, no DST) for ALL regions. Each row''s date is the calendar day '
    'in AEST, identical to NEM trading_day. Used for Finding 6 weather drivers '
    'and Finding 8 ML features.';

COMMENT ON COLUMN raw.weather_daily.date IS
    'Calendar date in AEST (Australia/Brisbane). Equals NEM trading_day.';

COMMENT ON COLUMN raw.weather_daily.regionid IS
    'NEM region. Capital city used as weather proxy: '
    'NSW1=Sydney, VIC1=Melbourne, QLD1=Brisbane, SA1=Adelaide, TAS1=Hobart. '
    'Coordinates and tz constants in pipeline/fetch_open_meteo.py.';

COMMENT ON COLUMN raw.weather_daily.t_max_c IS
    'Open-Meteo temperature_2m_max, daily maximum 2 m temperature, °C.';

COMMENT ON COLUMN raw.weather_daily.t_min_c IS
    'Open-Meteo temperature_2m_min, daily minimum 2 m temperature, °C.';

COMMENT ON COLUMN raw.weather_daily.solar_mj_m2 IS
    'Open-Meteo shortwave_radiation_sum, daily total downwelling shortwave '
    'radiation at the surface, MJ/m^2/day. Direct proxy for rooftop PV '
    'generation potential.';

COMMENT ON COLUMN raw.weather_daily.sunshine_seconds IS
    'Open-Meteo sunshine_duration, total sunshine seconds in the day '
    '(direct radiation > 120 W/m^2 threshold). Range 0..86400.';

COMMENT ON COLUMN raw.weather_daily.precip_mm IS
    'Open-Meteo precipitation_sum, daily precipitation total, mm.';


-- =====================================================================
-- Sanity check queries (run after pipeline/load_*.py)
-- =====================================================================
-- 1. Per-region row count + date coverage
--    SELECT regionid, COUNT(*), MIN(settlementdate), MAX(settlementdate)
--    FROM raw.region_5min GROUP BY 1 ORDER BY 1;
--
--    SELECT regionid, COUNT(*), MIN(interval_datetime), MAX(interval_datetime)
--    FROM raw.rooftop_pv_30min GROUP BY 1 ORDER BY 1;
--
--    SELECT regionid, COUNT(*), MIN(date), MAX(date)
--    FROM raw.weather_daily GROUP BY 1 ORDER BY 1;
--
-- 2. NULL audit on weather (should be zero across the board)
--    SELECT regionid,
--           SUM((t_max_c          IS NULL)::int) AS null_tmax,
--           SUM((t_min_c          IS NULL)::int) AS null_tmin,
--           SUM((solar_mj_m2      IS NULL)::int) AS null_solar,
--           SUM((sunshine_seconds IS NULL)::int) AS null_sun,
--           SUM((precip_mm        IS NULL)::int) AS null_precip
--    FROM raw.weather_daily GROUP BY 1 ORDER BY 1;
--
-- 3. Climatology smoke check (Adelaide: peak Jan, trough Jul)
--    SELECT EXTRACT(month FROM date)::int AS mo,
--           ROUND(AVG(t_max_c)::numeric, 1)     AS avg_tmax_c,
--           ROUND(AVG(solar_mj_m2)::numeric, 1) AS avg_solar
--    FROM raw.weather_daily WHERE regionid = 'SA1'
--    GROUP BY 1 ORDER BY 1;
