-- =====================================================================
-- Raw layer on Snowflake — mirrors Postgres raw.* schema with Snowflake
-- type equivalents. Run in Snowsight after 01–03 are validated.
--
-- Idempotent by design: every table uses CREATE TABLE IF NOT EXISTS so
-- re-running this file on an already-bootstrapped account is a no-op
-- and CANNOT wipe production data. Schema changes (column adds, type
-- widens) must be applied via explicit ALTER TABLE in a follow-up
-- migration file — do not edit the CREATE block to amend a live
-- column.
--
-- Differences vs Postgres (db/01_raw_schema.sql):
--   - TIMESTAMP            → TIMESTAMP_NTZ   (AEST naive, no tz)
--   - TEXT                 → VARCHAR
--   - NUMERIC(12,4)        → NUMBER(12,4)
--   - SMALLINT             → NUMBER(2,0)
--   - DATE                 → DATE
--   - INTEGER              → NUMBER
--   - PRIMARY KEY          → declared as metadata only; Snowflake does not
--                            enforce. Dedup happens in dbt staging.
--   - + _INGESTED_AT       → TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP.
--                            Used by dbt staging models (dedupe_latest
--                            macro) to pick the most recent version when
--                            Snowpipe re-ingests a re-uploaded parquet —
--                            idempotency lives in the staging layer, not
--                            at the raw table.
-- =====================================================================

USE ROLE R_NEM_RW;
USE WAREHOUSE WH_NEM;
USE DATABASE NEM;
USE SCHEMA RAW;

-- ---------------------------------------------------------------------
-- 1) REGION_5MIN — 5-min dispatch × 5 NEM regions
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS REGION_5MIN (
    SETTLEMENTDATE                  TIMESTAMP_NTZ      NOT NULL,
    REGIONID                        VARCHAR(8)         NOT NULL,
    INTERVENTION                    NUMBER(2, 0)       NOT NULL,

    TOTALDEMAND                     NUMBER(12, 4),
    AVAILABLEGENERATION             NUMBER(12, 4),
    TOTALINTERMITTENTGENERATION     NUMBER(12, 4),
    UIGF                            NUMBER(12, 4),
    SEMISCHEDULE_CLEAREDMW          NUMBER(12, 4),
    DEMAND_AND_NONSCHEDGEN          NUMBER(12, 4),
    NETINTERCHANGE                  NUMBER(12, 4),

    RRP                             NUMBER(12, 4),

    _INGESTED_AT                    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_REGION_5MIN PRIMARY KEY (SETTLEMENTDATE, REGIONID) NOT ENFORCED
)
COMMENT = 'NEM 5-min regional dispatch + price. Mirrors Postgres raw.region_5min. INTERVENTION=0 only. AEST (UTC+10) naive. Snowpipe target via NEM_PIPE_DISPATCH.';

-- Clustering key on time + region helps dbt staging models that bucket
-- by trading_day and the future predict.py feature reads.
ALTER TABLE REGION_5MIN CLUSTER BY (SETTLEMENTDATE, REGIONID);


-- ---------------------------------------------------------------------
-- 2) ROOFTOP_PV_30MIN — 30-min ROOFTOP_PV_ACTUAL MEASUREMENT × 5 regions
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ROOFTOP_PV_30MIN (
    INTERVAL_DATETIME               TIMESTAMP_NTZ      NOT NULL,
    REGIONID                        VARCHAR(8)         NOT NULL,
    POWER                           NUMBER(12, 4),
    QI                              NUMBER(8, 4),

    _INGESTED_AT                    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_ROOFTOP_PV_30MIN PRIMARY KEY (INTERVAL_DATETIME, REGIONID) NOT ENFORCED
)
COMMENT = 'AEMO ROOFTOP_PV_ACTUAL — MW per 30-min × 5 main NEM regions. Mirrors Postgres raw.rooftop_pv_30min. TYPE=MEASUREMENT only. Snowpipe target via NEM_PIPE_ROOFTOP.';

ALTER TABLE ROOFTOP_PV_30MIN CLUSTER BY (INTERVAL_DATETIME, REGIONID);


-- ---------------------------------------------------------------------
-- 3) WEATHER_DAILY — Open-Meteo Archive daily aggregates × 5 region capitals
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS WEATHER_DAILY (
    DATE                            DATE               NOT NULL,
    REGIONID                        VARCHAR(8)         NOT NULL,
    T_MAX_C                         NUMBER(5, 2),
    T_MIN_C                         NUMBER(5, 2),
    SOLAR_MJ_M2                     NUMBER(6, 2),
    SUNSHINE_SECONDS                NUMBER(7, 0),
    PRECIP_MM                       NUMBER(6, 2),

    _INGESTED_AT                    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_WEATHER_DAILY PRIMARY KEY (DATE, REGIONID) NOT ENFORCED
)
COMMENT = 'Daily weather aggregates per NEM region capital. Mirrors Postgres raw.weather_daily. TZ=Australia/Brisbane (AEST). Snowpipe target via NEM_PIPE_WEATHER.';

-- Weather is tiny; clustering would be overkill.


-- ---------------------------------------------------------------------
-- Verify
-- ---------------------------------------------------------------------
SHOW TABLES IN SCHEMA NEM.RAW;

SELECT TABLE_NAME, ROW_COUNT, BYTES, COMMENT
  FROM NEM.INFORMATION_SCHEMA.TABLES
 WHERE TABLE_SCHEMA = 'RAW'
   AND TABLE_TYPE = 'BASE TABLE'
 ORDER BY TABLE_NAME;

-- Expected: 3 tables, all 0 rows (will be populated via Snowpipe).
