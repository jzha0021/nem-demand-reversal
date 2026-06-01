-- =====================================================================
-- File format + Snowpipes for the parquet-based S3 → Snowflake ingestion.
-- Run in Snowsight after 04_raw_schema.sql.
--
-- Rerun behaviour: this file uses CREATE OR REPLACE for the file format
-- and the 3 pipes. That's safe — the FILE FORMAT and PIPE objects own
-- no rows, so re-creation cannot destroy data in NEM.RAW.*. The only
-- side effect is that each pipe's per-file load history resets, so
-- Snowpipe will not retroactively re-ingest already-loaded files. If
-- you need that, follow up with ALTER PIPE <name> REFRESH per pipe.
--
-- Architecture
-- ------------
-- Fetcher writes parquet to s3://bucket/parsed/{dispatch,rooftop,weather}/
-- New objects trigger an S3 event → Snowflake's per-pipe SQS queue →
-- Snowpipe runs the COPY INTO inside each CREATE PIPE statement.
--
-- Column matching is CASE_INSENSITIVE — parquet writer (see
-- pipeline/_common.py::upload_parquet_to_s3) emits columns matching the
-- table column names. _INGESTED_AT is NOT in the parquet; the table
-- default fills it at COPY time.
--
-- After this file runs, take the `notification_channel` value from the
-- SHOW PIPES output below and plug it into the AWS S3 event
-- notification (see db/snowflake/aws/s3_event_notification.md). All
-- three pipes share the same notification channel — Snowflake routes
-- each event to the right pipe via the COPY INTO stage path.
-- =====================================================================

USE ROLE R_NEM_RW;
USE WAREHOUSE WH_NEM;
USE DATABASE NEM;
USE SCHEMA RAW;

-- ---------------------------------------------------------------------
-- 1) Single PARQUET file format reused by all three pipes.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT PARQUET_FF
  TYPE = PARQUET
  COMPRESSION = SNAPPY
  COMMENT = 'Standard parquet format for NEM Snowpipe ingestion';

-- ---------------------------------------------------------------------
-- 2) NEM_PIPE_DISPATCH — s3://.../parsed/dispatch/*.parquet
--
-- MATCH_BY_COLUMN_NAME lets Snowflake auto-map parquet columns to table
-- columns by name (case-insensitive). Avoids the hand-written VARIANT
-- cast pattern, which mangled parquet TIMESTAMP[ns] into "Invalid date"
-- during the first smoke test. Snowflake handles type narrowing
-- automatically when the parquet logical type can fit the table column.
-- ---------------------------------------------------------------------
CREATE OR REPLACE PIPE NEM_PIPE_DISPATCH
  AUTO_INGEST = TRUE
  COMMENT = 'Auto-ingest dispatch parsed parquet → REGION_5MIN'
  AS
COPY INTO NEM.RAW.REGION_5MIN
FROM @NEM.RAW.S3_NEM_STAGE/parsed/dispatch/
FILE_FORMAT = (FORMAT_NAME = PARQUET_FF)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\\.parquet';

-- ---------------------------------------------------------------------
-- 3) NEM_PIPE_ROOFTOP — s3://.../parsed/rooftop/*.parquet
-- ---------------------------------------------------------------------
CREATE OR REPLACE PIPE NEM_PIPE_ROOFTOP
  AUTO_INGEST = TRUE
  COMMENT = 'Auto-ingest rooftop parsed parquet → ROOFTOP_PV_30MIN'
  AS
COPY INTO NEM.RAW.ROOFTOP_PV_30MIN
FROM @NEM.RAW.S3_NEM_STAGE/parsed/rooftop/
FILE_FORMAT = (FORMAT_NAME = PARQUET_FF)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\\.parquet';

-- ---------------------------------------------------------------------
-- 4) NEM_PIPE_WEATHER — s3://.../parsed/weather/*.parquet
-- ---------------------------------------------------------------------
CREATE OR REPLACE PIPE NEM_PIPE_WEATHER
  AUTO_INGEST = TRUE
  COMMENT = 'Auto-ingest weather parsed parquet → WEATHER_DAILY'
  AS
COPY INTO NEM.RAW.WEATHER_DAILY
FROM @NEM.RAW.S3_NEM_STAGE/parsed/weather/
FILE_FORMAT = (FORMAT_NAME = PARQUET_FF)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\\.parquet';

-- ---------------------------------------------------------------------
-- 5) MONITOR grants for R_NEM_READ — Streamlit's pipe-health badges
--    call SYSTEM$PIPE_STATUS, which requires MONITOR (not OWNERSHIP).
--    Granted per pipe because Snowflake forbids bulk grants on PIPE
--    type; if a new pipe is added above, append one line here.
-- ---------------------------------------------------------------------
GRANT MONITOR ON PIPE NEM.RAW.NEM_PIPE_DISPATCH TO ROLE R_NEM_READ;
GRANT MONITOR ON PIPE NEM.RAW.NEM_PIPE_ROOFTOP  TO ROLE R_NEM_READ;
GRANT MONITOR ON PIPE NEM.RAW.NEM_PIPE_WEATHER  TO ROLE R_NEM_READ;

-- ---------------------------------------------------------------------
-- 6) Verify pipes are created + extract SQS ARN for S3 notification config
-- ---------------------------------------------------------------------
SHOW PIPES IN SCHEMA NEM.RAW;

-- The crucial output: `notification_channel` column gives the SQS ARN
-- Snowflake will listen on. Use it to configure the AWS S3 event
-- notification (`db/snowflake/aws/s3_event_notification.md` walks the
-- Console + CLI steps).
--
-- All three pipes share the SAME notification_channel — by design.
-- Snowflake routes events to the right pipe via the COPY INTO stage
-- path inside each PIPE definition.
SELECT
    "name",
    "notification_channel" AS sqs_arn,
    "definition"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" LIKE 'NEM_PIPE_%'
ORDER BY "name";
