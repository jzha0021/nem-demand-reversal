-- =====================================================================
-- Snowflake account bootstrap — run once in Snowsight as ACCOUNTADMIN.
--
-- Creates the database, warehouse, schemas, roles, and users used by
-- dbt + cron + Streamlit. Deployment-specific identifiers (your
-- Snowflake account name, your admin username) live outside the
-- numbered scripts; before running, replace every
-- <YOUR_SNOWFLAKE_ADMIN_USER> with the actual admin user (the one you
-- registered with) so the GRANT TO USER statements bind correctly.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------
-- 1) Compute warehouse — XSMALL, auto-suspend in 60 seconds so idle
--    moments don't burn credits. Trial gives $400; this size + suspend
--    rule means a daily cron + ad-hoc dbt runs cost well under $5/month.
-- ---------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS WH_NEM
  WITH WAREHOUSE_SIZE = 'XSMALL'
       AUTO_SUSPEND = 60
       AUTO_RESUME = TRUE
       INITIALLY_SUSPENDED = TRUE
       COMMENT = 'NEM demand reversal — dbt + cron + ad-hoc';

-- ---------------------------------------------------------------------
-- 2) Database + schemas
--    RAW       — Snowpipe / COPY destination (mirrors Postgres raw.*)
--    ANALYTICS — dbt build target (mirrors Postgres analytics.*)
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS NEM
  COMMENT = 'NEM demand reversal — cloud DW';

USE DATABASE NEM;

CREATE SCHEMA IF NOT EXISTS RAW
  COMMENT = 'Raw ingestion target (mirrors Postgres raw schema)';

CREATE SCHEMA IF NOT EXISTS ANALYTICS
  COMMENT = 'dbt build target (mirrors Postgres analytics schema)';

-- ---------------------------------------------------------------------
-- 3a) R_NEM_RW — read/write role used by dbt + the daily cron + ad-hoc
--     Snowsight sessions. Owns the build side of the pipeline.
-- ---------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS R_NEM_RW
  COMMENT = 'NEM project read/write role (dbt + cron + interactive)';

GRANT USAGE ON WAREHOUSE WH_NEM TO ROLE R_NEM_RW;
GRANT OPERATE ON WAREHOUSE WH_NEM TO ROLE R_NEM_RW;

GRANT USAGE ON DATABASE NEM TO ROLE R_NEM_RW;

GRANT USAGE ON SCHEMA NEM.RAW       TO ROLE R_NEM_RW;
GRANT USAGE ON SCHEMA NEM.ANALYTICS TO ROLE R_NEM_RW;

-- Object creation on both schemas; SELECT on FUTURE tables/views
-- so dbt re-runs don't need a separate grant pass.
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE PIPE,
      CREATE FILE FORMAT, CREATE FUNCTION
  ON SCHEMA NEM.RAW TO ROLE R_NEM_RW;

GRANT CREATE TABLE, CREATE VIEW
  ON SCHEMA NEM.ANALYTICS TO ROLE R_NEM_RW;

GRANT SELECT ON FUTURE TABLES IN SCHEMA NEM.RAW       TO ROLE R_NEM_RW;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA NEM.RAW       TO ROLE R_NEM_RW;
GRANT SELECT ON FUTURE TABLES IN SCHEMA NEM.ANALYTICS TO ROLE R_NEM_RW;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA NEM.ANALYTICS TO ROLE R_NEM_RW;

-- ---------------------------------------------------------------------
-- 3b) R_NEM_READ — least-privilege read-only role for the public
--     Streamlit dashboard. SELECT on analytics views + raw tables
--     (Streamlit's "table row counts" panel reads RAW row counts) +
--     MONITOR on the pipes (Streamlit's health badges call
--     SYSTEM$PIPE_STATUS, which requires MONITOR not OWNERSHIP).
-- ---------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS R_NEM_READ
  COMMENT = 'NEM project read-only role (public Streamlit dashboard)';

GRANT USAGE ON WAREHOUSE WH_NEM TO ROLE R_NEM_READ;
GRANT USAGE ON DATABASE NEM TO ROLE R_NEM_READ;
GRANT USAGE ON SCHEMA NEM.RAW       TO ROLE R_NEM_READ;
GRANT USAGE ON SCHEMA NEM.ANALYTICS TO ROLE R_NEM_READ;

GRANT SELECT ON ALL    TABLES IN SCHEMA NEM.RAW       TO ROLE R_NEM_READ;
GRANT SELECT ON FUTURE TABLES IN SCHEMA NEM.RAW       TO ROLE R_NEM_READ;
GRANT SELECT ON ALL    VIEWS  IN SCHEMA NEM.RAW       TO ROLE R_NEM_READ;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA NEM.RAW       TO ROLE R_NEM_READ;
GRANT SELECT ON ALL    TABLES IN SCHEMA NEM.ANALYTICS TO ROLE R_NEM_READ;
GRANT SELECT ON FUTURE TABLES IN SCHEMA NEM.ANALYTICS TO ROLE R_NEM_READ;
GRANT SELECT ON ALL    VIEWS  IN SCHEMA NEM.ANALYTICS TO ROLE R_NEM_READ;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA NEM.ANALYTICS TO ROLE R_NEM_READ;

-- Per-pipe MONITOR grants for R_NEM_READ live at the END of
-- 05_file_format_and_pipes.sql, after each CREATE PIPE — pipes do not
-- exist yet at this point in the numbered runbook, and Snowflake
-- forbids bulk grants on PIPE type (no GRANT ... ON ALL/FUTURE PIPES).

-- ---------------------------------------------------------------------
-- 4) Attach roles. <YOUR_SNOWFLAKE_ADMIN_USER> (interactive / dbt) gets RW + READ;
--    Streamlit's connection should authenticate as a user that only
--    holds R_NEM_READ — never RW.
-- ---------------------------------------------------------------------
GRANT ROLE R_NEM_RW   TO USER <YOUR_SNOWFLAKE_ADMIN_USER>;
GRANT ROLE R_NEM_READ TO USER <YOUR_SNOWFLAKE_ADMIN_USER>;

-- Make R_NEM_RW the default role for the admin user so notebooks /
-- Snowsight queries pick it up automatically without USE ROLE every
-- session.
ALTER USER <YOUR_SNOWFLAKE_ADMIN_USER> SET DEFAULT_ROLE = R_NEM_RW;
ALTER USER <YOUR_SNOWFLAKE_ADMIN_USER> SET DEFAULT_WAREHOUSE = WH_NEM;
ALTER USER <YOUR_SNOWFLAKE_ADMIN_USER> SET DEFAULT_NAMESPACE = NEM.RAW;

-- ---------------------------------------------------------------------
-- 5) Service users — RSA key-pair authentication.
--
--    Paid Snowflake accounts force MFA on TYPE=PERSON users, so a
--    headless pipeline / Streamlit cannot authenticate as the admin
--    user above (which stays PERSON for Snowsight access). Two
--    dedicated TYPE=SERVICE users — one RW for CI/cron, one READ for
--    Streamlit — are isolated by role and authenticate via key-pair.
--
--    Generate two PKCS8 PEM key pairs locally before running this:
--        openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM \
--            -out nem_ci.p8 -nocrypt
--        openssl rsa -in nem_ci.p8 -pubout -out nem_ci.pub
--        (repeat with streamlit.p8 / streamlit.pub)
--    The RSA_PUBLIC_KEY value is the body of the .pub file with the
--    BEGIN/END headers stripped and the newlines removed (Snowflake
--    accepts both single-line base64 and multi-line — newlines are
--    fine inside the quoted string).
-- ---------------------------------------------------------------------

-- 5a) Pipeline / CI service user (RW)
CREATE USER IF NOT EXISTS NEM_CI
  TYPE              = SERVICE
  DEFAULT_ROLE      = R_NEM_RW
  DEFAULT_WAREHOUSE = WH_NEM
  DEFAULT_NAMESPACE = NEM.RAW
  COMMENT           = 'Headless cron / dbt / predict — key-pair auth, RW role';

GRANT ROLE R_NEM_RW TO USER NEM_CI;

-- 5b) Streamlit dashboard service user (READ-only)
CREATE USER IF NOT EXISTS STREAMLIT_DASHBOARD
  TYPE              = SERVICE
  DEFAULT_ROLE      = R_NEM_READ
  DEFAULT_WAREHOUSE = WH_NEM
  DEFAULT_NAMESPACE = NEM.ANALYTICS
  COMMENT           = 'Public Streamlit dashboard — key-pair auth, read-only';

GRANT ROLE R_NEM_READ TO USER STREAMLIT_DASHBOARD;

-- After running this file, register each user's public key in Snowsight
-- (NOT here — keeps the key body out of the query history of the script).
-- Snowflake's ALTER USER ... SET doesn't accept TYPE and RSA_PUBLIC_KEY
-- in a single SET clause, so migrations from an existing PASSWORD user
-- need two statements:
--    -- New users (NEM_CI) are already TYPE=SERVICE from the CREATE above:
--    ALTER USER NEM_CI            SET RSA_PUBLIC_KEY = '<body of nem_ci.pub>';
--    -- Existing PERSON users (legacy STREAMLIT_DASHBOARD) migrate in two:
--    ALTER USER STREAMLIT_DASHBOARD SET TYPE = SERVICE;
--    ALTER USER STREAMLIT_DASHBOARD SET RSA_PUBLIC_KEY = '<body of streamlit.pub>';
-- Local dev .env points SNOWFLAKE_PRIVATE_KEY_PATH at the matching .p8;
-- CI / Streamlit Cloud paste the multi-line PEM directly into their
-- secret stores (see .env.example + .streamlit/secrets.toml.example).

-- ---------------------------------------------------------------------
-- 6) Verify
-- ---------------------------------------------------------------------
USE ROLE R_NEM_RW;
USE WAREHOUSE WH_NEM;
USE DATABASE NEM;

SHOW WAREHOUSES LIKE 'WH_NEM';
SHOW DATABASES  LIKE 'NEM';
SHOW SCHEMAS    IN DATABASE NEM;
SHOW GRANTS     TO ROLE R_NEM_RW;

-- Expected: WH_NEM (XSMALL, SUSPENDED), NEM database, RAW + ANALYTICS
-- + PUBLIC + INFORMATION_SCHEMA schemas, ~15 grants on R_NEM_RW.
