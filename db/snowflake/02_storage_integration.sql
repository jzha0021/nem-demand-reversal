-- =====================================================================
-- Snowflake → S3 storage integration. Run in Snowsight AFTER 01_account_setup.sql.
--
-- This object lets Snowflake assume an AWS IAM role to read the S3
-- bucket. The role's trust policy must reference Snowflake-side values
-- that we won't know until AFTER this CREATE runs, so the flow is:
--
--   1.  Run this file as ACCOUNTADMIN
--   2.  Read STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID from the
--       DESC STORAGE INTEGRATION output at the bottom of this file
--   3.  Paste those two values into `db/snowflake/aws/trust_policy.json`
--       (principal + sts:ExternalId condition) and re-create the AWS
--       IAM role `snowflake-nem-storage` with the updated trust policy
--   4.  Run 03_stage_and_test.sql to mount the stage + verify
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- Replace the placeholders below with your actual values before running:
--   <YOUR_AWS_ACCOUNT_ID> — the 12-digit AWS account hosting the S3 bucket
--   <YOUR_S3_BUCKET>      — the bucket name (project convention:
--                          nem-demand-reversal-<user>-<account>-<region>-<suffix>)
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_NEM
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/snowflake-nem-storage'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://<YOUR_S3_BUCKET>/'
  )
  COMMENT = 'Read-only bridge into the NEM demand reversal S3 bucket';

-- Allow the working role to mount stages on top of this integration.
-- Without this grant, R_NEM_RW can't create or use stages that reference
-- S3_NEM (ACCOUNTADMIN can but we don't want to run dbt as ACCOUNTADMIN).
GRANT USAGE ON INTEGRATION S3_NEM TO ROLE R_NEM_RW;

-- ---------------------------------------------------------------------
-- The DESC output below has two key rows:
--
--   STORAGE_AWS_IAM_USER_ARN  — a Snowflake-side principal
--     (looks like arn:aws:iam::<long-id>:user/<longer-id>)
--   STORAGE_AWS_EXTERNAL_ID   — a UUID-like token
--
-- Both feed the AWS IAM role's trust policy at
-- db/snowflake/aws/trust_policy.json: the principal is the entity
-- Snowflake authenticates as, and the external ID prevents a
-- confused-deputy attack from another Snowflake tenant.
-- ---------------------------------------------------------------------
DESC STORAGE INTEGRATION S3_NEM;
