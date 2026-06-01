-- =====================================================================
-- External stage + verification. Run in Snowsight AFTER:
--   - 01_account_setup.sql
--   - 02_storage_integration.sql
--   - AWS IAM role `snowflake-nem-storage` exists (see db/snowflake/aws/)
--
-- Mounts the project S3 bucket as a Snowflake external stage. The LIST
-- queries at the bottom prove the integration trust chain works end-to-
-- end (Snowflake principal → AWS role → S3 read).
-- =====================================================================

USE ROLE R_NEM_RW;
USE WAREHOUSE WH_NEM;
USE DATABASE NEM;
USE SCHEMA RAW;

-- ---------------------------------------------------------------------
-- 1) Stage — references the storage integration so any object key under
--    the bucket URL becomes addressable as @S3_NEM_STAGE/<key>.
-- ---------------------------------------------------------------------
-- Replace <YOUR_S3_BUCKET> with the value from .env's S3_BUCKET (must
-- match the STORAGE_ALLOWED_LOCATIONS bound in 02_storage_integration.sql).
CREATE STAGE IF NOT EXISTS S3_NEM_STAGE
  STORAGE_INTEGRATION = S3_NEM
  URL = 's3://<YOUR_S3_BUCKET>/'
  COMMENT = 'NEM raw zips, backed by S3 via S3_NEM storage integration';

-- ---------------------------------------------------------------------
-- 2) Verify end-to-end. If trust policy or external ID is wrong, LIST
--    fails with an error mentioning AssumeRole / 403. If permission
--    policy lacks ListBucket, LIST returns 0 rows even though the role
--    can be assumed.
-- ---------------------------------------------------------------------

-- Whole-bucket listing — expect ~995 rows matching the local
-- `aws s3 ls --recursive` count.
LIST @S3_NEM_STAGE;

-- Tighter scoped LISTs — quick sanity that both subtrees are visible.
LIST @S3_NEM_STAGE/raw/dispatch/;
LIST @S3_NEM_STAGE/raw/rooftop/;

-- ---------------------------------------------------------------------
-- 3) Quick count + prefix breakdown via RESULT_SCAN of the LAST LIST.
--    (Snowflake's LIST output is a temporary result set we can query.)
-- ---------------------------------------------------------------------
SELECT
    REGEXP_SUBSTR("name", '[^/]+/[^/]+/[^/]+/')         AS top_three_prefix,
    COUNT(*)                                            AS objects,
    ROUND(SUM("size") / 1024.0, 1)                      AS total_kb
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1)))               -- the most recent LIST above
GROUP BY 1
ORDER BY 1
LIMIT 20;
