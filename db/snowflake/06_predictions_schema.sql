-- =====================================================================
-- Predictions log on Snowflake — mirrors Postgres analytics.predictions
-- (db/02_predictions_schema.sql) with Snowflake type equivalents.
--
-- Order matters: dbt's v_prediction_vs_actual model references this
-- table via {{ source('analytics_ops', 'predictions') }}; the source
-- must exist before `dbt build` runs.
-- =====================================================================

USE ROLE R_NEM_RW;
USE WAREHOUSE WH_NEM;
USE DATABASE NEM;
USE SCHEMA ANALYTICS;

CREATE TABLE IF NOT EXISTS PREDICTIONS (
    PREDICT_FOR_DATE    DATE             NOT NULL,
    REGIONID            VARCHAR(8)       NOT NULL,
    P_REVERSAL          NUMBER(7, 6)     NOT NULL,
    PREDICTED_LABEL     NUMBER(2, 0)     NOT NULL,
    MODEL_VERSION       VARCHAR(64)      NOT NULL,
    PREDICTED_AT        TIMESTAMP_LTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_PREDICTIONS
        PRIMARY KEY (PREDICT_FOR_DATE, REGIONID, MODEL_VERSION) NOT ENFORCED,
    CONSTRAINT CHK_P_RANGE
        CHECK (P_REVERSAL BETWEEN 0 AND 1),
    CONSTRAINT CHK_LABEL_BINARY
        CHECK (PREDICTED_LABEL IN (0, 1))
)
COMMENT = 'Next-day reversal forecast log (D from D-1 features). Mirrors Postgres analytics.predictions. Written by pipeline/predict.py --target snowflake. PK allows multiple model_versions per (date, region) for A/B comparison.';

-- Clustering by region + date helps the dbt v_prediction_vs_actual join
-- and the rolling AUC queries Streamlit will run.
ALTER TABLE PREDICTIONS CLUSTER BY (REGIONID, PREDICT_FOR_DATE);

SHOW TABLES LIKE 'PREDICTIONS' IN SCHEMA NEM.ANALYTICS;
