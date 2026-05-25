-- =====================================================================
-- 03_predictions_schema.sql — Phase 2 prediction log
-- =====================================================================
-- Run AFTER 02_analytics_views.sql:
--   psql -d nem -f db/03_predictions_schema.sql
--
-- One row per (predict_for_date, regionid, model_version). Written by
-- pipeline/predict.py; read by monitoring queries + Power BI.
--
-- Multiple model_versions can coexist for the same (date, region) — this
-- enables A/B comparison and version rollback without losing history.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS analytics;


-- ---------------------------------------------------------------------
-- analytics.predictions — 24h-ahead reversal probability log
-- ---------------------------------------------------------------------
-- predict_for_date  = the D for which we predicted is_reversal (= 1 if D's
--                     min-demand hour ∈ {10..15}).
-- p_reversal        = LR pipeline predict_proba()[:, 1] for the positive class.
-- predicted_label   = (p_reversal >= 0.5) cast to smallint. Derived, but
--                     stored for cheap SQL aggregation.
-- model_version     = string ID dumped alongside the joblib artefact (e.g.
--                     'leak_free_lr_v1_2026-05-14'). Tied to the training
--                     end-date + feature list snapshot.
-- predicted_at      = wall-clock UTC when predict.py ran. Use to audit
--                     daily-cron freshness and detect missed runs.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.predictions (
    predict_for_date    date            NOT NULL,
    regionid            text            NOT NULL,
    p_reversal          numeric(7, 6)   NOT NULL,
    predicted_label     smallint        NOT NULL,
    model_version       text            NOT NULL,
    predicted_at        timestamptz     NOT NULL DEFAULT now(),

    PRIMARY KEY (predict_for_date, regionid, model_version),

    CONSTRAINT chk_p_range       CHECK (p_reversal BETWEEN 0 AND 1),
    CONSTRAINT chk_label_binary  CHECK (predicted_label IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_predictions_region_date
    ON analytics.predictions (regionid, predict_for_date);

COMMENT ON TABLE analytics.predictions IS
    'Phase 2 24h-ahead reversal forecast log. Written by pipeline/predict.py. '
    'PK allows multiple model_versions per (date, region) for A/B comparison.';

COMMENT ON COLUMN analytics.predictions.predict_for_date IS
    'Target date D for which is_reversal was predicted. Uses NEM trading_day '
    '(interval-START convention) — same definition as analytics.v_ml_features.';

COMMENT ON COLUMN analytics.predictions.p_reversal IS
    'sklearn predict_proba()[:, 1] for is_reversal=1. Range [0, 1].';

COMMENT ON COLUMN analytics.predictions.model_version IS
    'String ID dumped with the joblib artefact, e.g. leak_free_lr_v1_YYYY-MM-DD. '
    'Pins the training end-date and feature list; bump on retrain.';


-- ---------------------------------------------------------------------
-- v_prediction_vs_actual — reconciliation view (read-only)
-- ---------------------------------------------------------------------
-- Joins predictions to the realised outcome from v_ml_features. Use for
-- daily/weekly AUC tracking once enough out-of-sample rows accumulate.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_prediction_vs_actual AS
SELECT
    p.predict_for_date,
    p.regionid,
    p.model_version,
    p.p_reversal,
    p.predicted_label,
    p.predicted_at,
    f.is_reversal::int                                  AS actual_is_reversal,
    (p.predicted_label = f.is_reversal::int)            AS hit
FROM analytics.predictions p
LEFT JOIN analytics.v_ml_features f
    ON f.regionid = p.regionid AND f.trading_day = p.predict_for_date;

COMMENT ON VIEW analytics.v_prediction_vs_actual IS
    'Reconciles predictions with realised is_reversal from v_ml_features. '
    'actual_is_reversal is NULL when the predict_for_date has not yet been '
    'ingested (forward-looking predictions awaiting D+1 dispatch close).';


-- =====================================================================
-- Sanity queries
-- =====================================================================
-- 1. Latest prediction per region per model
--    SELECT regionid, model_version, MAX(predict_for_date) AS latest, MAX(predicted_at) AS run_at
--    FROM analytics.predictions GROUP BY 1, 2 ORDER BY 1, 2;
--
-- 2. Rolling AUC inputs (compute AUC in pandas/sklearn from these rows)
--    SELECT predict_for_date, p_reversal, actual_is_reversal
--    FROM analytics.v_prediction_vs_actual
--    WHERE regionid = 'VIC1' AND model_version = 'leak_free_lr_v1_2026-05-14'
--      AND actual_is_reversal IS NOT NULL
--    ORDER BY predict_for_date;
