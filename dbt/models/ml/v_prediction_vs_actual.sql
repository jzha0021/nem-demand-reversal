{{ config(materialized='view') }}

-- Joins predict.py's logged probabilities to realised is_reversal from
-- v_ml_features. Use for daily / weekly AUC tracking once enough
-- out-of-sample rows accumulate.
--
-- actual_is_reversal / hit are NULL until the predict_for_date is a
-- complete trading day (n_intervals = 288 dispatch intervals). The
-- daily cron runs at AEST 01:00 and predicts for "today", so for ~24 h
-- the prediction sits next to a partial dispatch day; gating on
-- n_intervals = 288 prevents the dashboard from showing a misleading
-- "missed" badge while the trading day is still in progress.

SELECT
    p.predict_for_date,
    p.regionid,
    p.model_version,
    p.p_reversal,
    p.predicted_label,
    p.predicted_at,
    CASE WHEN f.n_intervals = 288
         THEN {{ bool_to_int('f.is_reversal') }}
    END                                                                AS actual_is_reversal,
    CASE WHEN f.n_intervals = 288
         THEN (p.predicted_label = {{ bool_to_int('f.is_reversal') }})
    END                                                                AS hit
FROM {{ source('analytics_ops', 'predictions') }} p
LEFT JOIN {{ ref('v_ml_features') }} f
    ON f.regionid = p.regionid AND f.trading_day = p.predict_for_date
