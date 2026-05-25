{{ config(materialized='view') }}

-- Joins predict.py's logged probabilities to realised is_reversal from
-- v_ml_features. Use for daily / weekly AUC tracking once enough
-- out-of-sample rows accumulate.
--
-- actual_is_reversal is NULL when predict_for_date has not yet been
-- ingested (forward-looking predictions awaiting D+1 dispatch close).

SELECT
    p.predict_for_date,
    p.regionid,
    p.model_version,
    p.p_reversal,
    p.predicted_label,
    p.predicted_at,
    f.is_reversal::int                                  AS actual_is_reversal,
    (p.predicted_label = f.is_reversal::int)            AS hit
FROM {{ source('analytics_ops', 'predictions') }} p
LEFT JOIN {{ ref('v_ml_features') }} f
    ON f.regionid = p.regionid AND f.trading_day = p.predict_for_date
