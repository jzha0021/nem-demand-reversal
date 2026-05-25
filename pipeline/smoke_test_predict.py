"""
Smoke test: replay the NB02 test set through pipeline/predict.py and check
the realised AUC matches the value pickled into the artefact.

Why this test
-------------
`predict.py` reimplements NB02 cell 24's feature derivation (holiday flag,
time_idx, lag-7 rooftop P95, semisched-share-yesterday). A silent refactor
in either side would silently degrade production accuracy. This script is
the wire that fails loudly if that ever happens.

What it does
------------
1.  Loads `models/leak_free_lr.joblib` — single source of truth for the
    feature contract, the fitted pipeline, the train / test split date,
    and the recorded NB02 test AUC.
2.  Calls predict.py's `build_feature_frame` + `filter_to_targets` over
    the test window [split_date, test_end_date].
3.  Runs the dumped pipeline's `predict_proba` on those features.
4.  Joins the predictions to actual `is_reversal` from v_ml_features and
    computes AUC.
5.  Asserts |AUC_replay − AUC_artefact| < 1e-6. Tolerance is float-roundoff
    only; any larger gap means the predict.py / NB02 feature builders drifted.

Run
---
    python pipeline/smoke_test_predict.py
    python pipeline/smoke_test_predict.py --artefact models/leak_free_lr.joblib
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from sklearn.metrics import roc_auc_score
from sqlalchemy import text

# Reuse predict.py's logic verbatim — keeps the smoke test honest.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import predict as predict_mod  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke-test predict.py vs NB02 AUC.")
    ap.add_argument("--artefact", type=Path,
                    default=predict_mod.DEFAULT_ARTEFACT)
    ap.add_argument("--tol", type=float, default=1e-6,
                    help="Max |AUC_replay − AUC_artefact| (default 1e-6).")
    args = ap.parse_args()

    artefact = predict_mod.load_artefact(args.artefact)
    region   = artefact["target_region"]
    feature_cols = list(artefact["features_leakfree"])
    pipeline = artefact["pipeline"]
    split_date  = artefact["split_date"]
    test_end    = artefact["test_end_date"]
    first_day   = artefact["first_trading_day"]
    auc_expected = float(artefact["test_auc"])

    test_dates = pd.date_range(split_date, test_end, freq="D").date.tolist()
    print(f"[smoke] artefact={args.artefact.name}")
    print(f"[smoke] region={region}  n_test_dates={len(test_dates)}  "
          f"[{split_date} .. {test_end}]")
    print(f"[smoke] artefact test_auc={auc_expected:.6f}")

    engine = predict_mod.get_engine()
    feats = predict_mod.build_feature_frame(engine, test_dates, region, first_day)
    X_df  = predict_mod.filter_to_targets(feats, feature_cols, test_dates)

    if X_df.empty:
        sys.exit("ERROR: no feature rows after filtering — DB ingested up to "
                 f"{test_end}?")

    p = pipeline.predict_proba(X_df[feature_cols])[:, 1]

    actual = pd.read_sql(
        text(
            """
            SELECT trading_day, is_reversal::int AS is_reversal
            FROM analytics.v_ml_features
            WHERE regionid = :region
              AND trading_day BETWEEN :a AND :b
            """
        ),
        engine,
        params={"region": region, "a": split_date, "b": test_end},
        parse_dates=["trading_day"],
    )

    merged = X_df[["trading_day"]].assign(p=p).merge(
        actual, on="trading_day", how="inner",
    )
    if len(merged) != len(X_df):
        sys.exit(f"ERROR: {len(X_df) - len(merged)} predicted rows had no "
                 "actual is_reversal — check v_ml_features coverage.")

    auc_replay = roc_auc_score(merged["is_reversal"], merged["p"])
    gap = abs(auc_replay - auc_expected)

    print(f"[smoke] AUC replay     = {auc_replay:.6f}")
    print(f"[smoke] AUC artefact   = {auc_expected:.6f}")
    print(f"[smoke] |gap|          = {gap:.2e}   tol={args.tol:.0e}")

    if gap > args.tol:
        print("[smoke] FAIL — predict.py and NB02 feature builders have drifted.")
        return 1
    print("[smoke] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
