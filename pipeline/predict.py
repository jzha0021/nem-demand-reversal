"""
24h-ahead reversal probability — daily inference CLI.

Loads the leak-free Logistic Regression pipeline dumped at the end of
notebooks/02_reversal_classifier.ipynb and writes P(is_reversal) for one or
more target dates D into analytics.predictions.

The feature contract is the artefact's single source of truth:
    artefact['features_leakfree']  — column order
    artefact['first_trading_day']  — anchors time_idx (row position relative
                                     to NB02 training-time row 0)
    artefact['target_region']      — VIC1 in Phase 1; reusable in Phase 3
    artefact['model_version']      — pinned to (training-end-date + feature set)

Notebook-derived features replicated here verbatim:
    is_public_holiday         — holidays.Australia(subdiv='VIC')
    time_idx                  — (D - first_trading_day).days
    lag7_rooftop_p95_mw       — v_ml_features.rooftop_p95_mw at D-7
    semisched_share_yesterday — AVG(semischedule_clearedmw / totaldemand) over D-1

Usage
-----
    python pipeline/predict.py --date 2026-03-15
    python pipeline/predict.py --backfill 2025-01-01 2025-01-07
    python pipeline/predict.py --date 2026-03-15 --dry-run

Timing constraint
-----------------
Predict.py for target date D requires D-1's full-day dispatch (288 5-min
intervals) AND D-1's full-day rooftop (48 30-min intervals) — analytics.
v_rooftop_daily NULL-guards the rooftop metrics when n_intervals < 48, and
predict.py drops rows with any NULL feature. Schedule the daily cron AFTER
AEMO has fully published D-1 (safe target: AEST 01:00 next day; rooftop
publication lag is ~20 min so the tail of D-1 lands shortly after midnight).
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import holidays
import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ARTEFACT = PROJECT_ROOT / "models" / "leak_free_lr.joblib"


# ---------------------------------------------------------------------------
# DB connection (mirrors pipeline/export_for_powerbi.py)
# ---------------------------------------------------------------------------
def get_engine() -> Engine:
    load_dotenv(PROJECT_ROOT / ".env")
    user = os.getenv("DB_USER", "postgres")
    pwd  = os.getenv("DB_PWD")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "nem")
    if not pwd:
        sys.exit("ERROR: DB_PWD not set in .env")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}")


# ---------------------------------------------------------------------------
# Artefact loading + sanity
# ---------------------------------------------------------------------------
def load_artefact(path: Path) -> dict:
    if not path.exists():
        sys.exit(
            f"ERROR: model artefact not found at {path}\n"
            "Run the joblib.dump cell at the end of "
            "notebooks/02_reversal_classifier.ipynb to produce it."
        )
    art = joblib.load(path)
    required = {
        "pipeline", "features_leakfree", "first_trading_day",
        "target_region", "model_version",
    }
    missing = required - set(art)
    if missing:
        sys.exit(f"ERROR: artefact missing keys: {sorted(missing)}")
    return art


# ---------------------------------------------------------------------------
# Feature builder
# ---------------------------------------------------------------------------
# Notebook-derived features that don't come straight from v_ml_features.
# Keep this in sync with notebooks/02_reversal_classifier.ipynb cell 24.
# ---------------------------------------------------------------------------
def build_feature_frame(
    engine: Engine,
    target_dates: list[date],
    region: str,
    first_trading_day: date,
) -> pd.DataFrame:
    """Return one row per target date with all FEATURES_LEAKFREE columns.

    Rows with NULL on any feature are dropped (matching NB02 cell 25
    `vic_clean = vic.dropna(subset=needed_cols)`).
    """
    # Pull a window that covers every target plus the 7-day lookback needed
    # by lag7_rooftop_p95_mw. Pulling everything from first_trading_day is
    # cheap (~1300 rows) and keeps the lag-7 / D-1 logic identical to NB02.
    target_min = min(target_dates)
    target_max = max(target_dates)
    window_start = min(first_trading_day, target_min - timedelta(days=8))

    feats = pd.read_sql(
        text(
            """
            SELECT *
            FROM analytics.v_ml_features
            WHERE regionid = :region
              AND trading_day BETWEEN :start AND :end
            ORDER BY trading_day
            """
        ),
        engine,
        params={"region": region, "start": window_start, "end": target_max},
        parse_dates=["trading_day"],
    )
    if feats.empty:
        sys.exit(
            f"ERROR: no rows in analytics.v_ml_features for region={region} "
            f"between {window_start} and {target_max}. Did you run pipeline/"
            "fetch_*.py + load_*.py for this date range?"
        )

    # --- Notebook-derived features (mirror NB02 cell 24 exactly) ---
    region_to_subdiv = {
        "VIC1": "VIC", "NSW1": "NSW", "QLD1": "QLD",
        "SA1": "SA",  "TAS1": "TAS",
    }
    au_holidays = holidays.Australia(subdiv=region_to_subdiv[region])
    feats["is_public_holiday"] = (
        feats["trading_day"].dt.date.apply(lambda d: d in au_holidays).astype(int)
    )

    feats["time_idx"] = (feats["trading_day"].dt.date
                         .apply(lambda d: (d - first_trading_day).days))

    feats["lag7_rooftop_p95_mw"] = feats["rooftop_p95_mw"].shift(7)

    util = pd.read_sql(
        text(
            """
            SELECT trading_day,
                   AVG(semischedule_clearedmw) / NULLIF(AVG(totaldemand), 0)
                       AS share_today
            FROM analytics.stg_region_5min
            WHERE regionid = :region
              AND trading_day BETWEEN :start AND :end
            GROUP BY 1
            ORDER BY 1
            """
        ),
        engine,
        params={"region": region, "start": window_start, "end": target_max},
        parse_dates=["trading_day"],
    )
    util["semisched_share_yesterday"] = util["share_today"].shift(1)
    feats = feats.merge(
        util[["trading_day", "semisched_share_yesterday"]],
        on="trading_day", how="left",
    )

    feats["is_weekend"]       = feats["is_weekend"].astype(int)
    feats["prev_is_reversal"] = feats["prev_is_reversal"].astype("Int64")

    return feats


def filter_to_targets(
    feats: pd.DataFrame,
    feature_cols: list[str],
    target_dates: list[date],
) -> pd.DataFrame:
    """Restrict to target_dates and drop rows with any feature NaN.

    Returns the same column order as `feature_cols` plus `trading_day` and
    `regionid` as bookkeeping columns.
    """
    target_set = {pd.Timestamp(d) for d in target_dates}
    sliced = feats[feats["trading_day"].isin(target_set)].copy()

    needed = ["trading_day", "regionid"] + feature_cols
    sliced = sliced[needed].dropna(subset=feature_cols)

    return sliced.reset_index(drop=True)


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------
UPSERT_SQL = text("""
    INSERT INTO analytics.predictions
        (predict_for_date, regionid, p_reversal, predicted_label,
         model_version, predicted_at)
    VALUES (:d, :r, :p, :lab, :ver, :ts)
    ON CONFLICT (predict_for_date, regionid, model_version)
    DO UPDATE SET
        p_reversal      = EXCLUDED.p_reversal,
        predicted_label = EXCLUDED.predicted_label,
        predicted_at    = EXCLUDED.predicted_at
""")


def write_predictions(
    engine: Engine,
    rows: list[dict],
) -> None:
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(UPSERT_SQL, rows)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="24h-ahead reversal probability inference.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Target date D to predict for (YYYY-MM-DD).",
    )
    grp.add_argument(
        "--backfill",
        nargs=2, metavar=("START", "END"),
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Replay an inclusive date range (smoke test / backfill).",
    )
    ap.add_argument(
        "--region", default=None,
        help="Override target region (defaults to artefact target_region).",
    )
    ap.add_argument(
        "--artefact", type=Path, default=DEFAULT_ARTEFACT,
        help=f"Path to joblib artefact (default: {DEFAULT_ARTEFACT}).",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Print predictions; do not write to analytics.predictions.",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    artefact = load_artefact(args.artefact)

    region = args.region or artefact["target_region"]
    feature_cols: list[str] = list(artefact["features_leakfree"])
    pipeline = artefact["pipeline"]
    first_day: date = artefact["first_trading_day"]
    if isinstance(first_day, datetime):
        first_day = first_day.date()
    model_version = artefact["model_version"]

    if args.date:
        target_dates = [args.date]
    else:
        start, end = args.backfill
        if end < start:
            sys.exit("ERROR: --backfill END is before START")
        target_dates = [start + timedelta(days=i)
                        for i in range((end - start).days + 1)]

    print(f"[predict] region={region}  model_version={model_version}")
    print(f"[predict] first_trading_day={first_day}  n_dates={len(target_dates)}")

    engine = get_engine()
    feats = build_feature_frame(engine, target_dates, region, first_day)
    X_df = filter_to_targets(feats, feature_cols, target_dates)

    if X_df.empty:
        sys.exit(
            "ERROR: no rows with complete features for the requested dates. "
            "Either the dates are not yet ingested or D-1 / D-7 lookbacks "
            "fall outside the data window."
        )

    n_dropped = len(target_dates) - len(X_df)
    if n_dropped:
        present = set(X_df["trading_day"].dt.date)
        missing = sorted(set(target_dates) - present)
        print(f"[predict] dropped {n_dropped} target dates with missing features: "
              f"{missing}")

    p = pipeline.predict_proba(X_df[feature_cols])[:, 1]
    pred_label = (p >= 0.5).astype(int)

    out = pd.DataFrame({
        "predict_for_date": X_df["trading_day"].dt.date,
        "regionid":         region,
        "p_reversal":       p,
        "predicted_label":  pred_label,
        "model_version":    model_version,
    })

    print("\n[predict] results")
    print(out.to_string(index=False, float_format=lambda x: f"{x:.5f}"))

    if args.dry_run:
        print("\n[predict] --dry-run: skipping DB write")
        return 0

    ts = datetime.utcnow()
    rows = [
        {"d": r.predict_for_date, "r": r.regionid,
         "p": float(r.p_reversal), "lab": int(r.predicted_label),
         "ver": r.model_version, "ts": ts}
        for r in out.itertuples(index=False)
    ]
    write_predictions(engine, rows)
    print(f"\n[predict] upserted {len(rows)} rows into analytics.predictions")
    return 0


if __name__ == "__main__":
    sys.exit(main())
