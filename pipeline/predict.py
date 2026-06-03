"""
Next-day reversal probability — daily inference CLI (predicts trading day D from D-1 features).

Loads the leak-free Logistic Regression pipeline dumped at the end of
notebooks/02_reversal_classifier.ipynb and writes P(is_reversal) for one or
more target dates D into analytics.predictions.

The feature contract is the artefact's single source of truth:
    artefact['features_leakfree']  — column order
    artefact['first_trading_day']  — anchors time_idx (row position relative
                                     to NB02 training-time row 0)
    artefact['target_region']      — VIC1 today; other regions reusable on retrain
    artefact['model_version']      — pinned to (training-end-date + feature set)

Notebook-derived features replicated here verbatim:
    is_public_holiday         — holidays.Australia(subdiv='VIC')
    time_idx                  — (D - first_trading_day).days
    lag7_rooftop_p95_mw       — v_ml_features.rooftop_p95_mw at D-7
    semisched_share_yesterday — AVG(semischedule_clearedmw / totaldemand) over D-1

Usage
-----
    python pipeline/predict.py --date 2026-03-15                       # default Postgres (local)
    python pipeline/predict.py --date 2026-03-15 --target snowflake    # cloud DW
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
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import holidays
import joblib
import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ARTEFACT = PROJECT_ROOT / "models" / "leak_free_lr.joblib"


# ---------------------------------------------------------------------------
# DB target dispatch — Postgres (local dev SOT) vs Snowflake (cloud DW)
# ---------------------------------------------------------------------------
# Same analytics-layer SQL works on both targets (dbt models mirror each
# other under analytics on Postgres / NEM.ANALYTICS on Snowflake). The
# only divergences are the connection factory and the prediction UPSERT
# syntax (Postgres ON CONFLICT vs Snowflake MERGE).

@dataclass(frozen=True)
class DBTarget:
    """Per-target metadata: connection factory + schema-qualified table names."""
    name: str
    tbl_features: str       # qualified analytics.v_ml_features
    tbl_stg_region: str     # qualified analytics.stg_region_5min
    tbl_predictions: str    # qualified analytics.predictions


POSTGRES = DBTarget(
    name="postgres",
    tbl_features="analytics.v_ml_features",
    tbl_stg_region="analytics.stg_region_5min",
    tbl_predictions="analytics.predictions",
)

SNOWFLAKE = DBTarget(
    name="snowflake",
    tbl_features="NEM.ANALYTICS.V_ML_FEATURES",
    tbl_stg_region="NEM.ANALYTICS.STG_REGION_5MIN",
    tbl_predictions="NEM.ANALYTICS.PREDICTIONS",
)


def get_engine(target: DBTarget):
    """Return a SQLAlchemy engine. Both Postgres and Snowflake go through
    SQLAlchemy so callers can use a single ``:name`` paramstyle (and the
    same ``pd.read_sql(text(sql), engine, params=...)`` invocation) across
    targets. Snowflake uses snowflake-sqlalchemy with key-pair auth.
    """
    from sqlalchemy import create_engine
    load_dotenv(PROJECT_ROOT / ".env")

    if target.name == "postgres":
        pwd = os.getenv("DB_PWD")
        if not pwd:
            sys.exit("ERROR: DB_PWD not set in .env")
        user = os.getenv("DB_USER", "postgres")
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        name = os.getenv("DB_NAME", "nem")
        return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}")

    if target.name == "snowflake":
        # Late import so smoke tests that only touch Postgres don't pull
        # the snowflake-sqlalchemy / cryptography stack onto the path.
        sys.path.insert(0, str(PROJECT_ROOT / "pipeline"))
        from _common import get_snowflake_engine
        return get_snowflake_engine(schema="ANALYTICS")

    sys.exit(f"ERROR: unknown DB target {target.name!r}")


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

    # sklearn version drift check — joblib pickles are tightly coupled to
    # the sklearn version they were fit on (internal class layouts and
    # private attrs change between versions). Refuse to run if the
    # current sklearn doesn't match the one stored in the artefact;
    # silent incompatibility risks degraded probabilities without any
    # exception being raised.
    pinned = art.get("sklearn_lib_version")
    if pinned:
        import sklearn
        if sklearn.__version__ != pinned:
            sys.exit(
                f"ERROR: sklearn version drift — artefact fit on "
                f"{pinned}, current is {sklearn.__version__}. Either "
                f"reinstall scikit-learn=={pinned} or retrain the "
                f"artefact via notebooks/02_reversal_classifier.ipynb."
            )

    return art


# ---------------------------------------------------------------------------
# Feature builder — same SQL on both targets, only table qualifiers differ
# ---------------------------------------------------------------------------
def build_feature_frame(
    engine,
    target: DBTarget,
    target_dates: list[date],
    region: str,
    first_trading_day: date,
) -> pd.DataFrame:
    """Return one row per trading_day with all FEATURES_LEAKFREE columns.

    Rows with NULL on any feature are dropped (matching NB02 cell 25
    `vic_clean = vic.dropna(subset=needed_cols)`).
    """
    from sqlalchemy import text

    target_min = min(target_dates)
    target_max = max(target_dates)
    window_start = min(first_trading_day, target_min - timedelta(days=8))

    feats_sql = text(f"""
        SELECT *
        FROM {target.tbl_features}
        WHERE regionid = :region
          AND trading_day BETWEEN :start AND :end
        ORDER BY trading_day
    """)
    feats = pd.read_sql(
        feats_sql, engine,
        params={"region": region, "start": window_start, "end": target_max},
        parse_dates=["trading_day"],
    )
    # Snowflake's SQLAlchemy dialect returns column names uppercased by default —
    # normalise to lowercase so downstream feature builder is target-agnostic.
    feats.columns = [c.lower() for c in feats.columns]
    if feats.empty:
        sys.exit(
            f"ERROR: no rows in {target.tbl_features} for region={region} "
            f"between {window_start} and {target_max}. Did the dbt build / "
            "Snowpipe ingest cover this range?"
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

    util_sql = text(f"""
        SELECT trading_day,
               AVG(semischedule_clearedmw) / NULLIF(AVG(totaldemand), 0)
                   AS share_today
        FROM {target.tbl_stg_region}
        WHERE regionid = :region
          AND trading_day BETWEEN :start AND :end
        GROUP BY 1
        ORDER BY 1
    """)
    util = pd.read_sql(
        util_sql, engine,
        params={"region": region, "start": window_start, "end": target_max},
        parse_dates=["trading_day"],
    )
    util.columns = [c.lower() for c in util.columns]
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
# UPSERT — different syntax per target
# ---------------------------------------------------------------------------
def write_predictions(engine, target: DBTarget, rows: list[dict[str, Any]]) -> None:
    """Idempotent upsert. Same row repeatedly = same final state.

    Both targets go through SQLAlchemy with ``:name`` paramstyle; only the
    upsert syntax diverges (Postgres ON CONFLICT vs Snowflake MERGE).
    """
    if not rows:
        return
    from sqlalchemy import text

    if target.name == "postgres":
        upsert = text(f"""
            INSERT INTO {target.tbl_predictions}
                (predict_for_date, regionid, p_reversal, predicted_label,
                 model_version, predicted_at)
            VALUES (:d, :r, :p, :lab, :ver, :ts)
            ON CONFLICT (predict_for_date, regionid, model_version)
            DO UPDATE SET
                p_reversal      = EXCLUDED.p_reversal,
                predicted_label = EXCLUDED.predicted_label,
                predicted_at    = EXCLUDED.predicted_at
        """)
    elif target.name == "snowflake":
        upsert = text(f"""
            MERGE INTO {target.tbl_predictions} AS t
            USING (
                SELECT
                    CAST(:d   AS DATE)          AS predict_for_date,
                    CAST(:r   AS VARCHAR)       AS regionid,
                    CAST(:p   AS FLOAT)         AS p_reversal,
                    CAST(:lab AS INTEGER)       AS predicted_label,
                    CAST(:ver AS VARCHAR)       AS model_version,
                    CAST(:ts  AS TIMESTAMP_LTZ) AS predicted_at
            ) AS s
            ON  t.PREDICT_FOR_DATE = s.predict_for_date
            AND t.REGIONID         = s.regionid
            AND t.MODEL_VERSION    = s.model_version
            WHEN MATCHED THEN UPDATE SET
                P_REVERSAL      = s.p_reversal,
                PREDICTED_LABEL = s.predicted_label,
                PREDICTED_AT    = s.predicted_at
            WHEN NOT MATCHED THEN INSERT (
                PREDICT_FOR_DATE, REGIONID, P_REVERSAL, PREDICTED_LABEL,
                MODEL_VERSION, PREDICTED_AT
            ) VALUES (
                s.predict_for_date, s.regionid, s.p_reversal,
                s.predicted_label, s.model_version, s.predicted_at
            )
        """)
    else:
        sys.exit(f"ERROR: write_predictions: unknown DB target {target.name!r}")

    with engine.begin() as conn:
        # Snowflake MERGE doesn't support executemany of multi-row params via
        # SQLAlchemy — execute row-by-row. Postgres handles the list directly.
        if target.name == "snowflake":
            for row in rows:
                conn.execute(upsert, row)
        else:
            conn.execute(upsert, rows)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Next-day reversal probability inference (D from D-1 features).",
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
        "--target", choices=["postgres", "snowflake"], default="postgres",
        help="DB target (default: postgres for the local dev loop).",
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
    target = POSTGRES if args.target == "postgres" else SNOWFLAKE
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

    print(f"[predict] target={target.name}  region={region}  model_version={model_version}")
    print(f"[predict] first_trading_day={first_day}  n_dates={len(target_dates)}")

    engine = get_engine(target)
    try:
        feats = build_feature_frame(engine, target, target_dates, region, first_day)
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
        write_predictions(engine, target, rows)
        print(f"\n[predict] upserted {len(rows)} rows into {target.tbl_predictions}")
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
