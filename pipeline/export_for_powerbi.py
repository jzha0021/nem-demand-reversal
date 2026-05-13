"""
Export analytical artefacts to CSV for Power BI dashboard consumption.

Strategy
--------
Power BI cannot run OLS or Fisher exact tests, so anything statistical that
the dashboard needs visualised must be precomputed here and exported as a
flat CSV table. The two artefacts produced:

1. dashboard/data/h2_partial_resid.csv
   Frisch-Waugh-Lovell partial-residual data for Chart 4 (Finding 4 magnitude
   leg). For each region, contains one row per month with the rooftop-PV
   flow residual (x) and the deepest_min_demand residual (y) after
   partialling out `time_idx + C(calendar_month)`. The scatter of (x, y)
   in Power BI reproduces the same picture as NB02 cell 8.

   Also includes per-region OLS β̂_p95 and p-value as constant columns so
   each small-multiples panel can annotate its own coefficient.

2. dashboard/data/h8_or.csv
   Per-region odds ratio + 95% CI + chi-square p for Chart 5 (Finding 7
   negative-pricing × reversal co-occurrence). Five rows. Mirrors NB02
   cell 21 output exactly.

Idempotent: overwrites the CSVs on each run. Output directory is
created if absent.

Prerequisites
-------------
1.  .env populated with DB_PWD (same as ingestion scripts)
2.  All raw tables loaded and analytics views in place:
      - raw.region_5min     (dispatch)
      - raw.rooftop_pv_30min
      - analytics.v_h2_panel
      - analytics.v_region_5min
      - analytics.v_daily_demand_summary

Usage
-----
    python pipeline/export_for_powerbi.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from dotenv import load_dotenv
from scipy import stats
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REGION_ORDER = ["SA1", "VIC1", "NSW1", "QLD1", "TAS1"]

# Display-layer labels. Backend code + SQL views keep the AEMO codes
# (SA1, VIC1, ...) for traceability; the dashboard / README / tooltips
# use these cleaner labels.
REGION_SHORT = {
    "SA1":  "SA",
    "VIC1": "VIC",
    "NSW1": "NSW",
    "QLD1": "QLD",
    "TAS1": "TAS",
}
REGION_LONG = {
    "SA1":  "South Australia",
    "VIC1": "Victoria",
    "NSW1": "New South Wales",
    "QLD1": "Queensland",
    "TAS1": "Tasmania",
}

OUT_DIR = Path(__file__).resolve().parent.parent / "dashboard" / "data"


def add_display_labels(df: pd.DataFrame, region_col: str = "region") -> pd.DataFrame:
    """Append region_short and region_long columns based on AEMO region code."""
    out = df.copy()
    out["region_short"] = out[region_col].map(REGION_SHORT)
    out["region_long"]  = out[region_col].map(REGION_LONG)
    return out


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------
def get_engine():
    load_dotenv()
    user = os.getenv("DB_USER", "postgres")
    pwd  = os.getenv("DB_PWD")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "nem")
    if not pwd:
        sys.exit("ERROR: DB_PWD not set in .env")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}")


# ---------------------------------------------------------------------------
# Export 1: Finding 4 partial-residual data (Chart 4)
# ---------------------------------------------------------------------------
def export_h2_partial_resid(engine, out_path: Path) -> None:
    """Per region: residualise p95_rooftop_mw and deepest_min_demand on
    (time_idx + C(calendar_month)), export the residual pair plus original
    month label + region β̂_p95 and p-value as annotations.
    """
    panel = pd.read_sql(
        "SELECT regionid, month, reversal_pct, deepest_min_demand, "
        "       p95_rooftop_mw, calendar_month "
        "FROM analytics.v_h2_panel ORDER BY regionid, month",
        engine,
        parse_dates=["month"],
    )

    rows = []
    region_stats = []
    for region in REGION_ORDER:
        d = panel[panel.regionid == region].dropna(
            subset=["p95_rooftop_mw", "deepest_min_demand"]
        ).copy()
        d["time_idx"] = np.arange(len(d))

        # Main OLS for annotation (trend-controlled + HAC SE)
        main = smf.ols(
            "deepest_min_demand ~ p95_rooftop_mw + time_idx + C(calendar_month)",
            data=d,
        ).fit(cov_type="HAC", cov_kwds={"maxlags": 6})

        # Partial-out regressions for residuals
        fit_y = smf.ols(
            "deepest_min_demand ~ time_idx + C(calendar_month)", data=d
        ).fit()
        fit_x = smf.ols(
            "p95_rooftop_mw ~ time_idx + C(calendar_month)", data=d
        ).fit()

        d_out = pd.DataFrame({
            "region": region,
            "month":  d.month.values,
            "x_residual": fit_x.resid.values,   # p95_rooftop_mw anomaly
            "y_residual": fit_y.resid.values,   # deepest_min_demand anomaly
            "p95_rooftop_mw_raw":     d.p95_rooftop_mw.values,
            "deepest_min_demand_raw": d.deepest_min_demand.values,
        })
        rows.append(d_out)

        region_stats.append({
            "region": region,
            "beta_p95":  main.params["p95_rooftop_mw"],
            "p_value":   main.pvalues["p95_rooftop_mw"],
            "r_squared": main.rsquared,
            "n_obs":     int(main.nobs),
        })

    out = pd.concat(rows, ignore_index=True)

    # Merge per-region stats so each row carries its annotation columns
    stats_df = pd.DataFrame(region_stats)
    out = out.merge(stats_df, on="region", how="left")

    # Display-layer labels (Power BI / README use these; backend keeps codes)
    out = add_display_labels(out, region_col="region")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print(f"[h2_partial_resid] wrote {len(out)} rows to {out_path}")
    print(stats_df.to_string(index=False))


# ---------------------------------------------------------------------------
# Export 2: Finding 7 odds ratio per region (Chart 5)
# ---------------------------------------------------------------------------
def export_h8_or(engine, out_path: Path) -> None:
    """Per region: 2x2 contingency of (is_reversal, had_neg_rrp), Fisher
    exact OR + 95% CI + chi-square p. Five rows.
    """
    panel = pd.read_sql(
        """
        WITH daily_rrp AS (
            SELECT regionid,
                   trading_day,
                   BOOL_OR(is_negative_rrp) AS had_neg_rrp
            FROM analytics.v_region_5min
            GROUP BY 1, 2
        )
        SELECT s.regionid,
               s.trading_day,
               (s.min_demand_hour BETWEEN 10 AND 15) AS is_reversal,
               COALESCE(r.had_neg_rrp, FALSE)         AS had_neg_rrp
        FROM analytics.v_daily_demand_summary s
        LEFT JOIN daily_rrp r USING (regionid, trading_day)
        """,
        engine,
        parse_dates=["trading_day"],
    )

    rows = []
    for region in REGION_ORDER:
        d = panel[panel.regionid == region]
        ct = pd.crosstab(d.is_reversal, d.had_neg_rrp)

        # Make sure 2x2 layout is regular even if some cells are zero
        for r_idx in [False, True]:
            if r_idx not in ct.index:
                ct.loc[r_idx] = [0, 0]
        for c_idx in [False, True]:
            if c_idx not in ct.columns:
                ct[c_idx] = [0, 0]
        ct = ct.reindex(index=[False, True], columns=[False, True]).fillna(0).astype(int)

        a = int(ct.loc[True,  True])   # rev=1, neg=1
        b = int(ct.loc[True,  False])  # rev=1, neg=0
        c = int(ct.loc[False, True])   # rev=0, neg=1
        d_ = int(ct.loc[False, False]) # rev=0, neg=0

        odds_ratio, fisher_p = stats.fisher_exact(ct.values)
        if min(a, b, c, d_) > 0:
            # Wald log-OR 95% CI: OR * exp(±1.96 * sqrt(1/a + 1/b + 1/c + 1/d))
            se_log_or = np.sqrt(1/a + 1/b + 1/c + 1/d_)
            or_lo = odds_ratio * np.exp(-1.96 * se_log_or)
            or_hi = odds_ratio * np.exp( 1.96 * se_log_or)
        else:
            or_lo, or_hi = np.nan, np.nan

        chi2, chi2_p, _, _ = stats.chi2_contingency(ct.values)

        n_rev = a + b
        n_neg = a + c
        rows.append({
            "region":          region,
            "n_reversal_days": n_rev,
            "n_neg_rrp_days":  n_neg,
            "p_neg_given_rev":     a / n_rev    if n_rev > 0 else np.nan,
            "p_neg_given_non_rev": c / (c + d_) if (c + d_) > 0 else np.nan,
            "odds_ratio":   odds_ratio,
            "or_95_lower":  or_lo,
            "or_95_upper":  or_hi,
            "chi2_p":       chi2_p,
            "fisher_p":     fisher_p,
        })

    out = pd.DataFrame(rows)
    out = add_display_labels(out, region_col="region")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print(f"\n[h8_or] wrote {len(out)} rows to {out_path}")
    print(out.to_string(index=False))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def export_region_lookup(out_path: Path) -> None:
    """Five-row lookup mapping AEMO region code to display labels.
    Power BI imports this and creates a Many-to-One relationship from any
    fact table's `regionid` column to this lookup's `regionid`. Charts
    then use `region_short` (legends) or `region_long` (titles/tooltips).
    """
    lookup = pd.DataFrame([
        {"regionid": k, "region_short": REGION_SHORT[k], "region_long": REGION_LONG[k],
         "sort_order": i}
        for i, k in enumerate(REGION_ORDER)
    ])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lookup.to_csv(out_path, index=False)
    print(f"\n[region_lookup] wrote {len(lookup)} rows to {out_path}")
    print(lookup.to_string(index=False))


def main():
    engine = get_engine()

    export_region_lookup(OUT_DIR / "region_lookup.csv")
    export_h2_partial_resid(engine, OUT_DIR / "h2_partial_resid.csv")
    export_h8_or(engine, OUT_DIR / "h8_or.csv")

    print(f"\nDone. CSV files in: {OUT_DIR}")
    print("Import into Power BI via Get Data → Text/CSV.")


if __name__ == "__main__":
    main()
