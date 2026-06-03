"""
NEM Demand Reversal — public dashboard.

Reads NEM.ANALYTICS (Snowflake) and renders three sections:
  1. Monthly reversal rate by region (Findings 1–2)
  2. Latest 14-day P(reversal) predictions with hit/miss markers (Finding 8)
  3. Pipeline health (Snowpipe last ingest + latest prediction freshness)

Deployment
----------
Streamlit Community Cloud reads Snowflake creds from
`.streamlit/secrets.toml`. Local dev uses the same file (gitignored). See
`.streamlit/secrets.toml.example` for the expected shape.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import text

# Reuse the pipeline's key-pair auth helper so Streamlit + cron share one
# engine factory (one source of truth for Snowflake connection semantics).
sys.path.insert(0, str(Path(__file__).resolve().parent / "pipeline"))
from _common import get_snowflake_engine  # noqa: E402

# ---------------------------------------------------------------------------
# Page config + theme
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="NEM Demand Reversal",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 5-region palette — matches dashboard/theme.json so the Streamlit URL and
# the Power BI report tell the same visual story. AEMO's regionid codes
# (NSW1, VIC1, ...) are renamed at the presentation layer; raw rows in
# Snowflake still use the codes.
REGION_NAMES = {
    "NSW1": "New South Wales",
    "QLD1": "Queensland",
    "SA1":  "South Australia",
    "TAS1": "Tasmania",
    "VIC1": "Victoria",
}
REGION_ORDER = ["New South Wales", "Queensland", "South Australia", "Tasmania", "Victoria"]
REGION_COLORS = {
    "New South Wales": "#1f77b4",
    "Queensland":      "#ff7f0e",
    "South Australia": "#2ca02c",
    "Tasmania":        "#9467bd",
    "Victoria":        "#d62728",
}


# ---------------------------------------------------------------------------
# Snowflake connection — read-only via SQLAlchemy
# ---------------------------------------------------------------------------
@st.cache_resource(ttl=3600)
def get_engine():
    """Cached SQLAlchemy engine. Streamlit Cloud secrets supply the creds.

    Key-pair auth: ``private_key`` in secrets.toml is the raw PEM string
    (multi-line triple-quoted). Account / user / role come from the same
    [snowflake] section; we mirror them into os.environ so the shared
    factory in _common.get_snowflake_engine() picks them up uniformly
    across local cron, CI, and Streamlit.
    """
    import os as _os
    sf = st.secrets["snowflake"]
    _os.environ["SNOWFLAKE_ACCOUNT"]   = sf["account"]
    _os.environ["SNOWFLAKE_USER"]      = sf["user"]
    # Streamlit ships least-privilege: R_NEM_READ has SELECT on
    # analytics + raw + MONITOR on the 3 pipes, no CREATE / OPERATE.
    _os.environ["SNOWFLAKE_ROLE"]      = sf.get("role", "R_NEM_READ")
    _os.environ["SNOWFLAKE_WAREHOUSE"] = sf.get("warehouse", "WH_NEM")
    _os.environ["SNOWFLAKE_DATABASE"]  = sf.get("database", "NEM")
    return get_snowflake_engine(
        schema=sf.get("schema", "ANALYTICS"),
        private_key_bytes=sf["private_key"].encode(),
    )


@st.cache_data(ttl=300)
def fetch_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run a parameterised SELECT and return a lowercase-column DataFrame."""
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params or {})
    df.columns = [c.lower() for c in df.columns]
    return df


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("Minimum Operational Demand Reversal — NEM")
st.caption(
    "Rooftop PV pushed the National Electricity Market from max-demand-constrained "
    "to min-demand-constrained. This dashboard quantifies the spatial-temporal "
    "gradient and tracks a next-day reversal forecast from D-1 features."
)

st.markdown(
    "**Data pipeline:** NEMWeb CURRENT → S3 (raw zip + parquet) → "
    "Snowpipe → Snowflake → dbt → Streamlit. "
    "Cron runs at AEST 01:00 daily."
)


# ---------------------------------------------------------------------------
# Section 1 — Monthly reversal rate by region
# ---------------------------------------------------------------------------
st.header("Reversal rate by region — monthly")
st.caption(
    "% of trading days each month whose minimum-demand interval lands in "
    "the midday window (10–15 AEST). Each line is one NEM region."
)

monthly = fetch_query("""
    SELECT regionid, month, reversal_pct, n_trading_days, n_reversal_days
      FROM NEM.ANALYTICS.V_MONTHLY_REVERSAL_RATE
     ORDER BY regionid, month
""")

if monthly.empty:
    st.warning("v_monthly_reversal_rate is empty — has dbt build run on Snowflake?")
else:
    monthly["month"] = pd.to_datetime(monthly["month"])
    monthly["region"] = monthly["regionid"].map(REGION_NAMES)

    chart = (
        alt.Chart(monthly)
        .mark_line(point=True, size=2.5)
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("reversal_pct:Q", title="Reversal days (%)"),
            color=alt.Color(
                "region:N",
                title="Region",
                scale=alt.Scale(
                    domain=REGION_ORDER,
                    range=[REGION_COLORS[r] for r in REGION_ORDER],
                ),
            ),
            tooltip=[
                alt.Tooltip("region:N", title="Region"),
                alt.Tooltip("month:T", format="%Y-%m"),
                alt.Tooltip("reversal_pct:Q", format=".1f"),
                "n_trading_days",
                "n_reversal_days",
            ],
        )
        .properties(height=380)
        .interactive()
    )
    st.altair_chart(chart, width="stretch")


# ---------------------------------------------------------------------------
# Section 2 — Latest 14-day P(reversal) predictions
# ---------------------------------------------------------------------------
st.header("Next-day reversal probability — Victoria, latest 14 days")
st.caption(
    "Predicted at AEST 01:00 each morning for the trading day that just "
    "started, conditioned on D-1's settled features (demand, weather, "
    "rooftop). Logistic regression, held-out test AUC 0.755. The pickled "
    "artefact targets Victoria only; per-region retraining is a future "
    "extension. Bar color — green: predicted matched actual; red: missed; "
    "grey: trading day still in progress so the actual is not yet observed."
)

# Explicit regionid filter so ad-hoc predict.py runs for other regions
# (e.g. an operator testing a retrained NSW model) don't leak into the
# Victoria-titled panel.
predictions = fetch_query("""
    SELECT predict_for_date,
           regionid,
           model_version,
           p_reversal,
           predicted_label,
           actual_is_reversal,
           hit
      FROM NEM.ANALYTICS.V_PREDICTION_VS_ACTUAL
     WHERE regionid = 'VIC1'
       AND predict_for_date >= DATEADD(day, -14, CURRENT_DATE())
     ORDER BY predict_for_date DESC
""")

if predictions.empty:
    st.info(
        "No predictions logged in the last 14 days. The daily cron writes "
        "one row per night for Victoria; this panel populates as runs land."
    )
else:
    predictions["predict_for_date"] = pd.to_datetime(predictions["predict_for_date"])
    predictions["region"] = predictions["regionid"].map(REGION_NAMES)

    pred_chart = (
        alt.Chart(predictions)
        .transform_calculate(
            # hit is BOOLEAN in Snowflake; v_prediction_vs_actual gates it on
            # n_intervals = 288 so it's NULL until the trading day closes.
            # Vega-Lite's `===` operator distinguishes true / false / null,
            # letting us colour pending bars grey rather than mis-classifying
            # NULL as a miss.
            hit_state="datum.hit === true ? 'hit' : datum.hit === false ? 'miss' : 'pending'"
        )
        .mark_bar()
        .encode(
            x=alt.X("predict_for_date:T", title="Target date D"),
            y=alt.Y("p_reversal:Q", title="P(reversal)",
                    scale=alt.Scale(domain=[0, 1])),
            color=alt.Color(
                "hit_state:N",
                scale=alt.Scale(
                    domain=["hit", "miss", "pending"],
                    range=["#2ca02c", "#d62728", "#999999"],
                ),
                legend=alt.Legend(title="Outcome", orient="top-right"),
            ),
            tooltip=[
                alt.Tooltip("region:N", title="Region"),
                alt.Tooltip("predict_for_date:T", format="%Y-%m-%d"),
                alt.Tooltip("p_reversal:Q", format=".3f"),
                "predicted_label",
                "actual_is_reversal",
                "hit",
            ],
        )
        .properties(height=320)
    )
    st.altair_chart(pred_chart, width="stretch")


# ---------------------------------------------------------------------------
# Section 3 — Pipeline health
# ---------------------------------------------------------------------------
st.header("Pipeline health")

col1, col2, col3 = st.columns(3)

pipe_info = fetch_query("""
    SELECT 'DISPATCH'  AS pipe, SYSTEM$PIPE_STATUS('NEM.RAW.NEM_PIPE_DISPATCH') AS status
    UNION ALL
    SELECT 'ROOFTOP',  SYSTEM$PIPE_STATUS('NEM.RAW.NEM_PIPE_ROOFTOP')
    UNION ALL
    SELECT 'WEATHER',  SYSTEM$PIPE_STATUS('NEM.RAW.NEM_PIPE_WEATHER')
""")


def parse_status(row) -> tuple[str, str, float]:
    payload = json.loads(row["status"])
    state = payload.get("executionState", "?")
    last = payload.get("lastIngestedTimestamp") or ""
    if last:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        stale_h = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
    else:
        stale_h = float("nan")
    return state, last, stale_h


with col1:
    st.subheader("Snowpipes")
    for _, row in pipe_info.iterrows():
        state, last_iso, stale_h = parse_status(row)
        emoji = "✓" if state == "RUNNING" and stale_h < 26 else "✗"
        st.write(
            f"{emoji} `NEM_PIPE_{row['pipe']}` — {state} · "
            f"last ingest {stale_h:.1f}h ago" if pd.notna(stale_h)
            else f"{emoji} `NEM_PIPE_{row['pipe']}` — {state} · never"
        )

with col2:
    st.subheader("dbt analytics layer")
    table_counts = fetch_query("""
        SELECT 'REGION_5MIN'     AS table_name, COUNT(*) AS n_rows FROM NEM.RAW.REGION_5MIN
        UNION ALL SELECT 'ROOFTOP_PV_30MIN',     COUNT(*) FROM NEM.RAW.ROOFTOP_PV_30MIN
        UNION ALL SELECT 'WEATHER_DAILY',        COUNT(*) FROM NEM.RAW.WEATHER_DAILY
        UNION ALL SELECT 'PREDICTIONS',          COUNT(*) FROM NEM.ANALYTICS.PREDICTIONS
    """)
    for _, row in table_counts.iterrows():
        st.write(f"`{row['table_name']}` · {row['n_rows']:,} rows")

with col3:
    st.subheader("Latest prediction")
    latest = fetch_query("""
        SELECT MAX(predict_for_date) AS d,
               MAX(predicted_at)     AS t
          FROM NEM.ANALYTICS.PREDICTIONS
    """)
    if not latest.empty and latest.iloc[0]["d"] is not None:
        st.write(f"target_date = `{latest.iloc[0]['d']}`")
        st.write(f"predicted_at = `{latest.iloc[0]['t']}`")
    else:
        st.info("No predictions written yet.")


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.caption(
    "Code, methodology, and the operational runbook: "
    "[github.com/jzha0021/nem-demand-reversal]"
    "(https://github.com/jzha0021/nem-demand-reversal). "
    "Data sources: AEMO NEMWeb (DispatchIS, ROOFTOP_PV_ACTUAL), Open-Meteo Archive."
)
