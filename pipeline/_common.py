"""
Shared definitions for the NEM ingestion + analysis pipeline.

Single source of truth for:
- KEEP_COLS — narrowing of merged DISPATCHREGIONSUM × DISPATCHPRICE
- ROOFTOP_COLS — narrowing for ROOFTOP_PV_ACTUAL
- REVERSAL_HOURS — hour-of-day set defining the midday reversal window
- add_trading_period() — derives `trading_day` and `hour` columns from any
  AEMO interval-ending timestamp, parameterised by interval length

Convention reminder:
    AEMO timestamps are interval-ENDING. The interval [23:55, 00:00 next day]
    has SETTLEMENTDATE = 00:00 next day. Naive `.dt.date` on this puts the
    interval into the WRONG day. add_trading_period subtracts the interval
    length to recover interval-START, which buckets correctly.
"""

from __future__ import annotations

import csv
import io
import zipfile

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Column narrowing — DISPATCHREGIONSUM ⨝ DISPATCHPRICE merged frame
# ---------------------------------------------------------------------------
# Order MUST match db/01_raw_schema.sql column order; load_to_postgres.py relies
# on that ordering for the COPY column list. Do not reorder lightly.
KEEP_COLS: list[str] = [
    "SETTLEMENTDATE",
    "REGIONID",
    "INTERVENTION",
    "TOTALDEMAND",
    "AVAILABLEGENERATION",
    "TOTALINTERMITTENTGENERATION",
    "UIGF",
    "SEMISCHEDULE_CLEAREDMW",
    "DEMAND_AND_NONSCHEDGEN",
    "NETINTERCHANGE",
    "RRP",
]

# ---------------------------------------------------------------------------
# Column narrowing — ROOFTOP_PV_ACTUAL
# ---------------------------------------------------------------------------
# Filtering: WHERE TYPE = 'MEASUREMENT' AND REGIONID in main 5 regions.
# We keep QI as a quality signal but it's mostly redundant after type filter.
# After filter, PK is (interval_datetime, regionid) — TYPE drops out.
ROOFTOP_COLS: list[str] = [
    "INTERVAL_DATETIME",
    "REGIONID",
    "POWER",
    "QI",
]

ROOFTOP_MAIN_REGIONS: list[str] = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

# ---------------------------------------------------------------------------
# Column narrowing — weather_daily (Open-Meteo Archive API)
# ---------------------------------------------------------------------------
# Daily aggregates per NEM region capital. All 5 regions use
# Australia/Brisbane timezone (AEST, no DST) so daily aggregation windows
# align with the NEM trading_day used everywhere else.
# Order MUST match db/01_raw_schema.sql column order; load_weather.py
# relies on that ordering for the COPY column list.
WEATHER_COLS: list[str] = [
    "date",
    "regionid",
    "t_max_c",
    "t_min_c",
    "solar_mj_m2",
    "sunshine_seconds",
    "precip_mm",
]

# ---------------------------------------------------------------------------
# Reversal window
# ---------------------------------------------------------------------------
# AEST hour-of-day set in which a daily min-demand interval is classified
# as "midday reversal". 6-hour daytime block 10:00–16:00 (interval-START
# convention; see add_trading_period below for why we use start, not end).
#
# Any SQL view using `BETWEEN 10 AND 15` or similar must remain consistent
# with this set. If REVERSAL_HOURS changes, change the views too.
REVERSAL_HOURS: frozenset[int] = frozenset({10, 11, 12, 13, 14, 15})

# ---------------------------------------------------------------------------
# Interval lengths (parameter for add_trading_period)
# ---------------------------------------------------------------------------
# Use these constants — do NOT pass raw integers — so the convention is
# auditable across callers.
DISPATCH_INTERVAL_MINUTES = 5     # DISPATCHREGIONSUM, DISPATCHPRICE
ROOFTOP_INTERVAL_MINUTES = 30     # ROOFTOP_PV_ACTUAL


def add_trading_period(
    df: pd.DataFrame,
    timestamp_col: str = "SETTLEMENTDATE",
    interval_minutes: int = DISPATCH_INTERVAL_MINUTES,
) -> pd.DataFrame:
    """Add `trading_day` (date) and `hour` (int 0-23) columns to `df`.

    `df[timestamp_col]` is treated as the interval-ENDING timestamp of an
    AEMO dispatch / forecast interval. Subtract `interval_minutes` to get
    the interval-START, then bucket by date and hour.

    Examples:
        # 5-min DISPATCHREGIONSUM (default)
        df = add_trading_period(df)

        # 30-min ROOFTOP_PV_ACTUAL
        df = add_trading_period(df,
                                timestamp_col="INTERVAL_DATETIME",
                                interval_minutes=ROOFTOP_INTERVAL_MINUTES)

    Returns a copy of `df` with two new columns appended.
    """
    out = df.copy()
    out[timestamp_col] = pd.to_datetime(out[timestamp_col])
    interval_start = out[timestamp_col] - pd.Timedelta(minutes=interval_minutes)
    out["trading_day"] = interval_start.dt.date
    out["hour"] = interval_start.dt.hour
    return out


# ---------------------------------------------------------------------------
# NEMWeb CURRENT MMS-format CSV parser (used by fetch_*_current.py)
# ---------------------------------------------------------------------------
# AEMO publishes CURRENT zips containing a single CSV in the multi-table MMS
# format:
#     C, <comment row>
#     I, <PACKAGE>, <TABLE>, <VERSION>, <col1>, <col2>, ...
#     D, <PACKAGE>, <TABLE>, <VERSION>, <val1>, <val2>, ...
#     I, <PACKAGE>, <ANOTHER_TABLE>, ...
#     C, "END OF REPORT", <row count>
#
# `I` rows are headers (their column list applies to subsequent `D` rows for
# the same table). Multiple tables can appear in one CSV (DispatchIS bundles
# 7 tables; rooftop has just 1).
#
# Time semantics + table naming differences vs MMSDM are documented in
# docs/NEMWEB_CURRENT_SCHEMA_DIFF.md — both end up as interval-END timestamps.

NEMWEB_USR_AGENT = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) nem_demand_reversal/phase2"
    )
}


def download_nemweb_zip(url: str, timeout: int = 90) -> bytes:
    """Fetch a single NEMWeb zip, raise for HTTP errors. Returns raw bytes."""
    r = requests.get(url, headers=NEMWEB_USR_AGENT, timeout=timeout)
    r.raise_for_status()
    return r.content


def parse_mms_zip(blob: bytes) -> dict[str, pd.DataFrame]:
    """Parse an MMS-format zip blob into {<PACKAGE>_<TABLE>: DataFrame}.

    All cells are returned as strings — callers cast to the dtypes they need
    (matches the existing pipeline: COPY FROM CSV does the final coercion at
    the Postgres boundary).

    Empty / unknown table names produce a key like '_<row index>'; callers
    should filter on the expected table key explicitly.
    """
    z = zipfile.ZipFile(io.BytesIO(blob))
    csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
    if not csvs:
        raise RuntimeError(f"no .csv in zip; namelist={z.namelist()}")
    text = z.read(csvs[0]).decode("utf-8", errors="replace")

    tables: dict[str, list[list[str]]] = {}
    headers: dict[str, list[str]] = {}
    current_key: str | None = None

    for line in text.splitlines():
        if not line or line[0] == "C":
            continue
        parts = next(csv.reader([line]))
        rtype = parts[0]
        if rtype == "I" and len(parts) >= 5:
            current_key = f"{parts[1]}_{parts[2]}"
            headers[current_key] = parts[4:]
            tables.setdefault(current_key, [])
        elif rtype == "D" and current_key is not None and len(parts) >= 5:
            tables[current_key].append(parts[4:])

    out: dict[str, pd.DataFrame] = {}
    for key, rows in tables.items():
        cols = headers[key]
        # Pad / truncate any oddly-shaped rows so DataFrame ctor doesn't choke
        normalised = [r[: len(cols)] + [None] * (len(cols) - len(r)) for r in rows]
        out[key] = pd.DataFrame(normalised, columns=cols)
    return out


def df_to_insert_rows(df: pd.DataFrame) -> list[tuple]:
    """DataFrame → list of tuples ready for psycopg2 execute_values.

    Replaces every NaN / NaT / pd.NA with Python None so psycopg2 emits SQL
    NULL instead of the float literal 'NaN' (which Postgres would happily
    accept into a numeric column, producing a NaN value rather than NULL —
    a semantic split from the MMSDM path that uses COPY ... NULL '').
    """
    arr = df.to_numpy(dtype=object)
    arr[pd.isna(arr)] = None
    return list(map(tuple, arr))
