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
import os
import zipfile
from functools import lru_cache

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
        "(KHTML, like Gecko) nem_demand_reversal/1.0"
    )
}


def make_nemweb_session() -> requests.Session:
    """A requests Session with retry+backoff suitable for NEMWeb scraping.

    NEMWeb occasionally 403s mid-sequence under sustained polling (WAF /
    rate-limit signals fire when a single IP downloads hundreds of zips
    back-to-back). Exponential backoff on 403/429/5xx clears nearly all
    transient blocks; the few permanently-missing files surface after the
    retry budget is exhausted.
    """
    from urllib3.util.retry import Retry
    from requests.adapters import HTTPAdapter

    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,                           # 0s, 2s, 4s, 8s, 16s
        status_forcelist=[403, 429, 500, 502, 503, 504],
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def download_nemweb_zip(
    url: str,
    session: requests.Session | None = None,
    timeout: int = 90,
) -> bytes:
    """Fetch a single NEMWeb zip, raise for HTTP errors. Returns raw bytes.

    Caller may pass a shared :func:`make_nemweb_session` to amortise the
    retry adapter + TCP pool across many downloads. When ``session`` is
    ``None`` a fresh retry-enabled session is built per call (slower but
    fine for ad-hoc use).
    """
    sess = session if session is not None else make_nemweb_session()
    r = sess.get(url, headers=NEMWEB_USR_AGENT, timeout=timeout)
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


# ---------------------------------------------------------------------------
# S3 raw-zip mirror (used by fetch_*_current.py when S3_BUCKET is set)
# ---------------------------------------------------------------------------
# boto3 is imported lazily so notebooks that only use add_trading_period etc.
# don't need it on the path. Local: AWS_PROFILE=nem in .env picks up
# ~/.aws/credentials. CI: OIDC exports AWS_* env vars so default credential
# chain works without a profile.

@lru_cache(maxsize=1)
def _get_s3_client():
    import boto3
    return boto3.client("s3", region_name=os.getenv("AWS_REGION", "ap-southeast-2"))


def s3_enabled() -> bool:
    """S3 mirror is opt-in via S3_BUCKET env var. Returns False when unset."""
    return bool(os.getenv("S3_BUCKET"))


def upload_blob_to_s3(
    blob: bytes,
    key: str,
    *,
    bucket: str | None = None,
    skip_if_exists: bool = True,
) -> tuple[bool, str]:
    """Upload `blob` to `s3://<bucket>/<key>`. Idempotent via HEAD-then-PUT.

    Returns ``(uploaded, reason)``. ``uploaded=False`` means the key already
    existed and was skipped — same idea as ``ON CONFLICT DO NOTHING`` at
    the DB layer, applied to the object store.
    """
    from botocore.exceptions import ClientError
    bucket = bucket or os.getenv("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET env var not set (and no `bucket=` override)")
    client = _get_s3_client()
    if skip_if_exists:
        try:
            client.head_object(Bucket=bucket, Key=key)
            return False, "exists"
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
                raise
    client.put_object(Bucket=bucket, Key=key, Body=blob)
    return True, "uploaded"


# ---------------------------------------------------------------------------
# Parsed parquet → S3 (Snowpipe ingestion source)
# ---------------------------------------------------------------------------
# Fetchers accumulate the parsed DataFrame across their per-zip loop and
# call upload_parquet_to_s3() once after the loop. One parquet per run
# keeps Snowpipe per-file billing trivial and avoids Snowflake's
# small-file inefficiency. Filename is UTC-timestamped + UUID-suffixed so
# concurrent runs don't collide. Duplicate rows across runs are expected
# and deduped downstream by the dbt staging models.

def upload_parquet_to_s3(
    df: "pd.DataFrame",
    source: str,
    *,
    bucket: str | None = None,
    timestamp_utc: "pd.Timestamp | None" = None,
    key: str | None = None,
) -> tuple[str, int]:
    """Write `df` as snappy parquet, upload to `s3://<bucket>/parsed/<source>/...`.

    The default key embeds a UTC ISO timestamp + short UUID so a daily
    cron writes a unique file per run. Pass an explicit ``key=`` (full
    path under the bucket root) for deterministic uploads — backfill
    uses ``parsed/<source>/backfill_<YYYY-MM>.parquet`` so re-runs hit
    Snowpipe's load-history dedupe instead of writing fresh duplicates.

    Empty df is a no-op — callers don't need to guard. Returns ``('', 0)``
    in that case.
    """
    import io
    import uuid

    if df.empty:
        return "", 0

    bucket = bucket or os.getenv("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET env var not set (and no `bucket=` override)")

    if key is None:
        ts = (timestamp_utc if timestamp_utc is not None
              else pd.Timestamp.now("UTC")).strftime("%Y-%m-%dT%H-%M-%SZ")
        suffix = uuid.uuid4().hex[:8]
        key = f"parsed/{source}/{ts}_{suffix}.parquet"

    # Snowflake's parquet reader silently mangles native parquet
    # TIMESTAMP[us]/[ns] columns when ingested through a
    # MATCH_BY_COLUMN_NAME pipe — the row lands but downstream
    # queries render the column as "Invalid date". Writing every
    # date-like column as an ISO-8601 string sidesteps the whole codec:
    # Snowflake implicitly casts VARCHAR → TIMESTAMP_NTZ / DATE on COPY,
    # which is rock-solid. Cost: parquet stores them as strings rather
    # than native parquet TIMESTAMP, invisible to the rest of the
    # pipeline.
    import datetime as _dt
    import re as _re

    # AEMO MMS timestamps land as raw strings "YYYY/MM/DD HH:MM:SS"
    # (slashes, not dashes). Snowflake's auto-cast VARCHAR→TIMESTAMP_NTZ
    # doesn't recognise the slash format and the COPY fails with
    # "Failed to cast variant value ... to TIMESTAMP_NTZ". Parse those
    # strings to datetime first, then the next branch re-emits ISO.
    _mms_ts_re = _re.compile(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$")

    df = df.copy()
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_datetime64_any_dtype(s):
            df[col] = s.dt.strftime("%Y-%m-%d %H:%M:%S")
        elif s.dtype == object:
            first_nn = s.dropna()
            if first_nn.empty:
                continue
            sample = first_nn.iloc[0]
            if isinstance(sample, _dt.date):
                df[col] = s.astype("string")
            elif isinstance(sample, str) and _mms_ts_re.match(sample):
                df[col] = (pd.to_datetime(s, format="%Y/%m/%d %H:%M:%S")
                           .dt.strftime("%Y-%m-%d %H:%M:%S"))

    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    body = buf.getvalue()

    _get_s3_client().put_object(Bucket=bucket, Key=key, Body=body)
    return key, len(body)


# ---------------------------------------------------------------------------
# Snowflake engine factory — RSA key-pair auth
# ---------------------------------------------------------------------------
# Snowflake's 2025 paid-account policy enforces MFA on TYPE=PERSON users,
# which a headless pipeline / Streamlit can't satisfy. TYPE=SERVICE users
# bypass MFA but explicitly forbid password auth — only RSA key-pair (or
# OAuth) works. This factory loads the PKCS8 private key and hands it to
# snowflake-connector-python via connect_args.
#
# Key sourcing (first non-empty wins):
#   1. private_key_bytes parameter   — Streamlit passes st.secrets bytes
#   2. SNOWFLAKE_PRIVATE_KEY_PATH    — local dev (.env points at a .p8 file)
#   3. SNOWFLAKE_PRIVATE_KEY         — CI / Streamlit Cloud (raw PEM string)
#
# Public key is registered once per user in Snowsight via
#   ALTER USER <U> SET RSA_PUBLIC_KEY = '<base64 body, no PEM headers>';

def get_snowflake_engine(
    *,
    schema: str | None = None,
    private_key_bytes: bytes | None = None,
):
    """Return a SQLAlchemy engine for Snowflake using key-pair auth.

    Parameters
    ----------
    schema
        Default schema for the session. Falls back to env SNOWFLAKE_SCHEMA,
        then to 'ANALYTICS'. Each caller can override per its needs (raw
        ingest scripts want RAW; analytics queries want ANALYTICS).
    private_key_bytes
        Optional raw PEM bytes. Streamlit reads from ``st.secrets`` and
        passes the value through here so it doesn't need env vars at all.
    """
    from snowflake.sqlalchemy import URL
    from sqlalchemy import create_engine
    from cryptography.hazmat.primitives import serialization

    pem: bytes | None = private_key_bytes
    if pem is None:
        key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        key_raw  = os.getenv("SNOWFLAKE_PRIVATE_KEY")
        if key_path:
            with open(key_path, "rb") as f:
                pem = f.read()
        elif key_raw:
            pem = key_raw.encode()
        else:
            import sys
            sys.exit(
                "ERROR: no Snowflake private key found. Set "
                "SNOWFLAKE_PRIVATE_KEY_PATH (local file) or "
                "SNOWFLAKE_PRIVATE_KEY (raw PEM) in env — see .env.example."
            )

    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    p_key = serialization.load_pem_private_key(
        pem,
        password=passphrase.encode() if passphrase else None,
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    required = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":    os.getenv("SNOWFLAKE_USER"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        import sys
        sys.exit(
            f"ERROR: missing SNOWFLAKE_{'/'.join(m.upper() for m in missing)} "
            f"in env — see .env.example for the contract"
        )

    return create_engine(
        URL(
            account=required["account"],
            user=required["user"],
            role=os.getenv("SNOWFLAKE_ROLE", "R_NEM_RW"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "WH_NEM"),
            database=os.getenv("SNOWFLAKE_DATABASE", "NEM"),
            schema=schema or os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
        ),
        connect_args={"private_key": pkb},
    )
