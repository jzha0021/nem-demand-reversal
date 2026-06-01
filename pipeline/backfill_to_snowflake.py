"""
One-shot backfill: Postgres raw.* → S3 parsed/{dispatch,rooftop,weather}/
parquet → Snowpipe → Snowflake NEM.RAW.*.

Seeds the Snowflake raw layer from an existing local Postgres install.
Assumes the Snowflake side (db / schemas / pipes / storage integration /
S3 event notification) is already provisioned per `db/snowflake/`. Reads
each source table in monthly chunks, writes snappy parquet, and drops
each chunk into S3 — Snowpipe auto-ingests as files land.

Idempotency
-----------
Keys are deterministic: ``parsed/<source>/backfill_<YYYY-MM>.parquet``
(plus ``backfill_full.parquet`` for weather). Re-running the script
re-uploads the same keys, which Snowpipe's load history recognises as
"already loaded" and skips — no duplicate rows in Snowflake.

Usage
-----
    python pipeline/backfill_to_snowflake.py                  # all 3 sources
    python pipeline/backfill_to_snowflake.py --source dispatch
    python pipeline/backfill_to_snowflake.py --dry-run        # plan only
    python pipeline/backfill_to_snowflake.py --since 2025-01  # later months only
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Iterator

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "pipeline"))

from _common import upload_parquet_to_s3

load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Sources — per-source SQL + key prefix + column casing for parquet
# ---------------------------------------------------------------------------
# Snowflake's MATCH_BY_COLUMN_NAME is case-insensitive, but writing the
# parquet with UPPER column names matches the Snowflake table identifier
# exactly, which keeps the COPY trace readable.

SOURCES = {
    "dispatch": {
        "table":      "raw.region_5min",
        "time_col":   "settlementdate",
        "select_sql": """
            SELECT settlementdate              AS "SETTLEMENTDATE",
                   regionid                    AS "REGIONID",
                   intervention                AS "INTERVENTION",
                   totaldemand                 AS "TOTALDEMAND",
                   availablegeneration         AS "AVAILABLEGENERATION",
                   totalintermittentgeneration AS "TOTALINTERMITTENTGENERATION",
                   uigf                        AS "UIGF",
                   semischedule_clearedmw      AS "SEMISCHEDULE_CLEAREDMW",
                   demand_and_nonschedgen      AS "DEMAND_AND_NONSCHEDGEN",
                   netinterchange              AS "NETINTERCHANGE",
                   rrp                         AS "RRP"
              FROM raw.region_5min
             WHERE settlementdate >= :month_start
               AND settlementdate <  :month_end
             ORDER BY settlementdate, regionid
        """,
    },
    "rooftop": {
        "table":      "raw.rooftop_pv_30min",
        "time_col":   "interval_datetime",
        "select_sql": """
            SELECT interval_datetime AS "INTERVAL_DATETIME",
                   regionid          AS "REGIONID",
                   power             AS "POWER",
                   qi                AS "QI"
              FROM raw.rooftop_pv_30min
             WHERE interval_datetime >= :month_start
               AND interval_datetime <  :month_end
             ORDER BY interval_datetime, regionid
        """,
    },
}


# ---------------------------------------------------------------------------
# DB engine — SQLAlchemy so pandas.read_sql doesn't warn about raw DBAPI
# ---------------------------------------------------------------------------
def get_engine():
    missing = [v for v in ("DB_NAME", "DB_USER", "DB_PWD") if os.getenv(v) is None]
    if missing:
        sys.exit(f"Missing env vars in .env: {missing}")
    user = os.getenv("DB_USER")
    pwd  = os.getenv("DB_PWD")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}")


# ---------------------------------------------------------------------------
# Month iteration
# ---------------------------------------------------------------------------
def discover_month_range(engine, table: str, time_col: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return ``(first_month_start, last_month_start)`` covering all rows."""
    with engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT MIN({time_col}), MAX({time_col}) FROM {table}")
        ).fetchone()
    lo, hi = row
    if lo is None or hi is None:
        sys.exit(f"{table} is empty — nothing to backfill")
    return (pd.Timestamp(lo).to_period("M").to_timestamp(),
            pd.Timestamp(hi).to_period("M").to_timestamp())


def iter_months(start: pd.Timestamp, end_inclusive: pd.Timestamp,
                since: str | None = None) -> Iterator[pd.Timestamp]:
    floor = pd.Timestamp(since).to_period("M").to_timestamp() if since else start
    cur = max(floor, start)
    while cur <= end_inclusive:
        yield cur
        cur = (cur + pd.offsets.MonthBegin(1))


# ---------------------------------------------------------------------------
# Per-source backfill
# ---------------------------------------------------------------------------
def backfill_chunked(engine, source: str, since: str | None,
                     dry_run: bool) -> dict:
    """Backfill dispatch or rooftop, one parquet per calendar month."""
    cfg = SOURCES[source]
    first, last = discover_month_range(engine, cfg["table"], cfg["time_col"])
    months = list(iter_months(first, last, since=since))

    print(f"\n[{source}] Postgres rows span {first:%Y-%m} → {last:%Y-%m}")
    print(f"[{source}] backfilling {len(months)} month(s)"
          f"{f' (since {since})' if since else ''}")
    if dry_run:
        for m in months:
            print(f"  would upload parsed/{source}/backfill_{m:%Y-%m}.parquet")
        return {"months": len(months), "rows": 0, "bytes": 0}

    total_rows = 0
    total_bytes = 0
    pbar = tqdm(months, unit="mo")
    for month_start in pbar:
        month_end = month_start + pd.offsets.MonthBegin(1)
        pbar.set_description(f"{source} {month_start:%Y-%m}")
        t0 = time.time()
        df = pd.read_sql(
            text(cfg["select_sql"]), engine,
            params={"month_start": month_start, "month_end": month_end},
        )
        if df.empty:
            pbar.write(f"  {month_start:%Y-%m}: 0 rows — skipped")
            continue
        key = f"parsed/{source}/backfill_{month_start:%Y-%m}.parquet"
        s3_key, size = upload_parquet_to_s3(df, source=source, key=key)
        total_rows += len(df)
        total_bytes += size
        pbar.write(f"  {month_start:%Y-%m}: {len(df):>7,} rows  "
                   f"{size / 1024:>6.0f} KB  {time.time() - t0:4.1f}s  "
                   f"→ {s3_key}")
    pbar.close()
    return {"months": len(months), "rows": total_rows, "bytes": total_bytes}


def backfill_weather(engine, dry_run: bool) -> dict:
    """Backfill the entire weather table as a single parquet (it's tiny)."""
    print("\n[weather] reading raw.weather_daily (single chunk — small table)")
    sql = text("""
        SELECT date              AS "DATE",
               regionid          AS "REGIONID",
               t_max_c           AS "T_MAX_C",
               t_min_c           AS "T_MIN_C",
               solar_mj_m2       AS "SOLAR_MJ_M2",
               sunshine_seconds  AS "SUNSHINE_SECONDS",
               precip_mm         AS "PRECIP_MM"
          FROM raw.weather_daily
         ORDER BY date, regionid
    """)
    if dry_run:
        with engine.connect() as conn:
            n = conn.execute(text("SELECT COUNT(*) FROM raw.weather_daily")).scalar()
        print(f"  would upload parsed/weather/backfill_full.parquet ({n:,} rows)")
        return {"months": 1, "rows": 0, "bytes": 0}

    t0 = time.time()
    df = pd.read_sql(sql, engine)
    key = "parsed/weather/backfill_full.parquet"
    s3_key, size = upload_parquet_to_s3(df, source="weather", key=key)
    print(f"  {len(df):>7,} rows  {size / 1024:>6.0f} KB  "
          f"{time.time() - t0:4.1f}s  → {s3_key}")
    return {"months": 1, "rows": len(df), "bytes": size}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--source",
        choices=["dispatch", "rooftop", "weather", "all"],
        default="all",
        help="Which source(s) to backfill. Default: all 3.",
    )
    ap.add_argument(
        "--since", default=None,
        help="Backfill from this month onward (YYYY-MM). Default: earliest "
             "month present in Postgres. Only affects dispatch + rooftop "
             "(weather is single-chunk).",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Print the upload plan + row counts without writing to S3.",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    if not os.getenv("S3_BUCKET"):
        sys.exit("S3_BUCKET env var not set — see .env.example")

    sources_to_run = (["dispatch", "rooftop", "weather"]
                      if args.source == "all" else [args.source])

    engine = get_engine()
    try:
        summary = {}
        for src in sources_to_run:
            if src == "weather":
                summary[src] = backfill_weather(engine, dry_run=args.dry_run)
            else:
                summary[src] = backfill_chunked(
                    engine, src, since=args.since, dry_run=args.dry_run
                )
    finally:
        engine.dispose()

    print("\n" + "=" * 72)
    print("Backfill complete.")
    print(f"{'source':<10}  {'months':>8}  {'rows':>12}  {'size':>10}")
    print("-" * 48)
    for src, s in summary.items():
        size_mb = s["bytes"] / (1024 * 1024)
        print(f"{src:<10}  {s['months']:>8,}  {s['rows']:>12,}  {size_mb:>8.2f} MB")
    print("=" * 72)
    print("Snowpipe will auto-ingest each parquet within ~30-60s of upload.")
    print("Verify in Snowsight:")
    print("  SELECT 'dispatch', COUNT(*) FROM NEM.RAW.REGION_5MIN")
    print("   UNION ALL SELECT 'rooftop', COUNT(*) FROM NEM.RAW.ROOFTOP_PV_30MIN")
    print("   UNION ALL SELECT 'weather', COUNT(*) FROM NEM.RAW.WEATHER_DAILY;")
    return 0


if __name__ == "__main__":
    sys.exit(main())
