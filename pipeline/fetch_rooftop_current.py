"""
Real-time incremental fetch of ROOFTOP_PV_ACTUAL (MEASUREMENT) from NEMWeb
CURRENT. Sister script to fetch_aemo_current.py.

Strategy
--------
1.  Scrape https://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/ for the
    list of PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_*.zip files (~48/day per
    type; retention ~14 days).
2.  Filter to MEASUREMENT zips inside [since, until] (filename batch
    timestamp = INTERVAL_DATETIME + 30 min — see docs/NEMWEB_CURRENT_SCHEMA_DIFF.md).
3.  Per zip: download, parse via _common.parse_mms_zip(), narrow to ROOFTOP_COLS,
    keep only the 5 main regions.
4.  Bulk INSERT into raw.rooftop_pv_30min with ON CONFLICT DO NOTHING.

Usage
-----
    python pipeline/fetch_rooftop_current.py                          # last 8 days
    python pipeline/fetch_rooftop_current.py --since 2026-05-10
    python pipeline/fetch_rooftop_current.py --dry-run --limit 3
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tqdm import tqdm

# psycopg2 imported lazily inside get_conn() / insert_rows() so a CI
# run with --no-db doesn't need the Postgres driver installed.

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "pipeline"))

from _common import (
    NEMWEB_USR_AGENT,
    ROOFTOP_COLS,
    ROOFTOP_MAIN_REGIONS,
    df_to_insert_rows,
    download_nemweb_zip,
    make_nemweb_session,
    parse_mms_zip,
    s3_enabled,
    upload_blob_to_s3,
    upload_parquet_to_s3,
)

load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ROOFTOP_URL = "https://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/"

# PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_{YYYYMMDDHHMMSS}_{run_id}.zip
# The filename timestamp is the publication BATCH label, which is 30 min
# after the actual INTERVAL_DATETIME. So a filename '20260514153000' carries
# the interval that ended at 2026-05-14 15:00:00 — we filter on the filename
# timestamp here only as an approximation, the authoritative timestamp comes
# from the CSV body.
FILENAME_RE = re.compile(
    r"PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_(\d{14})_\d+\.zip$",
    re.IGNORECASE,
)

TABLE_KEY = "ROOFTOP_ACTUAL"
ROOFTOP_COLS_LOWER = [c.lower() for c in ROOFTOP_COLS]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_conn():
    import psycopg2  # lazy: CI with --no-db doesn't need Postgres driver
    missing = [v for v in ("DB_NAME", "DB_USER", "DB_PWD") if os.getenv(v) is None]
    if missing:
        sys.exit(f"Missing env vars in .env: {missing}")
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PWD"),
    )


# ---------------------------------------------------------------------------
# Discover MEASUREMENT zips
# ---------------------------------------------------------------------------
def list_zips_in_window(since: datetime, until: datetime,
                        session: requests.Session) -> list[tuple[datetime, str]]:
    """Return [(filename_ts, url), ...] for MEASUREMENT zips in window.

    Filename timestamp is INTERVAL_DATETIME + 30 min, so window membership is
    approximate. The actual INTERVAL_DATETIME inside the CSV is what gets
    written to the DB.
    """
    r = session.get(ROOFTOP_URL, headers=NEMWEB_USR_AGENT, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    out: list[tuple[datetime, str]] = []
    for a in soup.find_all("a"):
        href = a.get("href") or ""
        fname = href.rsplit("/", 1)[-1]
        m = FILENAME_RE.search(fname)
        if not m:
            continue
        ts = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
        if since <= ts <= until:
            out.append((ts, ROOFTOP_URL + fname))
    return sorted(out)


# ---------------------------------------------------------------------------
# Per-zip processing
# ---------------------------------------------------------------------------
def extract_rows(blob: bytes) -> pd.DataFrame:
    tables = parse_mms_zip(blob)
    if TABLE_KEY not in tables:
        raise RuntimeError(f"zip missing {TABLE_KEY}; saw {list(tables)}")
    df = tables[TABLE_KEY]

    # Defensive: filter by TYPE column (filename should already guarantee this)
    if "TYPE" in df.columns:
        df = df[df["TYPE"] == "MEASUREMENT"]

    # Drop sub-regions (QLDC/QLDN/QLDS/TASN/TASS)
    df = df[df["REGIONID"].isin(ROOFTOP_MAIN_REGIONS)]

    missing = [c for c in ROOFTOP_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"rooftop frame missing ROOFTOP_COLS: {missing}")
    return df[ROOFTOP_COLS]


# ---------------------------------------------------------------------------
# Bulk INSERT
# ---------------------------------------------------------------------------
def insert_rows(conn, schema: str, df: pd.DataFrame) -> tuple[int, int]:
    if df.empty:
        return 0, 0
    df = df.copy()
    df["INTERVAL_DATETIME"] = pd.to_datetime(df["INTERVAL_DATETIME"])
    df["POWER"] = pd.to_numeric(df["POWER"], errors="coerce")
    df["QI"]    = pd.to_numeric(df["QI"],    errors="coerce")

    cols_sql = ", ".join(ROOFTOP_COLS_LOWER)
    placeholders = "(" + ", ".join(["%s"] * len(ROOFTOP_COLS_LOWER)) + ")"
    insert_sql = (
        f"INSERT INTO {schema}.rooftop_pv_30min ({cols_sql}) "
        f"VALUES %s "
        f"ON CONFLICT (interval_datetime, regionid) DO NOTHING"
    )
    import psycopg2.extras
    rows = df_to_insert_rows(df)
    with conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur, insert_sql, rows, template=placeholders, page_size=500,
            )
            inserted = cur.rowcount
    return len(rows), inserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    today_aest = datetime.utcnow() + timedelta(hours=10)
    ap.add_argument(
        "--since", type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        default=today_aest.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=8),
        help="Start of window (inclusive), YYYY-MM-DD AEST. Default: D-8 00:00 "
             "(predict.py D-7 lookback + 1 day slack).",
    )
    ap.add_argument(
        "--until", type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        default=None,
        help="End of window (inclusive end-of-day), YYYY-MM-DD AEST. Default: now.",
    )
    ap.add_argument("--limit", type=int, default=None,
                    help="Fetch at most N zips (smoke testing).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Skip DB INSERT + S3 upload; print row counts only.")
    ap.add_argument("--no-s3", action="store_true",
                    help="Skip S3 raw-zip mirror even if S3_BUCKET is set.")
    ap.add_argument("--no-parquet", action="store_true",
                    help="Skip parsed parquet upload to s3://.../parsed/rooftop/.")
    ap.add_argument("--no-db", action="store_true",
                    help="Skip Postgres INSERT (CI runs that only mirror to S3).")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    schema = os.getenv("DB_SCHEMA", "raw")

    if args.until is not None:
        until = args.until.replace(hour=23, minute=59, second=59)
    else:
        until = datetime.utcnow() + timedelta(hours=10) + timedelta(minutes=30)

    print(f"[fetch] window AEST: {args.since}  →  {until}")
    print(f"[fetch] DB schema:   {schema}  dry_run={args.dry_run}")

    session = make_nemweb_session()
    try:
        zips = list_zips_in_window(args.since, until, session)
    except Exception as e:
        sys.exit(f"FAIL listing {ROOFTOP_URL}: {type(e).__name__}: {e}")
    print(f"[fetch] {len(zips)} MEASUREMENT zips in window")
    if args.limit:
        zips = zips[: args.limit]
        print(f"[fetch] limited to first {len(zips)}")

    if not zips:
        return 0

    use_db = not (args.dry_run or args.no_db)
    use_s3 = not args.dry_run and not args.no_s3 and s3_enabled()
    use_parquet = not args.dry_run and not args.no_parquet and s3_enabled()
    conn = get_conn() if use_db else None
    print(f"[fetch] db={'on' if use_db else 'off'}  "
          f"s3_zip={'on' if use_s3 else 'off'}  "
          f"s3_parquet={'on' if use_parquet else 'off'}")

    total_attempted = total_inserted = 0
    s3_uploaded = s3_already = 0
    parsed_dfs: list[pd.DataFrame] = []
    failed: list[tuple[str, str]] = []

    pbar = tqdm(zips, unit="zip")
    for ts, url in pbar:
        pbar.set_description(ts.strftime("%m-%d %H:%M"))
        try:
            t0 = time.time()
            blob = download_nemweb_zip(url, session=session, timeout=60)

            if use_s3:
                fname = url.rsplit("/", 1)[-1]
                s3_key = f"raw/rooftop/{ts:%Y-%m-%d}/{fname}"
                uploaded, _ = upload_blob_to_s3(blob, s3_key)
                if uploaded:
                    s3_uploaded += 1
                else:
                    s3_already += 1

            df = extract_rows(blob)
            if use_parquet:
                parsed_dfs.append(df)
            if args.dry_run:
                total_attempted += len(df)
                pbar.write(f"[{ts:%Y-%m-%d %H:%M}] {len(df):2d} rows  "
                           f"{time.time() - t0:4.1f}s  (dry-run)")
                continue
            if not use_db:
                total_attempted += len(df)
                pbar.write(f"[{ts:%Y-%m-%d %H:%M}] {len(df):2d} rows  "
                           f"{time.time() - t0:4.1f}s  (s3-only)")
                continue
            attempted, inserted = insert_rows(conn, schema, df)
            total_attempted += attempted
            total_inserted += inserted
            pbar.write(f"[{ts:%Y-%m-%d %H:%M}] {attempted:2d} attempted  "
                       f"{inserted:2d} new  {attempted - inserted:2d} dup  "
                       f"{time.time() - t0:4.1f}s")
        except Exception as e:
            failed.append((url, f"{type(e).__name__}: {e}"))
            pbar.write(f"[{ts:%Y-%m-%d %H:%M}] FAIL {type(e).__name__}: {e}")
    pbar.close()

    if conn is not None:
        conn.close()

    parquet_key, parquet_bytes = "", 0
    if use_parquet and parsed_dfs:
        batched = pd.concat(parsed_dfs, ignore_index=True)
        parquet_key, parquet_bytes = upload_parquet_to_s3(batched, source="rooftop")

    print("\n" + "=" * 72)
    print(f"Complete. attempted={total_attempted}  inserted={total_inserted}  "
          f"failed_zips={len(failed)}")
    if use_s3:
        print(f"S3 raw zip: uploaded={s3_uploaded}  already_present={s3_already}")
    if use_parquet and parquet_key:
        print(f"S3 parquet: s3://{os.getenv('S3_BUCKET')}/{parquet_key}  "
              f"({parquet_bytes:,} bytes)")
    if failed:
        for u, e in failed[:5]:
            print(f"  {u.rsplit('/', 1)[-1]}: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
