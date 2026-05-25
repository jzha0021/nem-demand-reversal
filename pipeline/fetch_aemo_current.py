"""
Real-time incremental fetch of DISPATCHREGIONSUM + DISPATCHPRICE from NEMWeb
CURRENT. Mirror of fetch_aemo.py but for the recent ~2-day window that MMSDM
hasn't published yet.

Strategy
--------
1.  Scrape https://nemweb.com.au/Reports/Current/DispatchIS_Reports/ for the
    list of PUBLIC_DISPATCHIS_*.zip files (~288/day; retention is ~2 days).
2.  Filter to filenames whose timestamp falls in [since, until].
3.  Per zip: download, parse via _common.parse_mms_zip(), extract
    DISPATCH_REGIONSUM + DISPATCH_PRICE D-rows, merge on
    (SETTLEMENTDATE, REGIONID, INTERVENTION).
4.  Filter INTERVENTION=0 (same rule as MMSDM path).
5.  Bulk INSERT into raw.region_5min with ON CONFLICT DO NOTHING. PK
    (settlementdate, regionid) makes the pipeline naturally idempotent — safe
    to re-run after partial failure.

No parquet intermediate — CURRENT's ~1500 rows/day don't justify the cost.
For historical bulk loads keep using pipeline/fetch_aemo.py + load_to_postgres.py.

Usage
-----
    python pipeline/fetch_aemo_current.py                            # last 2 days
    python pipeline/fetch_aemo_current.py --since 2026-05-13         # since explicit date
    python pipeline/fetch_aemo_current.py --since 2026-05-13 --until 2026-05-14
    python pipeline/fetch_aemo_current.py --dry-run --limit 5        # smoke test

Background — see docs/NEMWEB_CURRENT_SCHEMA_DIFF.md for why this exists
(nemosis 3.8.1 has no CURRENT routing for these tables).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "pipeline"))

from _common import (
    KEEP_COLS,
    NEMWEB_USR_AGENT,
    df_to_insert_rows,
    download_nemweb_zip,
    parse_mms_zip,
)

load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DISPATCH_URL = "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/"

# Filename pattern: PUBLIC_DISPATCHIS_{YYYYMMDDHHMM}_{run_id}.zip
# Filename timestamp = SETTLEMENTDATE = interval-END (5-min cadence).
FILENAME_RE = re.compile(r"PUBLIC_DISPATCHIS_(\d{12})_\d+\.zip$", re.IGNORECASE)

TABLE_REGIONSUM = "DISPATCH_REGIONSUM"
TABLE_PRICE = "DISPATCH_PRICE"

KEEP_COLS_LOWER = [c.lower() for c in KEEP_COLS]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_conn():
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
# Discover zips on NEMWeb directory listing
# ---------------------------------------------------------------------------
def list_zips_in_window(since: datetime, until: datetime,
                        session: requests.Session) -> list[tuple[datetime, str]]:
    """Scrape directory listing, return [(interval_end_dt, url), ...] in window."""
    r = session.get(DISPATCH_URL, headers=NEMWEB_USR_AGENT, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    out: list[tuple[datetime, str]] = []
    for a in soup.find_all("a"):
        href = a.get("href") or ""
        fname = href.rsplit("/", 1)[-1]
        m = FILENAME_RE.search(fname)
        if not m:
            continue
        ts = datetime.strptime(m.group(1), "%Y%m%d%H%M")
        if since <= ts <= until:
            out.append((ts, DISPATCH_URL + fname))
    return sorted(out)


# ---------------------------------------------------------------------------
# Per-zip processing
# ---------------------------------------------------------------------------
def extract_merged_rows(blob: bytes) -> pd.DataFrame:
    """Pull REGIONSUM + PRICE from one zip, merge to a single narrowed frame."""
    tables = parse_mms_zip(blob)
    if TABLE_REGIONSUM not in tables or TABLE_PRICE not in tables:
        raise RuntimeError(
            f"zip missing expected tables; saw {list(tables)}"
        )

    rs = tables[TABLE_REGIONSUM]
    pr = tables[TABLE_PRICE]

    # INTERVENTION = 0 filter — same rule as pipeline/fetch_aemo.py:118
    rs = rs[rs["INTERVENTION"] == "0"]
    pr = pr[pr["INTERVENTION"] == "0"]

    merged = rs.merge(
        pr[["SETTLEMENTDATE", "REGIONID", "INTERVENTION", "RRP"]],
        on=["SETTLEMENTDATE", "REGIONID", "INTERVENTION"],
        how="inner",
        validate="one_to_one",
    )

    missing = [c for c in KEEP_COLS if c not in merged.columns]
    if missing:
        raise RuntimeError(f"merged frame missing KEEP_COLS: {missing}")
    return merged[KEEP_COLS]


# ---------------------------------------------------------------------------
# Bulk INSERT with ON CONFLICT DO NOTHING
# ---------------------------------------------------------------------------
def insert_rows(conn, schema: str, df: pd.DataFrame) -> tuple[int, int]:
    """Returns (rows_attempted, rows_inserted). Uses execute_values for speed."""
    if df.empty:
        return 0, 0

    # Type coercion: SETTLEMENTDATE → datetime, numerics → float, INTERVENTION → int
    df = df.copy()
    df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"])
    df["INTERVENTION"] = df["INTERVENTION"].astype(int)
    for col in ("TOTALDEMAND", "AVAILABLEGENERATION", "TOTALINTERMITTENTGENERATION",
                "UIGF", "SEMISCHEDULE_CLEAREDMW", "DEMAND_AND_NONSCHEDGEN",
                "NETINTERCHANGE", "RRP"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    cols_sql = ", ".join(KEEP_COLS_LOWER)
    placeholders = "(" + ", ".join(["%s"] * len(KEEP_COLS_LOWER)) + ")"
    insert_sql = (
        f"INSERT INTO {schema}.region_5min ({cols_sql}) "
        f"VALUES %s "
        f"ON CONFLICT (settlementdate, regionid) DO NOTHING"
    )
    rows = df_to_insert_rows(df)
    with conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur, insert_sql, rows, template=placeholders, page_size=500,
            )
            inserted = cur.rowcount
    return len(rows), inserted


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    today = datetime.utcnow() + timedelta(hours=10)   # AEST
    ap.add_argument(
        "--since", type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        default=today.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2),
        help="Start of window (inclusive), YYYY-MM-DD AEST. Default: D-2 00:00.",
    )
    ap.add_argument(
        "--until", type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        default=None,
        help="End of window (inclusive end-of-day), YYYY-MM-DD AEST. Default: now.",
    )
    ap.add_argument(
        "--limit", type=int, default=None,
        help="Fetch at most N zips (for smoke testing).",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Skip DB INSERT; print row counts only.",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    schema = os.getenv("DB_SCHEMA", "raw")

    # Until = end-of-day if supplied, else now AEST + a bit of slack
    if args.until is not None:
        until = args.until.replace(hour=23, minute=59, second=59)
    else:
        until = datetime.utcnow() + timedelta(hours=10) + timedelta(minutes=15)

    print(f"[fetch] window AEST: {args.since}  →  {until}")
    print(f"[fetch] DB schema:   {schema}  dry_run={args.dry_run}")

    session = requests.Session()
    try:
        zips = list_zips_in_window(args.since, until, session)
    except Exception as e:
        sys.exit(f"FAIL listing {DISPATCH_URL}: {type(e).__name__}: {e}")
    print(f"[fetch] {len(zips)} zips in window")
    if args.limit:
        zips = zips[: args.limit]
        print(f"[fetch] limited to first {len(zips)}")

    if not zips:
        print("[fetch] nothing to do — exiting")
        return 0

    conn = None if args.dry_run else get_conn()

    total_attempted = total_inserted = total_skipped_existing = 0
    failed: list[tuple[str, str]] = []

    pbar = tqdm(zips, unit="zip")
    for ts, url in pbar:
        pbar.set_description(ts.strftime("%m-%d %H:%M"))
        try:
            t0 = time.time()
            blob = download_nemweb_zip(url, timeout=30)
            df = extract_merged_rows(blob)
            if args.dry_run:
                total_attempted += len(df)
                pbar.write(f"[{ts:%Y-%m-%d %H:%M}] {len(df):3d} rows  "
                           f"{time.time() - t0:4.1f}s  (dry-run)")
                continue
            attempted, inserted = insert_rows(conn, schema, df)
            total_attempted += attempted
            total_inserted += inserted
            total_skipped_existing += attempted - inserted
            pbar.write(f"[{ts:%Y-%m-%d %H:%M}] {attempted:3d} attempted  "
                       f"{inserted:3d} new  {attempted - inserted:3d} dup  "
                       f"{time.time() - t0:4.1f}s")
        except Exception as e:
            failed.append((url, f"{type(e).__name__}: {e}"))
            pbar.write(f"[{ts:%Y-%m-%d %H:%M}] FAIL {type(e).__name__}: {e}")
    pbar.close()

    if conn is not None:
        conn.close()

    print("\n" + "=" * 72)
    print(f"Complete. attempted={total_attempted}  inserted={total_inserted}  "
          f"dup-skipped={total_skipped_existing}  failed_zips={len(failed)}")
    if failed:
        print("\nFailed zips (first 5):")
        for u, e in failed[:5]:
            print(f"  {u.rsplit('/', 1)[-1]}: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
