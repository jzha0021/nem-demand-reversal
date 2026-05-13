"""
Load monthly parquet files into raw.region_5min via Postgres COPY FROM STDIN.

Strategy
--------
- One transaction per month.
- Idempotent: detects rows already present for a given (year, month) and
  skips by default. With --force, deletes and re-loads.
- Reads connection details from .env (see .env.example).

Prerequisites
-------------
1.  cp .env.example .env  ; fill in DB_PWD
2.  psql -d <DB_NAME> -f db/01_raw_schema.sql
3.  python pipeline/fetch_aemo.py     # produce parquet files first

Usage
-----
    python pipeline/load_to_postgres.py                    # all parquet files
    python pipeline/load_to_postgres.py --start 2024-01 --end 2024-12
    python pipeline/load_to_postgres.py --month 2024-12
    python pipeline/load_to_postgres.py --force            # reload existing months
"""

from __future__ import annotations

import argparse
import calendar
import io
import os
import re
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm

from _common import KEEP_COLS

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet" / "region_5min"

# Load .env at import time so os.getenv() works in module-level code too.
load_dotenv(PROJECT_ROOT / ".env")

PARQUET_NAME_RE = re.compile(r"region_5min_(\d{4})-(\d{2})\.parquet$")

TABLE = "region_5min"  # schema name comes from $DB_SCHEMA env var

# Postgres column order = lowercased KEEP_COLS in same order. Must match
# db/01_raw_schema.sql column order.
COLUMNS_LOWER = [c.lower() for c in KEEP_COLS]


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def get_conn():
    # .env was loaded at module import; just read.
    missing = [
        v for v in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PWD")
        if os.getenv(v) is None
    ]
    if missing:
        raise RuntimeError(
            f"Missing env vars in .env: {missing}. "
            f"Copy .env.example → .env and fill them in."
        )
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PWD"),
    )


def list_parquet_files(start: str | None, end: str | None, month: str | None):
    """Return sorted list of (year, month, path) tuples filtered by window."""
    out = []
    for p in sorted(PARQUET_DIR.glob("region_5min_*.parquet")):
        match = PARQUET_NAME_RE.search(p.name)
        if not match:
            continue
        y, m = int(match.group(1)), int(match.group(2))

        if month:
            tag_y, tag_m = map(int, month.split("-"))
            if (y, m) != (tag_y, tag_m):
                continue
        else:
            if start:
                sy, sm = map(int, start.split("-"))
                if (y, m) < (sy, sm):
                    continue
            if end:
                ey, em = map(int, end.split("-"))
                if (y, m) > (ey, em):
                    continue
        out.append((y, m, p))
    return out


def month_bounds(year: int, month: int) -> tuple[str, str]:
    """Return (start_excl, end_incl) ISO timestamps for month (year, month).

    A row "belongs to" month M iff its interval-START is in M, i.e. its
    SETTLEMENTDATE (interval-END) is in (start_of_M, start_of_M+1].
    Use with `WHERE settlementdate > %s AND settlementdate <= %s`,
    NOT `>= AND <` — the naive form misclassifies the SETTLEMENTDATE
    that lands exactly on the next month's first instant.
    """
    start_excl = f"{year:04d}-{month:02d}-01 00:00:00"
    if month == 12:
        end_incl = f"{year + 1:04d}-01-01 00:00:00"
    else:
        end_incl = f"{year:04d}-{month + 1:02d}-01 00:00:00"
    return start_excl, end_incl


def month_already_loaded(cur, schema: str, year: int, month: int) -> int:
    """Return existing row count for (year, month), 0 if none."""
    start_excl, end_incl = month_bounds(year, month)
    cur.execute(
        f"SELECT COUNT(*) FROM {schema}.{TABLE} "
        f"WHERE settlementdate > %s AND settlementdate <= %s",
        (start_excl, end_incl),
    )
    return cur.fetchone()[0]


def delete_month(cur, schema: str, year: int, month: int) -> int:
    start_excl, end_incl = month_bounds(year, month)
    cur.execute(
        f"DELETE FROM {schema}.{TABLE} "
        f"WHERE settlementdate > %s AND settlementdate <= %s",
        (start_excl, end_incl),
    )
    return cur.rowcount


def copy_one_month(conn, schema: str, year: int, month: int, parquet_path: Path,
                   force: bool) -> dict:
    """Load a single month inside a single transaction. Returns timing/stats."""
    t0 = time.time()
    df = pd.read_parquet(parquet_path)

    # Normalise column names to match Postgres schema (lowercase).
    df.columns = [c.lower() for c in df.columns]
    missing = [c for c in COLUMNS_LOWER if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"Parquet {parquet_path.name} missing expected columns: {missing}"
        )
    df = df[COLUMNS_LOWER]

    # CSV buffer. NaN → '' which Postgres COPY treats as NULL.
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    csv_bytes = buf.tell()  # capture size BEFORE seek(0); after seek tell=0
    buf.seek(0)

    cols_sql = ", ".join(COLUMNS_LOWER)
    copy_sql = (
        f"COPY {schema}.{TABLE} ({cols_sql}) "
        f"FROM STDIN WITH (FORMAT csv, NULL '')"
    )

    with conn:  # auto-commit on success / rollback on exception
        with conn.cursor() as cur:
            existing = month_already_loaded(cur, schema, year, month)
            if existing and not force:
                return {
                    "status": "skipped",
                    "rows_existing": existing,
                    "rows_loaded": 0,
                    "elapsed_s": time.time() - t0,
                }
            if existing and force:
                deleted = delete_month(cur, schema, year, month)
            else:
                deleted = 0

            cur.copy_expert(copy_sql, buf)
            loaded = cur.rowcount

    return {
        "status": "loaded",
        "rows_existing": existing,
        "rows_deleted": deleted,
        "rows_loaded": loaded,
        "csv_kb": csv_bytes / 1024,
        "elapsed_s": time.time() - t0,
    }


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", help="YYYY-MM (inclusive)")
    ap.add_argument("--end", help="YYYY-MM (inclusive)")
    ap.add_argument("--month", help="Single YYYY-MM")
    ap.add_argument(
        "--force",
        action="store_true",
        help="Delete existing rows for the month before reloading",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    schema = os.getenv("DB_SCHEMA", "raw")

    files = list_parquet_files(args.start, args.end, args.month)
    if not files:
        print(f"No parquet files in {PARQUET_DIR} match the filter.")
        return 1
    print(f"Found {len(files)} parquet files to process. Schema: {schema}")

    try:
        conn = get_conn()
    except Exception as e:
        print(f"FAIL: {type(e).__name__}: {e}")
        return 1

    skipped = loaded = 0
    failed: list[tuple[int, int, str]] = []

    pbar = tqdm(files, unit="mo")
    for y, m, path in pbar:
        tag = f"{y:04d}-{m:02d}"
        pbar.set_description(tag)
        try:
            stats = copy_one_month(conn, schema, y, m, path, force=args.force)
        except Exception as e:
            failed.append((y, m, f"{type(e).__name__}: {e}"))
            pbar.write(f"[{tag}] FAIL — {type(e).__name__}: {e}")
            continue

        if stats["status"] == "skipped":
            skipped += 1
            pbar.write(
                f"[{tag}] SKIP ({stats['rows_existing']:,} rows already present; "
                f"use --force to reload)"
            )
        else:
            loaded += 1
            del_msg = (
                f"  (deleted {stats['rows_deleted']:,})"
                if stats.get("rows_deleted") else ""
            )
            pbar.write(
                f"[{tag}] {stats['elapsed_s']:5.1f}s  "
                f"{stats['rows_loaded']:>6,} rows  "
                f"{stats['csv_kb']:6.0f} KB csv"
                f"{del_msg}"
            )

    pbar.close()
    conn.close()

    print("\n" + "=" * 72)
    print(f"Complete. {loaded} loaded, {skipped} skipped, {len(failed)} failed.")
    if failed:
        print("\nFailed months:")
        for y, m, err in failed:
            print(f"  {y}-{m:02d}: {err}")
        return 1
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
