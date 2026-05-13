"""
Load per-region weather parquet files into raw.weather_daily via Postgres COPY FROM STDIN.

Strategy
--------
- One transaction per region (5 transactions total for full load).
- Date-range-aware idempotency (cron-friendly, mirrors fetch_open_meteo.py):
    * empty table for region → COPY everything in parquet
    * DB MAX(date) < parquet MAX(date) → COPY only date > db_max (incremental tail)
    * DB MAX(date) >= parquet MAX(date) → skip (fully loaded)
    * --force → delete the parquet's [min, max] range and reload entirely
- Reads connection details from .env (see .env.example).

Why incremental tail load (not full delete-and-reload by default)
-----------------------------------------------------------------
Cron use case: the fetcher appends new dates to the parquet each run,
then load_weather.py copies only the new tail rows into DB. A blanket
delete-and-reload would re-COPY all 6,700 rows every time — fine for
size, but unnecessary churn. The PK (date, regionid) prevents duplicate
inserts at the COPY boundary, so the incremental path is safe.

--force is preserved for the case where parquet content changed in
place (e.g. ERA5 revision back-fix) and DB needs to match exactly.

Prerequisites
-------------
1.  cp .env.example .env  ; fill in DB_PWD
2.  psql -d <DB_NAME> -f db/01_raw_schema.sql
3.  python pipeline/fetch_open_meteo.py     # produce parquet files first

Usage
-----
    python pipeline/load_weather.py                 # all regions
    python pipeline/load_weather.py --region SA1
    python pipeline/load_weather.py --force         # reload existing
"""

from __future__ import annotations

import argparse
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

from _common import WEATHER_COLS

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet" / "weather_daily"

# Load .env so any os.getenv() below sees the values.
load_dotenv(PROJECT_ROOT / ".env")

PARQUET_NAME_RE = re.compile(r"weather_daily_([A-Z0-9]+)\.parquet$")

TABLE = "weather_daily"  # schema name comes from $DB_SCHEMA env var
COLUMNS_LOWER = [c.lower() for c in WEATHER_COLS]


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def get_conn():
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


def list_parquet_files(region: str | None):
    """Return sorted list of (regionid, path) tuples filtered by --region."""
    out = []
    for p in sorted(PARQUET_DIR.glob("weather_daily_*.parquet")):
        match = PARQUET_NAME_RE.search(p.name)
        if not match:
            continue
        regionid = match.group(1)
        if region and regionid != region:
            continue
        out.append((regionid, p))
    return out


def get_db_max_date(cur, schema: str, regionid: str):
    """Return MAX(date) for region, or None if region has no rows yet."""
    cur.execute(
        f"SELECT MAX(date) FROM {schema}.{TABLE} WHERE regionid = %s",
        (regionid,),
    )
    return cur.fetchone()[0]


def delete_region_range(cur, schema: str, regionid: str,
                        start_d, end_d) -> int:
    cur.execute(
        f"DELETE FROM {schema}.{TABLE} "
        f"WHERE regionid = %s AND date BETWEEN %s AND %s",
        (regionid, start_d, end_d),
    )
    return cur.rowcount


def _copy_df(cur, schema: str, df: pd.DataFrame) -> tuple[int, float]:
    """COPY a DataFrame's rows into the target table. Returns (rows_loaded, csv_kb)."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    csv_kb = buf.tell() / 1024
    buf.seek(0)

    cols_sql = ", ".join(COLUMNS_LOWER)
    copy_sql = (
        f"COPY {schema}.{TABLE} ({cols_sql}) "
        f"FROM STDIN WITH (FORMAT csv, NULL '')"
    )
    cur.copy_expert(copy_sql, buf)
    return cur.rowcount, csv_kb


def copy_one_region(conn, schema: str, regionid: str,
                    parquet_path: Path, force: bool) -> dict:
    """Load one region inside a single transaction. Returns timing/stats.

    Branches:
      force=True  → delete parquet's [min, max] range, COPY all parquet rows
      empty DB    → COPY all parquet rows
      tail behind → COPY only date > db_max
      tail ahead  → skip (DB already at or past parquet max)
    """
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

    # Defensive: parquet must contain only the claimed regionid (filename
    # is the contract). A mismatch means parquet was hand-edited or
    # filename was renamed — fail loud.
    bad_rows = df[df["regionid"] != regionid]
    if not bad_rows.empty:
        raise RuntimeError(
            f"Parquet {parquet_path.name} has {len(bad_rows)} row(s) where "
            f"regionid != '{regionid}'. Refusing to load."
        )

    parquet_min = df["date"].min()
    parquet_max = df["date"].max()

    with conn:  # auto-commit on success / rollback on exception
        with conn.cursor() as cur:
            db_max = get_db_max_date(cur, schema, regionid)

            if force:
                deleted = delete_region_range(cur, schema, regionid,
                                              parquet_min, parquet_max)
                loaded, csv_kb = _copy_df(cur, schema, df)
                return {
                    "status":       "force-reload",
                    "rows_deleted": deleted,
                    "rows_loaded":  loaded,
                    "csv_kb":       csv_kb,
                    "loaded_range": (str(parquet_min), str(parquet_max)),
                    "elapsed_s":    time.time() - t0,
                }

            if db_max is None:
                # Empty for this region: COPY everything.
                loaded, csv_kb = _copy_df(cur, schema, df)
                return {
                    "status":       "fresh",
                    "rows_loaded":  loaded,
                    "csv_kb":       csv_kb,
                    "loaded_range": (str(parquet_min), str(parquet_max)),
                    "elapsed_s":    time.time() - t0,
                }

            if db_max >= parquet_max:
                # DB already covers parquet — nothing new to add.
                return {
                    "status":      "skipped",
                    "db_max":      str(db_max),
                    "parquet_max": str(parquet_max),
                    "elapsed_s":   time.time() - t0,
                }

            # Incremental tail: only rows with date > db_max are new.
            tail = df[df["date"] > db_max]
            if tail.empty:
                # Defensive — shouldn't happen given the >= check above.
                return {
                    "status":    "skipped",
                    "db_max":    str(db_max),
                    "elapsed_s": time.time() - t0,
                }
            loaded, csv_kb = _copy_df(cur, schema, tail)
            return {
                "status":       "tail",
                "rows_loaded":  loaded,
                "csv_kb":       csv_kb,
                "loaded_range": (
                    str(tail["date"].min()), str(tail["date"].max())
                ),
                "db_max_before": str(db_max),
                "elapsed_s":    time.time() - t0,
            }


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--region", help="Single REGIONID (default: all)")
    ap.add_argument(
        "--force", action="store_true",
        help="Delete existing rows in the parquet's date range before reloading",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    schema = os.getenv("DB_SCHEMA", "raw")

    files = list_parquet_files(args.region)
    if not files:
        print(f"No parquet files in {PARQUET_DIR} match the filter.")
        return 1
    print(f"Found {len(files)} parquet files to process. Schema: {schema}")

    try:
        conn = get_conn()
    except Exception as e:
        print(f"FAIL: {type(e).__name__}: {e}")
        return 1

    counts = {"fresh": 0, "tail": 0, "force-reload": 0, "skipped": 0}
    failed: list[tuple[str, str]] = []

    pbar = tqdm(files, unit="reg")
    for regionid, path in pbar:
        pbar.set_description(regionid)
        try:
            stats = copy_one_region(conn, schema, regionid, path,
                                    force=args.force)
        except Exception as e:
            failed.append((regionid, f"{type(e).__name__}: {e}"))
            pbar.write(f"[{regionid}] FAIL — {type(e).__name__}: {e}")
            continue

        status = stats["status"]
        counts[status] = counts.get(status, 0) + 1

        if status == "skipped":
            pbar.write(
                f"[{regionid}] SKIP (db_max={stats.get('db_max')} >= "
                f"parquet_max={stats.get('parquet_max')}; "
                f"use --force to overwrite)"
            )
        elif status == "tail":
            d0, d1 = stats["loaded_range"]
            pbar.write(
                f"[{regionid}] TAIL  {stats['elapsed_s']:5.1f}s  "
                f"+{stats['rows_loaded']:,} rows ({d0} → {d1})  "
                f"db_max was {stats['db_max_before']}  "
                f"{stats['csv_kb']:6.0f} KB"
            )
        elif status == "fresh":
            d0, d1 = stats["loaded_range"]
            pbar.write(
                f"[{regionid}] FRESH {stats['elapsed_s']:5.1f}s  "
                f"{stats['rows_loaded']:>5,} rows  ({d0} → {d1})  "
                f"{stats['csv_kb']:6.0f} KB"
            )
        else:  # force-reload
            d0, d1 = stats["loaded_range"]
            pbar.write(
                f"[{regionid}] FORCE {stats['elapsed_s']:5.1f}s  "
                f"{stats['rows_loaded']:>5,} rows  ({d0} → {d1})  "
                f"deleted {stats['rows_deleted']:,}  "
                f"{stats['csv_kb']:6.0f} KB"
            )

    pbar.close()
    conn.close()

    print("\n" + "=" * 72)
    print(
        f"Complete. {counts['fresh']} fresh, {counts['tail']} tail, "
        f"{counts['force-reload']} force-reload, {counts['skipped']} skipped, "
        f"{len(failed)} failed."
    )
    if failed:
        print("\nFailed regions:")
        for r, err in failed:
            print(f"  {r}: {err}")
        return 1
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
