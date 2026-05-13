"""
Full-batch ingestion: 44 months × 5 regions × ROOFTOP_PV_ACTUAL.

Strategy
--------
- Monthly chunked: one parquet per month, named rooftop_30min_YYYY-MM.parquet.
- Idempotent: if the target parquet already exists, the month is skipped.
  Re-running after a partial failure resumes from the first missing month.
- Two ingestion-time filters:
    1. TYPE = 'MEASUREMENT' (drops AEMO's SATELLITE backup rows — same row
       coverage as MEASUREMENT in the project window, no analytical value to
       keep both. Decision rationale in db/01_raw_schema.sql.)
    2. REGIONID IN main 5 (drops sub-regions QLDC/QLDN/QLDS/TASN/TASS which
       no analysis uses).
- Column-narrowed to ROOFTOP_COLS (defined in pipeline/_common.py).
- Per-month row-count sanity (5 × 48 × days_in_month) with 1% threshold,
  same gating logic as fetch_aemo.py.

Output
------
data/parquet/rooftop_30min/rooftop_30min_YYYY-MM.parquet  (one file per month)

Usage
-----
    python pipeline/fetch_rooftop.py                       # full batch 2022-08 → 2026-03
    python pipeline/fetch_rooftop.py --start 2024-01 --end 2024-12
    python pipeline/fetch_rooftop.py --month 2024-12        # just one month
    python pipeline/fetch_rooftop.py --force                # re-download even if parquet exists

"""

from __future__ import annotations

import argparse
import calendar
import sys
import time
from pathlib import Path
from typing import Iterator

import pandas as pd
from tqdm import tqdm

from _common import ROOFTOP_COLS, ROOFTOP_MAIN_REGIONS

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_CACHE = PROJECT_ROOT / "data" / "raw" / "nemosis_cache"
PARQUET_OUT = PROJECT_ROOT / "data" / "parquet" / "rooftop_30min"

# Project window. Matches fetch_aemo.py so downstream joins have aligned
# coverage; extending one side without the other creates NULL gaps in
# v_h2_panel / v_ml_features.
DEFAULT_START = "2022-08"
DEFAULT_END = "2026-03"

TABLE = "ROOFTOP_PV_ACTUAL"
INTERVALS_PER_DAY = 48           # 30-min cadence
N_REGIONS = len(ROOFTOP_MAIN_REGIONS)


# -----------------------------------------------------------------------------
# Month iteration
# -----------------------------------------------------------------------------
def iter_months(start: str, end: str) -> Iterator[tuple[int, int]]:
    """Yield (year, month) tuples from start (inclusive) to end (inclusive).

    start / end format: 'YYYY-MM'
    """
    sy, sm = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def parquet_path(year: int, month: int) -> Path:
    return PARQUET_OUT / f"rooftop_30min_{year:04d}-{month:02d}.parquet"


def nemosis_window(year: int, month: int) -> tuple[str, str]:
    """Return (start, end) strings in nemosis format 'YYYY/MM/DD HH:MM:SS'.

    Same (start, end] semantics as fetch_aemo.py — nemosis returns rows whose
    interval-end timestamp is in (start, end]. For Dec 2024 this means rows
    from 2024-12-01 00:30 (covers Dec 1 00:00→00:30) through 2025-01-01 00:00
    (covers Dec 31 23:30→Jan 1 00:00) inclusive.
    """
    start = f"{year:04d}/{month:02d}/01 00:00:00"
    if month == 12:
        end = f"{year + 1:04d}/01/01 00:00:00"
    else:
        end = f"{year:04d}/{month + 1:02d}/01 00:00:00"
    return start, end


# -----------------------------------------------------------------------------
# Per-month fetch
# -----------------------------------------------------------------------------
def fetch_one_month(year: int, month: int, dynamic_data_compiler) -> dict:
    """Download one month of ROOFTOP_PV_ACTUAL, filter, narrow, write parquet.

    Returns dict with timing + size stats for the progress log.
    """
    start_str, end_str = nemosis_window(year, month)
    out = parquet_path(year, month)

    t0 = time.time()
    df = dynamic_data_compiler(
        start_time=start_str,
        end_time=end_str,
        table_name=TABLE,
        raw_data_location=str(RAW_CACHE),
        fformat="feather",
        keep_csv=False,
        select_columns=None,
    )
    download_s = time.time() - t0
    rows_raw = len(df)

    # ------------------------------------------------------------------
    # Filter rows: TYPE = MEASUREMENT, main 5 regions only
    # ------------------------------------------------------------------
    df = df[
        (df["TYPE"] == "MEASUREMENT")
        & (df["REGIONID"].isin(ROOFTOP_MAIN_REGIONS))
    ].copy()

    # ------------------------------------------------------------------
    # Column narrowing (defensive — fail loud if AEMO schema drifts)
    # ------------------------------------------------------------------
    missing = [c for c in ROOFTOP_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"ROOFTOP_COLS not in df for {year}-{month:02d}: {missing}. "
            f"Available: {df.columns.tolist()}"
        )
    df = df[ROOFTOP_COLS]

    # ------------------------------------------------------------------
    # Sanity: row count = N_REGIONS × INTERVALS_PER_DAY × days_in_month.
    # AEMO has no DST so any deviation > 1% means real data loss
    # (network gaps, MEASUREMENT outage, MMSDM publication holes) and
    # should fail loud rather than be silently ingested.
    # ------------------------------------------------------------------
    days = calendar.monthrange(year, month)[1]
    expected = N_REGIONS * INTERVALS_PER_DAY * days
    actual = len(df)
    pct_missing = 100 * (expected - actual) / expected
    if abs(pct_missing) > 1.0:
        raise RuntimeError(
            f"{year}-{month:02d}: row count {actual:,} vs expected {expected:,} "
            f"({pct_missing:+.2f}%). Threshold |1%| exceeded. "
            f"Investigate before proceeding (do NOT just rerun with --force). "
            f"Pre-filter rows: {rows_raw:,}."
        )

    df["INTERVAL_DATETIME"] = pd.to_datetime(df["INTERVAL_DATETIME"])
    df.to_parquet(out, compression="snappy", index=False)
    size_mb = out.stat().st_size / 1024 / 1024

    return {
        "download_s": download_s,
        "rows_raw": rows_raw,
        "rows": actual,
        "expected": expected,
        "pct_missing": pct_missing,
        "size_mb": size_mb,
    }


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default=DEFAULT_START, help=f"YYYY-MM (default: {DEFAULT_START})")
    ap.add_argument("--end", default=DEFAULT_END, help=f"YYYY-MM inclusive (default: {DEFAULT_END})")
    ap.add_argument("--month", help="Single month YYYY-MM (overrides --start/--end)")
    ap.add_argument(
        "--force",
        action="store_true",
        help="Re-download and overwrite existing parquet files",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    RAW_CACHE.mkdir(parents=True, exist_ok=True)
    PARQUET_OUT.mkdir(parents=True, exist_ok=True)

    try:
        from nemosis import dynamic_data_compiler
    except ImportError as e:
        print(f"FAIL: nemosis not installed. ({e})")
        return 1

    if args.month:
        start = end = args.month
    else:
        start, end = args.start, args.end

    months = list(iter_months(start, end))
    print(f"Target window: {start} → {end}  ({len(months)} months)")
    print(f"Output dir:    {PARQUET_OUT}")
    print(f"Force:         {args.force}\n")

    skipped = 0
    succeeded = 0
    failed: list[tuple[int, int, str]] = []

    pbar = tqdm(months, unit="mo")
    for y, m in pbar:
        out = parquet_path(y, m)
        tag = f"{y:04d}-{m:02d}"
        pbar.set_description(tag)

        if out.exists() and not args.force:
            skipped += 1
            pbar.write(f"[{tag}] SKIP (parquet exists, {out.stat().st_size / 1e6:.1f} MB)")
            continue

        try:
            stats = fetch_one_month(y, m, dynamic_data_compiler)
        except Exception as e:
            failed.append((y, m, f"{type(e).__name__}: {e}"))
            pbar.write(f"[{tag}] FAIL — {type(e).__name__}: {e}")
            continue

        succeeded += 1
        msg = (
            f"[{tag}] {stats['download_s']:5.1f}s  "
            f"{stats['rows']:>5,} rows  "
            f"({stats['pct_missing']:+.2f}%)  "
            f"{stats['size_mb']:.1f} MB"
        )
        pbar.write(msg)

    pbar.close()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print(f"Complete. {succeeded} fetched, {skipped} skipped, {len(failed)} failed.")
    if failed:
        print("\nFailed months:")
        for y, m, err in failed:
            print(f"  {y}-{m:02d}: {err}")
        print("\nRe-run the same command — successful months will be skipped.")
        return 1

    if succeeded:
        all_files = sorted(PARQUET_OUT.glob("rooftop_30min_*.parquet"))
        total_mb = sum(f.stat().st_size for f in all_files) / 1024 / 1024
        print(f"\nTotal parquet on disk: {len(all_files)} files, {total_mb:.0f} MB")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
