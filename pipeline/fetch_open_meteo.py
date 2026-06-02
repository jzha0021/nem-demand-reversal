"""
Production fetcher: daily weather per NEM region capital, via Open-Meteo Archive API.

Strategy
--------
- One parquet per region (5 files total), NOT chunked monthly.
  Open-Meteo Archive supports a single full-window request (~1339 days,
  ~30 KB CSV per region), so monthly chunking would 5× the request count
  for no benefit.
- Date-range-aware idempotency (cron-friendly):
    * existing parquet fully covers requested window → skip
    * existing parquet covers requested start but not the tail → fetch
      only the gap [stored_max+1d, req_end], append, rewrite parquet
    * existing parquet's start ≠ requested start → full refetch (rare;
      window start is locked at 2022-08-01 by design)
    * --force always full-refetches and overwrites
- Hard sanity gate on freshly fetched data (matches the 1% tolerance
  used by fetch_aemo.py / fetch_rooftop.py):
    * row count must equal expected days exactly for the FETCHED range
      (Open-Meteo never gaps historical windows under ERA5 backfill —
      any mismatch is structural)
    * each daily variable must have ≥99% non-null coverage on the
      fetched range
- All 5 regions queried with timezone='Australia/Brisbane' (AEST, no DST)
  so daily aggregation windows align with NEM trading_day. Per-city tz
  would shift Adelaide / Melbourne / Sydney / Hobart day boundaries by
  ±30 min / ±1 h around DST transitions and year-round, polluting
  downstream weather joins — keep the unified tz.

ERA5 publication latency
------------------------
Open-Meteo Archive's ERA5 backend has ~5-day nominal latency but in
practice publishes within ~1 day of the trading day closing. The
default --end is today − 1 to reflect that empirical lag, which is
the freshness predict.py needs (model features include D-1 weather).
If ERA5 lags further on a given day the 99 % coverage gate trips,
the fetcher exits non-zero, and the daily cron fails red so the
operator notices — silent regression past D-1 weather would let
predict.py drop today's row.

For catch-up runs where the operator is willing to accept staler
weather (historical backfill, model retrain experiments), pass
--max-walkback N to retry with progressively older --end values up
to N days back. The daily production cron does NOT pass this flag.

Output
------
data/parquet/weather_daily/weather_daily_<REGIONID>.parquet  (one per region)

Usage
-----
    python pipeline/fetch_open_meteo.py                     # default end = today − 1d
    python pipeline/fetch_open_meteo.py --region SA1
    python pipeline/fetch_open_meteo.py --end 2026-05-27    # explicit catch-up tail
    python pipeline/fetch_open_meteo.py --max-walkback 7    # accept up to today − 8d on coverage miss
    python pipeline/fetch_open_meteo.py --force             # re-download all

"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

from _common import WEATHER_COLS, make_nemweb_session, s3_enabled, upload_parquet_to_s3

# Module-level retry-enabled session reused across all region fetches.
# Open-Meteo Archive occasionally hands out a slow SSL handshake or a
# 5xx; the urllib3 Retry adapter (5 attempts, exponential backoff,
# status_forcelist covers 5xx + 429) auto-retries so transient upstream
# blips don't fail the daily cron. The session is named "nemweb" by
# the helper for historical reasons — the retry config is generic.
_session = None


def _get_session():
    global _session
    if _session is None:
        _session = make_nemweb_session()
    return _session

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARQUET_OUT = PROJECT_ROOT / "data" / "parquet" / "weather_daily"

ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"

# Project window start — must match fetch_aemo.py / fetch_rooftop.py.
DEFAULT_START = "2022-08-01"


def _default_end() -> str:
    """Dynamic default for --end: today AEST minus 1 day.

    AEST-anchored (Australia/Brisbane, no DST) so behaviour is
    identical on local dev machines and UTC CI runners. Plain
    date.today() runs 10 h behind AEST on GitHub Actions, which
    silently shifts end to D-2 around the 01:00 AEST cron — weather
    for D-1 then never lands and predict.py drops the D row in
    dropna() with the misleading "no rows with complete features"
    exit.
    """
    import datetime as _dt
    today_aest = (_dt.datetime.utcnow() + _dt.timedelta(hours=10)).date()
    return (today_aest - timedelta(days=1)).strftime("%Y-%m-%d")

# Unified tz for all 5 regions so daily aggregation windows align with
# NEM trading_day. See module docstring for rationale.
_TZ = "Australia/Brisbane"

# Region capital → coordinates (population centre as weather proxy for the
# region's operational demand).
REGIONS: dict[str, dict] = {
    "NSW1": {"city": "Sydney",    "lat": -33.869, "lon": 151.209, "tz": _TZ},
    "QLD1": {"city": "Brisbane",  "lat": -27.470, "lon": 153.025, "tz": _TZ},
    "SA1":  {"city": "Adelaide",  "lat": -34.928, "lon": 138.600, "tz": _TZ},
    "TAS1": {"city": "Hobart",    "lat": -42.882, "lon": 147.327, "tz": _TZ},
    "VIC1": {"city": "Melbourne", "lat": -37.814, "lon": 144.963, "tz": _TZ},
}

# Open-Meteo daily variable name → schema column name.
# Order here also defines column order in the parquet (after `date` and
# `regionid` are prepended), which must match WEATHER_COLS.
API_TO_COL: dict[str, str] = {
    "temperature_2m_max":      "t_max_c",
    "temperature_2m_min":      "t_min_c",
    "shortwave_radiation_sum": "solar_mj_m2",
    "sunshine_duration":       "sunshine_seconds",
    "precipitation_sum":       "precip_mm",
}

# Coverage threshold: any variable below this is a hard fail. Matches the
# 1% tolerance used by fetch_aemo.py / fetch_rooftop.py.
COVERAGE_MIN_PCT = 99.0


# -----------------------------------------------------------------------------
# Per-region fetch
# -----------------------------------------------------------------------------
def parquet_path(regionid: str) -> Path:
    return PARQUET_OUT / f"weather_daily_{regionid}.parquet"


def _api_fetch(regionid: str, start: str, end: str) -> tuple[pd.DataFrame, float, dict]:
    """Hit Open-Meteo for [start, end] inclusive, return (normalised df, download_s, coverage_dict).

    Validates: schema, row count exactly equals expected day count, every
    variable ≥ COVERAGE_MIN_PCT non-null. Raises on any failure.
    Returns df with WEATHER_COLS column order + types ready for parquet.
    """
    cfg = REGIONS[regionid]
    params = {
        "latitude":   cfg["lat"],
        "longitude":  cfg["lon"],
        "start_date": start,
        "end_date":   end,
        "daily":      ",".join(API_TO_COL.keys()),
        "timezone":   cfg["tz"],
    }

    t0 = time.time()
    r = _get_session().get(ENDPOINT, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()
    download_s = time.time() - t0

    if "daily" not in payload:
        raise RuntimeError(
            f"{regionid}: response missing 'daily' key. "
            f"Response keys: {list(payload.keys())}. "
            f"Body head: {str(payload)[:300]}"
        )

    df = pd.DataFrame(payload["daily"])

    missing_api = [c for c in API_TO_COL if c not in df.columns]
    if missing_api or "time" not in df.columns:
        raise RuntimeError(
            f"{regionid}: API response missing expected daily fields. "
            f"Missing: {missing_api}, has time={'time' in df.columns}. "
            f"Got columns: {df.columns.tolist()}"
        )

    expected_days = (pd.to_datetime(end) - pd.to_datetime(start)).days + 1
    if len(df) != expected_days:
        raise RuntimeError(
            f"{regionid}: fetched row count {len(df):,} vs expected "
            f"{expected_days:,} ({len(df) - expected_days:+,} rows) for "
            f"range {start}→{end}. Open-Meteo Archive should return exact "
            f"day count for any historical window — investigate."
        )

    coverage = {
        api_col: df[api_col].notna().mean() * 100
        for api_col in API_TO_COL
    }
    bad = {k: v for k, v in coverage.items() if v < COVERAGE_MIN_PCT}
    if bad:
        details = ", ".join(f"{k}={v:.2f}%" for k, v in bad.items())
        raise RuntimeError(
            f"{regionid}: coverage below {COVERAGE_MIN_PCT}% threshold for "
            f"range {start}→{end} on {details}. ERA5 publication latency "
            f"may be the cause. The daily cron's contract is today−1; "
            f"for catch-up runs pass --max-walkback N to retry with "
            f"progressively older end-dates."
        )

    df = df.rename(columns={"time": "date", **API_TO_COL})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["regionid"] = regionid
    # sunshine_duration is float seconds in the API; schema column is integer.
    df["sunshine_seconds"] = df["sunshine_seconds"].round().astype("Int64")
    df = df[WEATHER_COLS]

    return df, download_s, coverage


def fetch_one_region(regionid: str, start: str, end: str,
                     force: bool, use_parquet_s3: bool = False) -> dict:
    """Date-range-aware fetch for one region. See module docstring for strategy.

    Returns dict with status='full'|'append'|'skip' and per-status stats.
    When ``use_parquet_s3`` is True, the newly-fetched rows are also
    uploaded to s3://.../parsed/weather/ for Snowpipe to ingest.
    """
    out = parquet_path(regionid)
    req_start_d = pd.to_datetime(start).date()
    req_end_d = pd.to_datetime(end).date()

    existing_df: pd.DataFrame | None = None
    if out.exists() and not force:
        existing_df = pd.read_parquet(out)
        if existing_df.empty:
            existing_df = None

    # ------------------------------------------------------------------
    # Decide what to do based on coverage of the existing parquet
    # ------------------------------------------------------------------
    if existing_df is not None:
        stored_min = existing_df["date"].min()
        stored_max = existing_df["date"].max()

        if stored_min == req_start_d and stored_max >= req_end_d:
            return {
                "status":      "skip",
                "stored_rows": len(existing_df),
                "stored_min":  str(stored_min),
                "stored_max":  str(stored_max),
                "size_kb":     out.stat().st_size / 1024,
            }

        if stored_min == req_start_d and stored_max < req_end_d:
            # Append-tail path: fetch [stored_max + 1d, req_end] only.
            gap_start = (pd.Timestamp(stored_max) + pd.Timedelta(days=1)
                         ).strftime("%Y-%m-%d")
            new_df, download_s, coverage = _api_fetch(
                regionid, gap_start, end
            )
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            # Defensive: PK is (date, regionid); drop dupes if any (shouldn't be).
            combined = combined.drop_duplicates(
                subset=["date", "regionid"], keep="last"
            ).sort_values("date").reset_index(drop=True)
            combined.to_parquet(out, compression="snappy", index=False)
            s3_key = ""
            if use_parquet_s3:
                s3_key, _ = upload_parquet_to_s3(new_df, source="weather")
            return {
                "status":           "append",
                "download_s":       download_s,
                "rows_added":       len(new_df),
                "rows_total":       len(combined),
                "appended_range":   (gap_start, end),
                "coverage_min_pct": min(coverage.values()),
                "size_kb":          out.stat().st_size / 1024,
                "s3_parquet_key":   s3_key,
            }

        # Start moved (or stored data is wider than requested): refetch full.
        # Fall through to the full-fetch branch below.

    # ------------------------------------------------------------------
    # Full fetch path: --force, no existing file, or start mismatch
    # ------------------------------------------------------------------
    df, download_s, coverage = _api_fetch(regionid, start, end)
    df.to_parquet(out, compression="snappy", index=False)
    s3_key = ""
    if use_parquet_s3:
        s3_key, _ = upload_parquet_to_s3(df, source="weather")
    return {
        "status":           "full",
        "download_s":       download_s,
        "rows_total":       len(df),
        "coverage_min_pct": min(coverage.values()),
        "size_kb":          out.stat().st_size / 1024,
        "s3_parquet_key":   s3_key,
    }


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--start", default=DEFAULT_START,
        help=f"YYYY-MM-DD (default: {DEFAULT_START})",
    )
    ap.add_argument(
        "--end", default=None,
        help="YYYY-MM-DD inclusive (default: today minus 1 day; the "
             "freshness predict.py's D-1 weather features require)",
    )
    ap.add_argument(
        "--max-walkback", type=int, default=0,
        help="On coverage-gate failure, retry with progressively older "
             "--end values up to N days back. Default 0 — daily cron "
             "wants live freshness or to fail red. Set 7+ for "
             "operator-driven catch-up runs.",
    )
    ap.add_argument(
        "--region",
        choices=sorted(REGIONS.keys()),
        help="Single REGIONID (default: all 5)",
    )
    ap.add_argument(
        "--force", action="store_true",
        help="Re-download and overwrite existing parquet files",
    )
    ap.add_argument(
        "--no-parquet", action="store_true",
        help="Skip uploading the per-region delta parquet to "
             "s3://.../parsed/weather/ (default: upload if S3_BUCKET is set).",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    end = args.end or _default_end()

    PARQUET_OUT.mkdir(parents=True, exist_ok=True)

    if args.region:
        regions = [args.region]
    else:
        regions = sorted(REGIONS.keys())

    use_parquet_s3 = (not args.no_parquet) and s3_enabled()

    print(f"Target window: {args.start} → {end}")
    print(f"Regions:       {regions}")
    print(f"Output dir:    {PARQUET_OUT}")
    print(f"Force:         {args.force}")
    print(f"S3 parquet:    {'on' if use_parquet_s3 else 'off'}\n")

    skipped = appended = full = 0
    failed: list[tuple[str, str]] = []

    pbar = tqdm(regions, unit="reg")
    for regionid in pbar:
        pbar.set_description(regionid)
        # Walk-back loop: --max-walkback controls how far back to retry on
        # an ERA5 coverage-gate trip. Default 0 means daily cron either
        # gets today-1 weather or fails red so predict.py for today
        # doesn't silently drop the row (D-1 weather features required).
        # Operators running a manual catch-up can pass --max-walkback 7+.
        try_end = end
        stats: dict | None = None
        last_err: Exception | None = None
        for attempt in range(args.max_walkback + 1):
            try:
                stats = fetch_one_region(regionid, args.start, try_end, args.force,
                                         use_parquet_s3=use_parquet_s3)
                if attempt > 0:
                    pbar.write(f"[{regionid}] WARN walked back {attempt} day(s) "
                               f"to end={try_end}; live inference for the "
                               f"untouched tail will lack D-1 weather")
                break
            except RuntimeError as e:
                # Only walk back on the ERA5 coverage-gate failure;
                # any other RuntimeError (schema, network) propagates.
                if "coverage below" not in str(e):
                    raise
                last_err = e
                try_end = (pd.Timestamp(try_end) - pd.Timedelta(days=1)
                           ).strftime("%Y-%m-%d")
        if stats is None:
            failed.append((regionid, f"COVERAGE_GATE: {last_err}"))
            pbar.write(f"[{regionid}] FAIL — coverage gate (max-walkback "
                       f"{args.max_walkback}): {last_err}")
            continue

        if stats["status"] == "skip":
            skipped += 1
            pbar.write(
                f"[{regionid}] SKIP ({stats['stored_rows']:,} rows in parquet "
                f"cover {stats['stored_min']}→{stats['stored_max']}; "
                f"{stats['size_kb']:.0f} KB)"
            )
        elif stats["status"] == "append":
            appended += 1
            g0, g1 = stats["appended_range"]
            pbar.write(
                f"[{regionid}] APPEND {stats['download_s']:5.1f}s  "
                f"+{stats['rows_added']:,} rows ({g0}→{g1})  "
                f"now {stats['rows_total']:,} total  "
                f"min cov {stats['coverage_min_pct']:.2f}%  "
                f"{stats['size_kb']:.0f} KB"
            )
        else:  # 'full'
            full += 1
            pbar.write(
                f"[{regionid}] FULL  {stats['download_s']:5.1f}s  "
                f"{stats['rows_total']:>5,} rows  "
                f"min cov {stats['coverage_min_pct']:.2f}%  "
                f"{stats['size_kb']:.0f} KB"
            )

    pbar.close()

    print("\n" + "=" * 72)
    print(
        f"Complete. {full} full, {appended} appended, "
        f"{skipped} skipped, {len(failed)} failed."
    )
    if failed:
        print("\nFailed regions:")
        for r, err in failed:
            print(f"  {r}: {err}")
        print("\nRe-run the same command — successful regions will be skipped.")
        return 1

    if full or appended or skipped:
        all_files = sorted(PARQUET_OUT.glob("weather_daily_*.parquet"))
        total_kb = sum(f.stat().st_size for f in all_files) / 1024
        print(f"\nTotal parquet on disk: {len(all_files)} files, {total_kb:.0f} KB")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
