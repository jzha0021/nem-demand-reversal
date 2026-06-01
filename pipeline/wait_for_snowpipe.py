"""
Wait until newly-uploaded parquet is queryable in Snowflake:
  1. Poll SYSTEM$PIPE_STATUS until pendingFileCount == 0 for all 3 pipes
  2. THEN poll MAX(settlementdate) / MAX(interval_datetime) / MAX(date)
     in the 3 raw tables until they advance past --min-fresh-date

pendingFileCount alone is a leaky signal: Snowpipe dequeues a file as
soon as it picks it up, but the COPY may still be running internally
for a few more seconds. predict.py downstream queries v_ml_features
(a view over raw tables); if it runs during that gap it sees stale
state, drops the target row in dropna(), and exits "no rows with
complete features". The freshness check closes that race.

Exits 0 when both gates pass, or after --timeout-seconds elapses
(best-effort — downstream check_pipe_status.py still gates on
lastIngestedTimestamp freshness, so silently-stuck pipes still get
caught).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PIPES = [
    "NEM.RAW.NEM_PIPE_DISPATCH",
    "NEM.RAW.NEM_PIPE_ROOFTOP",
    "NEM.RAW.NEM_PIPE_WEATHER",
]
FRESHNESS_PROBES = [
    ("dispatch", "SELECT MAX(settlementdate)::DATE FROM NEM.RAW.REGION_5MIN"),
    ("rooftop",  "SELECT MAX(interval_datetime)::DATE FROM NEM.RAW.ROOFTOP_PV_30MIN"),
    ("weather",  "SELECT MAX(\"DATE\") FROM NEM.RAW.WEATHER_DAILY"),
]


def get_engine():
    from snowflake.sqlalchemy import URL
    load_dotenv(PROJECT_ROOT / ".env")
    required = {
        "account":  os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":     os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        sys.exit(f"ERROR: missing SNOWFLAKE_{'/'.join(m.upper() for m in missing)} in env")
    return create_engine(URL(
        account=required["account"],
        user=required["user"],
        password=required["password"],
        role=os.getenv("SNOWFLAKE_ROLE", "R_NEM_RW"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "WH_NEM"),
        database=os.getenv("SNOWFLAKE_DATABASE", "NEM"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    ))


def _all_pipes_drained(conn, attempt: int) -> bool:
    """Return True iff every pipe reports state=RUNNING + pendingFileCount=0."""
    drained = True
    for pipe in PIPES:
        row = conn.execute(text(f"SELECT SYSTEM$PIPE_STATUS('{pipe}')")).fetchone()
        status = json.loads(row[0])
        pending = status.get("pendingFileCount", 0)
        state = status.get("executionState", "?")
        if pending > 0 or state != "RUNNING":
            drained = False
        print(f"  [drain attempt {attempt}] {pipe}: state={state} "
              f"pendingFileCount={pending}", flush=True)
    return drained


def _all_raws_fresh(conn, min_fresh: date, attempt: int) -> bool:
    """Return True iff every raw table's max timestamp is >= min_fresh."""
    fresh = True
    for label, sql in FRESHNESS_PROBES:
        max_d = conn.execute(text(sql)).scalar()
        is_fresh = (max_d is not None) and (max_d >= min_fresh)
        if not is_fresh:
            fresh = False
        print(f"  [fresh attempt {attempt}] {label}: max={max_d}  "
              f"need ≥ {min_fresh}  {'OK' if is_fresh else 'STALE'}",
              flush=True)
    return fresh


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--timeout-seconds", type=float, default=300.0,
                    help="Max total wait before giving up (default 300s = 5 min).")
    ap.add_argument("--poll-interval", type=float, default=15.0,
                    help="Seconds between polls (default 15s).")
    ap.add_argument("--min-fresh-date", type=str, default=None,
                    help="ISO date that all 3 raw tables' max ts must reach "
                         "(default = today AEST - 1 day, i.e. yesterday).")
    args = ap.parse_args()

    if args.min_fresh_date:
        min_fresh = date.fromisoformat(args.min_fresh_date)
    else:
        # default = D-1 AEST (yesterday in Australia/Brisbane). Open-Meteo
        # Archive's SLA is D-1 and the AEMO fetcher pulls up to "now AEST";
        # all 3 raws should reach this watermark on every cron run.
        import datetime as _dt
        min_fresh = (_dt.datetime.utcnow() + timedelta(hours=10)).date() - timedelta(days=1)

    engine = get_engine()
    deadline = time.time() + args.timeout_seconds
    attempt = 0
    try:
        # Gate 1 — drain Snowpipe queues
        while True:
            attempt += 1
            with engine.connect() as conn:
                if _all_pipes_drained(conn, attempt):
                    print(f"All 3 pipes drained after {attempt} poll(s).")
                    break
            remaining = deadline - time.time()
            if remaining <= 0:
                print(f"Timeout after {args.timeout_seconds}s in pipe-drain "
                      f"gate — proceeding to freshness gate anyway.",
                      flush=True)
                break
            sleep_for = min(args.poll_interval, max(1.0, remaining))
            print(f"  → sleeping {sleep_for:.0f}s "
                  f"({remaining:.0f}s budget remaining)", flush=True)
            time.sleep(sleep_for)

        # Gate 2 — raw tables' max ts must have reached the watermark
        attempt = 0
        while True:
            attempt += 1
            with engine.connect() as conn:
                if _all_raws_fresh(conn, min_fresh, attempt):
                    print(f"All 3 raw tables fresh through {min_fresh} "
                          f"after {attempt} poll(s).")
                    return 0
            remaining = deadline - time.time()
            if remaining <= 0:
                print(f"Timeout in freshness gate — proceeding anyway "
                      f"(downstream predict.py may fail if D-1 row missing).",
                      flush=True)
                return 0
            sleep_for = min(args.poll_interval, max(1.0, remaining))
            print(f"  → sleeping {sleep_for:.0f}s "
                  f"({remaining:.0f}s budget remaining)", flush=True)
            time.sleep(sleep_for)
    finally:
        engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
