"""
Poll SYSTEM$PIPE_STATUS for all 3 Snowpipes until pendingFileCount == 0
across the board, or until --timeout-seconds (default 300) elapses.

The daily cron uploads ~3 parquet files (dispatch + rooftop + weather)
each run; Snowpipe drains them within ~30-60 seconds when the queue is
quiet, but can lag minutes when a backfill or earlier failed pipes left
files behind. Sleeping a fixed 60 s racy — predict.py downstream
expects v_ml_features to include D-1 rows that the latest cron's
parquet just delivered, and a too-short sleep makes it drop the
target row in dropna().

Exits 0 when all 3 pipes report pendingFileCount == 0, or after the
timeout elapses (best-effort — downstream check_pipe_status.py still
gates on lastIngestedTimestamp freshness, so silently-stuck pipes
still get caught).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PIPES = [
    "NEM.RAW.NEM_PIPE_DISPATCH",
    "NEM.RAW.NEM_PIPE_ROOFTOP",
    "NEM.RAW.NEM_PIPE_WEATHER",
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--timeout-seconds", type=float, default=300.0,
                    help="Max total wait before giving up (default 300s = 5 min).")
    ap.add_argument("--poll-interval", type=float, default=15.0,
                    help="Seconds between SYSTEM$PIPE_STATUS calls (default 15s).")
    args = ap.parse_args()

    engine = get_engine()
    deadline = time.time() + args.timeout_seconds
    attempt = 0
    try:
        while True:
            attempt += 1
            all_drained = True
            with engine.connect() as conn:
                for pipe in PIPES:
                    row = conn.execute(text(
                        f"SELECT SYSTEM$PIPE_STATUS('{pipe}')"
                    )).fetchone()
                    status = json.loads(row[0])
                    pending = status.get("pendingFileCount", 0)
                    state = status.get("executionState", "?")
                    if pending > 0 or state != "RUNNING":
                        all_drained = False
                    print(f"  [attempt {attempt}] {pipe}: state={state} "
                          f"pendingFileCount={pending}", flush=True)
            if all_drained:
                print(f"All 3 pipes drained after {attempt} poll(s).")
                return 0
            remaining = deadline - time.time()
            if remaining <= 0:
                print(f"Timeout after {args.timeout_seconds}s with files still "
                      f"pending — proceeding anyway (downstream "
                      f"check_pipe_status.py will catch stuck pipes).",
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
