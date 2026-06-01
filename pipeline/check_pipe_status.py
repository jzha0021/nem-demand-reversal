"""
Snowpipe health check — used as the last step of .github/workflows/daily.yml.

Asserts, for each of NEM_PIPE_DISPATCH / NEM_PIPE_ROOFTOP / NEM_PIPE_WEATHER:
  * executionState == 'RUNNING'
  * lastIngestedTimestamp is within --max-stale-hours (default 26)

Exit code is 0 only when every pipe is healthy; non-zero causes the daily
CI run to fail red, surfacing a stale ingestion through GitHub's email /
PR-check notifications. The 26-hour default tolerates the normal 24-hour
cron cadence plus S3 event propagation jitter; a single missed cron run
pushes staleness toward ~48 h and trips this gate, which is the intent.

Usage
-----
    python pipeline/check_pipe_status.py
    python pipeline/check_pipe_status.py --max-stale-hours 48   # post-holiday tolerance
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
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
        sys.exit(f"ERROR: missing SNOWFLAKE_{'/'.join(m.upper() for m in missing)} "
                 f"in env (.env or shell) — see .env.example for the contract")
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
    ap = argparse.ArgumentParser(description="Snowflake Snowpipe health check.")
    ap.add_argument("--max-stale-hours", type=float, default=26.0,
                    help="Max gap between now and pipe's lastIngestedTimestamp "
                         "before failing (default 26h — normal 24h cadence + 2h "
                         "jitter; one missed daily run trips the gate).")
    args = ap.parse_args()
    max_stale = timedelta(hours=args.max_stale_hours)

    engine = get_engine()
    now = datetime.now(timezone.utc)
    failures: list[str] = []
    try:
        with engine.connect() as conn:
            for pipe in PIPES:
                row = conn.execute(text(
                    f"SELECT SYSTEM$PIPE_STATUS('{pipe}')"
                )).fetchone()
                status = json.loads(row[0])
                state = status.get("executionState", "?")
                last_ts_str = status.get("lastIngestedTimestamp", "") or ""
                if last_ts_str:
                    last_ts = datetime.fromisoformat(
                        last_ts_str.replace("Z", "+00:00")
                    )
                    stale_for = now - last_ts
                else:
                    last_ts = None
                    stale_for = timedelta(days=99999)

                healthy = state == "RUNNING" and stale_for < max_stale
                mark = "OK" if healthy else "FAIL"
                stale_h = stale_for.total_seconds() / 3600
                print(
                    f"[{mark}] {pipe:<32} state={state:<8} "
                    f"last_ingest={last_ts_str or '(never)'}  "
                    f"stale_for={stale_h:.1f}h"
                )
                if not healthy:
                    failures.append(
                        f"{pipe}: state={state}, stale_for={stale_h:.1f}h"
                    )
    finally:
        engine.dispose()

    if failures:
        print("\nUnhealthy pipes:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nAll 3 Snowpipes healthy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
