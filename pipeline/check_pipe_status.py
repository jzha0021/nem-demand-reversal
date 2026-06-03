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
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "pipeline"))
from _common import get_snowflake_engine  # noqa: E402

PIPES = [
    "NEM.RAW.NEM_PIPE_DISPATCH",
    "NEM.RAW.NEM_PIPE_ROOFTOP",
    "NEM.RAW.NEM_PIPE_WEATHER",
]


def get_engine():
    load_dotenv(PROJECT_ROOT / ".env")
    return get_snowflake_engine(schema="RAW")


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
