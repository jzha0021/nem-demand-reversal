"""
A1.2 dry-run: probe NEMWeb CURRENT for DISPATCH + ROOFTOP_PV_ACTUAL.

Read-only HTTP probe — fetches the latest published zip from each candidate
`Reports/Current/*` directory, parses the inner CSV's MMS-format `I` rows
(table headers), and prints columns for comparison against the MMSDM schema
used in pipeline/fetch_aemo.py + pipeline/fetch_rooftop.py.

Background
----------
nemosis 3.8.1 routes DISPATCHREGIONSUM / DISPATCHPRICE / ROOFTOP_PV_ACTUAL
exclusively through MMSDM_Historical_Data_SQLLoader URLs — i.e. monthly
archives published ~10 days after month-end. Daily inference cron cannot
wait that long, so Phase 2 needs a custom CURRENT scraper. This probe
validates the URL paths and confirms that NEMWeb CURRENT publishes the
same MMS schema (column names + ordering) as MMSDM, so the downstream
schema + view layer requires zero changes.

No data is persisted — runs in-memory and exits.

Usage
-----
    python pipeline/probe_nemweb_current.py
"""

from __future__ import annotations

import csv
import io
import sys
import zipfile

import requests
from bs4 import BeautifulSoup

NEMWEB = "https://nemweb.com.au"
USR_AGENT = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) nem_demand_reversal/A1.2-probe"
    )
}

# Candidate CURRENT report directories — best guesses based on AEMO naming
# convention. The probe lists each in turn and stops at the first one that
# returns HTTP 200 with at least one .zip. Wrong guesses fail loud.
DISPATCH_CANDIDATES = [
    "/Reports/Current/DispatchIS_Reports/",
    "/Reports/Current/Dispatch_Reports/",
    "/Reports/Current/Dispatch_SCADA/",
]
ROOFTOP_CANDIDATES = [
    "/Reports/Current/ROOFTOP_PV/ACTUAL/",
    "/Reports/Current/Rooftop_PV/ACTUAL/",
    "/Reports/Current/ROOFTOP_PV_ACTUAL/",
]

# MMSDM columns we need to find in the CURRENT schema (from pipeline/_common.py)
MMSDM_DISPATCH_KEEP = [
    "SETTLEMENTDATE", "REGIONID", "INTERVENTION",
    "TOTALDEMAND", "AVAILABLEGENERATION", "TOTALINTERMITTENTGENERATION",
    "UIGF", "SEMISCHEDULE_CLEAREDMW", "DEMAND_AND_NONSCHEDGEN",
    "NETINTERCHANGE", "RRP",
]
MMSDM_ROOFTOP_KEEP = ["INTERVAL_DATETIME", "REGIONID", "POWER", "QI"]


def list_dir(url: str) -> tuple[int, list[str]]:
    """Return (HTTP status, list of .zip filenames) from an AEMO directory."""
    r = requests.get(url, headers=USR_AGENT, timeout=30)
    if r.status_code != 200:
        return r.status_code, []
    soup = BeautifulSoup(r.text, "html.parser")
    hrefs = [a.get("href") for a in soup.find_all("a") if a.get("href")]
    # Strip any directory prefix from the href and keep only .zip filenames
    zips = [h.rsplit("/", 1)[-1] for h in hrefs if h.lower().endswith(".zip")]
    return 200, zips


def fetch_zip(url: str) -> bytes:
    r = requests.get(url, headers=USR_AGENT, timeout=90)
    r.raise_for_status()
    return r.content


def parse_mms_tables(blob: bytes) -> dict[str, list[str]]:
    """Return {'<PACKAGE>_<TABLE>': [columns...]} for every I-row in the zip.

    AEMO multi-table MMS CSV format:
        C, <comment row, header / footer>
        I, <PACKAGE>, <TABLE>, <VERSION>, <col1>, <col2>, ...    ← table header
        D, <PACKAGE>, <TABLE>, <VERSION>, <val1>, <val2>, ...    ← data rows
        I, <PACKAGE>, <ANOTHER_TABLE>, ...                       ← next table
        C, "END OF REPORT", <row count>
    """
    z = zipfile.ZipFile(io.BytesIO(blob))
    csv_names = [n for n in z.namelist() if n.lower().endswith(".csv")]
    if not csv_names:
        raise RuntimeError(f"no .csv in zip; got {z.namelist()}")
    fname = csv_names[0]
    raw = z.read(fname).decode("utf-8", errors="replace")

    tables: dict[str, list[str]] = {}
    for line in raw.splitlines():
        if not line.startswith("I,"):
            continue
        parts = next(csv.reader([line]))
        if len(parts) < 5:
            continue
        # parts = ['I', package, tablename, version, col1, col2, ...]
        key = f"{parts[1]}_{parts[2]}"
        tables[key] = parts[4:]
    return tables, fname


def probe(label: str, candidates: list[str], expected_cols: list[str],
          interesting_table_keyword: str) -> None:
    """List candidates, download the latest zip from the first that works."""
    print(f"\n{'=' * 76}")
    print(f"{label}")
    print(f"{'=' * 76}")

    chosen_url = None
    zips: list[str] = []
    for path in candidates:
        url = NEMWEB + path
        status, found = list_dir(url)
        print(f"  GET {path:50s}  HTTP {status}  zips={len(found)}")
        if status == 200 and found:
            chosen_url = url
            zips = found
            break

    if not chosen_url:
        print(f"  ! All {len(candidates)} candidate(s) failed for {label}")
        return

    latest = sorted(zips)[-1]
    zip_url = chosen_url + latest
    print(f"\n  picked directory : {chosen_url}")
    print(f"  latest file      : {latest}")
    print(f"  fetching         : {zip_url}")

    try:
        blob = fetch_zip(zip_url)
    except Exception as e:
        print(f"  ! FETCH FAILED: {e}")
        return
    print(f"  zip size         : {len(blob) / 1024:.1f} KiB")

    try:
        tables, inner = parse_mms_tables(blob)
    except Exception as e:
        print(f"  ! PARSE FAILED: {e}")
        return
    print(f"  inner csv        : {inner}")
    print(f"  tables in file   : {len(tables)}  → {list(tables)}")

    keyword = interesting_table_keyword.upper()
    relevant = {k: v for k, v in tables.items() if keyword in k.upper()}
    if not relevant:
        print(f"\n  ! No table key contains '{interesting_table_keyword}'")
        return

    for key, cols in relevant.items():
        present = set(cols)
        missing = [c for c in expected_cols if c not in present]
        extra = [c for c in cols if c not in expected_cols]
        verdict = "OK — all expected cols present" if not missing else f"MISSING {missing}"
        print(f"\n  -- {key} --")
        print(f"     cols ({len(cols)}): {cols}")
        print(f"     expected            {expected_cols}")
        print(f"     verdict             {verdict}")
        if extra:
            shown = ", ".join(extra[:8]) + (" ..." if len(extra) > 8 else "")
            print(f"     extras over MMSDM   ({len(extra)}) {shown}")


def main() -> int:
    probe(
        "DISPATCH (looking for DISPATCHREGIONSUM + DISPATCHPRICE columns)",
        DISPATCH_CANDIDATES,
        MMSDM_DISPATCH_KEEP,
        interesting_table_keyword="DISPATCH",
    )
    probe(
        "ROOFTOP_PV_ACTUAL",
        ROOFTOP_CANDIDATES,
        MMSDM_ROOFTOP_KEEP,
        interesting_table_keyword="ROOFTOP",
    )
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
