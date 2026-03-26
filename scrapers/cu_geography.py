"""
CU Geography — Phase 2a: Load NCUA credit union main office locations
----------------------------------------------------------------------
Downloads the NCUA federally-insured CU list and stores city+state
in the branch_markets table (same schema as FDIC branch data).

CUs are represented by their main office address — not branch-level
(NCUA doesn't publish branch locations publicly). This is sufficient
for market peer grouping at the city level.

Usage:
    python3 cu_geography.py              # load all CUs
    python3 cu_geography.py --stats      # show stats
"""

import argparse
import io
import os
import sys
import zipfile
from datetime import datetime, timezone

import requests
import openpyxl

sys.path.insert(0, os.path.dirname(__file__))
from schema import get_conn
from branch_geography import init_branch_table, stats

NCUA_ZIP_URL = (
    "https://ncua.gov/files/publications/analysis/"
    "federally-insured-credit-union-list-december-2025.zip"
)


def load_cu_locations(conn, verbose: bool = True) -> int:
    if verbose:
        print("Downloading NCUA federally-insured CU list...")

    r = requests.get(NCUA_ZIP_URL, timeout=60)
    r.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(r.content))
    fname = z.namelist()[0]
    with z.open(fname) as f:
        wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))

    headers = rows[0]
    col = {h: i for i, h in enumerate(headers) if h}

    # Map column names (headers have newlines)
    def find_col(keyword):
        for h, i in col.items():
            if h and keyword.lower() in str(h).lower():
                return i
        return None

    c_charter = find_col("charter number")
    c_name    = find_col("credit union name")
    c_city    = find_col("city")
    c_state   = find_col("state")
    c_zip     = find_col("zip")
    c_assets  = find_col("total assets")
    c_type    = find_col("credit union type")

    if verbose:
        print(f"  Loaded {len(rows)-1:,} CUs from NCUA file")
        print(f"  Columns: charter={c_charter} name={c_name} city={c_city} state={c_state}")

    now = datetime.now(timezone.utc).isoformat()
    inserts = []
    skipped = 0

    for row in rows[1:]:
        if not row:
            continue

        charter = str(row[c_charter]).strip() if row[c_charter] else ""
        name    = str(row[c_name]).strip()    if row[c_name]    else ""
        city    = str(row[c_city]).strip().title() if row[c_city] else ""
        state   = str(row[c_state]).strip().upper() if row[c_state] else ""
        zipcode = str(row[c_zip]).strip()     if row[c_zip]     else ""
        assets  = row[c_assets]               if c_assets is not None else None

        if not (charter and city and state):
            skipped += 1
            continue

        # Use 'ncua:{charter}' as the cert to match institutions table
        loc_id  = f"ncua:{charter}"
        mkey    = f"{city.lower()}|{state.lower()}"

        inserts.append((
            loc_id,      # id
            charter,     # cert (charter number)
            name,        # inst_name
            city,        # city
            state,       # state
            "",          # state_name
            zipcode,     # zip
            None,        # latitude (not available)
            None,        # longitude
            mkey,        # market_key
            now,         # loaded_at
        ))

    conn.executemany("""
        INSERT OR REPLACE INTO branch_markets
        (id, cert, inst_name, city, state, state_name, zip, latitude, longitude, market_key, loaded_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, inserts)
    conn.commit()

    if verbose:
        print(f"  Inserted {len(inserts):,} CU locations, skipped {skipped}")

    return len(inserts)


def main():
    parser = argparse.ArgumentParser(description="NCUA CU Geography Importer")
    parser.add_argument("--stats", action="store_true", help="Show branch_markets stats")
    args = parser.parse_args()

    conn = get_conn()
    init_branch_table(conn)

    if args.stats:
        stats(conn)
    else:
        load_cu_locations(conn)
        print()
        stats(conn)


if __name__ == "__main__":
    main()
