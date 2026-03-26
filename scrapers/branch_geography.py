"""
Branch Geography — Phase 1 of Deposit Ranking Report
------------------------------------------------------
Pulls FDIC branch-level location data and stores it in a
`branch_markets` table in rates.db.

Each row = one branch of one FDIC-insured institution,
with city + state as the market identifier.

Usage:
    python3 branch_geography.py                 # load all US branches (~78k)
    python3 branch_geography.py --state MD      # load one state only
    python3 branch_geography.py --stats         # show table stats
    python3 branch_geography.py --markets MD    # list markets (cities) in a state
    python3 branch_geography.py --peers "Baltimore" MD  # list peers in a market
"""

import argparse
import sqlite3
import time
import sys
import os

import requests

sys.path.insert(0, os.path.dirname(__file__))
from schema import get_conn

FDIC_API = "https://api.fdic.gov/banks/locations"
PAGE_SIZE = 1000


# ── Schema ────────────────────────────────────────────────────────────────────

def init_branch_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS branch_markets (
            id          TEXT PRIMARY KEY,   -- FDIC location ID
            cert        TEXT NOT NULL,      -- FDIC cert number → maps to institutions.id='fdic:{cert}'
            inst_name   TEXT,               -- institution name (denormalized for easy lookup)
            city        TEXT NOT NULL,
            state       TEXT NOT NULL,      -- 2-letter STALP
            state_name  TEXT,
            zip         TEXT,
            latitude    REAL,
            longitude   REAL,
            market_key  TEXT NOT NULL,      -- '{city}|{state}' normalized lowercase
            loaded_at   TEXT NOT NULL       -- ISO timestamp of import
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bm_market ON branch_markets(market_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bm_cert   ON branch_markets(cert)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bm_state  ON branch_markets(state)")
    conn.commit()


# ── FDIC Fetcher ──────────────────────────────────────────────────────────────

def fetch_branches(state: str = None, offset: int = 0) -> dict:
    params = {
        "fields": "CERT,NAMEFULL,CITY,STNAME,STALP,ZIPBR,LATITUDE,LONGITUDE",
        "limit":  PAGE_SIZE,
        "offset": offset,
        "output": "json",
        "sort_by": "CERT",
        "sort_order": "ASC",
    }
    if state:
        params["filters"] = f"STALP:{state.upper()}"

    r = requests.get(FDIC_API, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def load_branches(conn: sqlite3.Connection, state: str = None, verbose: bool = True):
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    offset = 0
    total_loaded = 0
    total_skipped = 0

    # Get total count first
    first = fetch_branches(state, offset=0)
    grand_total = first["meta"]["total"]
    if verbose:
        scope = state or "all states"
        print(f"Loading branches for {scope}: {grand_total:,} total")

    # Process first page
    rows = first["data"]
    while True:
        inserts = []
        for row in rows:
            d = row["data"]
            loc_id  = str(d.get("ID", ""))
            cert    = str(d.get("CERT", ""))
            name    = d.get("NAMEFULL", "")
            city    = (d.get("CITY") or "").strip().title()
            state_s = (d.get("STALP") or "").strip().upper()
            sname   = d.get("STNAME", "")
            zipbr   = d.get("ZIPBR", "")
            lat     = d.get("LATITUDE")
            lon     = d.get("LONGITUDE")
            mkey    = f"{city.lower()}|{state_s.lower()}"

            if not (loc_id and cert and city and state_s):
                total_skipped += 1
                continue

            inserts.append((loc_id, cert, name, city, state_s, sname,
                            zipbr, lat, lon, mkey, now))

        conn.executemany("""
            INSERT OR REPLACE INTO branch_markets
            (id, cert, inst_name, city, state, state_name, zip, latitude, longitude, market_key, loaded_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, inserts)
        conn.commit()
        total_loaded += len(inserts)

        offset += PAGE_SIZE
        if offset >= grand_total:
            break

        if verbose:
            pct = min(100, round(offset / grand_total * 100))
            print(f"  {offset:,}/{grand_total:,} ({pct}%)...", end="\r")

        time.sleep(0.2)  # polite rate limiting
        next_page = fetch_branches(state, offset=offset)
        rows = next_page["data"]
        if not rows:
            break

    if verbose:
        print(f"\nDone. Loaded {total_loaded:,} branches, skipped {total_skipped}.")

    return total_loaded


# ── Query helpers ─────────────────────────────────────────────────────────────

def get_market_peers(conn: sqlite3.Connection, city: str, state: str) -> list[dict]:
    """
    Return all FDIC institution certs with a branch in the given city+state market.
    Also tries to join with our institutions table to get names + type (bank/cu).
    """
    mkey = f"{city.strip().lower()}|{state.strip().lower()}"
    rows = conn.execute("""
        SELECT DISTINCT
            bm.cert,
            bm.inst_name,
            i.name    AS db_name,
            i.type    AS inst_type,
            i.assets_k,
            i.id      AS institution_id
        FROM branch_markets bm
        LEFT JOIN institutions i ON i.id = 'fdic:' || bm.cert
        WHERE bm.market_key = ?
        ORDER BY i.assets_k DESC NULLS LAST, bm.inst_name
    """, (mkey,)).fetchall()
    return [dict(r) for r in rows]


def list_markets(conn: sqlite3.Connection, state: str) -> list[tuple]:
    """List all markets (cities) in a state with branch counts."""
    rows = conn.execute("""
        SELECT city, state, COUNT(DISTINCT cert) as institutions, COUNT(*) as branches
        FROM branch_markets
        WHERE UPPER(state) = UPPER(?)
        GROUP BY city, state
        ORDER BY institutions DESC
    """, (state,)).fetchall()
    return rows


def stats(conn: sqlite3.Connection):
    total = conn.execute("SELECT COUNT(*) FROM branch_markets").fetchone()[0]
    states = conn.execute("SELECT COUNT(DISTINCT state) FROM branch_markets").fetchone()[0]
    markets = conn.execute("SELECT COUNT(DISTINCT market_key) FROM branch_markets").fetchone()[0]
    certs = conn.execute("SELECT COUNT(DISTINCT cert) FROM branch_markets").fetchone()[0]
    loaded = conn.execute("SELECT MAX(loaded_at) FROM branch_markets").fetchone()[0]
    print(f"Branch Markets Table")
    print(f"  Branches:     {total:,}")
    print(f"  Institutions: {certs:,} unique certs")
    print(f"  States:       {states}")
    print(f"  Markets:      {markets:,} unique city/state combos")
    print(f"  Last loaded:  {loaded or 'never'}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="FDIC Branch Geography Importer")
    parser.add_argument("--state",   help="Load only this state (e.g. MD)")
    parser.add_argument("--stats",   action="store_true", help="Show table stats")
    parser.add_argument("--markets", metavar="STATE", help="List markets in state")
    parser.add_argument("--peers",   nargs=2, metavar=("CITY", "STATE"),
                        help="List peer institutions in market")
    args = parser.parse_args()

    conn = get_conn()
    init_branch_table(conn)

    if args.stats:
        stats(conn)

    elif args.markets:
        rows = list_markets(conn, args.markets)
        if not rows:
            print(f"No data for {args.markets}. Run without --markets to load first.")
        else:
            print(f"{'City':<30} {'Inst':>6} {'Branches':>8}")
            print("-" * 48)
            for r in rows[:50]:
                print(f"{r[0]:<30} {r[2]:>6} {r[3]:>8}")

    elif args.peers:
        city, state = args.peers
        peers = get_market_peers(conn, city, state)
        if not peers:
            print(f"No peers found for {city}, {state}. Load data first.")
        else:
            print(f"\nPeer institutions in {city.title()}, {state.upper()} ({len(peers)} found):\n")
            print(f"{'Name':<45} {'Type':<6} {'Assets ($M)':>12} {'FDIC Cert':>10}")
            print("-" * 78)
            for p in peers:
                name = p["db_name"] or p["inst_name"] or "Unknown"
                itype = p["inst_type"] or "bank"
                assets = f"${p['assets_k']//1000:,}M" if p["assets_k"] else "-"
                print(f"{name[:44]:<45} {itype:<6} {assets:>12} {p['cert']:>10}")

    else:
        # Load branches
        load_branches(conn, state=args.state)
        print()
        stats(conn)


if __name__ == "__main__":
    main()
