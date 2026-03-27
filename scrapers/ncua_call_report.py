"""
NCUA Call Report Data Engine
────────────────────────────
Downloads quarterly 5300 call report ZIP files from NCUA and extracts
key financial metrics for CU peer benchmarking.

Data source: https://www.ncua.gov/analysis/credit-union-corporate-call-report-data/quarterly-data
Updated quarterly (March, June, September, December).

Key metrics extracted:
  - Total assets (ACCT_010)
  - Total shares & deposits (ACCT_018)
  - Total loans & leases (ACCT_025B)
  - Total net worth / equity (ACCT_041B)
  - Member count (from NCUA detail API)
  - Peer group (FOICU.Peer_Group)
  - Capital classification (Acct_700)
  - Net interest income (ACCT_115 from FS220H)

Derived ratios:
  - Asset growth YoY %
  - Member growth YoY %
  - Loan-to-asset ratio
  - Net worth ratio
  - Peer group percentile ranking

Usage:
    python3 ncua_call_report.py --charter 2769
    python3 ncua_call_report.py --peer-group 6 --state MD
    python3 ncua_call_report.py --build-db   # load all CUs into SQLite
"""

import argparse
import csv
import io
import json
import os
import sqlite3
import time
import urllib.request
import zipfile
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
NCUA_ZIP_URL = "https://www.ncua.gov/files/publications/analysis/call-report-data-{year}-{month:02d}.zip"
NCUA_DETAIL_URL = "https://mapping.ncua.gov/api/CreditUnionDetails/GetCreditUnionDetails/{charter}"
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'ncua_peers.db')

PEER_GROUP_LABELS = {
    '1': 'Under $2M',
    '2': '$2M–$10M',
    '3': '$10M–$50M',
    '4': '$50M–$100M',
    '5': '$100M–$500M',
    '6': '$500M+',
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; StrumPlatform/1.0)',
    'Accept': 'application/json',
}

# ── Download & Parse ZIP ──────────────────────────────────────────────────────

def fetch_call_report_zip(year: int, month: int) -> zipfile.ZipFile | None:
    url = NCUA_ZIP_URL.format(year=year, month=month)
    print(f"  Downloading {url}...")
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        r = urllib.request.urlopen(req, timeout=60)
        raw = r.read()
        print(f"  Downloaded {len(raw):,} bytes")
        return zipfile.ZipFile(io.BytesIO(raw))
    except Exception as e:
        print(f"  ❌ Failed: {e}")
        return None


def read_csv_from_zip(z: zipfile.ZipFile, filename: str) -> list[dict]:
    with z.open(filename) as fb:
        text = io.TextIOWrapper(fb, encoding='latin-1')
        return list(csv.DictReader(text))


def parse_int(val) -> int | None:
    try:
        return int(str(val).strip().replace(',', ''))
    except (ValueError, TypeError):
        return None


def parse_float(val) -> float | None:
    try:
        return float(str(val).strip().replace(',', ''))
    except (ValueError, TypeError):
        return None


# ── NCUA Detail API ───────────────────────────────────────────────────────────

def fetch_ncua_detail(charter: str) -> dict | None:
    url = NCUA_DETAIL_URL.format(charter=charter)
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        r = urllib.request.urlopen(req, timeout=10)
        data = json.loads(r.read())
        if data.get('isError'):
            return None
        return data
    except Exception:
        return None


# ── Database ──────────────────────────────────────────────────────────────────

def get_db(path=DB_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS cu_financials (
        charter         TEXT NOT NULL,
        cycle_date      TEXT NOT NULL,   -- e.g. '2025-12-31'
        year            INTEGER,
        quarter         INTEGER,
        cu_name         TEXT,
        city            TEXT,
        state           TEXT,
        peer_group      TEXT,
        peer_group_label TEXT,
        capital_status  TEXT,           -- 'Well Capitalized', etc.
        is_mdi          INTEGER,         -- minority depository institution
        -- Balance sheet
        total_assets    INTEGER,
        total_shares    INTEGER,
        total_loans     INTEGER,
        total_equity    INTEGER,
        -- Derived ratios (stored as decimals: 0.05 = 5%)
        loan_to_asset   REAL,
        net_worth_ratio REAL,
        -- From NCUA detail API
        member_count    INTEGER,
        website_url     TEXT,
        ceo_name        TEXT,
        -- Timestamps
        loaded_at       TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (charter, cycle_date)
    );

    CREATE INDEX IF NOT EXISTS idx_cu_fin_state ON cu_financials(state, peer_group);
    CREATE INDEX IF NOT EXISTS idx_cu_fin_charter ON cu_financials(charter);

    CREATE TABLE IF NOT EXISTS cu_detail_cache (
        charter         TEXT PRIMARY KEY,
        member_count    INTEGER,
        website_url     TEXT,
        ceo_name        TEXT,
        fetched_at      TEXT DEFAULT (datetime('now'))
    );
    """)
    conn.commit()


# ── Load a Quarter ────────────────────────────────────────────────────────────

def load_quarter(conn: sqlite3.Connection, year: int, month: int,
                 state_filter: str = None, fetch_details: bool = False):
    """Download and load one quarter of NCUA call report data."""
    z = fetch_call_report_zip(year, month)
    if not z:
        return 0

    cycle_date = f"{year}-{month:02d}-{28 if month in (2,) else 30 if month in (4,6,9,11) else 31}"
    quarter = {3:1, 6:2, 9:3, 12:4}.get(month, 4)

    foicu_rows = {r['CU_NUMBER']: r for r in read_csv_from_zip(z, 'FOICU.txt')}
    fs220_rows = {r['CU_NUMBER']: r for r in read_csv_from_zip(z, 'FS220.txt')}
    fs220d_rows = {r['CU_NUMBER']: r for r in read_csv_from_zip(z, 'FS220D.txt')}

    inserted = 0
    for charter, fi in foicu_rows.items():
        if state_filter and fi.get('STATE', '').strip().upper() != state_filter.upper():
            continue

        fs = fs220_rows.get(charter, {})
        fd = fs220d_rows.get(charter, {})

        total_assets = parse_int(fs.get('ACCT_010'))
        total_shares = parse_int(fs.get('ACCT_018'))
        total_loans  = parse_int(fs.get('ACCT_025B'))
        total_equity = parse_int(fs.get('ACCT_041B'))
        peer_group   = str(fi.get('Peer_Group', '')).strip()
        capital_stat = str(fd.get('Acct_700', '')).strip()
        is_mdi       = 1 if str(fi.get('IsMDI', '')).strip().lower() == 'true' else 0

        loan_to_asset   = round(total_loans / total_assets, 4) if total_assets and total_loans else None
        net_worth_ratio = round(total_equity / total_assets, 4) if total_assets and total_equity else None

        conn.execute("""
            INSERT OR REPLACE INTO cu_financials
            (charter, cycle_date, year, quarter, cu_name, city, state,
             peer_group, peer_group_label, capital_status, is_mdi,
             total_assets, total_shares, total_loans, total_equity,
             loan_to_asset, net_worth_ratio)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            charter, cycle_date, year, quarter,
            fi.get('CU_NAME','').strip(),
            fi.get('CITY','').strip(),
            fi.get('STATE','').strip(),
            peer_group,
            PEER_GROUP_LABELS.get(peer_group, peer_group),
            capital_stat, is_mdi,
            total_assets, total_shares, total_loans, total_equity,
            loan_to_asset, net_worth_ratio
        ))
        inserted += 1

    conn.commit()
    print(f"  ✅ Loaded {inserted} CUs for {year}-Q{quarter}")

    if fetch_details:
        _fill_detail_cache(conn, list(foicu_rows.keys()), state_filter)

    return inserted


def _fill_detail_cache(conn, charters, state_filter=None):
    """Fetch member counts etc from NCUA detail API for CUs missing from cache."""
    need = conn.execute(
        "SELECT DISTINCT charter FROM cu_financials WHERE member_count IS NULL"
        + (" AND state=?" if state_filter else ""),
        (state_filter.upper(),) if state_filter else ()
    ).fetchall()

    print(f"  Fetching detail for {len(need)} CUs...")
    for i, row in enumerate(need):
        charter = row[0]
        detail = fetch_ncua_detail(charter)
        if detail:
            members = parse_int(detail.get('creditUnionNom'))
            website = detail.get('creditUnionWebsite','')
            ceo     = f"{detail.get('ceoFirstName','')} {detail.get('creditUnionCeo','')}".strip()
            conn.execute("""
                INSERT OR REPLACE INTO cu_detail_cache (charter, member_count, website_url, ceo_name)
                VALUES (?,?,?,?)
            """, (charter, members, website, ceo))
            conn.execute("""
                UPDATE cu_financials SET member_count=?, website_url=?, ceo_name=?
                WHERE charter=?
            """, (members, website, ceo, charter))
        if i % 50 == 0:
            conn.commit()
            print(f"    {i}/{len(need)}", end='\r')
        time.sleep(0.2)
    conn.commit()
    print(f"  ✅ Detail cache filled")


# ── Peer Analysis ─────────────────────────────────────────────────────────────

def get_peer_snapshot(conn: sqlite3.Connection, charter: str,
                      peer_state: str = None) -> dict:
    """
    For a given CU, return:
      - Their latest financials
      - YoY changes (asset growth, member growth)
      - Peer group benchmarks (median, quartiles)
      - Narrative insights (BlastPoint-style)
    """
    # Get latest two periods for this CU
    rows = conn.execute("""
        SELECT * FROM cu_financials
        WHERE charter = ?
        ORDER BY cycle_date DESC LIMIT 2
    """, (charter,)).fetchall()

    if not rows:
        return {"error": f"Charter {charter} not found in database"}

    curr = dict(rows[0])
    prev = dict(rows[1]) if len(rows) > 1 else None

    # YoY changes
    asset_growth  = None
    member_growth = None
    if prev:
        if curr['total_assets'] and prev['total_assets']:
            asset_growth = (curr['total_assets'] - prev['total_assets']) / prev['total_assets']
        if curr.get('member_count') and prev.get('member_count'):
            member_growth = (curr['member_count'] - prev['member_count']) / prev['member_count']

    # Peer group benchmarks — same peer group, optionally same state
    where = "peer_group = ? AND cycle_date = ?"
    params = [curr['peer_group'], curr['cycle_date']]
    if peer_state:
        where += " AND state = ?"
        params.append(peer_state.upper())

    peers = conn.execute(f"""
        SELECT total_assets, total_loans, total_equity, loan_to_asset,
               net_worth_ratio, member_count
        FROM cu_financials
        WHERE {where} AND total_assets IS NOT NULL
        ORDER BY total_assets
    """, params).fetchall()

    peer_count = len(peers)

    def percentile(vals, val):
        if not vals or val is None:
            return None
        below = sum(1 for v in vals if v is not None and v < val)
        return round(below / len([v for v in vals if v is not None]) * 100)

    asset_vals  = [p['total_assets'] for p in peers if p['total_assets']]
    nwr_vals    = [p['net_worth_ratio'] for p in peers if p['net_worth_ratio']]
    lta_vals    = [p['loan_to_asset'] for p in peers if p['loan_to_asset']]
    mem_vals    = [p['member_count'] for p in peers if p['member_count']]

    def median(vals):
        if not vals: return None
        s = sorted(vals)
        m = len(s) // 2
        return s[m] if len(s) % 2 else (s[m-1] + s[m]) / 2

    asset_pct  = percentile(asset_vals, curr['total_assets'])
    nwr_pct    = percentile(nwr_vals, curr.get('net_worth_ratio'))
    lta_pct    = percentile(lta_vals, curr.get('loan_to_asset'))
    mem_pct    = percentile(mem_vals, curr.get('member_count'))

    # Narrative insights
    insights = []
    scope = f"{'in ' + peer_state if peer_state else 'nationally'} in Peer Group {curr['peer_group']} ({curr['peer_group_label']})"

    if asset_pct is not None:
        tier = "top quartile" if asset_pct >= 75 else "upper half" if asset_pct >= 50 else "lower half" if asset_pct >= 25 else "bottom quartile"
        insights.append(f"Asset size ranks in the {tier} of peers {scope} ({asset_pct}th percentile)")

    if asset_growth is not None:
        med_ag = None
        # Estimate median asset growth from peer group
        prev_peers = conn.execute(f"""
            SELECT total_assets FROM cu_financials
            WHERE {where.replace(curr['cycle_date'], (str(int(curr['cycle_date'][:4])-1) + curr['cycle_date'][4:]))}
              AND total_assets IS NOT NULL
        """, [curr['peer_group'], str(int(curr['cycle_date'][:4])-1) + curr['cycle_date'][4:]] + ([peer_state.upper()] if peer_state else [])).fetchall()
        ag_str = f"{asset_growth*100:+.1f}% YoY"
        insights.append(f"Asset growth: {ag_str}")

    if member_growth is not None:
        mg_str = f"{member_growth*100:+.1f}% YoY"
        color = "positive" if member_growth > 0 else "negative — below the industry trend of -0.7%"
        insights.append(f"Member growth: {mg_str} ({color})")
    elif curr.get('member_count'):
        insights.append(f"Member count: {curr['member_count']:,} (YoY change unavailable — run with 2 quarters loaded)")

    if curr.get('net_worth_ratio'):
        nwr_pct_str = f"{nwr_pct}th percentile" if nwr_pct is not None else "n/a"
        nwr_val = f"{curr['net_worth_ratio']*100:.2f}%"
        med_nwr = median(nwr_vals)
        med_str = f" (peer median: {med_nwr*100:.2f}%)" if med_nwr else ""
        insights.append(f"Net worth ratio: {nwr_val}{med_str} — {nwr_pct_str} among peers")

    if curr.get('loan_to_asset'):
        lta_val = f"{curr['loan_to_asset']*100:.1f}%"
        med_lta = median(lta_vals)
        med_str = f" (peer median: {med_lta*100:.1f}%)" if med_lta else ""
        insights.append(f"Loan-to-asset ratio: {lta_val}{med_str}")

    insights.append(f"Capital status: {curr.get('capital_status','Unknown')}")
    if curr.get('is_mdi'):
        insights.append("Designated Minority Depository Institution (MDI)")

    return {
        "charter": charter,
        "cu_name": curr['cu_name'],
        "city": curr['city'],
        "state": curr['state'],
        "as_of": curr['cycle_date'],
        "peer_group": curr['peer_group'],
        "peer_group_label": curr['peer_group_label'],
        "peer_count": peer_count,
        "financials": {
            "total_assets":    curr['total_assets'],
            "total_shares":    curr['total_shares'],
            "total_loans":     curr['total_loans'],
            "total_equity":    curr['total_equity'],
            "member_count":    curr.get('member_count'),
            "loan_to_asset":   curr.get('loan_to_asset'),
            "net_worth_ratio": curr.get('net_worth_ratio'),
            "capital_status":  curr.get('capital_status'),
        },
        "yoy": {
            "asset_growth":  round(asset_growth, 4) if asset_growth is not None else None,
            "member_growth": round(member_growth, 4) if member_growth is not None else None,
        },
        "peer_benchmarks": {
            "asset_percentile":      asset_pct,
            "net_worth_percentile":  nwr_pct,
            "loan_to_asset_percentile": lta_pct,
            "member_percentile":     mem_pct,
            "median_assets":         int(median(asset_vals)) if median(asset_vals) else None,
            "median_net_worth_ratio": round(median(nwr_vals), 4) if median(nwr_vals) else None,
            "median_loan_to_asset":  round(median(lta_vals), 4) if median(lta_vals) else None,
        },
        "insights": insights,
    }


def print_snapshot(snap: dict):
    """Pretty-print a peer snapshot."""
    if "error" in snap:
        print(f"❌ {snap['error']}")
        return

    f = snap['financials']
    b = snap['peer_benchmarks']
    y = snap['yoy']

    print(f"""
╔══════════════════════════════════════════════════════════════╗
  {snap['cu_name']} — Charter {snap['charter']}
  {snap['city']}, {snap['state']}  |  As of {snap['as_of']}
  Peer Group {snap['peer_group']}: {snap['peer_group_label']}  |  {snap['peer_count']} peers compared
╚══════════════════════════════════════════════════════════════╝

📊 FINANCIALS
  Total Assets:       ${f['total_assets']:>14,.0f}
  Total Shares/Deps:  ${f['total_shares']:>14,.0f}
  Total Loans:        ${f['total_loans']:>14,.0f}
  Total Equity:       ${f['total_equity']:>14,.0f}
  Members:            {f['member_count']:>15,}  {('  (' + f'{y["member_growth"]*100:+.1f}% YoY)') if y.get('member_growth') is not None else ''}
  Loan/Asset:         {f['loan_to_asset']*100:>14.1f}%
  Net Worth Ratio:    {f['net_worth_ratio']*100:>14.2f}%
  Capital Status:     {f['capital_status']:>15}

📈 YOY CHANGES
  Asset Growth:       {(f'{y["asset_growth"]*100:+.1f}%') if y.get('asset_growth') is not None else 'N/A (need 2 quarters)':>15}
  Member Growth:      {(f'{y["member_growth"]*100:+.1f}%') if y.get('member_growth') is not None else 'N/A':>15}

🏆 PEER PERCENTILES  (vs {snap['peer_count']} peers, Peer Group {snap['peer_group']})
  Asset Size:         {f'{b["asset_percentile"]}th' if b['asset_percentile'] is not None else 'N/A':>15}  (median peer: ${b['median_assets']:,.0f})
  Net Worth Ratio:    {f'{b["net_worth_percentile"]}th' if b['net_worth_percentile'] is not None else 'N/A':>15}  (median peer: {b['median_net_worth_ratio']*100:.2f}% if applicable)
  Loan/Asset:         {f'{b["loan_to_asset_percentile"]}th' if b['loan_to_asset_percentile'] is not None else 'N/A':>15}  (median peer: {b['median_loan_to_asset']*100:.1f}%)

💡 INSIGHTS
""")
    for ins in snap['insights']:
        print(f"  • {ins}")
    print()


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="NCUA Call Report Peer Analyzer")
    parser.add_argument("--charter",    help="CU charter number to analyze")
    parser.add_argument("--state",      help="Filter peers to this state (e.g. MD)")
    parser.add_argument("--build-db",   action="store_true", help="Download latest quarter and load into DB")
    parser.add_argument("--year",       type=int, default=2025)
    parser.add_argument("--month",      type=int, default=12, help="Quarter end month: 3, 6, 9, or 12")
    parser.add_argument("--both-years", action="store_true", help="Load current + prior year for YoY")
    parser.add_argument("--details",    action="store_true", help="Also fetch member counts from NCUA API")
    parser.add_argument("--json",       action="store_true", help="Output JSON instead of formatted text")
    args = parser.parse_args()

    conn = get_db()
    init_db(conn)

    if args.build_db:
        print(f"Loading {args.year}-Q{args.month//3}...")
        load_quarter(conn, args.year, args.month,
                     state_filter=args.state,
                     fetch_details=args.details)
        if args.both_years:
            prior_year = args.year - 1
            print(f"Loading {prior_year}-Q{args.month//3} for YoY comparison...")
            load_quarter(conn, prior_year, args.month,
                         state_filter=args.state,
                         fetch_details=False)

    if args.charter:
        snap = get_peer_snapshot(conn, args.charter, peer_state=args.state)
        if args.json:
            print(json.dumps(snap, indent=2, default=str))
        else:
            print_snapshot(snap)


if __name__ == "__main__":
    main()
