"""
Peer Group Builder — Phase 2b: Build market peer sets for Deposit Ranking Report
----------------------------------------------------------------------------------
Given a client institution + market (city, state), returns all peer
institutions (banks + CUs) in that market with their rates.

This is the core query engine for the Deposit Ranking Report.

Usage:
    python3 peer_group.py --market "Baltimore" MD
    python3 peer_group.py --market "Baltimore" MD --product cd --term 12
    python3 peer_group.py --client "Securityplus FCU" --market "Baltimore" MD
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from schema import get_conn


def get_peers(conn, city: str, state: str) -> list[dict]:
    """
    Return all institutions (banks + CUs) in a given city+state market.
    Joins branch_markets → institutions to get full details.
    """
    mkey = f"{city.strip().lower()}|{state.strip().lower()}"

    rows = conn.execute("""
        SELECT DISTINCT
            bm.cert,
            bm.inst_name                        AS branch_name,
            COALESCE(i.name, bm.inst_name)      AS name,
            COALESCE(i.type, 'bank')             AS inst_type,
            i.assets_k,
            i.id                                AS institution_id,
            i.website_url,
            -- ncua certs start with 'ncua:' prefix in branch_markets
            CASE WHEN bm.id LIKE 'ncua:%' THEN 'cu' ELSE 'bank' END AS source
        FROM branch_markets bm
        LEFT JOIN institutions i
            ON (i.id = 'fdic:' || bm.cert OR i.id = 'ncua:' || bm.cert)
        WHERE bm.market_key = ?
        ORDER BY i.assets_k DESC NULLS LAST, name
    """, (mkey,)).fetchall()

    return [dict(r) for r in rows]


def get_peer_rates(conn, city: str, state: str,
                   product: str = "cd", term_months: int = None,
                   week: str = None) -> list[dict]:
    """
    Return rates for all peers in a market, optionally filtered by product/term.
    Returns most recent rates if week not specified.
    """
    mkey = f"{city.strip().lower()}|{state.strip().lower()}"

    week_filter = "AND r.scraped_week = ?" if week else ""
    week_subq   = """
        AND r.scraped_week = (
            SELECT MAX(r2.scraped_week) FROM rates r2
            WHERE r2.institution_id = i.id AND r2.product = r.product
              AND (? IS NULL OR r2.term_months = ?)
        )
    """ if not week else ""

    term_filter = "AND r.term_months = ?" if term_months else ""

    params = [mkey]
    if product:
        params.append(product)
    if term_months:
        params.append(term_months)
        if not week:
            params.extend([term_months, term_months])
    elif not week:
        params.extend([None, None])
    if week:
        params.append(week)

    sql = f"""
        SELECT DISTINCT
            COALESCE(i.name, bm.inst_name)      AS name,
            COALESCE(i.type,
                CASE WHEN bm.id LIKE 'ncua:%' THEN 'cu' ELSE 'bank' END
            )                                   AS inst_type,
            i.assets_k,
            r.product,
            r.term_months,
            r.apy,
            r.min_balance,
            r.scraped_week,
            r.confidence,
            i.id                                AS institution_id
        FROM branch_markets bm
        LEFT JOIN institutions i
            ON (i.id = 'fdic:' || bm.cert OR i.id = 'ncua:' || bm.cert)
        LEFT JOIN rates r ON r.institution_id = i.id
            AND (? IS NULL OR r.product = ?)
            {term_filter}
            {week_subq}
            {week_filter}
        WHERE bm.market_key = ?
          AND r.apy IS NOT NULL
        ORDER BY r.term_months, r.apy DESC
    """

    # Rebuild params for this query structure
    qparams = []
    qparams.append(product)   # product IS NULL OR product = ?
    qparams.append(product)
    if term_months:
        qparams.append(term_months)
    if not week:
        qparams.append(term_months)  # subquery term_months
        qparams.append(term_months)
    if week:
        qparams.append(week)
    qparams.append(mkey)

    rows = conn.execute(sql, qparams).fetchall()
    return [dict(r) for r in rows]


def print_market_summary(conn, city: str, state: str):
    """Print a text summary of a market's peer institutions."""
    peers = get_peers(conn, city, state)

    if not peers:
        print(f"No institutions found for {city}, {state}.")
        print("Run branch_geography.py and cu_geography.py to load data first.")
        return

    banks = [p for p in peers if p.get("source") != "cu" or p.get("inst_type") == "bank"]
    cus   = [p for p in peers if p.get("source") == "cu" or p.get("inst_type") == "cu"]

    print(f"\n{'='*65}")
    print(f"  Market Peer Group: {city.title()}, {state.upper()}")
    print(f"  {len(peers)} institutions ({len(banks)} banks, {len(cus)} credit unions)")
    print(f"{'='*65}")

    if banks:
        print(f"\n  BANKS ({len(banks)})")
        print(f"  {'-'*60}")
        for p in banks:
            assets = f"${p['assets_k']//1000:,}M" if p.get("assets_k") else "  —"
            rates_flag = "✓" if p.get("institution_id") else " "
            print(f"  {rates_flag} {p['name'][:42]:<44} {assets:>10}")

    if cus:
        print(f"\n  CREDIT UNIONS ({len(cus)})")
        print(f"  {'-'*60}")
        for p in cus:
            assets = f"${p['assets_k']//1000:,}M" if p.get("assets_k") else "  —"
            rates_flag = "✓" if p.get("institution_id") else " "
            print(f"  {rates_flag} {p['name'][:42]:<44} {assets:>10}")

    print(f"\n  ✓ = institution matched in rates database")
    print(f"{'='*65}\n")


def main():
    parser = argparse.ArgumentParser(description="Peer Group Builder")
    parser.add_argument("--market", nargs=2, metavar=("CITY", "STATE"),
                        required=True, help="Market to analyze")
    parser.add_argument("--product", default=None,
                        help="Product filter (e.g. cd, savings, money_market)")
    parser.add_argument("--term",    type=int, default=None,
                        help="CD term in months (e.g. 12 for 1yr)")
    parser.add_argument("--week",    default=None,
                        help="Specific week to pull rates for (YYYY-WW)")
    parser.add_argument("--rates",   action="store_true",
                        help="Show rates instead of peer list")
    args = parser.parse_args()

    city, state = args.market
    conn = get_conn()

    if args.rates:
        rows = get_peer_rates(conn, city, state,
                              product=args.product,
                              term_months=args.term,
                              week=args.week)
        if not rows:
            print(f"No rate data found for {city}, {state}.")
            return

        print(f"\nRates for {city.title()}, {state.upper()}")
        if args.product:
            print(f"Product: {args.product}" + (f", {args.term}mo" if args.term else ""))
        print(f"\n{'Institution':<45} {'Type':<5} {'APY%':>6} {'Term':>5} {'Min Bal':>10} {'Week'}")
        print("-" * 85)
        for r in rows:
            apy     = f"{r['apy']*100:.2f}%" if r["apy"] else "  —"
            term    = f"{r['term_months']}mo" if r["term_months"] else "  —"
            minbal  = f"${r['min_balance']:,.0f}" if r["min_balance"] else "  —"
            print(f"{r['name'][:44]:<45} {r['inst_type']:<5} {apy:>6} {term:>5} {minbal:>10} {r['scraped_week']}")
    else:
        print_market_summary(conn, city, state)


if __name__ == "__main__":
    main()
