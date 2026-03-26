"""
Fix Market Rates — targeted re-scrape for a specific market's peer group
------------------------------------------------------------------------
Identifies peers in a market with missing/error rate data and re-scrapes
them using the best available method (Jina → PDF → direct HTTP).

Usage:
    python3 fix_market_rates.py --market "Baltimore" MD
    python3 fix_market_rates.py --market "Baltimore" MD --dry-run
    python3 fix_market_rates.py --market "Baltimore" MD --id fdic:3510
"""

import argparse
import io
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests

SCRAPER_DIR = os.path.join(os.path.dirname(__file__), '..', 'scrapers')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, SCRAPER_DIR)
from scrapers.schema import get_conn
from jina_scraper import fetch_jina, fetch_direct

JINA_TIMEOUT  = 25
DIRECT_TIMEOUT= 20

# Institutions that need a zip-code POST or special handling — use fallback URL
OVERRIDE_URLS = {
    "fdic:1039":  "https://www.pnc.com/en/rates.html",          # PNC — zip-agnostic page
    "fdic:628":   None,                                           # Chase — PDF handled separately
    "fdic:9846":  "https://www.truist.com/rates",                # Truist
    "fdic:3510":  "https://www.bankofamerica.com/deposits/bank-account-interest-rates/",
    "fdic:588":   "https://www.mtb.com/personal/savings-and-cds/certificates-of-deposit",
    "fdic:18409": "https://www.td.com/us/en/personal-banking/savings/savings-accounts",
}

# Chase PDF URL — we handle separately with pypdf
CHASE_PDF_URL = "https://www.chase.com/content/dam/chase-ux/ratesheets/pdfs/rdny1.pdf"


def extract_text_from_pdf(url: str) -> str | None:
    """Download and extract text from a PDF rate sheet."""
    try:
        from pypdf import PdfReader
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        reader = PdfReader(io.BytesIO(r.content))
        text = "\n".join(page.extract_text() or "" for page in reader.pages[:6])
        return text if text.strip() else None
    except Exception as e:
        print(f"    PDF extract error: {e}")
        return None


def scrape_institution(conn, inst: dict, dry_run: bool = False) -> bool:
    """
    Attempt to scrape rate data for one institution.
    Returns True if raw_section was updated successfully.
    """
    iid       = inst["id"]
    name      = inst["name"]
    rates_url = OVERRIDE_URLS.get(iid, inst["rates_url"] or inst["website_url"])

    if not rates_url:
        print(f"  ⚠️  {name} — no URL, skipping")
        return False

    print(f"  Scraping: {name[:50]}")
    print(f"    URL: {rates_url}")

    if dry_run:
        print(f"    [dry-run] would scrape")
        return False

    raw_section = None

    # Chase special case: PDF
    if iid == "fdic:628" or rates_url.endswith(".pdf"):
        print(f"    → PDF mode")
        raw_section = extract_text_from_pdf(rates_url if rates_url.endswith(".pdf") else CHASE_PDF_URL)
        if raw_section:
            print(f"    ✓ PDF: {len(raw_section)} chars")
    
    # Try Jina first
    if not raw_section:
        print(f"    → Jina")
        raw_section = fetch_jina(rates_url, timeout=JINA_TIMEOUT)
        if raw_section:
            print(f"    ✓ Jina: {len(raw_section)} chars")

    # Fall back to direct HTTP
    if not raw_section:
        print(f"    → Direct HTTP")
        raw_section = fetch_direct(rates_url, timeout=DIRECT_TIMEOUT)
        if raw_section:
            print(f"    ✓ Direct: {len(raw_section)} chars")

    if not raw_section:
        print(f"    ✗ All methods failed")
        conn.execute(
            "UPDATE institutions SET scrape_status='error', last_scraped_at=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), iid)
        )
        conn.commit()
        return False

    # Check for rate content
    text_lower = raw_section.lower()
    has_rates = any(kw in text_lower for kw in [
        'apy', 'annual percentage yield', 'certificate of deposit',
        'savings rate', 'cd rate', '% apy', 'interest rate'
    ])

    status = "ok" if has_rates else "no_rates"
    section_to_store = raw_section[:16000]

    conn.execute("""
        UPDATE institutions
        SET raw_section=?, scrape_status=?, last_scraped_at=?
        WHERE id=?
    """, (section_to_store, status, datetime.now(timezone.utc).isoformat(), iid))
    conn.commit()

    if has_rates:
        print(f"    ✓ Has rate content ({len(section_to_store)} chars stored)")
    else:
        print(f"    ⚠️  No rate keywords found (status=no_rates)")

    return has_rates


def run_llm_parse(conn, inst_ids: list[str], model: str = "openai") -> int:
    """Run LLM parser on freshly scraped institutions."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scrapers'))
    from llm_parser import parse_institution

    parsed = 0
    for iid in inst_ids:
        row = conn.execute(
            "SELECT id, name, raw_section FROM institutions WHERE id=? AND raw_section IS NOT NULL",
            (iid,)
        ).fetchone()
        if not row:
            continue

        print(f"  Parsing: {row['name'][:50]}")
        try:
            count = parse_institution(conn, dict(row), model=model)
            print(f"    → {count} rates extracted")
            parsed += count
        except Exception as e:
            print(f"    ✗ Parse error: {e}")
        time.sleep(0.5)

    return parsed


def fix_market(conn, city: str, state: str, 
               target_id: str = None, dry_run: bool = False,
               skip_parse: bool = False, model: str = "openai"):
    """Main entry point: fix all missing/error institutions in a market."""
    mkey = f"{city.strip().lower()}|{state.strip().lower()}"

    # Find institutions needing work
    rows = conn.execute("""
        SELECT DISTINCT
            i.id, i.name, i.type, i.rates_url, i.website_url,
            i.scrape_status, i.raw_section,
            (SELECT COUNT(*) FROM rates r WHERE r.institution_id = i.id) AS rate_count
        FROM branch_markets bm
        JOIN institutions i ON (i.id = 'fdic:' || bm.cert OR i.id = 'ncua:' || bm.cert)
        WHERE bm.market_key = ?
        AND (
            i.scrape_status IN ('error', 'retry_playwright')
            OR (SELECT COUNT(*) FROM rates r WHERE r.institution_id = i.id) = 0
        )
        AND i.scrape_status != 'no_rates'
        ORDER BY i.assets_k DESC NULLS LAST
    """, (mkey,)).fetchall()

    if target_id:
        rows = [r for r in rows if r["id"] == target_id]

    if not rows:
        print(f"✅ No institutions needing fixes in {city}, {state}.")
        return

    print(f"\n🔧 Fixing {len(rows)} institutions in {city.title()}, {state.upper()}:\n")

    scraped_ids = []
    for inst in rows:
        success = scrape_institution(conn, dict(inst), dry_run=dry_run)
        if success:
            scraped_ids.append(inst["id"])
        time.sleep(1.5)  # polite delay

    if skip_parse or dry_run or not scraped_ids:
        print(f"\nDone scraping. {len(scraped_ids)} updated. Run with --parse to extract rates.")
        return

    print(f"\n🤖 Running LLM parser on {len(scraped_ids)} updated institutions...\n")
    total_rates = run_llm_parse(conn, scraped_ids, model=model)
    print(f"\n✅ Done. {total_rates} new rates extracted across {len(scraped_ids)} institutions.")


def main():
    parser = argparse.ArgumentParser(description="Fix missing rate data for a market")
    parser.add_argument("--market",  nargs=2, metavar=("CITY", "STATE"), required=True)
    parser.add_argument("--id",      help="Fix only this institution ID (e.g. fdic:3510)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be scraped")
    parser.add_argument("--scrape-only", action="store_true", help="Scrape but skip LLM parse")
    parser.add_argument("--model",   default="openai", choices=["openai", "ollama"],
                        help="LLM backend for rate extraction")
    args = parser.parse_args()

    city, state = args.market
    conn = get_conn()
    fix_market(conn, city, state,
               target_id=args.id,
               dry_run=args.dry_run,
               skip_parse=args.scrape_only,
               model=args.model)


if __name__ == "__main__":
    main()
