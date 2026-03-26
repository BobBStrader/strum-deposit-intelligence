"""
Playwright Scraper — Phase 4: JS-rendered rate pages
------------------------------------------------------
Handles sites that require JavaScript execution, zip code entry,
or dynamic content loading that Jina/direct HTTP can't handle.

Strategy per institution type:
  - Generic: load page, wait for rate content, grab text
  - Zip-gated (M&T, BofA, PNC): inject a zip code to unlock rates
  - PDF-linked: find and download linked rate PDFs

Usage (standalone):
    python3 playwright_scraper.py --id fdic:588
    python3 playwright_scraper.py --market "Baltimore" MD
    python3 playwright_scraper.py --id fdic:588 --url https://www.mtb.com/...
"""

import argparse
import io
import os
import re
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
from schema import get_conn

# ── Per-institution config ─────────────────────────────────────────────────────
# zip: inject this zip before scraping
# wait_for: CSS selector to wait for before extracting text
# click: CSS selector to click after zip entry
# pdf_pattern: regex to find PDF links on the page

INST_CONFIG = {
    "fdic:588": {   # M&T Bank
        "url":      "https://www.mtb.com/personal/savings-and-cds/certificates-of-deposit",
        "zip":      "21201",  # Baltimore MD zip
        "zip_selector": "input[placeholder*='zip'], input[name*='zip'], input[type='text']",
        "wait_for": "table, .rate-table, [class*='rate'], [class*='cd']",
        "wait_ms":  3000,
    },
    "fdic:1039": {  # PNC
        "url":      "https://www.pnc.com/en/rates.html",
        "zip":      "21201",
        "zip_selector": "input#zipCode, input[name='zipCode'], input[placeholder*='zip']",
        "wait_for": "table, .rates-table, [class*='rate']",
        "wait_ms":  4000,
    },
    "fdic:4832": {  # Shore United Bank
        "url":      "https://shoreunitedbank.com/savings",
        "wait_for": "table, [class*='rate'], .apy, [class*='interest']",
        "wait_ms":  3000,
    },
    "fdic:29613": { # Rosedale Bank
        "url":      "https://www.rosedale.bank/savings",
        "wait_for": "[class*='rate'], table, .apy",
        "wait_ms":  3000,
    },
    "fdic:7759": {  # Univest
        "url":      "https://www.univest.net/rates",
        "wait_for": "table, [class*='rate']",
        "wait_ms":  3000,
    },
    "fdic:24015": { # Harbor Bank
        "url":      "https://www.theharborbank.com/Personal/Certificates-of-Deposit-CDs",
        "wait_for": "table, [class*='rate'], [class*='cd']",
        "wait_ms":  3000,
    },
}

RATE_KEYWORDS = ['apy', 'annual percentage yield', 'certificate of deposit',
                 'savings rate', 'cd rate', 'interest rate', '% apy']


def scrape_with_playwright(iid: str, url: str = None, zip_code: str = None,
                            headless: bool = True, verbose: bool = True) -> str | None:
    """
    Scrape a rate page using Playwright headless Chromium.
    Returns extracted text content or None.
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    cfg = INST_CONFIG.get(iid, {})
    target_url  = url or cfg.get("url")
    zip_to_use  = zip_code or cfg.get("zip")
    zip_sel     = cfg.get("zip_selector")
    wait_for    = cfg.get("wait_for", "body")
    wait_ms     = cfg.get("wait_ms", 3000)

    if not target_url:
        if verbose: print(f"    No URL for {iid}")
        return None

    if verbose: print(f"    → Playwright: {target_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )
        page = ctx.new_page()

        try:
            page.goto(target_url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(wait_ms)

            # Handle zip-gated pages
            if zip_to_use and zip_sel:
                try:
                    zip_input = page.locator(zip_sel).first
                    if zip_input.is_visible(timeout=3000):
                        if verbose: print(f"    → Entering zip: {zip_to_use}")
                        zip_input.fill(zip_to_use)
                        zip_input.press("Enter")
                        page.wait_for_timeout(3000)
                except PWTimeout:
                    if verbose: print(f"    ⚠️  Zip field not found, continuing anyway")
                except Exception as e:
                    if verbose: print(f"    ⚠️  Zip entry error: {e}")

            # Wait for rate content
            try:
                page.wait_for_selector(wait_for, timeout=8000)
            except PWTimeout:
                if verbose: print(f"    ⚠️  Selector '{wait_for}' not found, using full page")

            # Check for PDF links first
            pdf_links = page.eval_on_selector_all(
                "a[href*='.pdf']",
                "els => els.map(e => e.href)"
            )
            rate_pdfs = [l for l in pdf_links if any(
                kw in l.lower() for kw in ['rate', 'cd', 'deposit', 'interest', 'savings']
            )]
            if rate_pdfs and verbose:
                print(f"    Found {len(rate_pdfs)} rate PDF(s): {rate_pdfs[0]}")

            if rate_pdfs:
                # Download and extract PDF
                try:
                    import requests
                    from pypdf import PdfReader
                    r = requests.get(rate_pdfs[0], timeout=20)
                    if r.status_code == 200:
                        text = "\n".join(
                            p2.extract_text() or "" 
                            for p2 in PdfReader(io.BytesIO(r.content)).pages[:8]
                        )
                        if text.strip() and any(kw in text.lower() for kw in RATE_KEYWORDS):
                            if verbose: print(f"    ✓ PDF extracted: {len(text)} chars")
                            browser.close()
                            return text
                except Exception as e:
                    if verbose: print(f"    PDF download error: {e}")

            # Extract full page text
            text = page.inner_text("body")

            # Also try to get tables as structured text
            try:
                tables = page.eval_on_selector_all("table", """
                    tables => tables.map(t => {
                        const rows = Array.from(t.querySelectorAll('tr'));
                        return rows.map(r => 
                            Array.from(r.querySelectorAll('td,th')).map(c => c.innerText.trim()).join(' | ')
                        ).join('\\n');
                    }).join('\\n\\n')
                """)
                if tables:
                    text = tables + "\n\n" + text
            except Exception:
                pass

            has_rates = any(kw in text.lower() for kw in RATE_KEYWORDS)
            if verbose:
                status = "✓ has rates" if has_rates else "⚠️  no rate keywords"
                print(f"    {status} ({len(text)} chars)")

            browser.close()
            return text if text.strip() else None

        except Exception as e:
            if verbose: print(f"    ✗ Playwright error: {e}")
            browser.close()
            return None


def scrape_market_playwright(conn, city: str, state: str,
                              dry_run: bool = False, verbose: bool = True) -> list[str]:
    """
    Find all institutions in a market with error/retry_playwright status
    and scrape them with Playwright.
    Returns list of successfully scraped institution IDs.
    """
    mkey = f"{city.strip().lower()}|{state.strip().lower()}"

    rows = conn.execute("""
        SELECT DISTINCT i.id, i.name, i.rates_url, i.website_url, i.scrape_status,
            (SELECT COUNT(*) FROM rates r WHERE r.institution_id = i.id) AS rate_count
        FROM branch_markets bm
        JOIN institutions i ON (i.id = 'fdic:' || bm.cert OR i.id = 'ncua:' || bm.cert)
        WHERE bm.market_key = ?
        AND (i.scrape_status IN ('error','retry_playwright','no_rates')
             OR (SELECT COUNT(*) FROM rates r WHERE r.institution_id = i.id) = 0)
        AND i.scrape_status != 'pending'
        ORDER BY i.assets_k DESC NULLS LAST
    """, (mkey,)).fetchall()

    if not rows:
        if verbose: print(f"No institutions need Playwright scraping in {city}, {state}.")
        return []

    if verbose:
        print(f"\n🎭 Playwright scraping {len(rows)} institutions in {city.title()}, {state.upper()}:\n")

    now = datetime.now(timezone.utc).isoformat()
    scraped = []

    for inst in rows:
        iid    = inst["id"]
        name   = inst["name"] or "Unknown"
        url    = INST_CONFIG.get(iid, {}).get("url") or inst["rates_url"] or inst["website_url"]

        if verbose: print(f"  {name[:50]}")

        if dry_run:
            if verbose: print(f"    [dry-run] would scrape {url}")
            continue

        if not url:
            if verbose: print(f"    ⚠️  No URL, skipping")
            continue

        text = scrape_with_playwright(iid, url=url, verbose=verbose)

        if text:
            has_rates = any(kw in text.lower() for kw in RATE_KEYWORDS)
            status = "ok" if has_rates else "no_rates"
            conn.execute("""
                UPDATE institutions SET raw_section=?, scrape_status=?, last_scraped_at=?
                WHERE id=?
            """, (text[:16000], status, now, iid))
            conn.commit()
            if has_rates:
                scraped.append(iid)
        else:
            conn.execute(
                "UPDATE institutions SET scrape_status='error', last_scraped_at=? WHERE id=?",
                (now, iid)
            )
            conn.commit()

        time.sleep(1)

    if verbose:
        print(f"\n✅ Playwright done. {len(scraped)}/{len(rows)} institutions have rate content.")

    return scraped


def main():
    parser = argparse.ArgumentParser(description="Playwright Rate Scraper")
    parser.add_argument("--id",      help="Scrape single institution by ID (e.g. fdic:588)")
    parser.add_argument("--url",     help="Override URL")
    parser.add_argument("--zip",     help="Override zip code for zip-gated sites")
    parser.add_argument("--market",  nargs=2, metavar=("CITY", "STATE"),
                        help="Scrape all problem institutions in a market")
    parser.add_argument("--parse",   action="store_true",
                        help="Run LLM parser after scraping")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-headless", action="store_true",
                        help="Show browser window (debug mode)")
    args = parser.parse_args()

    conn = get_conn()
    scraped_ids = []

    if args.id:
        iid = args.id
        row = conn.execute("SELECT name FROM institutions WHERE id=?", (iid,)).fetchone()
        name = row["name"] if row else iid
        print(f"Scraping: {name}")
        text = scrape_with_playwright(iid, url=args.url, zip_code=args.zip,
                                       headless=not args.no_headless)
        if text:
            has_rates = any(kw in text.lower() for kw in RATE_KEYWORDS)
            now = datetime.now(timezone.utc).isoformat()
            conn.execute("UPDATE institutions SET raw_section=?, scrape_status=?, last_scraped_at=? WHERE id=?",
                         (text[:16000], "ok" if has_rates else "no_rates", now, iid))
            conn.commit()
            print(f"✅ Saved {len(text[:16000])} chars (has_rates={has_rates})")
            if has_rates:
                scraped_ids.append(iid)
        else:
            print("✗ Failed")

    elif args.market:
        city, state = args.market
        scraped_ids = scrape_market_playwright(conn, city, state, dry_run=args.dry_run)

    else:
        parser.print_help()
        return

    if args.parse and scraped_ids and not args.dry_run:
        sys.path.insert(0, os.path.dirname(__file__))
        from llm_parser import run_parser
        print(f"\n🤖 Parsing {len(scraped_ids)} institutions...")
        run_parser(inst_ids=scraped_ids, backend='openai', force=True)


if __name__ == "__main__":
    main()
