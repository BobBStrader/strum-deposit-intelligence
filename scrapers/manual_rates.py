"""
Manual Rate Entry + Special Source Handler
-------------------------------------------
Two tools in one:

1. SPECIAL SOURCE SCRAPER — handles institutions whose rates come from
   non-standard sources (PDFs, special APIs, etc.)
   Currently supported:
     - Chase (fdic:628) — direct PDF download
     - Any institution with a .pdf rates_url

2. MANUAL RATE ENTRY — CLI to type in rates for institutions that
   can't be scraped (PNC, Harbor Bank, etc.)

Usage:
    # Scrape Chase PDF and parse
    python3 manual_rates.py --chase

    # Scrape any institution with a PDF rates_url in a market
    python3 manual_rates.py --pdf-market "Baltimore" MD

    # Manual entry mode for a specific institution
    python3 manual_rates.py --enter fdic:1039 "PNC Bank"

    # Bulk manual entry from a JSON file
    python3 manual_rates.py --import rates.json

    # Show what's missing in a market
    python3 manual_rates.py --missing "Baltimore" MD
"""

import argparse
import io
import json
import os
import re
import sys
import urllib.request
from datetime import datetime, timezone, date

import requests

sys.path.insert(0, os.path.dirname(__file__))
from schema import get_conn

PRODUCT_GROUP = {
    'savings':      'deposit_liquid',
    'checking':     'deposit_liquid',
    'money_market': 'deposit_liquid',
    'cd':           'deposit_term',
    'ira_cd':       'deposit_term',
    'mortgage':     'loan_secured',
    'home_equity':  'loan_secured',
    'auto_loan':    'loan_secured',
    'personal_loan':'loan_unsecured',
}

# Rate aggregator sources — for institutions that block direct scraping
AGGREGATOR_SOURCES = {
    "fdic:6384": {  # PNC Bank
        "name": "PNC Bank, National Association",
        "url":  "https://www.depositaccounts.com/banks/pnc-bank.html",
        # Table indexes on the page: savings=3, money_market=4, checking=5, cd=6, ira_cd=7
        "tables": {3: "savings", 4: "money_market", 5: "checking", 6: "cd", 7: "ira_cd"},
    },
}


def scrape_deposit_accounts(conn, institution_id: str, verbose: bool = True) -> int:
    """
    Scrape rates from DepositAccounts.com for institutions that block direct scraping.
    Parses structured HTML tables — no LLM needed.
    Returns count of rates inserted.
    """
    from bs4 import BeautifulSoup
    from llm_parser import passes_rules

    cfg = AGGREGATOR_SOURCES.get(institution_id)
    if not cfg:
        print(f"  No aggregator config for {institution_id}")
        return 0

    if verbose:
        print(f"  Fetching {cfg['name']} from DepositAccounts.com...")

    r = requests.get(cfg["url"], timeout=15,
        headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.find_all("table")

    def _parse_term(name: str):
        m = re.search(r"(\d+)\s*(Month|Mo|Year|Yr)", name, re.I)
        if not m:
            return None
        val, unit = int(m.group(1)), m.group(2).lower()
        return val * 12 if ("year" in unit or "yr" in unit) else val

    def _parse_min(s: str):
        m = re.search(r"\$([0-9,.]+)([km]?)", s.lower())
        if not m:
            return None
        v = float(m.group(1).replace(",", ""))
        if m.group(2) == "k": v *= 1000
        if m.group(2) == "m": v *= 1_000_000
        return v

    rates = []
    for tbl_idx, product in cfg["tables"].items():
        if tbl_idx >= len(tables):
            continue
        tbl = tables[tbl_idx]
        for row in tbl.find_all("tr")[1:]:  # skip header
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if not cells or not cells[0]:
                continue
            apy_str = cells[0].replace("%", "").replace("*", "").replace("†", "").strip()
            try:
                apy = float(apy_str) / 100
            except ValueError:
                continue
            min_bal = _parse_min(cells[1]) if len(cells) > 1 else None
            name    = cells[3] if len(cells) > 3 else ""
            term    = _parse_term(name) if product in ("cd", "ira_cd") else None
            rates.append({
                "product":     product,
                "term_months": term,
                "apy":         apy,
                "min_balance": min_bal,
                "notes":       name[:80],
            })

    if verbose:
        print(f"  Parsed {len(rates)} rates from page")

    now      = datetime.now(timezone.utc).isoformat()
    week     = date.today().isocalendar()
    week_str = f"{week[0]}-{week[1]:02d}"
    inserted = 0

    for r in rates:
        if not passes_rules(r["product"], r["apy"], r.get("term_months")):
            continue
        product = r["product"]
        conn.execute("""
            INSERT INTO rates
            (institution_id, scraped_at, scraped_week, product, group_id,
             term_months, apy, min_balance, notes, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'verified')
        """, (institution_id, now, week_str, product,
              PRODUCT_GROUP.get(product, "deposit_liquid"),
              r.get("term_months"), r["apy"], r.get("min_balance"), r.get("notes")))
        inserted += 1

    conn.commit()
    conn.execute(
        "UPDATE institutions SET scrape_status='ok', last_scraped_at=? WHERE id=?",
        (now, institution_id)
    )
    conn.commit()

    if verbose:
        print(f"  Inserted {inserted} rates")
        cds = sorted([r for r in rates if r["product"] == "cd" and r["apy"]],
                     key=lambda x: x.get("term_months") or 0)
        for c in cds[:6]:
            term = f"{c['term_months']}mo" if c["term_months"] else "—"
            print(f"    CD {term} @ {c['apy']*100:.2f}% (min ${c.get('min_balance') or 0:,.0f})")

    return inserted


# Known PDF sources per institution
PDF_SOURCES = {
    "fdic:628": {
        "name": "JPMorgan Chase Bank",
        "pdf_url": "https://www.chase.com/content/dam/chase-ux/ratesheets/pdfs/rdny1.pdf",
    },
}


def scrape_chase_pdf(conn, verbose: bool = True) -> int:
    """
    Special handler for Chase — splits into two focused LLM calls
    (CDs only, then savings/liquid) to avoid token truncation.
    Returns total rates inserted.
    """
    from pypdf import PdfReader
    from llm_parser import passes_rules, normalize_apy, OPENAI_MODEL

    key = _get_openai_key()
    now  = datetime.now(timezone.utc).isoformat()
    week = date.today().isocalendar()
    week_str = f"{week[0]}-{week[1]:02d}"

    PRODUCT_GROUP_LOCAL = {
        'savings': 'deposit_liquid', 'checking': 'deposit_liquid',
        'money_market': 'deposit_liquid', 'cd': 'deposit_term', 'ira_cd': 'deposit_term',
    }

    if verbose:
        print("  Downloading Chase PDF...")
    r = requests.get(
        "https://www.chase.com/content/dam/chase-ux/ratesheets/pdfs/rdny1.pdf",
        timeout=20, headers={'User-Agent': 'Mozilla/5.0'}
    )
    r.raise_for_status()
    text = "\n".join(p.extract_text() or "" for p in PdfReader(io.BytesIO(r.content)).pages)
    if verbose:
        print(f"  PDF: {len(text)} chars")

    conn.execute("UPDATE institutions SET raw_section=?, scrape_status='ok', last_scraped_at=? WHERE id='fdic:628'",
                 (text[:16000], now))
    conn.commit()

    def _call_llm(prompt):
        resp = requests.post(
            'https://api.openai.com/v1/chat/completions',
            headers={'Authorization': 'Bearer ' + key},
            json={'model': OPENAI_MODEL, 'temperature': 0,
                  'messages': [{'role': 'user', 'content': prompt}],
                  'max_tokens': 4000},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()['choices'][0]['message']['content']

    def _parse(raw):
        import re as _re
        m = _re.search(r'\[[\s\S]*\]', raw)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                # Truncated — fix by closing the array
                truncated = m.group(0).rstrip(',\n ')
                try:
                    return json.loads(truncated + ']')
                except Exception:
                    pass
        return []

    def _insert(rates, product_filter=None):
        n = 0
        for r in rates:
            if product_filter and r.get('product') not in product_filter:
                continue
            apy = normalize_apy(r.get('apy'))
            if apy is None:
                continue
            if not passes_rules(r.get('product', ''), apy, r.get('term_months')):
                continue
            product = r.get('product', '')
            conn.execute("""
                INSERT INTO rates
                (institution_id, scraped_at, scraped_week, product, group_id,
                 term_months, apy, min_balance, notes, confidence)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'verified')
            """, ('fdic:628', now, week_str, product,
                  PRODUCT_GROUP_LOCAL.get(product, 'deposit_liquid'),
                  r.get('term_months'), apy, r.get('min_balance'), r.get('notes')))
            n += 1
        conn.commit()
        return n

    total = 0

    # Pass 1: CDs (focused section extract)
    cd_start = text.find('CERTIFICATE OF DEPOSIT')
    cd_text  = text[cd_start:cd_start + 3000] if cd_start >= 0 else text[:3000]
    cd_prompt = (
        "Extract CD rates from this Chase rate table. "
        "The table has 4 tier columns: $0-$9,999 | $10,000-$99,999 | $100,000+ | Standard ($0+). "
        'Return JSON array: [{"product":"cd","term_months":1,"apy":0.0002,"min_balance":0,"notes":"standard"},...] '
        "Use decimal APY (3.50% = 0.035). Include all terms and all balance tiers as separate rows.\n\n"
        + cd_text + "\n\nJSON array:"
    )
    if verbose:
        print("  Pass 1: CDs...")
    raw_cd = _call_llm(cd_prompt)
    cds = _parse(raw_cd)
    n_cd = _insert(cds, product_filter={'cd', 'ira_cd'})
    if verbose:
        print(f"  → {len(cds)} CD rates extracted, {n_cd} inserted")
    total += n_cd

    # Pass 2: Savings / Checking / Money Market
    liq_prompt = (
        "From the Chase rate sheet below, extract ONLY savings, checking, and money market rates. "
        'Return JSON array: [{"product":"savings","term_months":null,"apy":0.0001,"min_balance":0,"notes":"standard"},...] '
        "Use decimal APY. Include all tiers.\n\n"
        + text[:6000] + "\n\nJSON array:"
    )
    if verbose:
        print("  Pass 2: Savings/Checking/MM...")
    raw_liq = _call_llm(liq_prompt)
    liquid  = _parse(raw_liq)
    n_liq   = _insert(liquid, product_filter={'savings', 'checking', 'money_market'})
    if verbose:
        print(f"  → {len(liquid)} liquid rates extracted, {n_liq} inserted")
    total += n_liq

    return total


# ── LLM Parser (direct, no rate-wait throttle) ────────────────────────────────

def _get_openai_key():
    try:
        import subprocess
        result = subprocess.run(
            'source ~/.op_service_account && op item get "OpenAI API Credentials" --vault ClawdBotVault --fields credential --reveal',
            shell=True, capture_output=True, text=True, executable='/bin/zsh'
        )
        return result.stdout.strip()
    except Exception:
        return os.environ.get('OPENAI_API_KEY', '')


def parse_with_llm(text: str, institution_name: str) -> list[dict]:
    """Extract rates from text using OpenAI directly (via requests, no throttle)."""
    from llm_parser import EXTRACT_PROMPT, parse_json, OPENAI_MODEL

    key = _get_openai_key()
    prompt = EXTRACT_PROMPT.format(institution=institution_name, page_text=text[:12000])
    resp = requests.post(
        'https://api.openai.com/v1/chat/completions',
        headers={'Authorization': 'Bearer ' + key},
        json={
            'model': OPENAI_MODEL, 'temperature': 0,
            'messages': [{'role': 'user', 'content': prompt}],
            'max_tokens': 4000,
        },
        timeout=90,
    )
    resp.raise_for_status()
    raw = resp.json()['choices'][0]['message']['content']
    return parse_json(raw) or []


def insert_rates(conn, institution_id: str, rates: list[dict],
                 source: str = 'scraped', confidence: str = 'verified') -> int:
    """Insert a list of rate dicts into the DB. Returns count inserted."""
    from llm_parser import passes_rules, normalize_apy
    now  = datetime.now(timezone.utc).isoformat()
    week = date.today().isocalendar()
    week_str = f"{week[0]}-{week[1]:02d}"
    inserted = 0
    for r in rates:
        apy = normalize_apy(r.get('apy'))
        if apy is None:
            continue
        if not passes_rules(r.get('product', ''), apy, r.get('term_months')):
            continue
        product = r.get('product', '')
        notes = r.get('notes', '') or ''
        if source == 'manual':
            notes = ('manual entry | ' + notes).strip(' |')
        conn.execute("""
            INSERT INTO rates
            (institution_id, scraped_at, scraped_week, product, group_id,
             term_months, apy, min_balance, notes, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (institution_id, now, week_str, product,
              PRODUCT_GROUP.get(product, 'deposit_liquid'),
              r.get('term_months'), apy, r.get('min_balance'), notes, confidence))
        inserted += 1
    conn.commit()
    return inserted


# ── PDF Scraper ───────────────────────────────────────────────────────────────

def scrape_pdf(conn, institution_id: str, pdf_url: str, name: str,
               verbose: bool = True) -> int:
    """Download a PDF, extract text, parse rates with LLM, insert into DB."""
    from pypdf import PdfReader

    if verbose:
        print(f"  Downloading PDF: {pdf_url}")
    r = requests.get(pdf_url, timeout=30, headers={'User-Agent': 'Mozilla/5.0'})
    r.raise_for_status()

    text = "\n".join(p.extract_text() or "" for p in PdfReader(io.BytesIO(r.content)).pages)
    if verbose:
        print(f"  Extracted {len(text)} chars from PDF")

    # Store raw section
    conn.execute("""
        UPDATE institutions SET raw_section=?, scrape_status='ok', last_scraped_at=?
        WHERE id=?
    """, (text[:16000], datetime.now(timezone.utc).isoformat(), institution_id))
    conn.commit()

    if verbose:
        print(f"  Parsing with LLM...")
    rates = parse_with_llm(text, name)
    if verbose:
        print(f"  Extracted {len(rates)} rates")

    count = insert_rates(conn, institution_id, rates, source='scraped')
    if verbose:
        print(f"  Inserted {count} rates")
        cds = [r for r in rates if r.get('product') == 'cd' and r.get('apy')]
        for c in sorted(cds, key=lambda x: x.get('term_months') or 0)[:8]:
            min_b = c.get('min_balance') or 0
            print(f"    CD {c['term_months']}mo @ {c['apy']*100:.2f}% (min ${min_b:,.0f})")

    return count


def run_pdf_market(conn, city: str, state: str) -> int:
    """Find all institutions in a market with PDF rate URLs and scrape them."""
    from pypdf import PdfReader

    mkey = f"{city.strip().lower()}|{state.strip().lower()}"

    # Known PDF institutions
    total = 0
    for iid, cfg in PDF_SOURCES.items():
        # Check if in this market
        in_market = conn.execute("""
            SELECT 1 FROM branch_markets bm
            JOIN institutions i ON (i.id='fdic:'||bm.cert OR i.id='ncua:'||bm.cert)
            WHERE bm.market_key=? AND i.id=?
        """, (mkey, iid)).fetchone()
        if not in_market:
            continue
        print(f"\n{cfg['name']} ({iid})")
        try:
            count = scrape_pdf(conn, iid, cfg['pdf_url'], cfg['name'])
            total += count
        except Exception as e:
            print(f"  ✗ Error: {e}")

    # Also check institutions with .pdf in rates_url
    rows = conn.execute("""
        SELECT DISTINCT i.id, i.name, i.rates_url
        FROM branch_markets bm
        JOIN institutions i ON (i.id='fdic:'||bm.cert OR i.id='ncua:'||bm.cert)
        WHERE bm.market_key=?
        AND i.rates_url LIKE '%.pdf'
        AND i.id NOT IN (SELECT DISTINCT institution_id FROM rates)
    """, (mkey,)).fetchall()

    for row in rows:
        print(f"\n{row['name']} ({row['id']})")
        try:
            count = scrape_pdf(conn, row['id'], row['rates_url'], row['name'])
            total += count
        except Exception as e:
            print(f"  ✗ Error: {e}")

    return total


# ── Manual Entry ──────────────────────────────────────────────────────────────

def manual_entry_cli(conn, institution_id: str, institution_name: str):
    """Interactive CLI to enter rates manually."""
    print(f"\n{'='*60}")
    print(f"  Manual Rate Entry: {institution_name}")
    print(f"  Institution ID: {institution_id}")
    print(f"{'='*60}")
    print("\nEnter rates one at a time. Press Enter with no input to finish.\n")
    print("Products: cd, savings, money_market, checking, ira_cd")
    print("Example: cd 12 4.50 10000   (product term_months apy_pct min_balance)")
    print("         savings 0 0.05 0   (use 0 for term on liquid products)")
    print()

    rates = []
    while True:
        try:
            line = input("Rate> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not line:
            break

        parts = line.split()
        if len(parts) < 3:
            print("  Need at least: product term_months apy_pct")
            continue

        try:
            product    = parts[0].lower()
            term       = int(parts[1]) if parts[1] != '0' else None
            apy_pct    = float(parts[2])
            min_bal    = float(parts[3]) if len(parts) > 3 else 0.0
            notes      = ' '.join(parts[4:]) if len(parts) > 4 else None

            if product not in PRODUCT_GROUP:
                print(f"  Unknown product '{product}'. Use: {', '.join(PRODUCT_GROUP)}")
                continue
            if apy_pct > 20:
                print(f"  APY {apy_pct}% seems too high. Enter as percentage (e.g. 4.50 for 4.50%).")
                continue

            rate = {
                'product':     product,
                'term_months': term,
                'apy':         apy_pct / 100,
                'min_balance': min_bal if min_bal > 0 else None,
                'notes':       notes,
            }
            rates.append(rate)
            label = f"{term}mo" if term else "liquid"
            print(f"  ✓ Added: {product} {label} @ {apy_pct:.2f}% (min ${min_bal:,.0f})")

        except (ValueError, IndexError) as e:
            print(f"  Parse error: {e}. Format: product term_months apy_pct [min_balance] [notes]")

    if not rates:
        print("\nNo rates entered.")
        return 0

    print(f"\n{len(rates)} rates to insert:")
    for r in rates:
        label = f"{r['term_months']}mo" if r['term_months'] else "liquid"
        print(f"  {r['product']} {label} @ {r['apy']*100:.2f}%")

    confirm = input("\nInsert these rates? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Cancelled.")
        return 0

    count = insert_rates(conn, institution_id, rates, source='manual', confidence='verified')
    conn.execute("UPDATE institutions SET scrape_status='ok' WHERE id=?", (institution_id,))
    conn.commit()
    print(f"✅ Inserted {count} rates for {institution_name}")
    return count


def import_json_rates(conn, json_path: str) -> int:
    """
    Import rates from a JSON file.
    Format:
    [
      {
        "institution_id": "fdic:1039",
        "name": "PNC Bank",
        "rates": [
          {"product": "cd", "term_months": 12, "apy": 0.045, "min_balance": 1000},
          ...
        ]
      }
    ]
    """
    with open(json_path) as f:
        data = json.load(f)

    total = 0
    for entry in data:
        iid   = entry['institution_id']
        name  = entry.get('name', iid)
        rates = entry.get('rates', [])
        print(f"Importing {len(rates)} rates for {name}...")
        count = insert_rates(conn, iid, rates, source='manual', confidence='verified')
        conn.execute("UPDATE institutions SET scrape_status='ok' WHERE id=?", (iid,))
        conn.commit()
        print(f"  ✓ {count} inserted")
        total += count

    return total


def show_missing(conn, city: str, state: str):
    """Show institutions in a market with no rate data."""
    mkey = f"{city.strip().lower()}|{state.strip().lower()}"
    rows = conn.execute("""
        SELECT DISTINCT
            i.id,
            COALESCE(i.name, bm.inst_name) AS name,
            i.scrape_status,
            i.rates_url,
            (SELECT COUNT(*) FROM rates r WHERE r.institution_id=i.id) AS rate_count
        FROM branch_markets bm
        LEFT JOIN institutions i ON (i.id='fdic:'||bm.cert OR i.id='ncua:'||bm.cert)
        WHERE bm.market_key=?
        ORDER BY rate_count ASC, name
    """, (mkey,)).fetchall()

    print(f"\nMissing rate data in {city.title()}, {state.upper()}:\n")
    print(f"  {'Institution':<45} {'Rates':>6} {'Status':<20} {'Has URL'}")
    print(f"  {'-'*80}")
    for r in rows:
        if r['rate_count'] > 0:
            continue
        has_url = "✓ " + (r['rates_url'] or '—')[:40] if r['rates_url'] else "✗"
        print(f"  {(r['name'] or '?')[:44]:<45} {r['rate_count']:>6} {(r['scrape_status'] or '—'):<20} {has_url}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Manual Rate Entry + PDF Scraper")
    parser.add_argument("--chase",      action="store_true", help="Scrape Chase PDF")
    parser.add_argument("--pnc",        action="store_true", help="Scrape PNC via DepositAccounts.com")
    parser.add_argument("--aggregator", metavar="INST_ID",   help="Scrape any institution via aggregator (e.g. fdic:6384)")
    parser.add_argument("--pdf-market", nargs=2, metavar=("CITY","STATE"),
                        help="Scrape all PDF-sourced institutions in a market")
    parser.add_argument("--enter",     nargs=2, metavar=("INST_ID","NAME"),
                        help="Manual rate entry for an institution")
    parser.add_argument("--import",    dest="import_file", metavar="FILE",
                        help="Import rates from a JSON file")
    parser.add_argument("--missing",   nargs=2, metavar=("CITY","STATE"),
                        help="Show institutions with no rate data in a market")
    args = parser.parse_args()

    conn = get_conn()

    if args.chase:
        print("\nScraping JPMorgan Chase Bank PDF (split-prompt method)...")
        count = scrape_chase_pdf(conn)
        print(f"\n✅ Chase: {count} rates inserted")

    elif args.pnc:
        print("\nScraping PNC Bank via DepositAccounts.com...")
        count = scrape_deposit_accounts(conn, "fdic:6384")
        print(f"\n✅ PNC: {count} rates inserted")

    elif args.aggregator:
        print(f"\nScraping {args.aggregator} via aggregator...")
        count = scrape_deposit_accounts(conn, args.aggregator)
        print(f"\n✅ {args.aggregator}: {count} rates inserted")

    elif args.pdf_market:
        city, state = args.pdf_market
        print(f"\nScraping PDF-sourced institutions in {city.title()}, {state.upper()}...")
        total = run_pdf_market(conn, city, state)
        print(f"\n✅ Total: {total} rates inserted")

    elif args.enter:
        iid, name = args.enter
        manual_entry_cli(conn, iid, name)

    elif args.import_file:
        total = import_json_rates(conn, args.import_file)
        print(f"\n✅ Total imported: {total} rates")

    elif args.missing:
        city, state = args.missing
        show_missing(conn, city, state)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
