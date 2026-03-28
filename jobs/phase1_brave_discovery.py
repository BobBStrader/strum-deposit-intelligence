#!/usr/bin/env python3
"""
Phase 1: Brave URL Discovery
Find loan + mortgage rate page URLs for all institutions using Brave Search.
Fast — no scraping, just URL discovery. ~2-3 hours for all 8,597 institutions.

Usage:
    python3 jobs/phase1_brave_discovery.py              # all institutions
    python3 jobs/phase1_brave_discovery.py --type cu    # CUs only
    python3 jobs/phase1_brave_discovery.py --type bank  # banks only
    python3 jobs/phase1_brave_discovery.py --reset      # redo already-discovered

Run this first, then run phase2_jina_scrape.py + phase3_playwright_scrape.py
"""

import argparse, sqlite3, json, time, datetime, re, requests, sys, os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'rates.db')

BRAVE_KEY = 'BSAV_DCBYpxwTArxRNJG6T-jMyfh7U4'

SKIP_KEYWORDS = [
    'trust company', 'private wealth', 'private bank', 'federal home loan',
    'federal reserve', 'bankers bank', 'industrial loan', 'investment bank',
]

# Keywords that suggest a URL is a rate page (not marketing/product page)
RATE_URL_KEYWORDS = ['rate', 'rates', 'loan-rates', 'borrow', 'lending', 'apu', 'apr']
MTG_URL_KEYWORDS  = ['rate', 'rates', 'mortgage-rates', 'home-loan', 'mortgage', 'refinance']


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.row_factory = sqlite3.Row
    return conn


def brave_search(query, count=5):
    try:
        r = requests.get(
            'https://api.search.brave.com/res/v1/web/search',
            headers={'Accept': 'application/json', 'X-Subscription-Token': BRAVE_KEY},
            params={'q': query, 'count': count},
            timeout=8)
        return r.json().get('web', {}).get('results', [])
    except:
        return []


def best_url(hits, domain, keywords):
    """Pick the best URL from Brave results — prefer same domain + rate keywords."""
    # First pass: same domain + rate keyword in path
    for h in hits:
        u = h.get('url', '')
        u_domain = re.sub(r'^https?://(www\.)?', '', u).split('/')[0]
        if domain and domain in u_domain:
            if any(k in u.lower() for k in keywords):
                return u
    # Second pass: same domain, any URL
    for h in hits:
        u = h.get('url', '')
        u_domain = re.sub(r'^https?://(www\.)?', '', u).split('/')[0]
        if domain and domain in u_domain:
            return u
    # Fallback: first result
    return hits[0].get('url') if hits else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--type',  choices=['cu', 'bank', 'all'], default='all')
    parser.add_argument('--limit', type=int, default=99999)
    parser.add_argument('--reset', action='store_true',
                        help='Re-discover URLs even if already set')
    args = parser.parse_args()

    conn = get_db()

    type_filter = ''
    if args.type == 'cu':   type_filter = "AND type='cu'"
    elif args.type == 'bank': type_filter = "AND type='bank'"

    url_filter = ''
    if not args.reset:
        url_filter = 'AND (loan_rates_url IS NULL OR mortgage_rates_url IS NULL)'

    institutions = conn.execute(f"""
        SELECT id, name, type, website_url, loan_rates_url, mortgage_rates_url
        FROM institutions
        WHERE active=1 {type_filter} {url_filter}
        ORDER BY name
        LIMIT {args.limit}
    """).fetchall()

    print(f"[Phase 1] Brave URL discovery — {len(institutions)} institutions to process")
    print(f"  Type: {args.type} | Reset: {args.reset}")

    loan_found = mtg_found = skipped = 0

    for i, inst in enumerate(institutions):
        name = inst['name']

        if any(k in name.lower() for k in SKIP_KEYWORDS):
            skipped += 1
            continue

        website = inst['website_url'] or ''
        domain  = re.sub(r'^https?://(www\.)?', '', website).split('/')[0]

        # ── Loan URL ──────────────────────────────────────────────────────────
        if not inst['loan_rates_url'] or args.reset:
            hits = brave_search(f'{name} auto loan rates {domain}', count=5)
            url = best_url(hits, domain, RATE_URL_KEYWORDS)
            if url:
                conn.execute('UPDATE institutions SET loan_rates_url=? WHERE id=?',
                            (url, inst['id']))
                conn.commit()
                loan_found += 1
            time.sleep(0.12)  # ~8 req/sec, well within paid tier

        # ── Mortgage URL ──────────────────────────────────────────────────────
        if not inst['mortgage_rates_url'] or args.reset:
            hits = brave_search(f'{name} mortgage rates {domain}', count=5)
            url = best_url(hits, domain, MTG_URL_KEYWORDS)
            if url:
                conn.execute('UPDATE institutions SET mortgage_rates_url=? WHERE id=?',
                            (url, inst['id']))
                conn.commit()
                mtg_found += 1
            time.sleep(0.12)

        if (i + 1) % 250 == 0:
            print(f"  [{i+1}/{len(institutions)}] loan_urls={loan_found} mtg_urls={mtg_found}")

    print(f"\n[Phase 1 DONE] loan_urls_found={loan_found} mtg_urls_found={mtg_found} skipped={skipped}")

    # Summary
    has_loan = conn.execute('SELECT COUNT(*) FROM institutions WHERE active=1 AND loan_rates_url IS NOT NULL').fetchone()[0]
    has_mtg  = conn.execute('SELECT COUNT(*) FROM institutions WHERE active=1 AND mortgage_rates_url IS NOT NULL').fetchone()[0]
    total    = conn.execute('SELECT COUNT(*) FROM institutions WHERE active=1').fetchone()[0]
    print(f"  Total with loan URL: {has_loan}/{total} ({has_loan/total*100:.1f}%)")
    print(f"  Total with mtg URL:  {has_mtg}/{total} ({has_mtg/total*100:.1f}%)")
    conn.close()


if __name__ == '__main__':
    main()
