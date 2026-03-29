#!/usr/bin/env python3
"""
Phase 1: Smart Brave URL Discovery
Finds the best loan + mortgage rate page URLs using scored URL ranking,
multiple query variants, and title/snippet signals.

Scoring logic:
- Same domain as institution website: +40 pts (required to be preferred)
- URL path contains rate-specific keywords: +30 pts
- URL path suggests a rate TABLE (not calculator/apply): +20 pts
- Title/snippet contains actual rate numbers (e.g. "4.99%"): +25 pts
- Title/snippet contains "APR": +10 pts
- URL is NOT a PDF (prefer HTML): +5 pts
- URL contains NEGATIVE signals (apply, calculator, estimator): -20 pts
- URL contains STRONG negatives (blog, news, about, contact): -40 pts

Usage:
    python3 jobs/phase1_brave_discovery.py              # all institutions
    python3 jobs/phase1_brave_discovery.py --type cu    # CUs only
    python3 jobs/phase1_brave_discovery.py --type bank  # banks only
    python3 jobs/phase1_brave_discovery.py --reset      # redo already-discovered
    python3 jobs/phase1_brave_discovery.py --limit 500  # first N institutions

Run this first, then run phase2_scrape.py
"""

import argparse, sqlite3, json, time, re, requests, sys, os

DB_PATH   = os.path.join(os.path.dirname(__file__), '..', 'db', 'rates.db')
BRAVE_KEY = 'BSAV_DCBYpxwTArxRNJG6T-jMyfh7U4'

SKIP_KEYWORDS = [
    'trust company', 'private wealth', 'private bank', 'federal home loan',
    'federal reserve', 'bankers bank', 'industrial loan', 'investment bank',
]

# ── Deposit (CD/savings) rate path signals ────────────────────────────────────
DEPOSIT_RATE_PATH = [
    'deposit-rate', 'deposit_rate', 'savings-rate', 'cd-rate', 'certificate-rate',
    'rates/savings', 'rates/cd', 'rates/deposit', 'rates/certificate',
    'interest-rate', 'current-rate', 'rate-center', 'rate-table',
    'rates.asp', 'rates.htm', 'rates.php', 'apy', 'dividend-rate',
]
DEPOSIT_SOFT = ['rate', 'rates', 'savings', 'deposit', 'cd', 'certificate', 'apy', 'yield']

# ── Scoring signals ───────────────────────────────────────────────────────────

# Strong positive: URL path clearly indicates a rate table/page
LOAN_RATE_PATH = [
    'loan-rate', 'loan_rate', 'auto-rate', 'vehicle-rate', 'borrow/rate',
    'lending-rate', 'rates/loan', 'rates/auto', 'rates/vehicle',
    'interest-rate', 'current-rate', 'rate-center', 'rate-table',
    'consumer-rate', 'personal-rate', 'rates.asp', 'rates.htm', 'rates.php',
]
MTG_RATE_PATH = [
    'mortgage-rate', 'mortgage_rate', 'home-loan-rate', 'home_loan_rate',
    'rates/mortgage', 'rates/home', 'rates/real-estate',
    'current-mortgage', 'mortgage-interest', 'rate-center',
    'mortgage.asp', 'mortgage.htm', 'rates.asp', 'rates.htm',
]

# Moderate positive: URL suggests the borrow/rates section
LOAN_SOFT = ['rate', 'rates', 'borrow', 'lending', 'loan', 'vehicle', 'auto', 'personal']
MTG_SOFT  = ['rate', 'rates', 'mortgage', 'home-loan', 'homeloans', 'refinance', 'heloc']

# Negative: URL is a product page, not a rate table
SOFT_NEG  = ['apply', 'calculator', 'estimat', 'get-started', 'prequalif', 'preapproval']
HARD_NEG  = ['blog', 'news', 'about', 'contact', 'career', 'login', 'account',
             'member', 'routing', 'branch', 'atm', 'location', 'event', 'press']


def score_url(url, title, snippet, domain, ptype):
    """Score a candidate URL 0-100. Higher = more likely to be a rate table page."""
    score = 0
    u   = url.lower()
    t   = (title or '').lower()
    s   = (snippet or '').lower()
    ts  = t + ' ' + s
    u_domain = re.sub(r'^https?://(www\.)?', '', url).split('/')[0].lower()

    # Domain match (required for high scores)
    same_domain = bool(domain) and domain.lower() in u_domain
    if same_domain:
        score += 40
    else:
        score -= 10  # off-domain hits are lower priority

    # URL path signals
    if ptype == 'loan':
        rate_paths = LOAN_RATE_PATH
        rate_soft  = LOAN_SOFT
    elif ptype == 'mortgage':
        rate_paths = MTG_RATE_PATH
        rate_soft  = MTG_SOFT
    else:  # deposit
        rate_paths = DEPOSIT_RATE_PATH
        rate_soft  = DEPOSIT_SOFT

    if any(k in u for k in rate_paths):
        score += 30
    elif any(k in u for k in rate_soft):
        score += 12

    # Title/snippet has actual rate numbers (strong signal it's a rate page)
    if re.search(r'\d+\.\d+\s*%', ts):
        score += 25
    if 'apr' in ts or 'apy' in ts:
        score += 10
    if any(w in ts for w in ['current rate', "today's rate", 'as low as', 'starting at',
                              'as high as', 'up to', 'earn up']):
        score += 8

    # Title suggests it's a rate page specifically
    if any(w in t for w in ['rate', 'rates', 'apr', 'apy']):
        score += 8

    # Negative signals
    if any(k in u for k in SOFT_NEG):
        score -= 20
    if any(k in u for k in HARD_NEG):
        score -= 40

    # PDF penalty (prefer HTML for scraping)
    if u.endswith('.pdf'):
        score -= 10

    # Bonus: short/clean path (rate pages tend to be top-level)
    path_depth = len([p for p in u.replace('https://', '').split('/') if p]) - 1
    if path_depth <= 2:
        score += 5

    return score


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


# Common rate page paths to try as a last resort when Brave finds nothing on-domain
FALLBACK_PATHS = {
    'deposit': [
        '/rates', '/current-rates', '/rate-center', '/rates/savings',
        '/rates/deposit', '/rates.html', '/rates.php', '/rates.asp',
        '/personal/rates', '/banking/rates', '/resources/rates',
        '/deposit-rates', '/savings-rates', '/cd-rates',
        '/tools/rates', '/services/rates', '/member-rates',
    ],
    'loan': [
        '/rates', '/loan-rates', '/rates/loan', '/rates/auto',
        '/rates/vehicle', '/lending-rates', '/borrow/rates',
        '/personal/loan-rates', '/personal-loans/rates',
        '/rates.html', '/rates.php', '/rates.asp',
        '/consumer-loans/rates', '/auto-loan-rates',
    ],
    'mortgage': [
        '/mortgage-rates', '/rates/mortgage', '/home-loan-rates',
        '/rates/home', '/mortgage/rates', '/rates/real-estate',
        '/personal/mortgage-rates', '/home-lending/rates',
        '/rates.html', '/rates', '/rate-center',
    ],
}


def check_fallback_paths(domain, ptype, min_score=20):
    """Try common rate page paths on the institution's own domain."""
    import requests
    paths = FALLBACK_PATHS.get(ptype, [])
    for path in paths:
        url = f'https://www.{domain}{path}'
        try:
            r = requests.head(url, timeout=5, allow_redirects=True,
                             headers={'User-Agent': 'Mozilla/5.0'})
            if r.status_code == 200:
                # Score it — it's on-domain by definition so starts high
                score = score_url(url, '', '', domain, ptype)
                if score >= min_score:
                    return url, score
        except:
            continue
        time.sleep(0.05)
    return None, 0


def derive_urls_from_deposit(deposit_url, ptype):
    """
    Given a known deposit rates URL, derive likely loan/mortgage URLs.

    If deposit URL is:  https://somecu.org/rates/savings
    Loan likely at:     https://somecu.org/rates/loans  OR  /rates/auto
    Mortgage likely at: https://somecu.org/rates/mortgage OR /rates/home

    Returns list of candidate URLs to check (HEAD requests).
    """
    if not deposit_url:
        return []

    u = deposit_url.lower()
    base = deposit_url.rstrip('/')

    # Strip the last path segment to get the rates section base
    parts = base.split('/')
    rates_base = '/'.join(parts[:-1]) if len(parts) > 3 else base

    candidates = []

    if ptype == 'loan':
        # Sibling paths under same rates section
        for suffix in ['loans', 'auto', 'auto-loans', 'vehicle', 'loan-rates',
                       'consumer', 'personal-loan', 'borrow', 'lending']:
            candidates.append(f'{rates_base}/{suffix}')
        # Also try replacing the deposit-specific segment
        for old, new in [('savings','loans'), ('deposit','loans'), ('cd','loans'),
                         ('certificates','loans'), ('savings-rates','loan-rates'),
                         ('deposit-rates','loan-rates'), ('cd-rates','loan-rates')]:
            if old in u:
                candidates.append(deposit_url.replace(old, new))
                candidates.append(deposit_url.lower().replace(old, 'auto-loans'))

    elif ptype == 'mortgage':
        for suffix in ['mortgage', 'mortgages', 'home-loans', 'home-loan-rates',
                       'real-estate', 'mortgage-rates', 'home']:
            candidates.append(f'{rates_base}/{suffix}')
        for old, new in [('savings','mortgage'), ('deposit','mortgage'), ('cd','mortgage'),
                         ('savings-rates','mortgage-rates'), ('deposit-rates','mortgage-rates'),
                         ('loan-rates','mortgage-rates'), ('loans','mortgage')]:
            if old in u:
                candidates.append(deposit_url.replace(old, new))

    return candidates


def check_derived_urls(deposit_url, domain, ptype):
    """Check deposit-URL-derived candidates with HEAD requests."""
    candidates = derive_urls_from_deposit(deposit_url, ptype)
    seen = set()
    for url in candidates:
        if url in seen or not url.startswith('http'):
            continue
        seen.add(url)
        try:
            r = requests.head(url, timeout=4, allow_redirects=True,
                             headers={'User-Agent': 'Mozilla/5.0'})
            if r.status_code == 200:
                score = score_url(url, '', '', domain, ptype)
                if score >= 20:
                    return url, score
        except:
            pass
        time.sleep(0.04)
    return None, 0


def find_best_url(name, domain, ptype, inst_type, deposit_url=None):
    """
    URL discovery with 4 layers:
    1. Derive from deposit URL (free, instant) — if deposit URL known
    2. Brave search (3 query variants, scored)
    3. Fallback path probing on institution domain
    Returns (url, score).
    """
    # Layer 1: Derive from known deposit URL (free, no API call)
    if deposit_url and ptype in ('loan', 'mortgage'):
        url, score = check_derived_urls(deposit_url, domain, ptype)
        if url and score >= 30:
            return url, score

    # Layer 2: Brave search — build query variants
    if ptype == 'loan':
        if inst_type == 'cu':
            queries = [
                f'{name} credit union auto loan rates',
                f'site:{domain} auto loan rates',
                f'{name} vehicle loan rates APR',
            ]
        else:
            queries = [
                f'{name} auto loan rates',
                f'site:{domain} auto loan rates',
                f'{name} car loan interest rates APR',
            ]
    elif ptype == 'mortgage':
        if inst_type == 'cu':
            queries = [
                f'{name} credit union mortgage rates',
                f'site:{domain} mortgage rates',
                f'{name} home loan rates today APR',
            ]
        else:
            queries = [
                f'{name} mortgage rates today',
                f'site:{domain} mortgage rates',
                f'{name} home loan interest rates APR',
            ]
    else:  # deposit
        if inst_type == 'cu':
            queries = [
                f'{name} credit union savings rates CD rates APY',
                f'site:{domain} savings rates',
                f'{name} certificate of deposit rates APY',
            ]
        else:
            queries = [
                f'{name} bank savings rates CD rates APY',
                f'site:{domain} CD rates savings rates',
                f'{name} certificate deposit interest rates APY',
            ]

    all_hits = []
    for q in queries:
        hits = brave_search(q, count=5)
        all_hits.extend(hits)
        time.sleep(0.12)

    if not all_hits:
        return None, 0

    # Score all hits and return the best
    scored = []
    for h in all_hits:
        url     = h.get('url', '')
        title   = h.get('title', '')
        snippet = h.get('description', '')
        if not url:
            continue
        s = score_url(url, title, snippet, domain, ptype)
        scored.append((s, url, title))

    # Deduplicate by URL, keep highest score
    seen = {}
    for score, url, title in scored:
        if url not in seen or score > seen[url][0]:
            seen[url] = (score, title)

    ranked = sorted(seen.items(), key=lambda x: x[1][0], reverse=True)

    # Only accept same-domain results — reject aggregators (Bankrate, WalletHub, etc.)
    for url, (score, title) in ranked:
        u_domain = re.sub(r'^https?://(www\.)?', '', url).split('/')[0].lower()
        if domain and domain.lower() in u_domain and score >= 30:
            return url, score

    # No good on-domain result from Brave — try common path patterns directly
    if domain:
        fallback_url, fallback_score = check_fallback_paths(domain, ptype)
        if fallback_url:
            return fallback_url, fallback_score

    return None, 0


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.row_factory = sqlite3.Row
    return conn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--type',  choices=['cu', 'bank', 'all'], default='all')
    parser.add_argument('--limit', type=int, default=99999)
    parser.add_argument('--reset', action='store_true',
                        help='Re-discover URLs even if already set')
    parser.add_argument('--min-score', type=int, default=30,
                        help='Minimum score to accept a URL (default: 30)')
    args = parser.parse_args()

    conn = get_db()

    type_filter = ''
    if args.type == 'cu':    type_filter = "AND type='cu'"
    elif args.type == 'bank': type_filter = "AND type='bank'"

    url_filter = '' if args.reset else \
        'AND (loan_rates_url IS NULL OR mortgage_rates_url IS NULL OR rates_url IS NULL)'

    institutions = conn.execute(f"""
        SELECT id, name, type, website_url, rates_url, loan_rates_url, mortgage_rates_url
        FROM institutions
        WHERE active=1 {type_filter} {url_filter}
        ORDER BY name
        LIMIT {args.limit}
    """).fetchall()

    import sys
    sys.stdout.reconfigure(line_buffering=True)
    print(f"[Phase 1] Smart Brave discovery — {len(institutions)} institutions", flush=True)
    print(f"  Type: {args.type} | Reset: {args.reset} | Min score: {args.min_score}", flush=True)
    print(f"  Discovering: deposit (rates_url) + loan + mortgage URLs", flush=True)

    dep_found = loan_found = mtg_found = 0
    dep_skipped = loan_skipped = mtg_skipped = skipped = 0

    for i, inst in enumerate(institutions):
        name      = inst['name']
        inst_type = inst['type']

        if any(k in name.lower() for k in SKIP_KEYWORDS):
            skipped += 1
            continue

        website = inst['website_url'] or ''
        domain  = re.sub(r'^https?://(www\.)?', '', website).split('/')[0]

        # ── Deposit URL (CD/savings rates) ────────────────────────────────────
        if not inst['rates_url'] or args.reset:
            url, score = find_best_url(name, domain, 'deposit', inst_type)
            if url and score >= args.min_score:
                conn.execute('UPDATE institutions SET rates_url=? WHERE id=?', (url, inst['id']))
                conn.commit()
                dep_found += 1
                if score >= 60:
                    print(f'  DEP  ✅ {name}: {url} (score={score})', flush=True)
            else:
                dep_skipped += 1

        # Use deposit URL as a hint for loan/mortgage discovery
        dep_url = inst['rates_url']

        # ── Loan URL ──────────────────────────────────────────────────────────
        if not inst['loan_rates_url'] or args.reset:
            url, score = find_best_url(name, domain, 'loan', inst_type, deposit_url=dep_url)
            if url and score >= args.min_score:
                conn.execute('UPDATE institutions SET loan_rates_url=? WHERE id=?', (url, inst['id']))
                conn.commit()
                loan_found += 1
                src = '(from deposit URL)' if dep_url and url.split('/')[2] in dep_url else ''
                if score >= 60:
                    print(f'  LOAN ✅ {name}: {url} (score={score}) {src}', flush=True)
            else:
                loan_skipped += 1

        # ── Mortgage URL ──────────────────────────────────────────────────────
        if not inst['mortgage_rates_url'] or args.reset:
            url, score = find_best_url(name, domain, 'mortgage', inst_type, deposit_url=dep_url)
            if url and score >= args.min_score:
                conn.execute('UPDATE institutions SET mortgage_rates_url=? WHERE id=?', (url, inst['id']))
                conn.commit()
                mtg_found += 1
                src = '(from deposit URL)' if dep_url and url.split('/')[2] in dep_url else ''
                if score >= 60:
                    print(f'  MTG  ✅ {name}: {url} (score={score}) {src}', flush=True)
            else:
                mtg_skipped += 1

        if (i + 1) % 25 == 0:
            pct = (i+1) / len(institutions) * 100
            print(f'  [{i+1}/{len(institutions)} {pct:.0f}%] '
                  f'dep={dep_found} loan={loan_found} mtg={mtg_found}', flush=True)

    print(f'\n[Phase 1 DONE]')
    print(f'  Deposit URLs found: {dep_found} | low-score: {dep_skipped}')
    print(f'  Loan    URLs found: {loan_found} | low-score: {loan_skipped}')
    print(f'  Mtg     URLs found: {mtg_found} | low-score: {mtg_skipped}')
    print(f'  Skipped (private/non-retail): {skipped}')

    total    = conn.execute('SELECT COUNT(*) FROM institutions WHERE active=1').fetchone()[0]
    has_dep  = conn.execute('SELECT COUNT(*) FROM institutions WHERE active=1 AND rates_url IS NOT NULL').fetchone()[0]
    has_loan = conn.execute('SELECT COUNT(*) FROM institutions WHERE active=1 AND loan_rates_url IS NOT NULL').fetchone()[0]
    has_mtg  = conn.execute('SELECT COUNT(*) FROM institutions WHERE active=1 AND mortgage_rates_url IS NOT NULL').fetchone()[0]
    print(f'\n  Total with deposit URL: {has_dep:,}/{total:,} ({has_dep/total*100:.1f}%)')
    print(f'  Total with loan URL:    {has_loan:,}/{total:,} ({has_loan/total*100:.1f}%)')
    print(f'  Total with mtg URL:     {has_mtg:,}/{total:,} ({has_mtg/total*100:.1f}%)')
    conn.close()


if __name__ == '__main__':
    main()
