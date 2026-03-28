#!/usr/bin/env python3
"""
Nightly Rate Scraper — runs locally on Mac Mini via cron
Uses Playwright for JS-rendered pages + Jina fallback for static
Processes institutions that haven't been scraped yet, ~500/hr

Usage:
    python3 jobs/nightly_scraper.py                    # process next batch
    python3 jobs/nightly_scraper.py --limit 500        # process 500 institutions
    python3 jobs/nightly_scraper.py --type cu          # credit unions only
    python3 jobs/nightly_scraper.py --type bank        # banks only
    python3 jobs/nightly_scraper.py --reset-failed     # retry previously failed
    python3 jobs/nightly_scraper.py --stats            # show coverage stats

Cron (Mac Mini, runs 11 PM nightly):
    0 23 * * * cd /Users/bob/.openclaw/workspace/deposit-intelligence && python3 jobs/nightly_scraper.py --limit 600 >> /tmp/nightly_scraper.log 2>&1
"""

import argparse, sqlite3, json, time, datetime, re, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'rates.db')
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config.json')

with open(CONFIG_PATH) as f:
    config = json.load(f)

OPENAI_KEY   = config['openai_api_key']
BRAVE_KEY    = 'BSAV_DCBYpxwTArxRNJG6T-jMyfh7U4'
SCRAPED_WEEK = datetime.date.today().strftime('%Y-%W')
NOW          = datetime.datetime.now().isoformat()

SKIP_KEYWORDS = [
    'trust company', 'private wealth', 'private bank', 'federal home loan',
    'federal reserve', 'bankers bank', 'industrial loan', 'investment bank',
]

UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.row_factory = sqlite3.Row
    return conn


# ── Scrapers ──────────────────────────────────────────────────────────────────

def brave_search(query, count=3):
    import requests
    try:
        r = requests.get(
            'https://api.search.brave.com/res/v1/web/search',
            headers={'Accept': 'application/json', 'X-Subscription-Token': BRAVE_KEY},
            params={'q': query, 'count': count}, timeout=8)
        return r.json().get('web', {}).get('results', [])
    except:
        return []


def jina_fetch(url, timeout=12):
    import requests
    try:
        r = requests.get(f'https://r.jina.ai/{url}',
                        headers={'Accept': 'text/plain'}, timeout=timeout)
        return r.text[:25000] if r.status_code == 200 else None
    except:
        return None


def playwright_fetch(url, wait_ms=4000):
    """Full JS rendering via Playwright — only available when running locally."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent=UA)
            page = ctx.new_page()
            page.goto(url, wait_until='networkidle', timeout=25000)
            page.wait_for_timeout(wait_ms)
            page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            page.wait_for_timeout(2000)
            text = page.inner_text('body')
            browser.close()
            return text[:25000] if text else None
    except Exception as e:
        return None


def fetch_with_fallback(url, name=None, ptype=None, conn=None, inst_id=None):
    """
    Fetch strategy:
    1. Jina (fast, ~1s) — if rates found, done
    2. If Jina returns nav-only content → URL is likely wrong:
       use Brave to find a better URL, save it, retry Jina on new URL
    3. If still no rates → Playwright (full JS rendering, ~6-8s)
    4. If still nothing → no_rates
    """
    # Step 1: Try Jina on the given URL
    text = jina_fetch(url)
    if text and has_rates(text):
        return text, 'jina'

    # Step 2: Jina returned content but no rates — URL might be a nav/marketing page
    # Use Brave to find the actual rate page URL
    if name and ptype:
        query = f'{name} {"auto loan" if ptype == "loan" else "mortgage"} rates'
        hits = brave_search(query, count=5)
        for h in hits:
            candidate = h.get('url', '')
            # Only consider URLs from the same domain
            original_domain = re.sub(r'^https?://(www\.)?', '', url).split('/')[0]
            candidate_domain = re.sub(r'^https?://(www\.)?', '', candidate).split('/')[0]
            if original_domain and original_domain in candidate_domain:
                if any(k in candidate.lower() for k in ['rate', 'loan', 'mortgage', 'borrow']):
                    if candidate != url:
                        # Found a better URL — save it and retry
                        if conn and inst_id:
                            col = 'loan_rates_url' if ptype == 'loan' else 'mortgage_rates_url'
                            conn.execute(f'UPDATE institutions SET {col}=? WHERE id=?', (candidate, inst_id))
                            conn.commit()
                        text2 = jina_fetch(candidate)
                        if text2 and has_rates(text2):
                            return text2, f'jina+brave({candidate})'
                        break  # found a candidate but still no rates, fall through to Playwright

    # Step 3: Playwright — handles JS-rendered rate widgets
    text = playwright_fetch(url)
    if text and has_rates(text):
        return text, 'playwright'

    return text, 'no_rates'


def has_rates(text):
    if not text:
        return False
    lines = [l for l in text.split('\n')
             if '%' in l and any(c.isdigit() for c in l) and len(l) < 200]
    return len(lines) >= 2


# ── GPT extraction ────────────────────────────────────────────────────────────

def gpt_extract(content, ptype, name):
    import openai
    client = openai.OpenAI(api_key=OPENAI_KEY)
    if ptype == 'loan':
        prompt = f"""Extract loan rates for {name}. Return JSON array, each item:
- product: new_auto_loan | used_auto_loan | personal_loan | home_equity_loan
- term_months: integer or null
- rate: float as PERCENTAGE (e.g. 4.74) or null if only APR shown
- apr: float as PERCENTAGE if shown separately, null if not. If only one rate labeled APR, set apr=value, rate=null.
- loan_term_label: string like "36 months" or null
- notes: brief note

Use lowest rate for ranges. Return [] if no loan rates found.
Content: {content[:7000]}"""
    else:
        prompt = f"""Extract mortgage rates for {name}. Return JSON array, each item:
- product: mortgage_fixed | mortgage_arm
- term_months: integer (360=30yr, 240=20yr, 180=15yr, 120=10yr) or null for ARM
- arm_initial_years: integer or null
- arm_adjust_months: integer or null
- rate: float as PERCENTAGE or null if only APR shown
- apr: float as PERCENTAGE if shown separately, null if not
- conforming: 1 for conforming, 0 for jumbo, null if unclear
- notes: brief note

Return [] if no mortgage rates found.
Content: {content[:7000]}"""
    try:
        resp = client.chat.completions.create(
            model='gpt-4o-mini',
            messages=[{'role': 'user', 'content': prompt}],
            response_format={'type': 'json_object'},
            temperature=0)
        raw = json.loads(resp.choices[0].message.content)
        if isinstance(raw, list):
            return raw
        for k in ['rates', 'data', 'results', 'items', 'loans', 'mortgages']:
            if k in raw and isinstance(raw[k], list):
                return raw[k]
        return []
    except:
        return []


def normalize(v):
    if v is None:
        return None
    try:
        f = float(v)
        return f / 100 if f > 0.40 else f
    except:
        return None


def insert_rates(conn, inst_id, items, ptype):
    n = 0
    for r in items:
        apy = normalize(r.get('rate'))
        apr = normalize(r.get('apr'))
        if apy is None and apr is None:
            continue
        prod = r.get('product', 'new_auto_loan' if ptype == 'loan' else 'mortgage_fixed')
        try:
            conn.execute("""
                INSERT OR IGNORE INTO rates
                (institution_id, scraped_at, scraped_week, product, apy, apr,
                 term_months, loan_term_label, arm_initial_years, arm_adjust_months,
                 conforming, notes, rate_type, confidence)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                inst_id, NOW, SCRAPED_WEEK, prod, apy, apr,
                r.get('term_months'), r.get('loan_term_label'),
                r.get('arm_initial_years'), r.get('arm_adjust_months'),
                r.get('conforming'), r.get('notes'),
                'fixed' if prod == 'mortgage_fixed' else ('arm' if prod == 'mortgage_arm' else None),
                'unverified'
            ))
            n += 1
        except:
            pass
    conn.commit()
    return n


# ── URL discovery ─────────────────────────────────────────────────────────────

def find_rate_urls(inst, conn):
    """Use Brave to find loan + mortgage rate URLs if not already set."""
    name = inst['name']
    website = inst['website_url'] or ''
    domain = re.sub(r'^https?://(www\.)?', '', website).split('/')[0]

    loan_url = inst['loan_rates_url']
    mtg_url = inst['mortgage_rates_url']

    if not loan_url:
        hits = brave_search(f'{name} auto loan rates {domain}')
        for h in hits:
            u = h.get('url', '')
            if domain and domain in u and any(k in u.lower() for k in ['rate', 'loan', 'borrow']):
                loan_url = u
                break
        if not loan_url and hits:
            loan_url = hits[0].get('url')
        if loan_url:
            conn.execute('UPDATE institutions SET loan_rates_url=? WHERE id=?',
                        (loan_url, inst['id']))
            conn.commit()
        time.sleep(0.15)

    if not mtg_url:
        hits = brave_search(f'{name} mortgage rates {domain}')
        for h in hits:
            u = h.get('url', '')
            if domain and domain in u and any(k in u.lower() for k in ['rate', 'mortgage', 'home']):
                mtg_url = u
                break
        if not mtg_url and hits:
            mtg_url = hits[0].get('url')
        if mtg_url:
            conn.execute('UPDATE institutions SET mortgage_rates_url=? WHERE id=?',
                        (mtg_url, inst['id']))
            conn.commit()
        time.sleep(0.15)

    return loan_url, mtg_url


# ── Main processing ───────────────────────────────────────────────────────────

def process_institution(conn, inst):
    inst_id = inst['id']
    name = inst['name']

    if any(k in name.lower() for k in SKIP_KEYWORDS):
        return 0, 0, 'skipped'

    # Check existing rates
    ex_loan = conn.execute(
        "SELECT COUNT(*) FROM rates WHERE institution_id=? AND (product LIKE '%auto%' OR product='personal_loan')",
        (inst_id,)).fetchone()[0]
    ex_mtg = conn.execute(
        "SELECT COUNT(*) FROM rates WHERE institution_id=? AND product LIKE 'mortgage%'",
        (inst_id,)).fetchone()[0]

    if ex_loan and ex_mtg:
        return 0, 0, 'already_done'

    # Find URLs if missing
    loan_url, mtg_url = find_rate_urls(inst, conn)

    loan_inserted = 0
    mtg_inserted = 0

    # Scrape + parse loans
    if not ex_loan and loan_url:
        raw, method = fetch_with_fallback(loan_url, name=name, ptype='loan', conn=conn, inst_id=inst_id)
        if raw and method != 'no_rates':
            conn.execute('UPDATE institutions SET loan_raw_section=?, loan_scrape_status=? WHERE id=?',
                        (raw[:25000], 'ok', inst_id))
            conn.commit()
            extracted = gpt_extract(raw, 'loan', name)
            loan_inserted = insert_rates(conn, inst_id, extracted, 'loan')
            if loan_inserted:
                print(f'  LOAN {name}: {loan_inserted} rates ({method})')
        elif raw:
            conn.execute('UPDATE institutions SET loan_scrape_status=? WHERE id=?',
                        ('no_rates', inst_id))
            conn.commit()

    # Scrape + parse mortgages
    if not ex_mtg and mtg_url:
        if mtg_url == loan_url:
            raw_src = conn.execute('SELECT loan_raw_section FROM institutions WHERE id=?',
                                   (inst_id,)).fetchone()
            raw = raw_src[0] if raw_src else None
            method = 'cached'
        else:
            raw, method = fetch_with_fallback(mtg_url, name=name, ptype='mortgage', conn=conn, inst_id=inst_id)

        if raw and method != 'no_rates':
            conn.execute('UPDATE institutions SET mortgage_raw_section=?, mortgage_scrape_status=? WHERE id=?',
                        (raw[:25000], 'ok', inst_id))
            conn.commit()
            extracted = gpt_extract(raw, 'mortgage', name)
            mtg_inserted = insert_rates(conn, inst_id, extracted, 'mortgage')
            if mtg_inserted:
                print(f'  MTG  {name}: {mtg_inserted} rates ({method})')
        elif raw:
            conn.execute('UPDATE institutions SET mortgage_scrape_status=? WHERE id=?',
                        ('no_rates', inst_id))
            conn.commit()

    # Mark as attempted
    conn.execute('UPDATE institutions SET last_scraped_at=? WHERE id=?', (NOW, inst_id))
    conn.commit()

    return loan_inserted, mtg_inserted, 'processed'


def show_stats(conn):
    total = conn.execute('SELECT COUNT(*) FROM institutions WHERE active=1').fetchone()[0]
    cu = conn.execute("SELECT COUNT(*) FROM institutions WHERE active=1 AND type='cu'").fetchone()[0]
    banks = conn.execute("SELECT COUNT(*) FROM institutions WHERE active=1 AND type='bank'").fetchone()[0]
    with_loan = conn.execute(
        "SELECT COUNT(DISTINCT institution_id) FROM rates r JOIN institutions i ON i.id=r.institution_id "
        "WHERE product LIKE '%auto%' OR product='personal_loan'").fetchone()[0]
    with_mtg = conn.execute(
        "SELECT COUNT(DISTINCT institution_id) FROM rates r JOIN institutions i ON i.id=r.institution_id "
        "WHERE product LIKE 'mortgage%'").fetchone()[0]
    total_rows = conn.execute('SELECT COUNT(*) FROM rates').fetchone()[0]
    scraped = conn.execute("SELECT COUNT(*) FROM institutions WHERE last_scraped_at IS NOT NULL AND active=1").fetchone()[0]

    print(f"""
=== National Rate Coverage ===
Total institutions:     {total:,} ({cu:,} CUs + {banks:,} banks)
Scraped (attempted):   {scraped:,} ({scraped/total*100:.1f}%)
With loan rates:       {with_loan:,} ({with_loan/total*100:.1f}%)
With mortgage rates:   {with_mtg:,} ({with_mtg/total*100:.1f}%)
Total rate rows:       {total_rows:,}
Unscraped remaining:   {total-scraped:,}
""")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Nightly rate scraper')
    parser.add_argument('--limit',        type=int, default=600,
                        help='Max institutions to process (default: 600)')
    parser.add_argument('--type',         choices=['cu', 'bank', 'all'], default='all',
                        help='Institution type to process')
    parser.add_argument('--offset',       type=int, default=None,
                        help='Start at specific offset (default: auto-resume)')
    parser.add_argument('--reset-failed', action='store_true',
                        help='Retry institutions with scrape errors')
    parser.add_argument('--stats',        action='store_true',
                        help='Show coverage stats and exit')
    args = parser.parse_args()

    conn = get_db()

    if args.stats:
        show_stats(conn)
        return

    # Build WHERE clause
    type_filter = ''
    if args.type == 'cu':
        type_filter = "AND type='cu'"
    elif args.type == 'bank':
        type_filter = "AND type='bank'"

    # Auto-resume: skip already-scraped institutions
    scraped_filter = 'AND last_scraped_at IS NULL'
    if args.reset_failed:
        scraped_filter = "AND (loan_scrape_status='error' OR mortgage_scrape_status='error')"

    offset_clause = f'OFFSET {args.offset}' if args.offset is not None else ''

    institutions = conn.execute(f"""
        SELECT id, name, type, website_url, loan_rates_url, mortgage_rates_url,
               loan_raw_section, mortgage_raw_section
        FROM institutions
        WHERE active=1 {type_filter} {scraped_filter}
        ORDER BY name
        LIMIT {args.limit} {offset_clause}
    """).fetchall()

    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}] "
          f"Processing {len(institutions)} institutions (type={args.type}, limit={args.limit})")

    total_loan = total_mtg = skipped = errors = 0

    for i, inst in enumerate(institutions):
        try:
            loan_n, mtg_n, status = process_institution(conn, inst)
            total_loan += loan_n
            total_mtg += mtg_n
            if status == 'skipped':
                skipped += 1
            if (i + 1) % 100 == 0:
                print(f"  [{i+1}/{len(institutions)}] loan_rates={total_loan} mtg_rates={total_mtg}")
        except Exception as e:
            errors += 1
            conn.execute('UPDATE institutions SET loan_scrape_status=? WHERE id=?',
                        ('error', inst['id']))
            conn.commit()

    print(f"\n[DONE] processed={len(institutions)} loan_rates={total_loan} "
          f"mtg_rates={total_mtg} skipped={skipped} errors={errors}")

    show_stats(conn)
    conn.close()


if __name__ == '__main__':
    main()
