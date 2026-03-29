#!/usr/bin/env python3
"""
Phase 2: Scrape + Parse
For all institutions with URLs, scrape with Jina (fast) then Playwright (JS fallback),
then GPT-parse and insert rates.

If a URL returns no rates (404, redirect, or empty content), triggers automatic
URL re-discovery via Phase 1 logic before giving up.

Run after phase1_brave_discovery.py.

Usage:
    python3 jobs/phase2_scrape.py               # all with URLs, not yet scraped
    python3 jobs/phase2_scrape.py --type cu
    python3 jobs/phase2_scrape.py --limit 1000
    python3 jobs/phase2_scrape.py --playwright-only   # only retry no_rates with Playwright
    python3 jobs/phase2_scrape.py --check-stale       # re-scrape existing rates, fix broken URLs
    python3 jobs/phase2_scrape.py --stale-days 90     # consider URLs stale after N days (default 90)
    python3 jobs/phase2_scrape.py --stats
"""

import argparse, sqlite3, json, time, datetime, re, sys, os, requests

DB_PATH     = os.path.join(os.path.dirname(__file__), '..', 'db', 'rates.db')
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config.json')

with open(CONFIG_PATH) as f:
    config = json.load(f)

OPENAI_KEY   = config['openai_api_key']
SCRAPED_WEEK = datetime.date.today().strftime('%Y-%W')
NOW          = datetime.datetime.now().isoformat()
UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'

SKIP_KEYWORDS = [
    'trust company','private wealth','private bank','federal home loan',
    'federal reserve','bankers bank','industrial loan','investment bank',
]


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.row_factory = sqlite3.Row
    return conn


def jina_fetch(url):
    try:
        r = requests.get(f'https://r.jina.ai/{url}', headers={'Accept': 'text/plain'}, timeout=12)
        return r.text[:25000] if r.status_code == 200 else None
    except: return None


def playwright_fetch(url):
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent=UA)
            page = ctx.new_page()
            page.goto(url, wait_until='networkidle', timeout=25000)
            page.wait_for_timeout(4000)
            page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            page.wait_for_timeout(2000)
            text = page.inner_text('body')
            browser.close()
            return text[:25000] if text else None
    except: return None


def has_rates(text):
    if not text: return False
    lines = [l for l in text.split('\n')
             if '%' in l and any(c.isdigit() for c in l) and len(l) < 200]
    return len(lines) >= 2


def url_is_alive(url):
    """Quick HEAD check — returns (alive, status_code, final_url)."""
    try:
        r = requests.head(url, timeout=8, allow_redirects=True,
                         headers={'User-Agent': UA})
        final = r.url
        # Consider alive if 200 and not redirected to a completely different domain
        orig_domain = re.sub(r'^https?://(www\.)?', '', url).split('/')[0]
        final_domain = re.sub(r'^https?://(www\.)?', '', final).split('/')[0]
        same_domain = orig_domain.lower() in final_domain.lower()
        return r.status_code == 200 and same_domain, r.status_code, final
    except:
        return False, 0, url


def rediscover_url(inst_id, name, domain, ptype, deposit_url, inst_type, conn):
    """
    A known URL has gone stale (404, redirect off-domain, or no rates).
    Try to find the new URL using:
    1. Deposit URL derivation (if we have a deposit URL)
    2. Brave search with updated queries
    3. Path probing on the domain
    Returns (new_url, score) or (None, 0).
    """
    # Import Phase 1 discovery logic
    sys.path.insert(0, os.path.dirname(__file__))
    try:
        from phase1_brave_discovery import find_best_url
        url, score = find_best_url(name, domain, ptype, inst_type, deposit_url=deposit_url)
        if url and score >= 30:
            # Save the new URL
            col = {'loan': 'loan_rates_url', 'mortgage': 'mortgage_rates_url',
                   'deposit': 'rates_url'}.get(ptype, 'rates_url')
            conn.execute(f'UPDATE institutions SET {col}=? WHERE id=?', (url, inst_id))
            conn.commit()
            return url, score
    except Exception as e:
        pass
    return None, 0


def scrape(url, use_playwright=True):
    """Jina first, Playwright fallback if no rates found."""
    text = jina_fetch(url)
    if text and has_rates(text):
        return text, 'jina'
    if use_playwright:
        text2 = playwright_fetch(url)
        if text2 and has_rates(text2):
            return text2, 'playwright'
    return text, 'no_rates'


def scrape_with_stale_detection(url, inst_id, name, domain, ptype, inst_type,
                                 deposit_url, conn, use_playwright=True):
    """
    Scrape a URL. If it returns no rates:
    1. Check if the URL is still alive (HEAD request)
    2. If dead/redirected: trigger URL re-discovery, retry on new URL
    3. If alive but no rates: try Playwright, then give up
    Returns (text, method, final_url_used).
    """
    # First attempt
    text, method = scrape(url, use_playwright) if use_playwright else (jina_fetch(url), 'jina')
    if method not in ('no_rates',) and text and has_rates(text):
        return text, method, url

    # No rates — check if URL is still valid
    alive, status, final_url = url_is_alive(url)

    if not alive or status in (404, 410, 301, 302):
        # URL is dead or redirected off-domain — find the new one
        print(f'    ⚠ Stale URL ({status}): {url}', flush=True)
        new_url, score = rediscover_url(inst_id, name, domain, ptype, deposit_url, inst_type, conn)
        if new_url and new_url != url:
            print(f'    ✅ New URL found (score={score}): {new_url}', flush=True)
            text2, method2 = scrape(new_url, use_playwright)
            if text2 and has_rates(text2):
                return text2, f'{method2}+rediscovered', new_url
        return text, 'stale_url', url

    # URL alive but no rates — Playwright already tried, give up
    return text, 'no_rates', url


PROMPT_LOAN = """Extract loan rates for {name}. Return JSON array, each item:
- product: new_auto_loan | used_auto_loan | personal_loan | home_equity_loan
- term_months: integer or null
- rate: PERCENTAGE float (e.g. 4.74) or null if only APR shown
- apr: PERCENTAGE float if shown separately, null if not. APR-only: set apr=val, rate=null.
- loan_term_label: string like "36 months" or null
- notes: brief note
Use lowest rate for ranges. Return [] if none found.
Content: {content}"""

PROMPT_MORTGAGE = """Extract mortgage rates for {name}. Return JSON array, each item:
- product: mortgage_fixed | mortgage_arm
- term_months: integer (360=30yr,240=20yr,180=15yr,120=10yr) or null for ARM
- arm_initial_years: integer or null
- arm_adjust_months: integer or null
- rate: PERCENTAGE float or null if only APR shown
- apr: PERCENTAGE float if shown separately, null if not
- conforming: 1=conforming, 0=jumbo, null=unclear
- notes: brief note
Return [] if none found.
Content: {content}"""

PROMPT_DEPOSIT = """Extract deposit rates for {name}. Return JSON array, each item:
- product: cd | savings | money_market | checking | ira_cd
- term_months: integer for CDs (e.g. 12,24,36,48,60) or null for liquid
- apy: PERCENTAGE float (e.g. 4.50 for 4.50%) or null if only APY not shown
- min_balance: integer in dollars (e.g. 1000) or null
- notes: brief note (e.g. "12mo CD", "high-yield savings", "promotional rate")
Use highest APY for ranges. Return [] if none found.
Content: {content}"""

PROMPTS = {'loan': PROMPT_LOAN, 'mortgage': PROMPT_MORTGAGE, 'deposit': PROMPT_DEPOSIT}

MODEL_FAST   = 'gpt-4o-mini'
MODEL_STRONG = 'gpt-4o'


def score_extraction(items, ptype, raw_text):
    """
    Score the quality of extracted rates 0-100.
    Low score → escalate to stronger model.

    Signals:
    - Number of items extracted (more = better up to a point)
    - Rate values are in plausible range
    - No suspiciously round numbers (3.0, 5.0 only) — suggests hallucination
    - Rate count matches % count in source text (catch under-extraction)
    - All required fields populated
    """
    if not items:
        return 0  # nothing extracted

    score = 0
    n = len(items)

    # Count % signs in source as proxy for expected rate rows
    pct_count = len([l for l in raw_text.split('\n')
                     if '%' in l and any(c.isdigit() for c in l) and len(l) < 200])

    # Reward extraction yield relative to source richness
    if pct_count > 0:
        yield_ratio = min(n / pct_count, 1.5)
        score += int(yield_ratio * 30)  # up to 30 pts
    else:
        score += min(n * 5, 20)

    # Rate value plausibility
    valid_rates = 0
    round_only  = True
    for item in items:
        rate = item.get('rate') or item.get('apr') or item.get('apy')
        if rate is not None:
            try:
                f = float(rate)
                if ptype in ('loan', 'mortgage'):
                    if 1.0 <= f <= 30.0:
                        valid_rates += 1
                    if f % 1.0 != 0:  # has decimal → not purely round
                        round_only = False
                elif ptype == 'deposit':
                    if 0.01 <= f <= 15.0:
                        valid_rates += 1
                    if f % 1.0 != 0:
                        round_only = False
            except:
                pass

    if n > 0:
        score += int((valid_rates / n) * 35)  # up to 35 pts for valid values

    # Penalty for all-round numbers (3.0, 5.0 etc.) — suggests hallucination
    if round_only and n > 2:
        score -= 20

    # Field completeness
    required = {
        'loan':     ['product', 'term_months'],
        'mortgage': ['product', 'term_months'],
        'deposit':  ['product', 'apy'],
    }.get(ptype, [])
    complete = sum(1 for item in items
                   if all(item.get(f) is not None for f in required))
    if n > 0:
        score += int((complete / n) * 20)  # up to 20 pts

    # Variety bonus — multiple products/terms extracted
    if ptype == 'deposit':
        products = {item.get('product') for item in items}
        score += min(len(products) * 3, 10)
    else:
        terms = {item.get('term_months') for item in items if item.get('term_months')}
        score += min(len(terms) * 3, 10)

    return max(0, min(score, 100))


def _call_gpt(client, model, prompt):
    """Raw GPT call, returns list or []."""
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{'role': 'user', 'content': prompt}],
            response_format={'type': 'json_object'},
            temperature=0)
        raw = json.loads(resp.choices[0].message.content)
        if isinstance(raw, list): return raw
        for k in ['rates','data','results','items','loans','mortgages',
                  'deposits','cds','savings']:
            if k in raw and isinstance(raw[k], list): return raw[k]
        return []
    except: return []


def gpt_extract(content, ptype, name, escalate_threshold=55):
    """
    Two-pass extraction:
    1. gpt-4o-mini (fast, cheap)
    2. If quality score < escalate_threshold → escalate to gpt-4o (strong)
    Returns (items, model_used, quality_score)
    """
    import openai
    client  = openai.OpenAI(api_key=OPENAI_KEY)
    prompt  = PROMPTS[ptype].format(name=name, content=content[:7000])

    # Pass 1: mini
    items = _call_gpt(client, MODEL_FAST, prompt)
    score = score_extraction(items, ptype, content)

    if score >= escalate_threshold:
        return items, MODEL_FAST, score

    # Pass 2: escalate to gpt-4o
    items2 = _call_gpt(client, MODEL_STRONG, prompt)
    score2 = score_extraction(items2, ptype, content)

    # Take whichever scored better
    if score2 >= score:
        return items2, MODEL_STRONG, score2
    return items, MODEL_FAST, score


def normalize(v):
    if v is None: return None
    try: f = float(v); return f/100 if f > 0.40 else f
    except: return None


def insert_rates(conn, inst_id, items, ptype):
    n = 0
    for r in items:
        apy = normalize(r.get('rate'))
        apr = normalize(r.get('apr'))
        if apy is None and apr is None: continue
        prod = r.get('product', 'new_auto_loan' if ptype == 'loan' else 'mortgage_fixed')
        try:
            conn.execute("""INSERT OR IGNORE INTO rates
                (institution_id,scraped_at,scraped_week,product,apy,apr,term_months,
                 loan_term_label,arm_initial_years,arm_adjust_months,conforming,notes,rate_type,confidence)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (inst_id,NOW,SCRAPED_WEEK,prod,apy,apr,
                 r.get('term_months'),r.get('loan_term_label'),
                 r.get('arm_initial_years'),r.get('arm_adjust_months'),
                 r.get('conforming'),r.get('notes'),
                 'fixed' if prod=='mortgage_fixed' else ('arm' if prod=='mortgage_arm' else None),
                 'unverified'))
            n += 1
        except: pass
    conn.commit()
    return n


def show_stats(conn):
    total    = conn.execute('SELECT COUNT(*) FROM institutions WHERE active=1').fetchone()[0]
    has_loan = conn.execute("SELECT COUNT(DISTINCT institution_id) FROM rates r JOIN institutions i ON i.id=r.institution_id WHERE product LIKE '%auto%' OR product='personal_loan'").fetchone()[0]
    has_mtg  = conn.execute("SELECT COUNT(DISTINCT institution_id) FROM rates r JOIN institutions i ON i.id=r.institution_id WHERE product LIKE 'mortgage%'").fetchone()[0]
    rows     = conn.execute('SELECT COUNT(*) FROM rates').fetchone()[0]
    scraped  = conn.execute("SELECT COUNT(*) FROM institutions WHERE last_scraped_at IS NOT NULL AND active=1").fetchone()[0]
    no_rates = conn.execute("SELECT COUNT(*) FROM institutions WHERE loan_scrape_status='no_rates' AND active=1").fetchone()[0]
    print(f"""
=== Coverage ===
Total institutions:   {total:,}
Scraped (attempted):  {scraped:,} ({scraped/total*100:.1f}%)
No-rates (confirmed): {no_rates:,}
With loan rates:      {has_loan:,} ({has_loan/total*100:.1f}%)
With mortgage rates:  {has_mtg:,} ({has_mtg/total*100:.1f}%)
Total rate rows:      {rows:,}
Remaining:            {total-scraped:,}
""")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--type',            choices=['cu','bank','all'], default='all')
    parser.add_argument('--limit',           type=int, default=99999)
    parser.add_argument('--playwright-only', action='store_true',
                        help='Only re-run institutions marked no_rates, using Playwright only')
    parser.add_argument('--check-stale',     action='store_true',
                        help='Re-scrape institutions with existing rates to find broken URLs')
    parser.add_argument('--stale-days',      type=int, default=90,
                        help='Re-scrape institutions not scraped in N days (default: 90)')
    parser.add_argument('--stats',           action='store_true')
    args = parser.parse_args()

    conn = get_db()
    if args.stats:
        show_stats(conn)
        return

    type_filter = ''
    if args.type == 'cu':   type_filter = "AND i.type='cu'"
    elif args.type == 'bank': type_filter = "AND i.type='bank'"

    stale_cutoff = (datetime.datetime.now() - datetime.timedelta(days=args.stale_days)).isoformat()

    if args.check_stale:
        # Re-check institutions whose rates are old — find broken URLs
        where = f"""WHERE i.active=1
            AND i.last_scraped_at < '{stale_cutoff}'
            AND (i.loan_rates_url IS NOT NULL OR i.mortgage_rates_url IS NOT NULL)
            {type_filter}"""
    elif args.playwright_only:
        where = f"WHERE i.active=1 AND (i.loan_scrape_status='no_rates' OR i.mortgage_scrape_status='no_rates') {type_filter}"
    else:
        # Normal: process institutions with URLs not yet scraped
        where = f"WHERE i.active=1 AND i.last_scraped_at IS NULL AND (i.loan_rates_url IS NOT NULL OR i.mortgage_rates_url IS NOT NULL) {type_filter}"

    institutions = conn.execute(f"""
        SELECT i.id, i.name, i.type, i.website_url, i.rates_url,
               i.loan_rates_url, i.mortgage_rates_url,
               i.loan_raw_section, i.mortgage_raw_section,
               i.loan_scrape_status, i.mortgage_scrape_status,
               i.last_scraped_at
        FROM institutions i
        {where}
        ORDER BY i.name
        LIMIT {args.limit}
    """).fetchall()

    use_playwright = True

    print(f"[Phase 2] Scraping {len(institutions)} institutions (playwright={'playwright-only' if args.playwright_only else 'fallback'})")

    total_loan = total_mtg = 0

    for i, inst in enumerate(institutions):
        inst_id    = inst['id']
        name       = inst['name']
        inst_type  = inst['type']
        website    = inst['website_url'] or ''
        domain     = re.sub(r'^https?://(www\.)?', '', website).split('/')[0]
        deposit_url = inst['rates_url']

        if any(k in name.lower() for k in SKIP_KEYWORDS):
            conn.execute('UPDATE institutions SET last_scraped_at=? WHERE id=?', (NOW, inst_id))
            conn.commit()
            continue

        # In stale-check mode, always re-scrape regardless of existing rates
        force = args.check_stale

        ex_loan = conn.execute("SELECT COUNT(*) FROM rates WHERE institution_id=? AND (product LIKE '%auto%' OR product='personal_loan')", (inst_id,)).fetchone()[0]
        ex_mtg  = conn.execute("SELECT COUNT(*) FROM rates WHERE institution_id=? AND product LIKE 'mortgage%'", (inst_id,)).fetchone()[0]

        loan_url = inst['loan_rates_url']
        mtg_url  = inst['mortgage_rates_url']

        # ── Loans ──────────────────────────────────────────────────────────────
        if loan_url and (not ex_loan or force):
            if args.playwright_only:
                raw = playwright_fetch(loan_url)
                method = 'playwright' if (raw and has_rates(raw)) else 'no_rates'
                final_url = loan_url
            else:
                raw, method, final_url = scrape_with_stale_detection(
                    loan_url, inst_id, name, domain, 'loan', inst_type, deposit_url, conn)

            if raw and method not in ('no_rates', 'stale_url'):
                conn.execute('UPDATE institutions SET loan_raw_section=?, loan_scrape_status=? WHERE id=?',
                            (raw[:25000], 'ok', inst_id))
                conn.commit()
                extracted, model_used, quality = gpt_extract(raw, 'loan', name)
                n = insert_rates(conn, inst_id, extracted, 'loan')
                total_loan += n
                escalated = '⬆' if model_used == MODEL_STRONG else ''
                if n: print(f'  LOAN {name}: {n} rates | q={quality}{escalated} ({method})', flush=True)
            else:
                status = 'stale_url' if method == 'stale_url' else 'no_rates'
                conn.execute('UPDATE institutions SET loan_scrape_status=? WHERE id=?', (status, inst_id))
                conn.commit()

        # ── Mortgages ──────────────────────────────────────────────────────────
        if mtg_url and (not ex_mtg or force):
            if mtg_url == loan_url:
                row = conn.execute('SELECT loan_raw_section FROM institutions WHERE id=?', (inst_id,)).fetchone()
                raw, method, final_url = (row[0], 'cached', mtg_url) if row and row[0] else (None, 'no_raw', mtg_url)
            elif args.playwright_only:
                raw = playwright_fetch(mtg_url)
                method = 'playwright' if (raw and has_rates(raw)) else 'no_rates'
                final_url = mtg_url
            else:
                raw, method, final_url = scrape_with_stale_detection(
                    mtg_url, inst_id, name, domain, 'mortgage', inst_type, deposit_url, conn)

            if raw and method not in ('no_rates', 'stale_url', 'no_raw'):
                conn.execute('UPDATE institutions SET mortgage_raw_section=?, mortgage_scrape_status=? WHERE id=?',
                            (raw[:25000], 'ok', inst_id))
                conn.commit()
                extracted, model_used, quality = gpt_extract(raw, 'mortgage', name)
                n = insert_rates(conn, inst_id, extracted, 'mortgage')
                total_mtg += n
                escalated = '⬆' if model_used == MODEL_STRONG else ''
                if n: print(f'  MTG  {name}: {n} rates | q={quality}{escalated} ({method})', flush=True)
            elif raw is None or method in ('no_rates', 'stale_url'):
                status = 'stale_url' if method == 'stale_url' else 'no_rates'
                conn.execute('UPDATE institutions SET mortgage_scrape_status=? WHERE id=?', (status, inst_id))
                conn.commit()

        conn.execute('UPDATE institutions SET last_scraped_at=? WHERE id=?', (NOW, inst_id))
        conn.commit()

        if (i + 1) % 100 == 0:
            print(f'  [{i+1}/{len(institutions)}] loan={total_loan} mtg={total_mtg}')

    print(f'\n[Phase 2 DONE] loan_rates={total_loan} mtg_rates={total_mtg}')
    show_stats(conn)
    conn.close()


if __name__ == '__main__':
    main()
