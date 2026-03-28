#!/usr/bin/env python3
"""
National Rate Discovery Worker
Assigned a slice of institutions — Brave finds URLs, Jina/Playwright scrapes, GPT parses.
Usage: python3 national_swarm_worker.py --offset 0 --limit 600 --worker-id 1
"""

import argparse, sqlite3, json, time, datetime, sys, os, re, requests
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'rates.db')
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config.json')

# Load config
with open(CONFIG_PATH) as f:
    config = json.load(f)

OPENAI_KEY  = config['openai_api_key']
OPENAI_MODEL = 'gpt-4o-mini'
BRAVE_KEY   = 'BSAV_DCBYpxwTArxRNJG6T-jMyfh7U4'
SCRAPED_WEEK = datetime.date.today().strftime('%Y-%W')
NOW          = datetime.datetime.now().isoformat()

# ── Skip list (private banking / no retail rates) ───────────────────────────
SKIP_KEYWORDS = [
    'trust company', 'private wealth', 'private bank', 'investment bank',
    'federal home loan', 'federal reserve', 'savings association',
    'industrial bank', 'bankers bank', 'bankers trust',
]

# ── Helpers ──────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.row_factory = sqlite3.Row
    return conn

def brave_search(query, count=3):
    try:
        r = requests.get(
            'https://api.search.brave.com/res/v1/web/search',
            headers={'Accept': 'application/json', 'X-Subscription-Token': BRAVE_KEY},
            params={'q': query, 'count': count},
            timeout=10
        )
        return r.json().get('web', {}).get('results', [])
    except Exception as e:
        return []

def jina_fetch(url, timeout=15):
    try:
        r = requests.get(f'https://r.jina.ai/{url}',
                        headers={'Accept': 'text/plain'}, timeout=timeout)
        return r.text[:25000] if r.status_code == 200 else None
    except:
        return None

def has_rates(text):
    if not text:
        return False
    rate_lines = [l for l in text.split('\n')
                  if '%' in l and any(c.isdigit() for c in l) and len(l) < 200]
    return len(rate_lines) >= 2

def gpt_extract(content, prompt_type, inst_name):
    import openai
    client = openai.OpenAI(api_key=OPENAI_KEY)

    if prompt_type == 'loan':
        prompt = f"""Extract loan rates from the following content for {inst_name}.

Return a JSON array. Each element:
- product: "new_auto_loan" | "used_auto_loan" | "personal_loan" | "home_equity_loan"
- term_months: integer or null
- rate: float — base interest rate as PERCENTAGE (e.g. 4.74), null if only APR shown
- apr: float — APR as PERCENTAGE if separately labeled, null if not shown. If only one rate labeled "APR", put in apr, leave rate null.
- vehicle_age_years: integer for used auto, null otherwise
- loan_term_label: string like "36 months", null
- notes: brief note

Rules: use lowest rate for ranges; return [] if no loan rates found.

Content:
{content[:8000]}"""
    else:
        prompt = f"""Extract mortgage rates from the following content for {inst_name}.

Return a JSON array. Each element:
- product: "mortgage_fixed" | "mortgage_arm"
- term_months: integer (360=30yr,240=20yr,180=15yr,120=10yr) or null for ARM
- arm_initial_years: integer or null
- arm_adjust_months: integer or null
- rate: float — interest rate as PERCENTAGE, null if only APR shown
- apr: float — APR as PERCENTAGE if shown separately, null if not
- conforming: 1=conforming, 0=jumbo, null=unclear
- notes: brief note

Rules: extract both rate+APR if both shown; return [] if no mortgage rates found.

Content:
{content[:8000]}"""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{'role': 'user', 'content': prompt}],
            response_format={'type': 'json_object'},
            temperature=0,
        )
        raw = resp.choices[0].message.content
        # handle both array and {rates:[...]} responses
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
        for k in ['rates', 'data', 'results', 'items']:
            if k in parsed and isinstance(parsed[k], list):
                return parsed[k]
        return []
    except Exception as e:
        return []

def normalize(val):
    if val is None: return None
    if val > 0.40: return val / 100
    return val

def insert_rates(conn, inst_id, rates_data, prompt_type):
    inserted = 0
    for r in rates_data:
        rate_val = normalize(r.get('rate'))
        apr_val  = normalize(r.get('apr'))
        if rate_val is None and apr_val is None:
            continue
        try:
            conn.execute("""
                INSERT INTO rates (institution_id, scraped_at, scraped_week, product,
                    apy, apr, term_months, loan_term_label, vehicle_age_years,
                    arm_initial_years, arm_adjust_months, conforming,
                    notes, rate_type, confidence)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                inst_id, NOW, SCRAPED_WEEK,
                r.get('product', 'new_auto_loan' if prompt_type == 'loan' else 'mortgage_fixed'),
                rate_val, apr_val,
                r.get('term_months'), r.get('loan_term_label'), r.get('vehicle_age_years'),
                r.get('arm_initial_years'), r.get('arm_adjust_months'), r.get('conforming'),
                r.get('notes'),
                'fixed' if r.get('product') == 'mortgage_fixed' else ('arm' if r.get('product') == 'mortgage_arm' else None),
                'unverified'
            ))
            inserted += 1
        except Exception as e:
            pass
    conn.commit()
    return inserted

# ── Main worker loop ──────────────────────────────────────────────────────────

def process_institution(conn, inst):
    inst_id   = inst['id']
    name      = inst['name']
    website   = inst['website_url'] or ''
    inst_type = inst['type']

    # Skip private banking / non-retail
    name_lower = name.lower()
    if any(kw in name_lower for kw in SKIP_KEYWORDS):
        return {'id': inst_id, 'name': name, 'status': 'skipped', 'loan': 0, 'mtg': 0}

    loan_url = inst['loan_rates_url']
    mtg_url  = inst['mortgage_rates_url']
    results  = {'id': inst_id, 'name': name, 'status': 'ok', 'loan': 0, 'mtg': 0}

    # ── Step 1: Brave URL discovery if missing ──
    domain = re.sub(r'^https?://(www\.)?', '', website).split('/')[0] if website else name

    if not loan_url:
        hits = brave_search(f'{name} auto loan rates {domain}', count=3)
        for h in hits:
            url = h.get('url', '')
            if domain in url and any(k in url.lower() for k in ['rate', 'loan', 'borrow']):
                loan_url = url
                break
        if not loan_url and hits:
            loan_url = hits[0].get('url')
        if loan_url:
            conn.execute('UPDATE institutions SET loan_rates_url=? WHERE id=?', (loan_url, inst_id))
            conn.commit()

    if not mtg_url:
        hits = brave_search(f'{name} mortgage rates {domain}', count=3)
        for h in hits:
            url = h.get('url', '')
            if domain in url and any(k in url.lower() for k in ['rate', 'mortgage', 'home-loan']):
                mtg_url = url
                break
        if not mtg_url and hits:
            mtg_url = hits[0].get('url')
        if mtg_url:
            conn.execute('UPDATE institutions SET mortgage_rates_url=? WHERE id=?', (mtg_url, inst_id))
            conn.commit()

    time.sleep(0.2)  # brief pause between Brave calls

    # ── Step 2: Check existing raw content or scrape ──
    loan_raw = inst['loan_raw_section']
    mtg_raw  = inst['mortgage_raw_section']

    # Check if already has rates parsed
    existing_loan = conn.execute(
        "SELECT COUNT(*) FROM rates WHERE institution_id=? AND (product LIKE '%auto%' OR product='personal_loan')",
        (inst_id,)).fetchone()[0]
    existing_mtg = conn.execute(
        "SELECT COUNT(*) FROM rates WHERE institution_id=? AND product LIKE 'mortgage%'",
        (inst_id,)).fetchone()[0]

    # Scrape loan if needed
    if not existing_loan and loan_url and not loan_raw:
        loan_raw = jina_fetch(loan_url)
        if loan_raw:
            conn.execute('UPDATE institutions SET loan_raw_section=?, loan_scrape_status=? WHERE id=?',
                        (loan_raw[:25000], 'ok', inst_id))
            conn.commit()

    # Scrape mortgage if needed
    if not existing_mtg and mtg_url and not mtg_raw:
        # avoid re-fetching if same URL as loan
        if mtg_url == loan_url and loan_raw:
            mtg_raw = loan_raw
        else:
            mtg_raw = jina_fetch(mtg_url)
        if mtg_raw:
            conn.execute('UPDATE institutions SET mortgage_raw_section=?, mortgage_scrape_status=? WHERE id=?',
                        (mtg_raw[:25000], 'ok', inst_id))
            conn.commit()

    # ── Step 3: GPT parse if we have raw content ──
    if not existing_loan and loan_raw and has_rates(loan_raw):
        extracted = gpt_extract(loan_raw, 'loan', name)
        results['loan'] = insert_rates(conn, inst_id, extracted, 'loan')

    if not existing_mtg and mtg_raw and has_rates(mtg_raw):
        extracted = gpt_extract(mtg_raw, 'mortgage', name)
        results['mtg'] = insert_rates(conn, inst_id, extracted, 'mortgage')

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--offset',    type=int, required=True)
    parser.add_argument('--limit',     type=int, required=True)
    parser.add_argument('--worker-id', type=int, default=1)
    parser.add_argument('--type',      choices=['cu', 'bank', 'all'], default='all')
    args = parser.parse_args()

    conn = get_db()

    where = ''
    if args.type == 'cu':
        where = "AND type='cu'"
    elif args.type == 'bank':
        where = "AND type='bank'"

    institutions = conn.execute(f"""
        SELECT id, name, type, website_url, loan_rates_url, mortgage_rates_url,
               loan_raw_section, mortgage_raw_section
        FROM institutions
        WHERE active=1 {where}
        ORDER BY name
        LIMIT ? OFFSET ?
    """, (args.limit, args.offset)).fetchall()

    print(f"[Worker {args.worker_id}] Processing {len(institutions)} institutions (offset={args.offset})")

    summary = {'total': len(institutions), 'loan_rates': 0, 'mtg_rates': 0, 'skipped': 0, 'errors': 0}

    for i, inst in enumerate(institutions):
        try:
            result = process_institution(conn, inst)
            summary['loan_rates'] += result['loan']
            summary['mtg_rates']  += result['mtg']
            if result['status'] == 'skipped':
                summary['skipped'] += 1
            if (i + 1) % 50 == 0:
                print(f"[Worker {args.worker_id}] {i+1}/{len(institutions)} — loan_rates={summary['loan_rates']} mtg_rates={summary['mtg_rates']}")
        except Exception as e:
            summary['errors'] += 1

    # Write summary
    out_path = f'/tmp/worker_{args.worker_id}_results.json'
    with open(out_path, 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"[Worker {args.worker_id}] DONE — {summary}")
    conn.close()

if __name__ == '__main__':
    main()
