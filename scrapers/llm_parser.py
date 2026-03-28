"""
LLM Rate Extractor + Self-Verifier
Supports two backends:
  - OpenAI (default): gpt-4o-mini — fast, cheap (~$0.001/institution)
    Two-pass fallback: if mini returns 0 rates, retries with gpt-4o automatically.
  - Ollama (fallback): qwen2.5:14b — local, free, slow

Two tasks:
  1. Extract: pull structured rates from raw_section
  2. Verify: confirm each extracted rate actually appears in the text
Both use the stored raw_section — no re-scraping needed.

Config loaded from: ../config.json (relative to this file)
  openai_api_key        — API key (falls back to OPENAI_API_KEY env var)
  openai_model          — primary model   (default: gpt-4o-mini)
  openai_fallback_model — fallback model  (default: gpt-4o)
"""
import urllib.request, json, re, time, os, threading
from datetime import datetime, timezone, date
from schema import get_conn


# ── Config loading ────────────────────────────────────────────────────────────
def _load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    try:
        with open(config_path) as f:
            return json.load(f)
    except Exception:
        return {}

_CONFIG = _load_config()

PRODUCT_GROUP_MAP = {
    'savings':        'deposit_liquid',
    'checking':       'deposit_liquid',
    'money_market':   'deposit_liquid',
    'cd':             'deposit_term',
    'ira_cd':         'deposit_term',
    'mortgage':       'loan_secured',
    'home_equity':    'loan_secured',
    'auto_loan':      'loan_secured',
    'personal_loan':  'loan_unsecured',
    'credit_card':    'loan_unsecured',
    'new_auto_loan':  'loan_secured',
    'used_auto_loan': 'loan_secured',
    'mortgage_fixed': 'loan_secured',
    'mortgage_arm':   'loan_secured',
}

def current_week():
    """Returns ISO week string 'YYYY-WW'."""
    iso = date.today().isocalendar()
    return f"{iso[0]}-{iso[1]:02d}"

# ── Backend config ────────────────────────────────────────────────────────────
def _get_openai_key():
    # 1. config.json (preferred)
    key = _CONFIG.get('openai_api_key')
    if key:
        return key
    # 2. environment variable
    key = os.environ.get('OPENAI_API_KEY')
    if key:
        return key
    # 3. 1Password fallback
    try:
        import subprocess
        result = subprocess.run(
            'source ~/.op_service_account && op item get "OpenAI API Credentials" --vault ClawdBotVault --fields credential --reveal',
            shell=True, capture_output=True, text=True, executable='/bin/zsh'
        )
        return result.stdout.strip() or None
    except Exception:
        return None

OPENAI_API_KEY       = _get_openai_key()
OPENAI_MODEL         = _CONFIG.get('openai_model', 'gpt-4.1-mini')         # primary: fast + cheap
OPENAI_FALLBACK_MODEL= _CONFIG.get('openai_fallback_model', 'gpt-4.1')     # fallback
OPENAI_MODEL_PRO     = OPENAI_FALLBACK_MODEL                                # alias kept for compatibility
OPENAI_URL           = 'https://api.openai.com/v1/chat/completions'

OLLAMA_URL     = 'http://localhost:11434/api/generate'
OLLAMA_MODEL   = 'qwen2.5:14b'

USE_OPENAI     = True  # toggled at runtime via --model flag
CALL_DELAY     = 0.1   # seconds between calls (rate limiter handles pacing)

# ── Global rate limiter: max 1 OpenAI call per 6s across ALL threads ─────────
_openai_lock      = threading.Lock()
_openai_last_call = 0.0
OPENAI_MIN_GAP    = 6.0  # seconds between calls (10/min = safe under Tier 1 RPM + TPM)

def _openai_rate_wait():
    """Block until it's safe to make another OpenAI call."""
    global _openai_last_call
    with _openai_lock:
        now   = time.time()
        gap   = now - _openai_last_call
        if gap < OPENAI_MIN_GAP:
            time.sleep(OPENAI_MIN_GAP - gap)
        _openai_last_call = time.time()

# ── Prompt: Extract ──────────────────────────────────────────────────────────
EXTRACT_PROMPT = """Extract ALL financial rates from this page content for {institution}.

Return ONLY a JSON array. Each item:
  "product": one of savings|checking|money_market|cd|ira_cd|mortgage|home_equity|auto_loan|personal_loan|credit_card
  "term_months": integer (null for savings/checking/MM; months for CDs and loans e.g. 360=30yr mortgage)
  "apy": DECIMAL form — 5.00% → 0.05, 4.50% → 0.045, 0.50% → 0.005 (null if not shown)
  "min_balance": dollars minimum for this tier (null if not stated)
  "notes": balance range, conditions, or null. If only APR (not APY) was shown, add "APR" to notes.

RULES:
- Decimals only. Never return 5.0 meaning 5% — that must be 0.05.
- APR vs APY: If the page shows APR instead of (or alongside) APY, use the APR value as "apy" and add "APR" to notes. Do NOT skip a product just because it shows APR.
- Include deposits (savings, checking, CD, money market) AND loans (mortgage, auto, HELOC, personal).
- Only rates explicitly shown — never guess.
- CD terms: term_months = the number of months. Convert weeks to months (13 weeks = 3mo, 26 weeks = 6mo, 52 weeks = 12mo). Accept any term from 1–120 months.
- TIERED RATES: If a product has multiple tiers by balance, emit ONE entry per tier.
  Example — Money Market with 3 tiers:
    {{"product":"money_market","term_months":null,"apy":0.005,"min_balance":0,"notes":"$0–$4,999"}}
    {{"product":"money_market","term_months":null,"apy":0.0075,"min_balance":5000,"notes":"$5,000–$49,999"}}
    {{"product":"money_market","term_months":null,"apy":0.01,"min_balance":50000,"notes":"$50,000+"}}
- Do NOT collapse tiers into one row. Do NOT pick only the highest.
- Return [] ONLY if truly no numeric rates appear on the page.

Page content:
{page_text}

JSON array:"""

# ── Prompt: Verify ───────────────────────────────────────────────────────────
VERIFY_PROMPT = """You are auditing extracted rate data for {institution}.

For each rate below, answer whether that EXACT percentage appears in the page content.
Return ONLY a JSON array with the same items plus:
  "verified": true if the exact % number appears on the page, false otherwise
  "snippet": the exact text from the page confirming it (or null)

Rates to verify:
{rates_json}

Page content:
{page_text}

JSON array:"""


def call_openai(prompt, timeout=30, model=None):
    """Call OpenAI chat completions. Returns response string or None.
    Retries up to 5x on 429 rate limits with exponential backoff."""
    payload = json.dumps({
        'model':       model or OPENAI_MODEL,
        'messages':    [{'role': 'user', 'content': prompt}],
        'temperature': 0,
        'max_tokens':  4000,
    }).encode()
    req = urllib.request.Request(
        OPENAI_URL, data=payload,
        headers={'Content-Type': 'application/json',
                 'Authorization': f'Bearer {OPENAI_API_KEY}'})
    for attempt in range(5):
        _openai_rate_wait()  # global rate limit: 1 call per 6s across all threads
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = json.loads(r.read())
                return data['choices'][0]['message']['content'].strip()
        except urllib.error.HTTPError as e:
            if e.code == 429:
                # Honor Retry-After header if present, else exponential backoff
                retry_after = int(e.headers.get('Retry-After', 2 ** attempt * 10))
                print(f"    OpenAI 429 — waiting {retry_after}s (attempt {attempt+1}/5)")
                time.sleep(retry_after)
            else:
                print(f"    OpenAI error: {e}")
                return None
        except Exception as e:
            print(f"    OpenAI error: {e}")
            return None
    print("    OpenAI error: max retries exceeded on 429")
    return None


def call_ollama(prompt, timeout=90):
    """Call Ollama. Returns response string or None."""
    payload = json.dumps({
        'model':   OLLAMA_MODEL,
        'prompt':  prompt,
        'stream':  False,
        'options': {'temperature': 0, 'num_predict': 2000},
    }).encode()
    req = urllib.request.Request(OLLAMA_URL, data=payload,
                                 headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read()).get('response', '').strip()
    except Exception as e:
        print(f"    Ollama error: {e}")
        return None


def ollama(prompt, timeout=90, model=None):
    """Route to OpenAI or Ollama based on USE_OPENAI flag."""
    if USE_OPENAI:
        return call_openai(prompt, timeout=timeout, model=model)
    return call_ollama(prompt, timeout=timeout)


def _llm_extract(prompt, name, timeout=90):
    """
    Single extraction call with automatic two-pass fallback (OpenAI only).
    If OPENAI_MODEL returns 0 rates, retries with OPENAI_FALLBACK_MODEL.
    Returns (extracted_list, model_used).
    """
    if not USE_OPENAI:
        raw = call_ollama(prompt, timeout=timeout)
        return parse_json(raw) or [], OLLAMA_MODEL

    raw = call_openai(prompt, timeout=timeout, model=OPENAI_MODEL)
    extracted = parse_json(raw) or []
    if extracted:
        return extracted, OPENAI_MODEL

    # Two-pass fallback: mini got nothing → try gpt-4o
    print(f"    gpt-4o-mini returned 0 rates for {name}, retrying with {OPENAI_FALLBACK_MODEL}...")
    raw2 = call_openai(prompt, timeout=timeout, model=OPENAI_FALLBACK_MODEL)
    return parse_json(raw2) or [], OPENAI_FALLBACK_MODEL


def parse_json(text):
    """Extract first JSON array from LLM response."""
    if not text:
        return None
    # Strip markdown code fences (```json ... ```)
    text = re.sub(r'```(?:json)?\s*', '', text).strip()
    # Greedy match — captures the full outermost array including nested objects
    m = re.search(r'\[[\s\S]*\]', text)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass
    return None


def normalize_apy(val):
    """Normalize APY to decimal. Returns None if invalid."""
    if val is None:
        return None
    try:
        val = float(val)
    except (TypeError, ValueError):
        return None
    if val <= 0 or val > 40:
        return None
    if val > 0.40:          # LLM returned percentage (e.g. 5.0) — convert
        return round(val / 100, 6)
    return round(val, 6)    # Already decimal (e.g. 0.05)


# ── Rate bounds for sanity check ─────────────────────────────────────────────
BOUNDS = {
    'savings':        (0.0001, 0.065),
    'checking':       (0.0001, 0.065),
    'money_market':   (0.0001, 0.065),
    'cd':             (0.0001, 0.080),
    'ira_cd':         (0.0001, 0.080),
    'mortgage':       (0.030,  0.150),
    'home_equity':    (0.030,  0.180),
    'auto_loan':      (0.020,  0.280),
    'personal_loan':  (0.050,  0.360),
    'credit_card':    (0.010,  0.360),
    'new_auto_loan':  (0.020,  0.200),
    'used_auto_loan': (0.020,  0.250),
    'mortgage_fixed': (0.030,  0.120),
    'mortgage_arm':   (0.025,  0.120),
}

VALID_CD_TERMS = set(range(1, 121))  # Accept any term 1–120 months


def passes_rules(product, apy, term_months):
    """Quick sanity check before sending to verifier."""
    if apy is None:
        return False, 'no APY'
    lo, hi = BOUNDS.get(product, (0.001, 0.40))
    if not (lo <= apy <= hi):
        return False, f'{apy*100:.3f}% out of range [{lo*100:.1f}–{hi*100:.1f}%] for {product}'
    if product in ('cd', 'ira_cd') and term_months not in VALID_CD_TERMS:
        return False, f'unusual term {term_months}mo'
    return True, 'ok'


def run_parser(inst_ids=None, force=False, model=None, backend=None):
    """
    Extract + verify rates for institutions with raw_section stored.
    inst_ids: list of institution IDs to process (None = all pending)
    force:    re-extract even if already done
    model:    override LLM model name (OpenAI only)
    backend:  'openai' | 'ollama' — sets USE_OPENAI flag for this run
    """
    global USE_OPENAI, OPENAI_MODEL
    if backend == 'openai':
        USE_OPENAI = True
    elif backend == 'ollama':
        USE_OPENAI = False
    if model:
        OPENAI_MODEL = model  # only used if USE_OPENAI is True

    conn = get_conn()
    c    = conn.cursor()
    now  = datetime.now(timezone.utc).isoformat()
    week = current_week()

    if inst_ids:
        placeholders = ','.join('?' * len(inst_ids))
        query = f"""SELECT id, name, raw_section FROM institutions
                    WHERE id IN ({placeholders}) AND raw_section IS NOT NULL"""
        rows = c.execute(query, inst_ids).fetchall()
    else:
        query = """SELECT i.id, i.name, i.raw_section FROM institutions i
                   WHERE i.scrape_status='ok' AND i.raw_section IS NOT NULL"""
        if not force:
            query += " AND NOT EXISTS (SELECT 1 FROM rates r WHERE r.institution_id=i.id)"
        rows = c.execute(query).fetchall()

    active_backend = 'openai' if USE_OPENAI else 'ollama'
    active_model   = OPENAI_MODEL if USE_OPENAI else OLLAMA_MODEL
    print(f"Parser: {len(rows)} institutions to process (backend: {active_backend}, model: {active_model})")

    total_extracted = total_verified = total_rejected = 0

    for i, row in enumerate(rows):
        inst_id  = row['id']
        name     = row['name']
        section  = row['raw_section']

        print(f"  [{i+1}/{len(rows)}] {name[:50]}", flush=True)

        # ── Step 1: Extract (with two-pass fallback for OpenAI) ─────────────
        prompt1   = EXTRACT_PROMPT.format(institution=name, page_text=section)
        extracted, used_model = _llm_extract(prompt1, name)

        if not extracted:
            print(f"    → 0 rates extracted")
            # Flag for Playwright retry on next pass
            c.execute("UPDATE institutions SET scrape_status='retry_playwright' WHERE id=?", (inst_id,))
            conn.commit()
            time.sleep(CALL_DELAY)
            continue

        # Normalize APYs and apply rule filter
        clean = []
        for r in extracted:
            if not isinstance(r, dict) or 'product' not in r:
                total_rejected += 1
                continue
            apy = normalize_apy(r.get('apy'))
            product = r.get('product', 'unknown')
            term = r.get('term_months')
            ok, reason = passes_rules(product, apy, term)
            if ok:
                clean.append({**r, 'apy': apy})
            else:
                total_rejected += 1

        if not clean:
            print(f"    → {len(extracted)} extracted, all failed rules")
            # Flag for Playwright retry — rules failed, content may be wrong page
            c.execute("UPDATE institutions SET scrape_status='retry_playwright' WHERE id=?", (inst_id,))
            conn.commit()
            time.sleep(CALL_DELAY)
            continue

        print(f"    → {len(clean)} passed rules (of {len(extracted)} extracted), verifying...", flush=True)

        # ── Step 2: Self-verify ──────────────────────────────────────────────
        rates_summary = [
            {'product': r['product'], 'term_months': r.get('term_months'),
             'apy_pct': round(r['apy'] * 100, 3)}
            for r in clean
        ]
        prompt2   = VERIFY_PROMPT.format(
            institution=name,
            rates_json=json.dumps(rates_summary, indent=2),
            page_text=section
        )
        # Use the same model that successfully extracted (mini or fallback)
        raw_resp2 = call_openai(prompt2, timeout=120, model=used_model) if USE_OPENAI \
                    else call_ollama(prompt2, timeout=120)
        verified  = parse_json(raw_resp2) or []

        # Build lookup: (product, term, apy_pct) → verified result
        verify_map = {}
        for v in verified:
            key = (v.get('product'), v.get('term_months'), v.get('apy_pct'))
            verify_map[key] = v

        # ── Step 3: Save to DB ───────────────────────────────────────────────
        # Wipe all existing rates for this institution+week before inserting
        # (handles tiered products correctly — can't key on product+term alone)
        c.execute("DELETE FROM rates WHERE institution_id=? AND scraped_week=?",
                  (inst_id, week))

        saved = 0
        for r in clean:
            apy_pct = round(r['apy'] * 100, 3)
            key     = (r['product'], r.get('term_months'), apy_pct)
            vresult = verify_map.get(key, {})
            is_verified = vresult.get('verified', False)
            snippet     = vresult.get('snippet')

            confidence = 'verified' if is_verified else 'unverified'

            group_id = PRODUCT_GROUP_MAP.get(r['product'])

            c.execute("""INSERT INTO rates
                         (institution_id, scraped_at, scraped_week, product, group_id,
                          term_months, apy, min_balance, notes, confidence, verified_snippet)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                      (inst_id, now, week, r['product'], group_id, r.get('term_months'),
                       r['apy'], r.get('min_balance'), r.get('notes'), confidence, snippet))
            saved += 1
            if is_verified:
                total_verified += 1
            total_extracted += 1

        conn.commit()
        verified_count = sum(1 for r in clean
                             if verify_map.get((r['product'], r.get('term_months'),
                                                round(r['apy']*100,3)), {}).get('verified'))
        print(f"    ✅ {verified_count} verified | ❓ {saved - verified_count} unverified")
        time.sleep(CALL_DELAY)

    conn.close()
    print(f"""
═══ Parse Complete ═══
  Extracted:  {total_extracted}
  Verified:   {total_verified}
  Rejected:   {total_rejected} (failed rules)
""")


# ── Prompt: Loan Extract ─────────────────────────────────────────────────────
LOAN_EXTRACT_PROMPT = """Extract loan rates from the following content.

Return a JSON array. Each element:
- product: "new_auto_loan" | "used_auto_loan" | "personal_loan" | "home_equity_loan" | "student_loan"
- term_months: integer or null
- rate: float — the base interest rate as PERCENTAGE (e.g. 4.49 for 4.49%), null if only APR shown
- apr: float — the APR as PERCENTAGE if shown separately, null if not shown. If only one rate shown labeled "APR", put here and leave rate null.
- vehicle_age_years: integer for used auto (e.g. 3 for "up to 3 years old"), null otherwise
- loan_amount_k: reference loan amount in thousands (25 for new auto $25k, 15 for used 2yr $15k, 9 for used 4yr $9k; null if not stated)
- loan_term_label: string label like "36 months", "48 months", null
- notes: brief note

Rules:
- Rate ranges: use the lowest (best/as-low-as) rate
- If page shows "as low as X% APR", set apr=X, rate=null
- Only extract clearly stated rates, not promotional teaser text
- If no rates found, return []
- Include ALL terms listed (36mo, 48mo, 60mo, 72mo, 84mo etc)
- For used auto with no age specified, use vehicle_age_years: 2

Content:
{page_text}

JSON array:"""

# ── Prompt: Loan Verify ──────────────────────────────────────────────────────
LOAN_VERIFY_PROMPT = """You are auditing extracted loan rate data for {institution}.

For each rate below, answer whether that EXACT percentage appears in the page content.
Return ONLY a JSON array with the same items plus:
  "verified": true if the exact % number appears on the page, false otherwise
  "snippet": the exact text from the page confirming it (or null)

Rates to verify:
{rates_json}

Page content:
{page_text}

JSON array:"""

# ── Prompt: Mortgage Extract ─────────────────────────────────────────────────
MORTGAGE_EXTRACT_PROMPT = """Extract mortgage rates from the following content.

Return a JSON array. Each element:
- product: "mortgage_fixed" | "mortgage_arm"
- term_months: integer (360=30yr, 180=15yr, 240=20yr, 120=10yr) or null for ARM
- arm_initial_years: integer (e.g. 5 for 5/1 ARM) or null
- arm_adjust_months: integer (e.g. 12 for annual, 6 for semi-annual) or null
- rate: float — interest rate as PERCENTAGE, null if only APR shown
- apr: float — APR as PERCENTAGE if shown separately. If only one rate shown labeled "APR", put here and leave rate null.
- rate_type: "fixed" for fixed-rate mortgages, "arm" for adjustable-rate
- conforming: 1 for conforming, 0 for jumbo, null if unclear
- notes: brief note

Rules:
- If page shows both rate and APR for a product, extract both
- Rate ranges: use the lowest (best/as-low-as) rate
- DO NOT extract points-only rates or closing cost info
- If no mortgage rates found, return []
- ARM notation: "5/1 ARM" = arm_initial_years=5, arm_adjust_months=12
  "5/6 ARM" = arm_initial_years=5, arm_adjust_months=6

Content:
{page_text}

JSON array:"""

# ── Prompt: Mortgage Verify ──────────────────────────────────────────────────
MORTGAGE_VERIFY_PROMPT = """You are auditing extracted mortgage rate data for {institution}.

For each rate below, answer whether that EXACT percentage appears in the page content.
Return ONLY a JSON array with the same items plus:
  "verified": true if the exact % number appears on the page, false otherwise
  "snippet": the exact text from the page confirming it (or null)

Rates to verify:
{rates_json}

Page content:
{page_text}

JSON array:"""


def run_loan_parser(inst_ids=None, force=False, model=None, backend=None):
    """
    Extract + verify auto/personal loan rates for institutions with raw_section or loan raw content.
    Saves extra loan fields (vehicle_age_years, loan_amount_k, loan_term_label) to rates table.

    inst_ids: list of institution IDs to process (None = all with loan_rates_url or rates_url)
    force:    re-extract even if already done this week
    model:    override LLM model name
    backend:  'openai' | 'ollama'
    """
    global USE_OPENAI, OPENAI_MODEL
    if backend == 'openai':
        USE_OPENAI = True
    elif backend == 'ollama':
        USE_OPENAI = False
    if model:
        OPENAI_MODEL = model

    conn = get_conn()
    c    = conn.cursor()
    now  = datetime.now(timezone.utc).isoformat()
    week = current_week()

    if inst_ids:
        placeholders = ','.join('?' * len(inst_ids))
        query = f"""SELECT id, name, raw_section FROM institutions
                    WHERE id IN ({placeholders}) AND raw_section IS NOT NULL"""
        rows = c.execute(query, inst_ids).fetchall()
    else:
        query = """SELECT i.id, i.name, i.raw_section FROM institutions i
                   WHERE i.scrape_status='ok' AND i.raw_section IS NOT NULL"""
        if not force:
            query += """
                AND NOT EXISTS (
                    SELECT 1 FROM rates r
                    WHERE r.institution_id=i.id
                      AND r.product IN ('new_auto_loan','used_auto_loan','personal_loan')
                      AND r.scraped_week=?
                )"""
            rows = c.execute(query, (week,)).fetchall()
        else:
            rows = c.execute(query).fetchall()

    active_backend = 'openai' if USE_OPENAI else 'ollama'
    active_model   = OPENAI_MODEL if USE_OPENAI else OLLAMA_MODEL
    print(f"Loan Parser: {len(rows)} institutions to process (backend: {active_backend}, model: {active_model})")

    total_extracted = total_verified = total_rejected = 0

    for i, row in enumerate(rows):
        inst_id = row['id']
        name    = row['name']
        section = row['raw_section']

        print(f"  [{i+1}/{len(rows)}] {name[:50]}", flush=True)

        # Step 1: Extract
        prompt1   = LOAN_EXTRACT_PROMPT.format(institution=name, page_text=section)
        extracted, used_model = _llm_extract(prompt1, name)

        if not extracted:
            print(f"    → 0 loan rates extracted")
            time.sleep(CALL_DELAY)
            continue

        # Normalize and filter — new prompt returns rate/apr as percentages
        clean = []
        for r in extracted:
            if not isinstance(r, dict) or 'product' not in r:
                total_rejected += 1
                continue
            product = r.get('product', 'unknown')
            term    = r.get('term_months')

            # Handle new rate/apr fields (percentages) or legacy apy field (decimal)
            rate_pct = r.get('rate')  # percentage (e.g. 4.49)
            apr_pct  = r.get('apr')   # percentage (e.g. 4.99)
            legacy_apy = r.get('apy') # decimal (legacy)

            # Determine effective rate for ranking/filtering
            if rate_pct is not None:
                apy = normalize_apy(rate_pct)  # convert pct to decimal
            elif apr_pct is not None:
                apy = normalize_apy(apr_pct)
            elif legacy_apy is not None:
                apy = normalize_apy(legacy_apy)
            else:
                apy = None

            ok, reason = passes_rules(product, apy, term)
            if ok:
                # Normalize apr field to decimal
                apr_decimal = normalize_apy(apr_pct) if apr_pct is not None else None
                clean.append({**r, 'apy': apy, 'apr_decimal': apr_decimal})
            else:
                total_rejected += 1

        if not clean:
            print(f"    → {len(extracted)} extracted, all failed rules")
            time.sleep(CALL_DELAY)
            continue

        print(f"    → {len(clean)} passed rules, verifying...", flush=True)

        # Step 2: Verify
        rates_summary = [
            {'product': r['product'], 'term_months': r.get('term_months'),
             'apy_pct': round(r['apy'] * 100, 3)}
            for r in clean
        ]
        prompt2  = LOAN_VERIFY_PROMPT.format(
            institution=name,
            rates_json=json.dumps(rates_summary, indent=2),
            page_text=section
        )
        raw_resp2 = call_openai(prompt2, timeout=120, model=used_model) if USE_OPENAI \
                    else call_ollama(prompt2, timeout=120)
        verified  = parse_json(raw_resp2) or []

        verify_map = {}
        for v in verified:
            key = (v.get('product'), v.get('term_months'), v.get('apy_pct'))
            verify_map[key] = v

        # Step 3: Save
        loan_products = ('new_auto_loan', 'used_auto_loan', 'personal_loan', 'home_equity')
        c.execute("""DELETE FROM rates
                     WHERE institution_id=? AND scraped_week=?
                       AND product IN ('new_auto_loan','used_auto_loan','personal_loan','home_equity')""",
                  (inst_id, week))

        saved = 0
        for r in clean:
            apy_pct = round(r['apy'] * 100, 3)
            key     = (r['product'], r.get('term_months'), apy_pct)
            vresult = verify_map.get(key, {})
            is_verified = vresult.get('verified', False)
            snippet     = vresult.get('snippet')
            confidence  = 'verified' if is_verified else 'unverified'
            group_id    = PRODUCT_GROUP_MAP.get(r['product'])

            # Build human label
            term_mo = r.get('term_months')
            vage    = r.get('vehicle_age_years')
            amtk    = r.get('loan_amount_k')
            if r['product'] == 'new_auto_loan':
                label = f"{term_mo}Mo New Auto {amtk}k" if term_mo and amtk else f"{term_mo}Mo New Auto"
            elif r['product'] == 'used_auto_loan':
                yr_str = f"{vage} Yr " if vage is not None else ""
                label  = f"{term_mo}Mo {yr_str}Used Auto {amtk}k" if term_mo and amtk else f"{term_mo}Mo Used Auto"
            else:
                label = f"{term_mo}Mo {r['product'].replace('_',' ').title()}" if term_mo else r['product']

            notes = r.get('notes') or 'APR'
            if 'APR' not in (notes or ''):
                notes = ('APR; ' + notes).strip('; ') if notes else 'APR'

            apr_val = r.get('apr_decimal')  # already normalized to decimal

            c.execute("""INSERT INTO rates
                         (institution_id, scraped_at, scraped_week, product, group_id,
                          term_months, apy, apr, min_balance, notes, confidence, verified_snippet,
                          loan_term_label, vehicle_age_years, loan_amount_k, rate_type)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (inst_id, now, week, r['product'], group_id, term_mo,
                       r['apy'], apr_val, r.get('min_balance'), notes, confidence, snippet,
                       label, vage, amtk, 'apr'))
            saved += 1
            if is_verified:
                total_verified += 1
            total_extracted += 1

        conn.commit()
        verified_count = sum(1 for r in clean
                             if verify_map.get((r['product'], r.get('term_months'),
                                                round(r['apy']*100,3)), {}).get('verified'))
        print(f"    ✅ {verified_count} verified | ❓ {saved - verified_count} unverified")
        time.sleep(CALL_DELAY)

    conn.close()
    print(f"""
═══ Loan Parse Complete ═══
  Extracted:  {total_extracted}
  Verified:   {total_verified}
  Rejected:   {total_rejected} (failed rules)
""")


def run_mortgage_parser(inst_ids=None, force=False, model=None, backend=None):
    """
    Extract + verify mortgage rates for institutions with raw_section stored.
    Saves extra mortgage fields (rate_type, arm_initial_years, arm_adjust_months, conforming).

    inst_ids: list of institution IDs to process (None = all with raw_section)
    force:    re-extract even if already done this week
    model:    override LLM model name
    backend:  'openai' | 'ollama'
    """
    global USE_OPENAI, OPENAI_MODEL
    if backend == 'openai':
        USE_OPENAI = True
    elif backend == 'ollama':
        USE_OPENAI = False
    if model:
        OPENAI_MODEL = model

    conn = get_conn()
    c    = conn.cursor()
    now  = datetime.now(timezone.utc).isoformat()
    week = current_week()

    if inst_ids:
        placeholders = ','.join('?' * len(inst_ids))
        query = f"""SELECT id, name, raw_section FROM institutions
                    WHERE id IN ({placeholders}) AND raw_section IS NOT NULL"""
        rows = c.execute(query, inst_ids).fetchall()
    else:
        query = """SELECT i.id, i.name, i.raw_section FROM institutions i
                   WHERE i.scrape_status='ok' AND i.raw_section IS NOT NULL"""
        if not force:
            query += """
                AND NOT EXISTS (
                    SELECT 1 FROM rates r
                    WHERE r.institution_id=i.id
                      AND r.product IN ('mortgage_fixed','mortgage_arm')
                      AND r.scraped_week=?
                )"""
            rows = c.execute(query, (week,)).fetchall()
        else:
            rows = c.execute(query).fetchall()

    active_backend = 'openai' if USE_OPENAI else 'ollama'
    active_model   = OPENAI_MODEL if USE_OPENAI else OLLAMA_MODEL
    print(f"Mortgage Parser: {len(rows)} institutions to process (backend: {active_backend}, model: {active_model})")

    total_extracted = total_verified = total_rejected = 0

    for i, row in enumerate(rows):
        inst_id = row['id']
        name    = row['name']
        section = row['raw_section']

        print(f"  [{i+1}/{len(rows)}] {name[:50]}", flush=True)

        # Step 1: Extract
        prompt1   = MORTGAGE_EXTRACT_PROMPT.format(institution=name, page_text=section)
        extracted, used_model = _llm_extract(prompt1, name)

        if not extracted:
            print(f"    → 0 mortgage rates extracted")
            time.sleep(CALL_DELAY)
            continue

        # Normalize and filter — new prompt returns rate/apr as percentages
        clean = []
        for r in extracted:
            if not isinstance(r, dict) or 'product' not in r:
                total_rejected += 1
                continue
            product = r.get('product', 'unknown')
            term    = r.get('term_months')

            # Handle new rate/apr fields (percentages) or legacy apy field (decimal)
            rate_pct = r.get('rate')  # percentage (e.g. 6.5)
            apr_pct  = r.get('apr')   # percentage (e.g. 6.65)
            legacy_apy = r.get('apy') # decimal (legacy)

            if rate_pct is not None:
                apy = normalize_apy(rate_pct)
            elif apr_pct is not None:
                apy = normalize_apy(apr_pct)
            elif legacy_apy is not None:
                apy = normalize_apy(legacy_apy)
            else:
                apy = None

            ok, reason = passes_rules(product, apy, term)
            if ok:
                apr_decimal = normalize_apy(apr_pct) if apr_pct is not None else None
                clean.append({**r, 'apy': apy, 'apr_decimal': apr_decimal})
            else:
                total_rejected += 1

        if not clean:
            print(f"    → {len(extracted)} extracted, all failed rules")
            time.sleep(CALL_DELAY)
            continue

        print(f"    → {len(clean)} passed rules, verifying...", flush=True)

        # Step 2: Verify
        rates_summary = [
            {'product': r['product'], 'term_months': r.get('term_months'),
             'apy_pct': round(r['apy'] * 100, 3)}
            for r in clean
        ]
        prompt2  = MORTGAGE_VERIFY_PROMPT.format(
            institution=name,
            rates_json=json.dumps(rates_summary, indent=2),
            page_text=section
        )
        raw_resp2 = call_openai(prompt2, timeout=120, model=used_model) if USE_OPENAI \
                    else call_ollama(prompt2, timeout=120)
        verified  = parse_json(raw_resp2) or []

        verify_map = {}
        for v in verified:
            key = (v.get('product'), v.get('term_months'), v.get('apy_pct'))
            verify_map[key] = v

        # Step 3: Save
        c.execute("""DELETE FROM rates
                     WHERE institution_id=? AND scraped_week=?
                       AND product IN ('mortgage_fixed','mortgage_arm')""",
                  (inst_id, week))

        saved = 0
        for r in clean:
            apy_pct  = round(r['apy'] * 100, 3)
            key      = (r['product'], r.get('term_months'), apy_pct)
            vresult  = verify_map.get(key, {})
            is_verified = vresult.get('verified', False)
            snippet     = vresult.get('snippet')
            confidence  = 'verified' if is_verified else 'unverified'
            group_id    = PRODUCT_GROUP_MAP.get(r['product'])

            rate_type      = r.get('rate_type', 'fixed' if r['product'] == 'mortgage_fixed' else 'arm')
            arm_init       = r.get('arm_initial_years')
            arm_adj        = r.get('arm_adjust_months')
            conforming_val = r.get('conforming', 1)
            term_mo        = r.get('term_months')
            apr_val        = r.get('apr_decimal')

            # Build human label
            if rate_type == 'arm' and arm_init:
                adj_str = f"/{arm_adj//12 if arm_adj and arm_adj >= 12 else arm_adj}" if arm_adj else ""
                label   = f"{arm_init}{adj_str} ARM Conforming" if conforming_val else f"{arm_init}{adj_str} ARM Jumbo"
            elif rate_type == 'fixed' and term_mo:
                yr = term_mo // 12
                label = f"{yr}Yr Fixed Conforming" if conforming_val else f"{yr}Yr Fixed Jumbo"
            else:
                label = r['product'].replace('_', ' ').title()

            c.execute("""INSERT INTO rates
                         (institution_id, scraped_at, scraped_week, product, group_id,
                          term_months, apy, apr, min_balance, notes, confidence, verified_snippet,
                          loan_term_label, rate_type, arm_initial_years, arm_adjust_months, conforming)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (inst_id, now, week, r['product'], group_id, term_mo,
                       r['apy'], apr_val, r.get('min_balance'), r.get('notes'), confidence, snippet,
                       label, rate_type, arm_init, arm_adj, conforming_val))
            saved += 1
            if is_verified:
                total_verified += 1
            total_extracted += 1

        conn.commit()
        verified_count = sum(1 for r in clean
                             if verify_map.get((r['product'], r.get('term_months'),
                                                round(r['apy']*100,3)), {}).get('verified'))
        print(f"    ✅ {verified_count} verified | ❓ {saved - verified_count} unverified")
        time.sleep(CALL_DELAY)

    conn.close()
    print(f"""
═══ Mortgage Parse Complete ═══
  Extracted:  {total_extracted}
  Verified:   {total_verified}
  Rejected:   {total_rejected} (failed rules)
""")


if __name__ == '__main__':
    run_parser(force=True)
