"""
Jina Reader Scraper — fetches rate pages, extracts rate-dense section, stores in DB

Fetch strategy (in order):
  1. Jina Reader (authenticated) — handles JS-heavy pages, cleans to markdown
  2. Direct HTTP fetch + BeautifulSoup — plain HTML sites, zero cost
  3. Playwright headless Chromium — JS-rendered pages (BofA, Navy Fed, etc.)
  4. Fail — log error, move on
"""
import urllib.request, urllib.error, time, re, os, socket
from datetime import datetime, timezone
from schema import get_conn

# NOTE: Do NOT set socket.setdefaulttimeout() globally here — it breaks
# long-running LLM API calls. Per-request timeouts are set in urlopen() calls.

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

JINA_BASE     = 'https://r.jina.ai/'
def _get_jina_key():
    key = os.environ.get('JINA_API_KEY')
    if key:
        return key
    try:
        import subprocess
        result = subprocess.run(
            'source ~/.op_service_account && op item get "Jina.ai API Credentials" --vault ClawdBotVault --fields credential --reveal',
            shell=True, capture_output=True, text=True, executable='/bin/zsh'
        )
        return result.stdout.strip()
    except Exception:
        return 'jina_128fae3e54b345d4956e24f2dc629163P62kJ6Qlon8qBr7S6QP0XnsdmM5k'

JINA_API_KEY  = _get_jina_key()
UA            = 'Mozilla/5.0 (compatible; RateScraper/1.0)'
DELAY              = 1.0   # seconds between requests
TIMEOUT            = 20    # per-request timeout (reduced from 30)
MAX_RETRIES        = 3
RETRY_DELAY        = 5
INST_HARD_TIMEOUT  = 60    # max seconds per institution (all fetch attempts combined)
SECTION_SIZE  = 16000  # chars to store as raw_section (increased for JS-heavy pages)

RATE_KEYWORDS = ['apy', 'annual percentage yield', '% apy', 'interest rate',
                 'certificate of deposit', 'savings rate', 'cd rate', 'mortgage rate',
                 'auto loan', 'personal loan', 'home equity']


def fetch_jina(url, timeout=TIMEOUT, retries=MAX_RETRIES):
    """Fetch URL via Jina Reader (authenticated). Returns markdown text or None."""
    jina_url = JINA_BASE + url
    req = urllib.request.Request(jina_url, headers={
        'User-Agent': UA,
        'Accept': 'text/plain',
        'X-Return-Format': 'markdown',
        'X-Timeout': str(timeout),
        'Authorization': f'Bearer {JINA_API_KEY}',
    })
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                text = r.read().decode('utf-8', errors='replace')
                return text if text and len(text) >= 200 else None
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = RETRY_DELAY * attempt * 2
                print(f"    ⚠️  429 rate limit — waiting {wait}s")
                time.sleep(wait)
            else:
                return None
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            if attempt < retries:
                time.sleep(RETRY_DELAY)
    return None


def fetch_direct(url, timeout=12):
    """
    Fallback: fetch URL directly and extract text via BeautifulSoup.
    Works well on plain HTML rate pages. Returns plain text or None.
    """
    if not HAS_BS4:
        return None
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if 'html' not in r.headers.get('content-type', ''):
                return None
            html = r.read().decode('utf-8', errors='replace')
        soup = BeautifulSoup(html, 'html.parser')
        # Remove nav, header, footer, scripts, styles
        for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            tag.decompose()
        text = soup.get_text(separator='\n', strip=True)
        # Collapse excessive blank lines
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text if len(text) >= 200 else None
    except Exception:
        return None


DEFAULT_ZIP = '10001'  # New York — used to bypass ZIP-gated rate pages

ZIP_SELECTORS = [
    'input[name="zipCode"]',
    'input[name="zip"]',
    'input[name="zip_code"]',
    'input[placeholder*="ZIP" i]',
    'input[placeholder*="zip" i]',
    'input[aria-label*="zip" i]',
    'input[id*="zip" i]',
    'input[type="text"][maxlength="5"]',
]

SUBMIT_SELECTORS = [
    'button[type="submit"]',
    'input[type="submit"]',
    'button:has-text("Go")',
    'button:has-text("Submit")',
    'button:has-text("Next")',
    'button:has-text("View Rates")',
    'button:has-text("Continue")',
]

def _try_fill_zip(page):
    """
    Detect and fill ZIP code prompt. Returns True if ZIP was submitted.
    """
    for sel in ZIP_SELECTORS:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.fill(DEFAULT_ZIP)
                # Try to submit
                for btn_sel in SUBMIT_SELECTORS:
                    try:
                        btn = page.query_selector(btn_sel)
                        if btn and btn.is_visible():
                            btn.click()
                            page.wait_for_load_state('networkidle', timeout=8000)
                            return True
                    except Exception:
                        continue
                # Fallback: press Enter
                try:
                    el.press('Enter')
                    page.wait_for_load_state('networkidle', timeout=8000)
                    return True
                except Exception:
                    pass
        except Exception:
            continue
    return False

def fetch_playwright(url, timeout=25):
    """
    Playwright headless Chromium fallback for JS-rendered pages.
    Auto-fills ZIP code (10001) if a ZIP prompt is detected.
    Returns text or None.
    NOTE: Run inside a thread with INST_HARD_TIMEOUT to prevent process-level crashes.
    """
    if not HAS_PLAYWRIGHT:
        return None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=UA)
            page.goto(url, wait_until='networkidle', timeout=timeout * 1000)

            # Try to bypass ZIP gate
            zip_filled = _try_fill_zip(page)
            if zip_filled:
                # Give page time to load rates after ZIP submission
                try:
                    page.wait_for_load_state('networkidle', timeout=8000)
                except Exception:
                    pass

            # Remove nav/header/footer before extracting text
            page.evaluate("""() => {
                ['nav','header','footer','aside','script','style'].forEach(tag => {
                    document.querySelectorAll(tag).forEach(el => el.remove());
                });
            }""")
            text = page.inner_text('body')
            browser.close()
            text = re.sub(r'\n{3,}', '\n\n', text.strip())
            return text if len(text) >= 200 else None
    except Exception as e:
        return None


def fetch_page(url):
    """
    Main fetch entry point:
      1. Jina (fast, token cost)
      2. Direct HTTP (free, plain HTML only)
      3. Playwright (free, handles JS rendering)
    Returns (text, source).
    """
    text = fetch_jina(url)
    if text and has_rate_signals(text):
        return text, 'jina'

    # Jina got nothing useful — try direct first (cheaper than Playwright)
    text_direct = fetch_direct(url)
    if text_direct and has_rate_signals(text_direct):
        return text_direct, 'direct'

    # Last resort: Playwright headless browser
    text_pw = fetch_playwright(url)
    if text_pw and has_rate_signals(text_pw):
        return text_pw, 'playwright'

    # Return whatever we got, even if no rate signals (caller will handle)
    return (text or text_direct or text_pw), ('jina' if text else 'direct' if text_direct else 'playwright' if text_pw else None)


def extract_rate_section(text, size=SECTION_SIZE):
    """
    Find the rate-dense section of the page.
    Strategy: prefer the first occurrence of 'apy' or a percentage pattern
    (e.g. '4.00%') as the anchor — these are strong signals of actual rate tables,
    not just navigation links. Fall back to other keywords if needed.
    """
    lower = text.lower()

    # Strong signals — actual rate data
    strong_keywords = ['apy', 'annual percentage yield']
    # Weak signals — may appear in nav
    weak_keywords   = ['interest rate', 'certificate of deposit', 'savings rate',
                       'cd rate', 'mortgage rate', 'auto loan', 'personal loan']

    best = len(text)

    # 1. Look for strong keywords first
    for kw in strong_keywords:
        idx = lower.find(kw)
        if 0 < idx < best:
            best = idx

    # 2. If no strong keyword, try percentage pattern like "4.00%" or "0.50%"
    if best == len(text):
        import re as _re
        m = _re.search(r'\d+\.\d+\s*%', text)
        if m and m.start() > 0:
            best = m.start()

    # 3. Fall back to weak keywords
    if best == len(text):
        for kw in weak_keywords:
            idx = lower.find(kw)
            if 0 < idx < best:
                best = idx

    # 4. Last resort — skip nav at top
    if best == len(text):
        best = min(2000, len(text) // 4)

    start = max(0, best - 300)
    return text[start:start + size]


def _fetch_worker(url, result_queue):
    """Module-level worker for multiprocessing crash isolation."""
    try:
        text, source = fetch_page(url)
        result_queue.put((text, source))
    except Exception:
        result_queue.put((None, None))


def has_rate_signals(text):
    """Quick check — does the page likely have rates?"""
    signals = ['apy', '%', 'rate', 'annual', 'interest', 'savings', 'cd',
               'certificate', 'mortgage', 'loan', 'equity']
    lower = text.lower()
    return sum(1 for s in signals if s in lower) >= 3


def _scrape_one(row):
    """
    Scrape a single institution. Returns (inst_id, status, section_len, source).
    Uses its own DB connection — safe to call from threads.
    """
    import multiprocessing as _mp

    inst_id   = row['id']
    name      = row['name']
    rates_url = row['rates_url']
    now       = datetime.now(timezone.utc).isoformat()

    conn = get_conn()
    c    = conn.cursor()

    def _finish(status, section=None):
        if section is not None:
            c.execute("UPDATE institutions SET last_scraped_at=?, scrape_status='ok', raw_section=? WHERE id=?",
                      (now, section, inst_id))
        else:
            c.execute(f"UPDATE institutions SET last_scraped_at=?, scrape_status='{status}' WHERE id=?",
                      (now, inst_id))
        conn.commit()
        conn.close()

    result_q = _mp.Queue()
    proc = _mp.Process(target=_fetch_worker, args=(rates_url, result_q))
    proc.start()
    proc.join(timeout=INST_HARD_TIMEOUT)

    if proc.is_alive():
        proc.terminate()
        proc.join(timeout=5)
        if proc.is_alive():
            proc.kill()
        _finish('error')
        return inst_id, 'timeout', 0, None

    try:
        text, source = result_q.get_nowait()
    except Exception:
        text, source = None, None

    if proc.exitcode != 0:
        _finish('error')
        return inst_id, 'crash', 0, None

    if not text:
        _finish('error')
        return inst_id, 'error', 0, None

    if not has_rate_signals(text):
        c.execute("UPDATE institutions SET last_scraped_at=?, scrape_status='no_rates', raw_section=NULL WHERE id=?",
                  (now, inst_id))
        conn.commit()
        conn.close()
        return inst_id, 'no_rates', 0, source

    section = extract_rate_section(text)
    _finish('ok', section)
    return inst_id, 'ok', len(section), source


def run_scraper(limit=None, type_filter=None, force=False, workers=1):
    """
    Scrape rate pages and store raw_section in institutions table.
    Returns list of institution_ids that were successfully scraped.

    workers: number of concurrent threads (default 1 = serial, max 20)
    """
    import threading
    import concurrent.futures

    workers = max(1, min(workers, 20))

    conn = get_conn()
    c    = conn.cursor()

    query = """SELECT id, name, rates_url FROM institutions
               WHERE active=1 AND rates_url IS NOT NULL"""
    if not force:
        query += " AND (last_scraped_at IS NULL OR scrape_status='error' OR last_scraped_at < datetime('now', '-6 days'))"
    if type_filter:
        query += f" AND type='{type_filter}'"
    query += " ORDER BY assets_k DESC NULLS LAST"
    if limit:
        query += f" LIMIT {limit}"

    rows = c.execute(query).fetchall()
    conn.close()
    total = len(rows)
    print(f"Scraper: {total} institutions to scrape (workers={workers})")

    ok = errors = no_rates = 0
    scraped_ids = []
    _lock = threading.Lock()

    def _handle_result(i, row, result):
        """Process result from _scrape_one, update shared counters."""
        nonlocal ok, errors, no_rates
        inst_id, status, section_len, source = result
        name = row['name']

        with _lock:
            if status == 'ok':
                ok += 1
                scraped_ids.append(inst_id)
                label = f" ✅ ({section_len} chars, {source})"
            elif status == 'no_rates':
                no_rates += 1
                label = f" — no rate signals ({source})"
            elif status == 'timeout':
                errors += 1
                label = f" ⏱️  hard timeout ({INST_HARD_TIMEOUT}s) — skipping"
            elif status == 'crash':
                errors += 1
                label = f" 💥 worker crashed — skipping"
            else:
                errors += 1
                label = " ❌ no response"

            print(f"  [{i+1}/{total}] {name[:45]:<45}{label}")

            done = ok + errors + no_rates
            if done % 50 == 0 and done > 0:
                print(f"\n  📊 Scraped {done}/{total} ({errors} errors) — ok={ok} no_rates={no_rates}\n")

    if workers == 1:
        # Serial path — keep existing sleep/delay behaviour
        for i, row in enumerate(rows):
            result = _scrape_one(row)
            _handle_result(i, row, result)
            time.sleep(DELAY)
    else:
        # Concurrent path — ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_idx = {pool.submit(_scrape_one, row): (i, row)
                             for i, row in enumerate(rows)}
            for future in concurrent.futures.as_completed(future_to_idx):
                i, row = future_to_idx[future]
                try:
                    result = future.result()
                except Exception as exc:
                    with _lock:
                        errors += 1
                        print(f"  [{i+1}/{total}] {row['name'][:45]:<45} 💥 exception: {exc}")
                    continue
                _handle_result(i, row, result)

    print(f"\n✅ Scrape done: {ok} ok | {no_rates} no rates | {errors} errors")
    return scraped_ids


def run_playwright_retry(limit=None):
    """
    Re-scrape institutions flagged as 'retry_playwright' using headless Chromium.
    These are institutions where Jina got content but GPT extracted 0 rates.
    Returns list of institution_ids successfully re-scraped.
    """
    if not HAS_PLAYWRIGHT:
        print("Playwright not available — skipping retry phase")
        return []

    conn = get_conn()
    c    = conn.cursor()

    query = """SELECT id, name, rates_url FROM institutions
               WHERE scrape_status='retry_playwright' AND rates_url IS NOT NULL
               ORDER BY assets_k DESC NULLS LAST"""
    if limit:
        query += f" LIMIT {limit}"
    rows = c.execute(query).fetchall()

    print(f"\nPlaywright retry: {len(rows)} institutions to re-scrape")
    ok = skipped = 0
    scraped_ids = []
    now = datetime.now(timezone.utc).isoformat()

    for i, row in enumerate(rows):
        inst_id   = row['id']
        name      = row['name']
        rates_url = row['rates_url']

        print(f"  [{i+1}/{len(rows)}] {name[:50]:<50}", end='', flush=True)

        text = fetch_playwright(rates_url)
        if not text or not has_rate_signals(text):
            skipped += 1
            print(f" — no rates via Playwright")
            c.execute("UPDATE institutions SET scrape_status='no_rates', raw_section=NULL WHERE id=?", (inst_id,))
            conn.commit()
            time.sleep(0.5)
            continue

        section = extract_rate_section(text)
        c.execute("""UPDATE institutions
                     SET last_scraped_at=?, scrape_status='ok', raw_section=?
                     WHERE id=?""", (now, section, inst_id))
        conn.commit()
        ok += 1
        scraped_ids.append(inst_id)
        print(f" ✅ ({len(section)} chars, playwright)")
        time.sleep(0.5)

    conn.close()
    print(f"\n✅ Playwright retry done: {ok} ok | {skipped} skipped")
    return scraped_ids


if __name__ == '__main__':
    ids = run_scraper(limit=5, type_filter='bank')
    print(f"\nScraped {len(ids)} institutions")
