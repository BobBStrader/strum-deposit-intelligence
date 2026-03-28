"""
URL Discovery — Find loan and mortgage rate page URLs for institutions.
-----------------------------------------------------------------------
Tries common URL path patterns against institution websites and saves
discovered URLs to the loan_rates_url and mortgage_rates_url columns.

Common paths tried:
  Loan:     /loans, /auto-loans, /borrow, /loan-rates, /lending, /auto-loan-rates
  Mortgage: /mortgage, /home-loans, /mortgage-rates, /home-equity, /mortgages

Usage:
    python3 url_discovery.py                    # all institutions without URLs
    python3 url_discovery.py --id "ncua:67790"  # single institution by DB id
    python3 url_discovery.py --force            # re-check all institutions
    python3 url_discovery.py --type loan        # only loan URLs
    python3 url_discovery.py --type mortgage    # only mortgage URLs
"""

import argparse
import gzip
import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
from schema import get_conn

# ── URL path candidates ───────────────────────────────────────────────────────
LOAN_PATHS = [
    "/loans",
    "/auto-loans",
    "/auto-loan-rates",
    "/borrow",
    "/loan-rates",
    "/lending",
    "/loans/auto",
    "/personal-loans",
    "/consumer-loans",
    "/vehicle-loans",
]

MORTGAGE_PATHS = [
    "/mortgage",
    "/mortgages",
    "/home-loans",
    "/mortgage-rates",
    "/home-equity",
    "/real-estate-loans",
    "/home-loan-rates",
    "/lending/mortgage",
]

REQUEST_TIMEOUT = 8   # seconds per URL probe
DELAY_BETWEEN   = 0.3 # seconds between requests


# ── Brave Search discovery ────────────────────────────────────────────────────

def _get_brave_key() -> str:
    key = os.environ.get("BRAVE_API_KEY")
    if key:
        return key
    try:
        result = subprocess.run(
            'source ~/.op_service_account && op item get "Brave Search API Credentials" '
            '--vault ClawdBotVault --fields credential --reveal',
            shell=True, capture_output=True, text=True, executable="/bin/zsh",
        )
        return result.stdout.strip()
    except Exception:
        return ""


BRAVE_API_KEY = _get_brave_key()


def brave_find_rate_url(inst_name: str, base_url: str, url_type: str) -> str | None:
    """
    Use Brave Search to find the exact loan or mortgage rate page for an institution.
    Returns the best URL found, or None.
    url_type: 'loan' | 'mortgage'
    """
    if not BRAVE_API_KEY:
        return None

    domain = urllib.parse.urlparse(base_url).netloc.lstrip("www.")
    if url_type == "loan":
        queries = [
            f"{inst_name} auto loan rates site:{domain}",
            f"{inst_name} loan rates APR site:{domain}",
        ]
    else:
        queries = [
            f"{inst_name} mortgage rates site:{domain}",
            f"{inst_name} home loan rates site:{domain}",
        ]

    for query in queries:
        try:
            url = ("https://api.search.brave.com/res/v1/web/search?q="
                   + urllib.parse.quote(query) + "&count=3")
            req = urllib.request.Request(url, headers={
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
                "X-Subscription-Token": BRAVE_API_KEY,
            })
            with urllib.request.urlopen(req, timeout=10) as r:
                raw = r.read()
                try:
                    raw = gzip.decompress(raw)
                except Exception:
                    pass
                data = json.loads(raw)

            results = data.get("web", {}).get("results", [])
            for result in results:
                result_url = result.get("url", "")
                # Only return URLs from the institution's own domain
                if domain and domain in result_url:
                    # Filter out generic/nav pages
                    skip_patterns = ["/contact", "/about", "/faq", "/news",
                                     "/branch", "/location", "/login", "/register",
                                     "/calculator", "/apply", "/sign-in"]
                    if not any(p in result_url.lower() for p in skip_patterns):
                        return result_url
        except Exception:
            pass
        time.sleep(0.5)

    return None


def probe_url(url: str) -> bool:
    """
    Check if a URL returns HTTP 200. Returns True if reachable.
    Follows redirects up to 3 hops.
    """
    try:
        req = urllib.request.Request(url, method="HEAD", headers={
            "User-Agent": "Mozilla/5.0 (compatible; RateBot/1.0)"
        })
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as e:
        # 405 = Method Not Allowed for HEAD — try GET instead
        if e.code == 405:
            try:
                req2 = urllib.request.Request(url, headers={
                    "User-Agent": "Mozilla/5.0 (compatible; RateBot/1.0)"
                })
                with urllib.request.urlopen(req2, timeout=REQUEST_TIMEOUT) as resp:
                    return resp.status == 200
            except Exception:
                return False
        # 301/302 redirects are followed automatically
        return False
    except Exception:
        return False


def discover_loan_url(base_url: str, inst_name: str = "") -> str | None:
    """
    Find the loan rate page for an institution.
    Strategy: 1) Brave Search (finds exact pages), 2) path probing fallback.
    """
    # 1. Brave Search — most accurate
    if inst_name and BRAVE_API_KEY:
        url = brave_find_rate_url(inst_name, base_url, "loan")
        if url:
            return url

    # 2. Path probing fallback
    base = base_url.rstrip("/")
    for path in LOAN_PATHS:
        candidate = base + path
        if probe_url(candidate):
            return candidate
        time.sleep(DELAY_BETWEEN)
    return None


def discover_mortgage_url(base_url: str, inst_name: str = "") -> str | None:
    """
    Find the mortgage rate page for an institution.
    Strategy: 1) Brave Search (finds exact pages), 2) path probing fallback.
    """
    # 1. Brave Search — most accurate
    if inst_name and BRAVE_API_KEY:
        url = brave_find_rate_url(inst_name, base_url, "mortgage")
        if url:
            return url

    # 2. Path probing fallback
    base = base_url.rstrip("/")
    for path in MORTGAGE_PATHS:
        candidate = base + path
        if probe_url(candidate):
            return candidate
        time.sleep(DELAY_BETWEEN)
    return None


def _discover_one(row_data, url_type, force):
    """
    Discover loan/mortgage URLs for a single institution.
    Returns (inst_id, loan_url_or_None, mortgage_url_or_None).
    Thread-safe — uses its own DB connection.
    """
    inst_id  = row_data["id"]
    base_url = row_data["website_url"]
    loan_url = row_data["loan_rates_url"]
    mtg_url  = row_data["mortgage_rates_url"]

    found_loan = found_mtg = None

    inst_name = row_data.get("name", "")

    if url_type in ("loan", "both") and (force or not loan_url):
        found_loan = discover_loan_url(base_url, inst_name)

    if url_type in ("mortgage", "both") and (force or not mtg_url):
        found_mtg = discover_mortgage_url(base_url, inst_name)

    return inst_id, found_loan, found_mtg


def run_discovery(inst_ids=None, force=False, url_type="both", workers=4):
    """
    Discover loan/mortgage URLs for institutions that don't have them yet.

    inst_ids:  list of institution IDs to process (None = all eligible)
    force:     re-check all institutions even if URLs already exist
    url_type:  'loan' | 'mortgage' | 'both'
    workers:   parallel threads (default 4, max 20)
    """
    import threading
    import concurrent.futures

    workers = max(1, min(workers, 20))
    conn = get_conn()
    c    = conn.cursor()
    now  = datetime.now(timezone.utc).isoformat()

    if inst_ids:
        placeholders = ",".join("?" * len(inst_ids))
        query = f"""SELECT id, name, website_url, loan_rates_url, mortgage_rates_url
                    FROM institutions WHERE id IN ({placeholders}) AND website_url IS NOT NULL"""
        rows = c.execute(query, inst_ids).fetchall()
    else:
        conditions = ["website_url IS NOT NULL", "active = 1"]
        if not force:
            url_conds = []
            if url_type in ("loan", "both"):
                url_conds.append("loan_rates_url IS NULL")
            if url_type in ("mortgage", "both"):
                url_conds.append("mortgage_rates_url IS NULL")
            if url_conds:
                conditions.append(f"({' OR '.join(url_conds)})")
        query = f"SELECT id, name, website_url, loan_rates_url, mortgage_rates_url FROM institutions WHERE {' AND '.join(conditions)}"
        rows = c.execute(query).fetchall()

    total = len(rows)
    print(f"URL Discovery: {total} institutions to process (type: {url_type}, workers: {workers})")

    found_loan = found_mortgage = 0
    _lock = threading.Lock()
    done_count = [0]

    def _handle(i, row, result):
        nonlocal found_loan, found_mortgage
        inst_id, loan_url, mtg_url = result
        name = row["name"]

        conn2 = get_conn()
        c2 = conn2.cursor()
        updates = []

        if loan_url:
            c2.execute("UPDATE institutions SET loan_rates_url=? WHERE id=?", (loan_url, inst_id))
            updates.append(f"loan={loan_url}")
            found_loan += 1
        if mtg_url:
            c2.execute("UPDATE institutions SET mortgage_rates_url=? WHERE id=?", (mtg_url, inst_id))
            updates.append(f"mtg={mtg_url}")
            found_mortgage += 1
        conn2.commit()
        conn2.close()

        with _lock:
            done_count[0] += 1
            summary = " | ".join(updates) if updates else "—"
            print(f"  [{done_count[0]}/{total}] {name[:45]:<45} {summary}", flush=True)

    row_dicts = [dict(r) for r in rows]

    if workers == 1:
        for i, row in enumerate(row_dicts):
            result = _discover_one(row, url_type, force)
            _handle(i, row, result)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_discover_one, row, url_type, force): (i, row)
                for i, row in enumerate(row_dicts)
            }
            for future in concurrent.futures.as_completed(futures):
                i, row = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    with _lock:
                        done_count[0] += 1
                        print(f"  [{done_count[0]}/{total}] {row['name'][:45]:<45} ERROR: {exc}", flush=True)
                    continue
                _handle(i, row, result)

    conn.close()
    print(f"""
═══ URL Discovery Complete ═══
  Loan URLs found:     {found_loan}
  Mortgage URLs found: {found_mortgage}
  Total processed:     {total}
""")


def main():
    parser = argparse.ArgumentParser(description="Loan/Mortgage URL Discovery")
    parser.add_argument("--id",     metavar="INST_ID",
                        help="Single institution DB id (e.g. ncua:67790)")
    parser.add_argument("--force",  action="store_true",
                        help="Re-discover even if URLs already set")
    parser.add_argument("--type",    choices=["loan", "mortgage", "both"], default="both",
                        help="Which URL type to discover (default: both)")
    parser.add_argument("--workers", type=int, default=4,
                        help="Parallel threads (default: 4, max: 20)")
    args = parser.parse_args()

    inst_ids = [args.id] if args.id else None
    run_discovery(inst_ids=inst_ids, force=args.force, url_type=args.type, workers=args.workers)


if __name__ == "__main__":
    main()
