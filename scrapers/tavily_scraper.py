"""
Tavily Extract Scraper
----------------------
Uses Tavily's Extract API as the primary rate page fetcher.

Advantages over Jina + direct HTTP:
  - AI-powered extraction handles JS-heavy pages without Playwright
  - Returns clean markdown optimized for LLM parsing
  - Supports query-guided reranking: chunks most relevant to rates bubble up
  - Batch support: up to 20 URLs per call
  - Cost: 1 credit/URL (basic) or 2 credits/URL (advanced) @ $0.008/credit

API: POST https://api.tavily.com/extract
Key: stored in 1Password → ClawdBotVault → "Tavily API Credentials" (field: credential)
     or TAVILY_API_KEY env var
     or config.json: tavily_api_key

Usage:
    text = fetch_tavily(url)                  # single URL
    results = fetch_tavily_batch(urls)        # list of URLs → {url: text}
"""

import json
import os
import subprocess
import time
import urllib.request
import urllib.error
from typing import Optional

TAVILY_URL = "https://api.tavily.com/extract"
RATE_QUERY = "deposit rates APY savings CD mortgage auto loan interest rate"


def _get_tavily_key() -> str:
    """Load Tavily API key from env, config.json, or 1Password."""
    key = os.environ.get("TAVILY_API_KEY")
    if key:
        return key

    # config.json
    config_path = os.path.join(os.path.dirname(__file__), "..", "config.json")
    try:
        with open(config_path) as f:
            cfg = json.load(f)
            key = cfg.get("tavily_api_key")
            if key:
                return key
    except Exception:
        pass

    # 1Password fallback
    try:
        result = subprocess.run(
            "source ~/.op_service_account && op item get \"Tavily API Credentials\" "
            "--vault ClawdBotVault --fields credential --reveal",
            shell=True, capture_output=True, text=True, executable="/bin/zsh",
        )
        key = result.stdout.strip()
        if key:
            return key
    except Exception:
        pass

    return ""


TAVILY_API_KEY = _get_tavily_key()

# Credit budget: warn if a single run would exceed this many credits
CREDIT_BUDGET_WARN = 500


def fetch_tavily(url: str, depth: str = "basic", timeout: int = 30,
                 retry_advanced: bool = True) -> Optional[str]:
    """
    Fetch a single URL via Tavily Extract.
    Returns cleaned markdown text, or None on failure.

    depth:           "basic" (1 credit) or "advanced" (2 credits)
    retry_advanced:  if basic fails, retry with advanced depth (uses headless browser)
    """
    results = fetch_tavily_batch([url], depth=depth, timeout=timeout)
    text = results.get(url)
    if text:
        return text

    # Basic failed → retry with advanced (headless browser, 2 credits)
    if retry_advanced and depth == "basic":
        results2 = fetch_tavily_batch([url], depth="advanced", timeout=timeout + 15)
        return results2.get(url)

    return None


def fetch_tavily_batch(
    urls: list[str],
    depth: str = "basic",
    timeout: int = 60,
    max_retries: int = 3,
) -> dict[str, Optional[str]]:
    """
    Fetch up to 20 URLs in a single Tavily Extract call.
    Returns: { url: markdown_text_or_None }

    Tavily batches up to 20 URLs per request — we chunk automatically.
    depth: "basic" (1 credit/URL) or "advanced" (2 credits/URL)
    """
    if not TAVILY_API_KEY:
        return {url: None for url in urls}

    results: dict[str, Optional[str]] = {}
    BATCH_SIZE = 20

    for batch_start in range(0, len(urls), BATCH_SIZE):
        batch = urls[batch_start : batch_start + BATCH_SIZE]

        payload = json.dumps({
            "urls": batch,
            "query": RATE_QUERY,           # rerank chunks by rate relevance
            "extract_depth": depth,
            "chunks_per_source": 5,        # top 5 most rate-relevant chunks per URL
            "api_key": TAVILY_API_KEY,     # included in body (works for both dev + prod keys)
        }).encode()

        req = urllib.request.Request(
            TAVILY_URL,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TAVILY_API_KEY}",  # also send as header
            },
        )

        for attempt in range(1, max_retries + 1):
            try:
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    data = json.loads(r.read())
                    for item in data.get("results", []):
                        url_key = item.get("url", "")
                        raw_content = item.get("raw_content") or item.get("content") or ""
                        results[url_key] = raw_content if raw_content.strip() else None
                    # Mark failed URLs explicitly
                    for failed in data.get("failed_results", []):
                        url_key = failed.get("url", "")
                        if url_key:
                            results[url_key] = None
                    break  # success

            except urllib.error.HTTPError as e:
                if e.code == 429:
                    wait = 2 ** attempt * 5
                    print(f"    Tavily 429 — waiting {wait}s (attempt {attempt}/{max_retries})")
                    time.sleep(wait)
                elif e.code in (401, 403):
                    print(f"    Tavily auth error {e.code} — check API key")
                    return {url: None for url in urls}
                else:
                    print(f"    Tavily HTTP {e.code} — attempt {attempt}/{max_retries}")
                    if attempt < max_retries:
                        time.sleep(2 * attempt)
            except Exception as e:
                print(f"    Tavily error: {e} — attempt {attempt}/{max_retries}")
                if attempt < max_retries:
                    time.sleep(2 * attempt)

        # Any URLs in batch that didn't come back get None
        for url in batch:
            if url not in results:
                results[url] = None

        # Polite delay between batches
        if batch_start + BATCH_SIZE < len(urls):
            time.sleep(0.5)

    return results


def has_rate_signals(text: str) -> bool:
    """Quick check — does the extracted content contain rate data?"""
    if not text:
        return False
    signals = ["apy", "%", "rate", "annual", "interest", "savings", "cd",
               "certificate", "mortgage", "loan", "equity"]
    lower = text.lower()
    return sum(1 for s in signals if s in lower) >= 3


def estimate_credits(url_count: int, depth: str = "basic") -> float:
    """Return estimated credit cost for a batch of URLs."""
    return url_count * (1 if depth == "basic" else 2)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python3 tavily_scraper.py <url>")
        sys.exit(1)

    url = sys.argv[1]
    print(f"Fetching: {url}")
    text = fetch_tavily(url)
    if text:
        print(f"\n✅ Got {len(text)} chars")
        print(f"Rate signals: {'yes' if has_rate_signals(text) else 'no'}")
        print("\n--- First 2000 chars ---")
        print(text[:2000])
    else:
        print("❌ No content returned")
