# Scraper Strategy — Deposit Intelligence

How we collect loan and mortgage rate data, what works, what doesn't, and why.

Last updated: 2026-03-28

---

## The Core Problem

Banks and credit unions publish rates in wildly different ways:
- Some have clean HTML tables (easy)
- Some render rates via JavaScript widgets (need Playwright)
- Some require a zip code or form submission to show rates (need Playwright + interaction)
- Some just don't publish public rates at all (N/O — can't be solved)

Our pipeline handles all the solvable cases. The unsolvable ones are documented below.

---

## Scraper Priority Order

For each institution, we attempt scrapers in this order:

```
1. Jina Reader API (free, fast, markdown output)
   → works for: static HTML rate pages, most CU sites
   → fails for: JS-rendered widgets, anti-bot sites

2. Playwright (headless Chromium + residential-ish UA)
   → works for: JS-rendered pages, lazy-loaded tables, scroll-triggered content
   → fails for: aggressive bot detection (Cloudflare Enterprise, Chase, PNC)

3. Aggregator fallback (DepositAccounts.com / Bankrate)
   → use when: direct scraping is blocked for deposit rates
   → note: loan/mortgage aggregators (Bankrate, LendingTree) hide lender names
             behind lead-gen forms — not useful for competitive reports

4. Manual / N/O
   → use when: institution simply doesn't publish public rates
```

---

## URL Discovery

Don't rely on guessing rate page paths. Use **Brave Search** to find the exact URL:

```
"securityplusfcu.org auto loan rates"
→ finds: securityplusfcu.org/support/information/rates/auto-recreational-loan-rates
```

This avoids the nav-only trap (scraping the homepage instead of the rate table).

**Common mistakes to avoid:**
- Scraping `/auto-loans` (product marketing page) instead of `/rates/auto-loan-rates`
- Using the wrong domain entirely (Point Breeze is `pbcu.com`, not `pointbreezecu.com`)
- Scraping the main `/rates` page when loan rates are on a sub-page

---

## Baltimore Market — Institution Status

### Auto Loan Rates

| Institution | Method | Status | Notes |
|---|---|---|---|
| Securityplus FCU | Playwright | ✅ | `/support/information/rates` — full table |
| MECU | Playwright | ✅ | `/Learn/Resources/Rates/Vehicle-Loan-Rates` — JS-rendered |
| Peake FCU | Playwright | ✅ | `/rates/loan/` — JS-rendered |
| Point Breeze CU | Playwright | ✅ | `pbcu.com/resources/popular-requests/rates/loan-rates` |
| State Employees CU (SECU MD) | Playwright | ✅ | `/rates-calculators/` with scroll to `#auto-loans` |
| NIH FCU | Jina | ✅ | Clean static HTML |
| PNC Bank | Jina (range only) | ⚠️ | Publishes range only (5.34%–20.69%); no term table |
| M&T Bank | Playwright | ✅ | Mortgage only; auto not publicly posted |
| Chase | ❌ N/O | Not published | No public auto loan rates anywhere |
| Wells Fargo | ❌ N/O | Not published | No public auto loan rates |
| Bank of America | Playwright | ✅ | Rates available via Playwright |
| Truist | Playwright | ✅ | Rates available via Playwright |
| First Financial of MD | ❌ Missing | Not in DB | Need to add institution |
| Rosedale Bank | ❌ N/O | No public table | JS iframe widget, no fallback |

### Mortgage Rates

| Institution | Method | Status | Notes |
|---|---|---|---|
| Securityplus FCU | Playwright | ✅ | `/support/information/rates/mortgage-rates` |
| MECU | Playwright | ✅ | Shows rate + APR columns — extract both |
| PNC Bank | Jina | ✅ | `pnc.com/mortgage/mortgage-rates.html` via Jina only (Playwright HTTP/2 errors) |
| Wells Fargo | Playwright | ✅ | `/mortgage/rates/` |
| State Employees CU | Playwright | ✅ | `/rates-calculators/#mortgages` |
| BofA | Playwright | ✅ | Full table with APR |
| Truist | Playwright | ✅ | |
| M&T Bank | Playwright | ✅ | 15/30yr fixed + 5yr ARM |
| First Peoples Community | Jina | ✅ | |
| LM FCU | Jina | ✅ | Home equity / 2nd mortgage |
| Homewood Federal Savings | Playwright | ✅ | 40 rates across terms |
| Chase | ❌ N/O | Not scrapable | Pure JS widget, no static fallback |
| Point Breeze CU | ❌ N/O | Calculator only | No rate table, only a rate calculator CTA |
| Peake FCU | ❌ N/O | Calculator only | Same — no rate table |
| Rosedale Bank | ❌ N/O | No public table | |
| First Financial of MD | ❌ Missing | Not in DB | Need to add institution |

---

## Why Aggregators Don't Help for Loan/Mortgage Competitive Reports

We tested Bankrate, NerdWallet, and LendingTree. Here's what happened:

- **Bankrate mortgage rates page** — shows national averages only. The lender comparison table is JS-rendered and shows "Top offers" not tied to specific named institutions.
- **LendingTree auto loans** — shows rates with no lender names. Click "See Results" → lead gen form. Not scrapable.
- **NerdWallet** — blocks Playwright (timeout), blocks Jina.
- **Bankrate individual lender pages** (`/mortgages/pnc-bank-mortgage-rates/`) — 404, no longer exist.

The aggregators' business model is lead generation, not data transparency. They deliberately hide lender identities until you enter your info.

**Exception:** For deposit rates, DepositAccounts.com publishes structured HTML tables with named institutions — that's why it works for the deposit ranking report. No equivalent exists for loan/mortgage competitive data.

---

## Why Chase Auto Rates Are Unavailable

Chase does not publish auto loan rates publicly. Period.

- Their auto loans page (`chase.com/personal/auto/auto-loans`) shows marketing copy only
- No rate table, no PDF, no calculator with rates
- Chase only provides rates at the dealer (dealer financing) or at point of application
- Bankrate/LendingTree don't show Chase by name (lead gen model)
- S&P/Datatrac get these via phone verification — not web scraping

**Bottom line:** Chase auto = N/O unless we pay Datatrac.

---

## PNC Technical Notes

PNC is partially solvable:
- **Mortgage:** Jina works on their mortgage rates page despite Playwright failing (HTTP/2 errors). Both rate and APR are available.
- **Auto:** Published as a range only (5.34%–20.69% APR). The dedicated auto rates page (`/auto-loan-rates.html`) returns 404. We store the floor rate per term from their calculator.

---

## SECU MD Technical Notes

SECU's rates are all on one page (`secumd.org/rates-calculators/`) but the loan/mortgage sections are below the fold and lazily loaded. The fix:

```python
page.goto(url, wait_until='networkidle')
page.wait_for_timeout(5000)
page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
page.wait_for_timeout(3000)
text = page.inner_text('body')
```

Without the scroll + extra wait, only deposit rates load. With it, the full page including auto and mortgage sections is available.

---

## Playwright Configuration That Works

```python
browser = p.chromium.launch(headless=True)
ctx = browser.new_context(
    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
page = ctx.new_page()
page.goto(url, wait_until='networkidle', timeout=30000)
page.wait_for_timeout(4000)
page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
page.wait_for_timeout(2000)
text = page.inner_text('body')
```

Key points:
- Use a real macOS Chrome UA — headless Chromium without a UA gets flagged
- `networkidle` wait catches most lazy-loading; fall back to `domcontentloaded` if it times out
- Always scroll to bottom — triggers lazy-loaded rate table sections
- Extra 2–4 second sleeps are necessary; some sites load rate data in a second XHR after initial page load

---

## APR vs Interest Rate

Many pages show only one value. GPT extraction rules:

| Page shows | `apy` column | `apr` column |
|---|---|---|
| "5.25% APR" (one value, labeled APR) | null | 0.0525 |
| "5.25% rate, 5.507% APR" (two values) | 0.0525 | 0.05507 |
| "5.25%" (unlabeled) | 0.0525 | null |

MECU is notable: their mortgage table has explicit Rate and APR columns side by side — always extract both.

---

## RateAPI.dev — Tested 2026-03-28

**What it covers:** Credit unions only. Mortgages only (auto loan endpoint returns empty for MD).

**What it doesn't cover:** Banks (Chase, PNC, WF, BofA, M&T). Auto loans.

**Pricing:**
- Free: 50 req/month
- Starter: $9/mo — 1,000 req/month
- Pro: $49/mo — 1,000 req/month + webhooks + analytics

**Verdict:** Not useful for bank competitive reports. Useful as a supplement for finding low-rate CUs statewide, but our Playwright pipeline already covers the specific Baltimore CUs we need.

---

## Datatrac — The S&P Source

Datatrac is the underlying data source for S&P's Deposit Ranking Reports and likely their loan reports too. They collect rates via phone calls + web scraping with human verification.

- No public pricing — enterprise sales only
- Estimated: $500–$2,000+/month depending on markets and products
- Daily updates, human-verified, 100k+ banking locations
- Would solve Chase auto, PNC auto, and all currently N/O institutions

**When to consider:** If Strum Platform signs 10+ clients for competitive rate reports, Datatrac becomes economically viable. For <10 clients, our web-scraping approach is more cost-effective.

---

## Adding a New Market

1. **Run institution discovery** (branch_geography + cu_geography already loaded)
2. **Find rate page URLs** using Brave Search for each institution — don't guess paths
3. **Test with Jina first** — it's free and fast
4. **Fall back to Playwright** for JS-rendered pages
5. **Document N/O institutions** — be explicit about why each one is missing
6. **Check domain** — CUs sometimes have non-obvious domains (e.g., `pbcu.com` for Point Breeze)

Typical time for a new market: 2–4 hours for URL discovery + initial scrape, 30 min for GPT parsing and report generation.
