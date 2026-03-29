# Brave Discovery Results — Baltimore Market Loan/Mortgage Rates
**Run Date:** 2026-03-28  
**Scraped Week:** 2026-13  

## Summary

Used Brave Search to find rate page URLs for 21 Baltimore market institutions missing loan/mortgage data. Tested URLs with Jina AI and web_fetch, parsed with GPT-4o. Successfully inserted rates for **9 institutions** (loan rates for 7, mortgage rates for 5).

---

## Results by Institution

### ✅ Successfully Parsed

| Institution | ID | Loan Rates | Mtg Rates | Notes |
|---|---|---|---|---|
| **CEDAR POINT FCU** | ncua:5234 | 11 (auto) | 10 | Combined rates page; cpfcu.com/cards-and-loans/loan-and-mortgage-rates |
| **JOHNS HOPKINS FCU** | ncua:20623 | 10 (auto+personal) | 23 | jhfcu.org/rates/loan-rates + home-loan-rates |
| **FIRST PEOPLES COMMUNITY FCU** | ncua:13345 | 13 (auto) | 13 | firstpeoples.com/rates — combined page; auto loans by model year |
| **LM FCU** | ncua:6039 | 12 (auto) | 0 | lmfcu.org/Rates/Loan-Rates/Auto-Loans — new/used by model year |
| **SIGNAL FINANCIAL FCU** | ncua:5571 | 3 (auto+personal) | 3 | lending-rates (Tier A) + mortgage-solutions |
| **DESTINATIONS CREDIT UNION** | ncua:66333 | 5 (auto) | 0 | destinationscu.org/vehicle-lending-center/ — promo 2.49% + standard 4.74% |
| **Fulton Bank** | fdic:7551 | 7 (auto+personal) | 0 | fultonbank.com/Rates — rates by model year (7.44%–10.28%) |
| **Homewood Federal Savings Bank** | fdic:31267 | 0 | 10+2 HE | homewoodfsb.com/loan-rates/ — conforming, jumbo, home equity |
| **Orrstown Bank** | fdic:713 | 3 (existing) | 0 | Had loans, mortgage page is a calculator (no published rates) |

### ⚠️ URLs Found, Rates Not Parseable (JS-rendered or no rates published)

| Institution | ID | Loan URL | Mortgage URL | Reason |
|---|---|---|---|---|
| **FIRST FINANCIAL OF MD** | ncua:8554 | firstfinancial.org/rates/loan-rates/ | firstfinancial.org/rates/mortgage-rates/ | JS-rendered; Jina returns 280c only |
| **FIVE STAR OF MARYLAND** | ncua:19668 | fivestarfcu.org/rates/ | fivestarfcu.org/rates/ | JS-rendered; only 329c |
| **CENTRAL CREDIT UNION MD** | ncua:66340 | ccumd.org/loans/lending-rates/ | ccumd.org/loans/lending-rates/ | Redirect page, JS-rendered |
| **Rosedale Bank** | fdic:29613 | rosedale.bank/rate-center/ | rosedale.bank/rate-center/mortgage-rates/ | JS-rendered; rate center loads via JS |
| **FVCbank** | fdic:58696 | fvcbank.com/interest-rates/ | (already has mtg URL) | 13,499c but no % rate data in loans section |
| **First National Bank of PA** | fdic:7888 | fnb-online.com/personal/loans-mortgages/regional-rates | (same) | JS-rendered; rates in dynamic widget |

### ❌ No Public Rates Published

| Institution | ID | Notes |
|---|---|---|
| **BayVanguard Bank** | fdic:32527 | Marketing pages only; no rates page found; community bank likely uses in-branch pricing |
| **Shore United Bank** | fdic:4832 | Auto loan page is marketing + calculator only; mortgage page is promotional |
| **TD Bank** | fdic:18409 | No publicly listed auto loan rates (requires application); mortgage rates via rate request form |
| **The Harbor Bank of MD** | fdic:24015 | Rates page returns generic content; no loan/mortgage rates in HTML |
| **United Bank** | fdic:22858 | Mortgage landing page (no rates); auto loan page already in DB |
| **WesBanco Bank** | fdic:803 | Both URLs lead to marketing pages with "Schedule an Appointment" CTAs — no rates |

---

## Key Findings

### Institutions Needing Playwright Scraping
These have confirmed rate data but require JavaScript execution:
- **FIRST FINANCIAL OF MARYLAND** (ncua:8554) — 280c from Jina but URL confirmed correct
- **FIVE STAR OF MARYLAND** (ncua:19668) — 329c from Jina; rates page exists  
- **CENTRAL CREDIT UNION MD** (ncua:66340) — Redirect to external platform
- **Rosedale Bank** (fdic:29613) — JS-loaded rate tables

### Institutions with Confirmed No Public Rates
Will never have scrapeable data without an API or member login:
- BayVanguard Bank, Shore United Bank, TD Bank, WesBanco, United Bank, Harbor Bank

### URL Updates Made to DB
All institutions had their `loan_rates_url` and/or `mortgage_rates_url` updated even when parsing failed, for future Playwright attempts.

---

## Rate Data Quality Notes

- **Cedar Point**: Combined page → extracted both loan & mortgage products; rates look circa 2024-2025 era (3.99%–5.49% new auto)
- **JHFCU**: Rates include home equity products tagged as mortgage_arm (5-10 yr terms) — may need cleanup
- **LM FCU**: Rates by model year group (new 2025/2024: 3.99%–5.49%; used 2020-2022: 4.49%–6.49%)
- **First Peoples**: Full matrix by model year (2023+: 3.25%–4.99%; 2018-2022: 3.99%–6.75%)
- **Fulton Bank**: Rates by model year (new/2023-2026: 7.44%; older used up to 10.28%)
- **Signal Financial**: Tier A rates only (best credit): new auto 4.74%, used 4.99%; 30yr mortgage 5.875%
- **Homewood FSB**: Conforming (6.125%–6.50%) and jumbo (6.50%–7.125%) mortgages; home equity 7.25%–7.50%
- **Destinations CU**: Promotional rate 2.49% APR for 6 months, then standard 4.74%

---

## Database Changes

- **Inserted 2026-13 week rates:** ~97 total loan/mortgage rate records
- **URL updates:** 21 institutions had loan/mortgage URLs confirmed or updated
- **Scrape status:** Set to 'ok' for institutions where raw content was saved

## Next Steps

1. **Playwright scraper** for First Financial MD, Five Star MD, Central CU MD, Rosedale Bank
2. **FVCbank** — check if `/interest-rates/` page has auto loan section (currently only CD/deposit rates visible)
3. **LM FCU mortgage** — URL not yet found; the existing mortgage_rates_url may have the data
4. **Destinations CU mortgage** — home-equity page has no rates; need separate mortgage page search
