# PARSE_RESULTS.md — Updated 2026-03-28

## Summary of Changes

### Schema Changes
- Added `apr REAL` column to `rates` table (stores APR as decimal, e.g. 0.0449 for 4.49%)
- Updated `scrapers/schema.py` migrate() to auto-add column

### Parser Updates (scrapers/llm_parser.py)
- Updated `LOAN_EXTRACT_PROMPT` to extract both `rate` and `apr` fields as PERCENTAGES
- Updated `MORTGAGE_EXTRACT_PROMPT` to extract both `rate` and `apr` fields as PERCENTAGES
- Updated loan insert code to store `apr` column (normalized from percentage to decimal)
- Updated mortgage insert code to store `apr` column

### Report Updates
- `jobs/loan_ranking_report.py`: Added APR column — now shows Rate | APR | Chg
- `jobs/mortgage_ranking_report.py`: Added APR column — now shows Rate | APR | Chg
- Both text and PDF modes updated

---

## Task 2: Re-Parse Missing Institutions

Ran `/tmp/reparse_missing.py` against 17 loan + 18 mortgage institutions with raw content but 0 rates.

**Results:**
- Most raw sections were navigation/homepage content — GPT correctly returned 0 rates
- **Orrstown Bank (fdic:713)**: 3 new auto loan rates extracted (7.05% APR)
- **Truist Bank (fdic:9846)**: 5 mortgage rates extracted (15Yr Fixed: 5.75%/6.05% APR, 30Yr Fixed: 6.10%/6.49% APR)

---

## Task 3: Re-Scrape JS-Heavy Sites

### Securityplus FCU (ncua:2769) — FULLY PARSED ✅
- Scraped correct rate subpages via Playwright
- **Loan URL**: `https://www.securityplusfcu.org/support/information/rates/auto-recreational-loan-rates`
- **Mortgage URL**: `https://www.securityplusfcu.org/support/information/rates/mortgage-rates`

**Auto Loan Rates captured (APR, as-low-as):**
| Term | New Auto | Used Auto |
|------|----------|-----------|
| 36mo | 4.49% | 4.99% |
| 48mo | 4.74% | 5.24% |
| 60mo | 4.99% | 5.49% |
| 72mo | 5.24% | 5.74% |
| 84mo | 5.99% | 6.49% |

**Mortgage Rates captured:**
| Product | Rate | APR |
|---------|------|-----|
| 10Yr Fixed | 5.25% | 5.71% |
| 15Yr Fixed | 5.50% | 5.90% |
| 20Yr Fixed | 5.70% | 6.07% |
| 30Yr Fixed | 5.75% | 6.08% |

### Wells Fargo (fdic:3511) — Mortgage PARSED ✅
- Scraped via Playwright (networkidle → domcontentloaded)
- 15Yr Fixed: **5.75% / 6.02% APR**
- 30Yr Fixed: **5.875% / 6.106% APR**
- 7/6 ARM: 6.125% / 6.41% APR

### PNC Bank (fdic:6384) — ERR_HTTP2_PROTOCOL_ERROR
- PNC auto loan page blocked Playwright; loan data unchanged (still has PNC rates from prior scrape)

### JPMorgan Chase (fdic:628) — Partial
- Chase page loaded (6945 chars) but no extractable loan rate tables (marketing content only)

### SECU Maryland (ncua:66330) — Insufficient
- Alternative URL returned 1358 chars — nav only; no rates extracted

---

## Competitive Comparison — Baltimore MD Market

### New Auto Loan — 36Mo (as-low-as APR)
| Rank | Institution | Rate | APR |
|------|-------------|------|-----|
| 1 | NATIONAL INSTITUTES OF HEALTH | 3.99% | — |
| **2** | **► SECURITYPLUS FCU** | **4.49%** | **—** |
| 3 | POINT BREEZE | 4.99% | — |
| 4 | PNC Bank | 5.34% | — |
| Group Avg | | 4.70% | |

**SECURITYPLUS is ranked #2 out of 4 institutions with rates (2nd most competitive)**

### 15Yr Fixed Mortgage — Conforming
| Rank | Institution | Rate | APR |
|------|-------------|------|-----|
| 1 | FIRST PEOPLES COMMUNITY | 5.50% | — |
| **T2** | **► SECURITYPLUS FCU** | **5.50%** | **5.90%** |
| T2 | FIRST PEOPLES COMMUNITY | 5.75% | — |
| T2 | Truist Bank | 5.75% | 6.05% |
| T2 | Wells Fargo | 5.75% | 6.02% |
| 6 | Bank of America | 5.88% | — |
| Group Avg | | 5.86% | |

**SECURITYPLUS is TIED FOR #1 on 15Yr Fixed at 5.50% (with FIRST PEOPLES COMMUNITY)**

---

## PDF Report Paths
- Loan Ranking Report: `/tmp/loan_report_v2.pdf`
- Mortgage Ranking Report: `/tmp/mortgage_report_v2.pdf`

---

## DB Rate Counts After All Parsing
```
mortgage_arm|23|3.25%|6.50%
mortgage_fixed|69|4.99%|7.50%
new_auto_loan|20|3.99%|7.05%
personal_loan|20|5.00%|12.50%
used_auto_loan|20|3.99%|15.39%
```
*(Total: 152 rates, +11 net new from this session)*

---

## Session 2026-03-28 — 4 New Institutions Scraped & Parsed

### Playwright Scrape Results

| Institution | DB ID | Loan Chars | Mortgage Chars | Notes |
|---|---|---|---|---|
| MECU (Municipal Empl. CU of Baltimore) | ncua:66787 | 6,579 | 3,638 | Clean table data, both rate + APR columns |
| Peake FCU | ncua:15394 | 7,984 | 4,592 | GPT returned 0 mortgage rates (rates page shows calculators only) |
| SECU MD | ncua:66330 | 8,140* | 4,676* | Required re-scrape with anchor URLs (#auto-loans, #mortgages) — initial scrape captured deposit rates only |
| Point Breeze CU | ncua:66585 | 10,611 | 2,010 | GPT returned 0 mortgage rates (mortgage section brief/CTA only) |

*SECU MD re-scraped via `#auto-loans` + `#personal-loans` combined for loans, `#mortgages` for mortgage.

**Bug fixed:** Initial insert had `vehicle_age_years=NULL` for all auto loans. Fixed to `vehicle_age_years=0` (new) and `vehicle_age_years=2` (used) to match report query expectations.

### Rates Inserted Per Institution

| Institution | Product | Count | Min Rate | Max Rate |
|---|---|---|---|---|
| MECU | mortgage_arm | 4 | 4.875% | 5.375% |
| MECU | mortgage_fixed | 8 | 4.875% | 6.0% |
| MECU | new_auto_loan | 4 | 4.74% | 5.99% |
| MECU | used_auto_loan | 4 | 4.99% | 6.24% |
| Peake FCU | home_equity_loan | 4 | 5.75% | 6.75% |
| Peake FCU | new_auto_loan | 3 | 4.75% | 5.75% |
| Peake FCU | personal_loan | 6 | 7.9% | 12.0% |
| Peake FCU | used_auto_loan | 3 | 4.75% | 5.75% |
| SECU MD | mortgage_arm | 7 | 5.0% | 6.125% |
| SECU MD | mortgage_fixed | 6 | 5.375% | 7.625% |
| SECU MD | new_auto_loan | 5 | 4.24% | 6.09% |
| SECU MD | used_auto_loan | 5 | 4.49% | 6.34% |
| Point Breeze | home_equity_loan | 4 | 5.24% | 6.5% |
| Point Breeze | new_auto_loan | 5 | 4.49% | 5.99% |
| Point Breeze | personal_loan | 3 | 5.0% | 8.95% |
| Point Breeze | used_auto_loan | 5 | 4.49% | 5.99% |

**Total new rows: 76 (53 initial + 23 SECU MD re-parse)**

### Updated S&P Comparison: New Auto 36Mo

| Rank | Institution | Rate |
|------|-------------|------|
| 1 | NATIONAL INSTITUTES OF HEALTH | 3.99% |
| 2 | **STATE EMPLOYEES CU MD** | **4.24%** *(new)* |
| T3 | ► SECURITYPLUS FCU | 4.49% |
| T3 | **POINT BREEZE CU** | **4.49%** *(new)* |
| 5 | PNC Bank | 5.34% |
| Group Avg | | 4.51% |

**SECURITYPLUS is now T3 (was T2 before SECU MD and Point Breeze added)**

### Updated S&P Comparison: 15Yr Fixed Mortgage — Conforming

| Rank | Institution | Rate | APR |
|------|-------------|------|-----|
| 1 | **MECU** | **5.125%** | **5.457%** *(new — BEST in market)* |
| 2 | T: MECU (2nd row) | 5.375% | 5.553% |
| T3 | FIRST PEOPLES COMMUNITY | 5.50% | — |
| T3 | ► SECURITYPLUS FCU | 5.50% | 5.90% |
| 5 | PNC Bank | 5.625% | 5.866% |
| T6 | FIRST PEOPLES COMMUNITY | 5.75% | — |
| T6 | SECU MD | 5.75% | 5.97% |
| T6 | Truist | 5.75% | 6.05% |
| T6 | Wells Fargo | 5.75% | 6.02% |
| 10 | Bank of America | 5.875% | — |
| Group Avg | | ~5.74% | |

**MECU is now the lowest 15Yr Fixed in the market at 5.125%**
**SECURITYPLUS is now #3 (was #1/#2 before MECU added)**

### Total DB Coverage

| Product | Total Rates | Institutions with Rates |
|---|---|---|
| mortgage_arm | 36 | — |
| mortgage_fixed | 87 | — |
| new_auto_loan | 37 | — |
| used_auto_loan | 37 | — |
| personal_loan | 26 | — |
| home_equity_loan | 8 | — |
| **TOTAL** | **231** | **16** |

### Coverage vs S&P Benchmark Peer List
- S&P benchmark typically tracks 40+ Baltimore-area institutions
- Currently have rates for **16 institutions** across the peer group
- CUs now covered: Securityplus, NIH FCU, MECU, Peake, SECU MD, Point Breeze (6 CUs)
- Major banks with rates: PNC, BofA, Wells Fargo, Truist, Manufacturers & Traders, Homewood, LM (~7)
- Coverage: **~38% of peer group** (16/42 institutions)
- Key gaps remaining: First Financial of MD, Cedar Point, Signal Financial, APL, Destinations, Johns Hopkins, Alliance Niagara, Five Star, Central CU of MD, Local 355

### PDF Paths (v3)
- Loan Ranking Report v3: `/tmp/loan_report_v3.pdf`
- Mortgage Ranking Report v3: `/tmp/mortgage_report_v3.pdf`
