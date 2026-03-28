# BUILD NOTES — Loan & Mortgage Ranking Reports
_Generated: 2026-03-28_

## Files Created / Modified

### Modified
| File | Changes |
|------|---------|
| `scrapers/schema.py` | Added `loan_rates_url`, `mortgage_rates_url` to institutions table; added 7 loan-specific columns to rates table; added `new_auto_loan`, `used_auto_loan`, `mortgage_fixed`, `mortgage_arm` to product_group_map; added full migrations |
| `scrapers/llm_parser.py` | Added 4 new loan product types to PRODUCT_GROUP_MAP + BOUNDS; added `LOAN_EXTRACT_PROMPT`, `LOAN_VERIFY_PROMPT`, `MORTGAGE_EXTRACT_PROMPT`, `MORTGAGE_VERIFY_PROMPT`; added `run_loan_parser()` and `run_mortgage_parser()` functions |

### Created
| File | Purpose |
|------|---------|
| `jobs/loan_ranking_report.py` | Auto loan ranking report (12 product configs, lowest-APR-first, PDF + text) |
| `jobs/mortgage_ranking_report.py` | Mortgage ranking report (10 product configs, no APR column, PDF + text) |
| `scrapers/url_discovery.py` | Probe institution websites for loan/mortgage rate page URLs |
| `jobs/run.py` | Orchestration runner with `--phase` flag for all pipeline phases |

---

## Syntax Check Results

All 6 files passed `python3 -m py_compile` with no errors:

```
scrapers/schema.py          ✅ OK
scrapers/llm_parser.py      ✅ OK
scrapers/url_discovery.py   ✅ OK
jobs/loan_ranking_report.py ✅ OK
jobs/mortgage_ranking_report.py ✅ OK
jobs/run.py                 ✅ OK
```

Schema migration was also applied against the live `db/rates.db` — all new columns confirmed present.

---

## CLI Usage Examples

### Schema / Migration
```bash
cd scrapers && python3 schema.py
# or via run.py:
python3 jobs/run.py --phase migrate
```

### URL Discovery (find loan/mortgage page URLs)
```bash
# Discover all missing URLs for all institutions:
python3 scrapers/url_discovery.py

# Single institution:
python3 scrapers/url_discovery.py --id "ncua:67790"

# Only loan URLs:
python3 scrapers/url_discovery.py --type loan

# Force re-check even if URLs already set:
python3 scrapers/url_discovery.py --force
```

### Loan Rate Parsing
```bash
# Parse loan rates for all institutions with raw_section stored:
python3 jobs/run.py --phase loan-parse

# Force re-parse:
python3 jobs/run.py --phase loan-parse --force

# Single institution:
python3 jobs/run.py --phase loan-parse --id "ncua:67790"
```

### Mortgage Rate Parsing
```bash
python3 jobs/run.py --phase mortgage-parse
python3 jobs/run.py --phase mortgage-parse --force
```

### Loan Ranking Report
```bash
# Text report (by city):
python3 jobs/loan_ranking_report.py --client "Securityplus FCU" --market Baltimore MD --text

# PDF report:
python3 jobs/loan_ranking_report.py --client "Securityplus FCU" --market Baltimore MD --output /tmp/loan_report.pdf

# By CBSA code:
python3 jobs/loan_ranking_report.py --client "Securityplus FCU" --cbsa 12580

# Via run.py:
python3 jobs/run.py --phase loan-report --client "Securityplus FCU" --market Baltimore MD --text
```

### Mortgage Ranking Report
```bash
# Text report:
python3 jobs/mortgage_ranking_report.py --client "Securityplus FCU" --market Baltimore MD --text

# PDF report:
python3 jobs/mortgage_ranking_report.py --client "Securityplus FCU" --market Baltimore MD --output /tmp/mtg_report.pdf

# By CBSA code:
python3 jobs/mortgage_ranking_report.py --client "Securityplus FCU" --cbsa 12580

# Via run.py:
python3 jobs/run.py --phase mortgage-report --client "Securityplus FCU" --market Baltimore MD --text
```

---

## Product Configs

### Loan Ranking Report — AUTO_CONFIGS (12 products)
| Label | Product | Term | Vehicle Age | Loan Amt |
|-------|---------|------|-------------|----------|
| 36Mo New Auto 25k | new_auto_loan | 36 | 0 (new) | $25k |
| 48Mo New Auto 25k | new_auto_loan | 48 | 0 | $25k |
| 60Mo New Auto 25k | new_auto_loan | 60 | 0 | $25k |
| 72Mo New Auto 25k | new_auto_loan | 72 | 0 | $25k |
| 36Mo 2 Yr Used Auto 15k | used_auto_loan | 36 | 2 | $15k |
| 48Mo 2 Yr Used Auto 15k | used_auto_loan | 48 | 2 | $15k |
| 60Mo 2 Yr Used Auto 15k | used_auto_loan | 60 | 2 | $15k |
| 72Mo 2 Yr Used Auto 15k | used_auto_loan | 72 | 2 | $15k |
| 36Mo 4 Yr Used Auto 9k | used_auto_loan | 36 | 4 | $9k |
| 48Mo 4 Yr Used Auto 9k | used_auto_loan | 48 | 4 | $9k |
| 60Mo 4 Yr Used Auto 9k | used_auto_loan | 60 | 4 | $9k |
| 72Mo 4 Yr Used Auto 9k | used_auto_loan | 72 | 4 | $9k |

### Mortgage Ranking Report — MORTGAGE_CONFIGS (10 products)
| Label | Product | ARM Init | ARM Adj | Term | Conforming |
|-------|---------|----------|---------|------|-----------|
| 1Yr ARM Conforming | mortgage_arm | 1 | 12mo | - | 1 |
| 3/1 ARM Conforming | mortgage_arm | 3 | 12mo | - | 1 |
| 5/1 ARM Conforming | mortgage_arm | 5 | 12mo | - | 1 |
| 7/1 ARM Conforming | mortgage_arm | 7 | 12mo | - | 1 |
| 3/6 ARM Conforming | mortgage_arm | 3 | 6mo | - | 1 |
| 5/6 ARM Conforming | mortgage_arm | 5 | 6mo | - | 1 |
| 7/6 ARM Conforming | mortgage_arm | 7 | 6mo | - | 1 |
| 10/6 ARM Conforming | mortgage_arm | 10 | 6mo | - | 1 |
| 15Yr Fixed Conforming | mortgage_fixed | - | - | 180mo | 1 |
| 30Yr Fixed Conforming | mortgage_fixed | - | - | 360mo | 1 |

---

## Limitations & Data Gaps

1. **No loan/mortgage data yet** — the parsers need to run against institutions that have loan rate pages scraped into `raw_section`. Run `url_discovery.py` first to populate `loan_rates_url`/`mortgage_rates_url`, then scrape those pages.

2. **Scraper doesn't use loan_rates_url yet** — `tavily_scraper.py` and `jina_scraper.py` currently only use `rates_url`. The `run.py` loan-scrape/mortgage-scrape phases include a graceful fallback note. To fully enable: update those scrapers to accept a `url_field` parameter so they pull from `loan_rates_url` / `mortgage_rates_url` when set.

3. **vehicle_age_years matching** — the DB query uses `IS ?` (SQLite-safe NULL-safe comparison). If the LLM extracts `vehicle_age_years: null` for new auto loans instead of `0`, those won't match `new_auto_loan` queries that filter on `vehicle_age_years IS 0`. Monitor parser output and adjust prompt if needed.

4. **ARM rates filtering** — ARM query uses `arm_initial_years IS ?` (NULL-safe). If a 1/1 ARM is rarely offered, those tables will simply be omitted from the report (no empty tables shown).

5. **Rate bounds for loans are wide by design** — new_auto_loan (2%–20%), used_auto_loan (2%–25%), mortgage_fixed (3%–12%), mortgage_arm (2.5%–12%). These avoid rejecting legitimate rates. Tighten if false positives appear.

6. **`personal_loan` product** — included in LOAN_EXTRACT_PROMPT but no report section exists for it yet. Data will be stored; a report section can be added to `loan_ranking_report.py` when needed.

7. **Mortgage report has no APR column** — intentional, matches S&P format. APR data (if extracted) goes in the `notes` field only.
