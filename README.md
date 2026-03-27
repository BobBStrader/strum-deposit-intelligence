# Strum Deposit Intelligence
### A Strum Platform™ Feature

A competitive deposit rate intelligence platform that automatically generates weekly ranking reports showing how a client credit union or bank compares to local market peers. Modeled after S&P Global's Deposit Ranking Report (median contract value: $53,344/year per Vendr). Delivered as a Strum Platform add-on module.

---

## What It Does

- **Scrapes publicly available deposit rates** from bank and credit union websites across a target market
- **Builds a local market peer group** using FDIC branch data (78k+ US branches) and NCUA CU data (4,287 CUs)
- **Generates a PDF or Excel ranking report** by CD term (1 month–5 years) at $10k and $100k minimums, plus savings, money market, and checking
- **Reports show:** institution name, APY, week-over-week change, N/O (not offered), and rate group average
- **Client institution is highlighted** with a ► marker so they can immediately see where they rank

---

## Business Context

| | |
|---|---|
| **S&P Global Deposit Ranking Report** | $21K–$217K/year (median **$53,344**, based on 47 purchases via Vendr) |
| **Strum Platform opportunity** | $3–5K/year per client — undercutting S&P by **80%+** |
| **Data costs** | $0 — all data sourced from public websites, no data vendor fees |

This tool replicates the core value proposition of S&P's product: a weekly, auto-generated, peer-ranked rate comparison delivered as a professional PDF report. The key differentiator is that we source data from public websites rather than paying data vendors, which makes the economics dramatically better.

---

## Architecture Overview

```
FDIC BankFind API ──► branch_geography.py ──► branch_markets table
NCUA API          ──► cu_geography.py     ──/
                                          │
Institution websites ──► jina_scraper.py           ──► rates table
                     ──► playwright_scraper.py     ──►
                     ──► manual_rates.py            ──►  (PDFs, manual entry)
                     ──► manual_rates.py --aggregator   (DepositAccounts.com fallback)
                                          │
                     llm_parser.py (gpt-4.1-mini) extracts rates
                                          │
                  peer_group.py ──────────► deposit_ranking_report.py ──► PDF
                                        └──► export_excel.py          ──► Excel
```

**Data flow:**
1. Branch geography loaders pull institution registries from FDIC and NCUA APIs into a local SQLite database
2. Scrapers pull raw HTML/PDF content from institution websites
3. `llm_parser.py` uses GPT-4.1-mini to extract structured APY data from unstructured text
4. `deposit_ranking_report.py` queries the database, builds peer rankings, and renders a ReportLab PDF

---

## Branding

All reports (PDF and Excel) include the **Strum Platform™ logo** in the top-right corner of every sheet/page. The logo file is expected at:

```
strum_platform_logo_rgba.png   (root of repo)
```

To update the logo, replace this file with a new RGBA PNG. The recommended size is at least 800px wide for sharp rendering in both Excel and PDF output. The logo is resized automatically — 160px wide on the Excel cover sheet, 110px wide on data sheets, and proportional in the PDF header.

If the file is missing, reports will generate without a logo (no error).

---

## Prerequisites

- **Python 3.11** or higher
- **OpenAI API key** with `gpt-4.1-mini` access — approximately $0.01 per institution parsed
- **Playwright Chromium:** `playwright install chromium`
- **SQLite** — bundled with Python, no separate install needed
- **Jina API key** (optional) — free tier available at [jina.ai](https://jina.ai) for better JavaScript-rendered page scraping

---

## Installation

```bash
git clone https://github.com/[org]/strum-deposit-intelligence
cd strum-deposit-intelligence
pip install -r requirements.txt
playwright install chromium
cp config.json.example config.json
# Edit config.json and add your OpenAI API key
```

**config.json fields:**
| Field | Description |
|---|---|
| `openai_api_key` | Your OpenAI API key |
| `openai_model` | Primary model — use `gpt-4.1-mini` (do NOT use reasoning models; see Troubleshooting) |
| `openai_fallback_model` | Fallback if mini fails — `gpt-4.1` |
| `default_workers` | Concurrent scrape threads (default: 5) |
| `max_workers` | Max concurrent threads (default: 10) |

---

## Quick Start: Generate Your First Report

### Step 1 — Initialize the database

```bash
python3 scrapers/schema.py
```

This creates `db/rates.db` with all required tables.

### Step 2 — Load branch geography (one-time, ~5 min for all US)

```bash
python3 scrapers/branch_geography.py   # all US banks (~78k branches)
python3 scrapers/cu_geography.py       # all US credit unions (~4,287)
```

These populate the `branch_markets` table so the peer group logic knows which institutions are in each market.

### Step 3 — Load your target market's institutions

```bash
# The FDIC and NCUA registries need to be loaded first
# Run the institution loader from the full rate-scraper pipeline (run.py --phase discover)
```

> **Note:** The full `run.py` pipeline (in the parent rate-scraper project) handles institution discovery and URL assignment. This repo focuses on the ranking report and rate scraping steps downstream of that.

### Step 4 — Scrape rates for your market

```bash
python3 scrapers/playwright_scraper.py --market "Baltimore" MD
python3 scrapers/manual_rates.py --chase   # Chase uses a PDF rate sheet
```

### Step 5 — Generate the report

```bash
# PDF
python3 jobs/deposit_ranking_report.py \
  --client "Your Client CU Name" \
  --market "Baltimore" MD \
  --output /path/to/report.pdf

# Excel (4-sheet workbook: Cover, Liquid Rates, CDs $10k, CDs $100k)
python3 jobs/export_excel.py \
  --client "Your Client CU Name" \
  --market "Baltimore" MD \
  --output /path/to/report.xlsx
```

The report will be written to the specified path. Open it to verify rankings look correct before delivering to the client.

---

## Adding a New Market (Step by Step)

1. **Choose your client institution** and their market city/state
2. **Check what peers exist:**
   ```bash
   python3 scrapers/peer_group.py --market "Seattle" WA
   ```
3. **Check which institutions are missing rates:**
   ```bash
   python3 scrapers/manual_rates.py --missing "Seattle" WA
   ```
4. **Scrape missing institutions:**
   ```bash
   python3 scrapers/playwright_scraper.py --market "Seattle" WA
   ```
5. **Handle special cases** — Chase PDF, manual entries for Cloudflare-blocked sites (see Known Limitations)
6. **Generate the report:**
   ```bash
   python3 jobs/deposit_ranking_report.py --client "..." --market "Seattle" WA --output report.pdf
   ```

---

## Script Reference

### scrapers/schema.py
SQLite schema definition and database connection helper. Defines all tables: `institutions`, `rates`, `product_groups`, `product_group_map`, `branch_markets`. Run directly to initialize a fresh database:
```bash
python3 scrapers/schema.py
```

---

### scrapers/branch_geography.py
Downloads FDIC branch location data via the public BankFind API (`api.fdic.gov`). Populates the `branch_markets` table with city/state/institution mappings. Run once to load all US branches (~78k). Supports single-state loads:
```bash
python3 scrapers/branch_geography.py --state MD
```

---

### scrapers/cu_geography.py
Downloads the NCUA federally-insured credit union list from the NCUA Mapping API. Populates `branch_markets` with CU main office addresses. Run once.

---

### scrapers/peer_group.py
Given a city and state, queries `branch_markets` to return all institutions (banks + CUs) in that market. This is the core of the peer group logic — it determines which institutions appear in the ranking report.

---

### scrapers/jina_scraper.py
Scrapes rate pages using the Jina Reader API, which converts HTML to clean markdown. Works well for most static pages. Falls back to a direct HTTP fetch if Jina is unavailable or times out.

---

### scrapers/playwright_scraper.py
Headless Chromium scraper for JavaScript-rendered pages. Handles:
- **Zip-code-gated sites** (M&T Bank, PNC) — submits a zip code to retrieve location-specific rates
- **PDF rate sheets** (TD Bank) — finds and downloads linked PDFs, then passes to `llm_parser.py`

Requires `playwright install chromium`.

---

### scrapers/llm_parser.py
Uses OpenAI `gpt-4.1-mini` to extract structured rate data from raw page text or PDF content. Outputs normalized APY values (decimal: `0.045` = 4.5%). Includes a two-pass verification step for accuracy.

> ⚠️ **IMPORTANT:** Use `gpt-4.1-mini` only. Do NOT use reasoning models (`o1`, `o3`, `gpt-5.4-mini`, etc.) — reasoning models consume tokens in hidden internal steps and return empty content for structured extraction tasks.

---

### scrapers/manual_rates.py
Handles special cases that automated scrapers can't reach:
- **`--chase`** — Scrapes Chase's public PDF rate sheet using a split-prompt method
- **`--pnc`** — Scrapes PNC Bank via DepositAccounts.com (Cloudflare-blocked direct). Added 50 rates (CDs, savings, MM, checking).
- **`--aggregator fdic:XXXX`** — Scrapes any institution via DepositAccounts.com if it's listed there. No LLM needed — parses structured HTML tables directly. Use as a last-resort fallback before manual entry.
- **`--enter fdic:{cert} "Institution Name"`** — Interactive CLI for manually entering rates for blocked sites
- **`--missing "City" ST`** — Lists institutions in a market that are missing current rates
- **JSON bulk import** — For batch-loading rates from an external source

`AGGREGATOR_SOURCES` (dict at top of file) maps institution IDs to their DepositAccounts.com URLs and table indexes. Add new entries here to extend aggregator coverage.

---

### jobs/deposit_ranking_report.py
Main PDF report generator. Queries the database, builds peer rankings for each product/term, and generates a professional PDF using ReportLab.

```bash
python3 jobs/deposit_ranking_report.py \
  --client "Securityplus FCU" \
  --market "Baltimore" MD \
  --output report.pdf

# Console output (no PDF)
python3 jobs/deposit_ranking_report.py \
  --client "Securityplus FCU" \
  --market "Baltimore" MD \
  --text
```

---

### jobs/export_excel.py
Excel report generator — outputs the same data as `deposit_ranking_report.py` but as an `.xlsx` workbook. Useful for clients who want to slice and filter the data themselves.

**Sheets:**
- **Cover** — client name, market, date, peer count
- **Liquid Rates** — Savings, Money Market, Checking ranked by APY
- **CDs $10k** — All CD terms (1mo–5yr) ranked, $10k minimum tier
- **CDs $100k** — All CD terms ranked, $100k minimum tier

Client rows are highlighted in gold with a ► marker. Week-over-week APY changes are shown in green (up) or red (down).

```bash
python3 jobs/export_excel.py \
  --client "Securityplus FCU" \
  --market "Baltimore" MD \
  --output report.xlsx
```

---

### jobs/fix_market_rates.py
Re-scrapes institutions in a market that have `error` or `missing` scrape status. Runs the Jina → direct HTTP pipeline in sequence. Useful after a scraping run with partial failures.

---

## Database Schema

The SQLite database lives at `db/rates.db`. All tables are created by `scrapers/schema.py`.

### institutions
Registry of all FDIC banks and NCUA credit unions.

| Column | Type | Description |
|---|---|---|
| `id` | TEXT PK | `fdic:{cert}` or `ncua:{charter}` |
| `name` | TEXT | Full institution name |
| `type` | TEXT | `bank` or `cu` |
| `assets_k` | INTEGER | Total assets in thousands |
| `website_url` | TEXT | Homepage URL |
| `rates_url` | TEXT | Direct URL to rates page |
| `scrape_status` | TEXT | `ok`, `error`, `missing`, `manual`, `blocked` |
| `raw_section` | TEXT | Raw scraped HTML/text (for debugging) |

### rates
All scraped rates. Append-only — records are never deleted (historical).

| Column | Type | Description |
|---|---|---|
| `institution_id` | TEXT FK | References `institutions.id` |
| `product` | TEXT | `cd`, `savings`, `money_market`, `checking` |
| `term_months` | INTEGER | CD term in months (null for non-CD products) |
| `apy` | REAL | Decimal: `0.045` = 4.5% |
| `min_balance` | INTEGER | Minimum balance in dollars |
| `scraped_week` | TEXT | ISO week: `YYYY-WW` |
| `confidence` | TEXT | `verified` or `unverified` |

### branch_markets
FDIC + NCUA branch locations. Used to build peer groups.

| Column | Type | Description |
|---|---|---|
| `institution_id` | TEXT | References `institutions.id` |
| `market_key` | TEXT | `{city}\|{state}` (lowercase, e.g. `baltimore\|md`) |
| `city` | TEXT | City name |
| `state` | TEXT | 2-letter state code |

### product_groups
Logical groupings: `deposit_liquid`, `deposit_term`, `loan_secured`, `loan_unsecured`.

---

## For .NET Developers

This Python codebase is designed to be handed off to a .NET team. There are three integration approaches, from simplest to most complete:

---

### Option A: Call Python as a subprocess

The simplest integration. Your .NET app invokes the Python scripts and reads the output PDF.

```csharp
using System.Diagnostics;

var process = new Process();
process.StartInfo.FileName = "python3";
process.StartInfo.Arguments = "jobs/deposit_ranking_report.py " +
    "--client \"Securityplus FCU\" " +
    "--market \"Baltimore\" MD " +
    "--output report.pdf";
process.StartInfo.WorkingDirectory = "/path/to/deposit-intelligence";
process.Start();
process.WaitForExit();

// Then serve report.pdf to the user
var pdfBytes = File.ReadAllBytes("report.pdf");
```

**Pros:** No rewrite needed. Scraping and parsing logic stays in Python (where it's easier to maintain).  
**Cons:** Requires Python runtime on the server. Subprocess management overhead.

---

### Option B: Use the SQLite database directly

The database (`db/rates.db`) is standard SQLite — no special drivers needed. Use `Microsoft.Data.Sqlite` or Entity Framework Core with the SQLite provider.

All the ranking logic in `deposit_ranking_report.py` is SQL queries + sorting — straightforward to reimplement in C#. PDF generation could use [QuestPDF](https://www.questpdf.com/), iTextSharp, or Telerik Reporting.

```csharp
using Microsoft.Data.Sqlite;

using var connection = new SqliteConnection("Data Source=db/rates.db");
connection.Open();

// Get all rates for a market in the current week
var cmd = connection.CreateCommand();
cmd.CommandText = @"
    SELECT i.name, r.product, r.term_months, r.apy, r.min_balance
    FROM rates r
    JOIN institutions i ON r.institution_id = i.id
    JOIN branch_markets bm ON bm.institution_id = i.id
    WHERE bm.market_key = $market
      AND r.scraped_week = $week
    ORDER BY r.product, r.term_months, r.apy DESC";
cmd.Parameters.AddWithValue("$market", "baltimore|md");
cmd.Parameters.AddWithValue("$week", "2025-12");

using var reader = cmd.ExecuteReader();
while (reader.Read()) {
    Console.WriteLine($"{reader["name"]}: {(double)reader["apy"] * 100:F2}%");
}
```

**Pros:** Full control over report design and delivery. No Python dependency at runtime.  
**Cons:** Need to reimplement the ranking/report logic in C#. Scraping still runs as separate Python jobs.

---

### Option C: Expose as a microservice

Wrap `deposit_ranking_report.py` in a FastAPI endpoint. Your .NET app calls the API and receives a PDF response.

```python
# jobs/api.py — add this to the project
from fastapi import FastAPI
from fastapi.responses import FileResponse
import subprocess, tempfile, os

app = FastAPI()

@app.get("/report/{state}/{city}/{client}")
def generate_report(state: str, city: str, client: str):
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
        output_path = f.name

    subprocess.run([
        "python3", "jobs/deposit_ranking_report.py",
        "--client", client,
        "--market", city, state,
        "--output", output_path
    ], check=True)

    return FileResponse(output_path, media_type="application/pdf",
                        filename=f"{client.replace(' ', '_')}_rates.pdf")
```

```bash
# Run the microservice
pip install fastapi uvicorn
uvicorn jobs.api:app --host 0.0.0.0 --port 8080
```

Your .NET app then calls `GET http://localhost:8080/report/MD/Baltimore/Securityplus%20FCU` and receives a PDF.

**Pros:** Clean separation of concerns. Python team owns the scraping/parsing. .NET team owns the UI/delivery.  
**Cons:** Requires running and maintaining the Python microservice alongside the .NET app.

---

## Weekly Automation (Cron)

```bash
# Add to crontab: runs every Monday at 6 AM
0 6 * * 1 cd /path/to/deposit-intelligence && \
  python3 jobs/deposit_ranking_report.py \
    --client "Securityplus FCU" \
    --market "Baltimore" MD \
    --output /var/reports/securityplus_$(date +\%Y-\%m-\%d).pdf
```

---

## Email Delivery via SendGrid

> **For the dev team:** This section describes what needs to be built to enable automated weekly report delivery. The report generation scripts are complete — this is the email wrapper layer that ties it together.

Strum Platform uses SendGrid for transactional email. The weekly report is delivered automatically every Monday as a PDF and/or Excel attachment.

---

### What needs to be built

Two new files:
1. **`jobs/send_report.py`** — SendGrid delivery helper (reusable function)
2. **`jobs/weekly_delivery.py`** — Weekly orchestrator: generate reports for all clients → email each one

---

### Step 1 — Dependencies

```bash
pip install sendgrid
```

Add to `requirements.txt`:
```
sendgrid>=6.9
```

---

### Step 2 — Environment variables (fill these in)

| Variable | Description |
|---|---|
| `SENDGRID_API_KEY` | **[PLACEHOLDER — dev team to fill in]** — Your SendGrid API key (starts with `SG.`). Store as an environment variable or Azure App Service secret. Never hardcode. |
| `REPORT_FROM_EMAIL` | **[PLACEHOLDER — dev team to fill in]** — The verified sender address (e.g. `reports@strumplatform.com`). Must be verified in SendGrid → Settings → Sender Authentication before going live. |

Set locally for testing:
```bash
export SENDGRID_API_KEY="SG.xxxxxxxxxxxxxxxxxxxx"   # ← replace with real key
export REPORT_FROM_EMAIL="reports@strumplatform.com"  # ← replace with verified sender
```

In production (Azure), set these as App Service application settings or Key Vault references.

---

### Step 3 — Build `jobs/send_report.py`

```python
# jobs/send_report.py
import os, base64, datetime
import sendgrid
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition

# ── PLACEHOLDERS — dev team to configure ─────────────────────────────────────
SENDGRID_API_KEY  = os.environ.get("SENDGRID_API_KEY")   # set in environment
FROM_EMAIL        = os.environ.get("REPORT_FROM_EMAIL")  # must be SendGrid-verified sender
# ─────────────────────────────────────────────────────────────────────────────

def send_report(client_name: str, to_email: str, pdf_path: str = None, xlsx_path: str = None):
    """
    Send a deposit ranking report to a client via SendGrid.
    Attaches PDF and/or Excel depending on which paths are provided.
    """
    if not SENDGRID_API_KEY:
        raise EnvironmentError("SENDGRID_API_KEY environment variable is not set.")
    if not FROM_EMAIL:
        raise EnvironmentError("REPORT_FROM_EMAIL environment variable is not set.")

    sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)
    week = datetime.date.today().strftime("%B %d, %Y")

    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=to_email,
        subject=f"Deposit Ranking Report — {client_name} — Week of {week}",
        html_content=f"""
        <p>Hi,</p>
        <p>Your weekly <strong>Deposit Ranking Report</strong> is attached for the week of {week}.</p>
        <p>This report shows how <strong>{client_name}</strong> ranks against local market peers
        across all deposit products — CDs, savings, money market, and checking.</p>
        <p>Let us know if you have any questions.</p>
        <br>
        <p style="color:#999;font-size:12px;">Powered by Strum Platform™ &mdash; strumplatform.com</p>
        """
    )

    # Attach PDF and/or Excel
    for path, mime, label in [
        (pdf_path,  "application/pdf",
         f"{client_name.replace(' ', '_')}_Deposit_Ranking_{week.replace(' ','_')}.pdf"),
        (xlsx_path, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
         f"{client_name.replace(' ', '_')}_Deposit_Ranking_{week.replace(' ','_')}.xlsx"),
    ]:
        if path and os.path.exists(path):
            with open(path, "rb") as f:
                encoded = base64.b64encode(f.read()).decode()
            message.attachment = Attachment(
                FileContent(encoded), FileName(label), FileType(mime), Disposition("attachment")
            )

    response = sg.send(message)
    print(f"  ✅ Sent to {to_email} — HTTP {response.status_code}")
    return response.status_code
```

---

### Step 4 — Build `jobs/weekly_delivery.py`

```python
# jobs/weekly_delivery.py
# Run every Monday morning via cron.
# Generates PDF + Excel for each client in clients.json, then emails via SendGrid.

import json, subprocess, tempfile, datetime, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from send_report import send_report

CLIENTS_FILE = os.path.join(os.path.dirname(__file__), "..", "clients.json")

clients = json.load(open(CLIENTS_FILE))
week    = datetime.date.today().strftime("%Y-%m-%d")

print(f"Weekly delivery — {week} — {len(clients)} client(s)")

for c in clients:
    print(f"\n→ {c['name']} ({c['market_city']}, {c['market_state']})")
    with tempfile.TemporaryDirectory() as tmpdir:
        slug     = c["name"].replace(" ", "_")
        pdf_path  = f"{tmpdir}/{slug}_{week}.pdf"
        xlsx_path = f"{tmpdir}/{slug}_{week}.xlsx"

        # Generate PDF report
        subprocess.run([
            "python3", "jobs/deposit_ranking_report.py",
            "--client", c["name"],
            "--market", c["market_city"], c["market_state"],
            "--output", pdf_path
        ], check=True)

        # Generate Excel report
        subprocess.run([
            "python3", "jobs/export_excel.py",
            "--client", c["name"],
            "--market", c["market_city"], c["market_state"],
            "--output", xlsx_path
        ], check=True)

        # Email both attachments
        send_report(c["name"], c["email"], pdf_path=pdf_path, xlsx_path=xlsx_path)

print("\nDone.")
```

---

### Step 5 — `clients.json` (one entry per Strum Platform client)

```json
[
  {
    "name": "Securityplus FCU",
    "market_city": "Baltimore",
    "market_state": "MD",
    "email": "PLACEHOLDER@clientdomain.com"
  }
]
```

> Replace `PLACEHOLDER@clientdomain.com` with the actual recipient at each client institution before going live.

---

### Step 6 — Cron (production)

```bash
# Runs every Monday at 7:00 AM server time
0 7 * * 1 cd /path/to/deposit-intelligence && python3 jobs/weekly_delivery.py >> /var/log/deposit_reports.log 2>&1
```

For Azure deployments, use Azure Logic Apps or a cron-triggered Azure Function instead of a raw crontab.

---

### SendGrid checklist for dev team

- [ ] Obtain SendGrid API key with **Mail Send** permission
- [ ] Set `SENDGRID_API_KEY` in the deployment environment (never commit to git)
- [ ] Verify the sender email in SendGrid → **Settings → Sender Authentication**
- [ ] Set `REPORT_FROM_EMAIL` in the deployment environment
- [ ] Test with a single client entry in `clients.json` before enabling the full cron
- [ ] Confirm attachments render correctly in Outlook (primary client email client)

---

## Multi-Client Configuration

The tool currently generates one report at a time. For multi-client production use, create a `clients.json`:

```json
[
  {
    "name": "Securityplus FCU",
    "market_city": "Baltimore",
    "market_state": "MD",
    "email": "jeff@securityplusfcu.org"
  },
  {
    "name": "Desert Financial CU",
    "market_city": "Phoenix",
    "market_state": "AZ",
    "email": "reports@desertfinancial.com"
  }
]
```

Then loop through clients in your cron job or pipeline:

```bash
python3 - <<'EOF'
import json, subprocess, datetime

clients = json.load(open("clients.json"))
week = datetime.date.today().strftime("%Y-%m-%d")

for c in clients:
    out = f"/var/reports/{c['name'].replace(' ', '_')}_{week}.pdf"
    subprocess.run([
        "python3", "jobs/deposit_ranking_report.py",
        "--client", c["name"],
        "--market", c["market_city"], c["market_state"],
        "--output", out
    ])
    print(f"Generated: {out}")
EOF
```

---

## Known Limitations

| Institution | Issue | Workaround |
|---|---|---|
| **PNC Bank** | Cloudflare anti-bot blocks direct scrapers | ✅ **Solved** — scraped via DepositAccounts.com: `python3 scrapers/manual_rates.py --pnc` |
| **The Harbor Bank** | Rate page requires login | No public rates available |
| **Rosedale Bank** | Rates in a JavaScript iframe widget | Check DepositAccounts.com: `python3 scrapers/manual_rates.py --aggregator fdic:{cert}` if listed |
| **Shore United Bank** | JS-rendered rates | Check DepositAccounts.com aggregator fallback |
| **FVCbank** | JS-rendered rates | Check DepositAccounts.com aggregator fallback |

### DepositAccounts.com Fallback

For institutions that block direct scraping (Cloudflare, JS iframes, login-gated), use `manual_rates.py` with the aggregator flag. This scrapes [DepositAccounts.com](https://depositaccounts.com) — a public rate aggregator that pre-processes JS-heavy sites — and parses their structured HTML tables directly (no LLM needed).

```bash
# PNC (pre-configured)
python3 scrapers/manual_rates.py --pnc

# Any institution listed on DepositAccounts.com
python3 scrapers/manual_rates.py --aggregator fdic:XXXX
```

To add a new institution to the aggregator fallback, add an entry to `AGGREGATOR_SOURCES` in `manual_rates.py` with its DepositAccounts.com URL and table index mapping.

**General limitations:**
- **Rate freshness** — Rates reflect the most recent successful scrape. Some institutions update rates more frequently than weekly; the report may lag by a few days.
- **Geographic granularity** — Credit unions are matched by main office address only. NCUA does not publish branch-level locations publicly. A CU headquartered in a suburb (e.g., Towson) may not appear in a Baltimore market query even if it has Baltimore branches.

---

## Data Sources

| Source | URL | Cost | What it provides |
|---|---|---|---|
| **FDIC BankFind Suite API** | api.fdic.gov | Free | Branch locations, institution registry, asset sizes |
| **NCUA Mapping API** | mapping.ncua.gov | Free | Credit union registry, main office addresses |
| **Institution websites** | Various | Free | Publicly-posted deposit rates |
| **OpenAI gpt-4.1-mini** | platform.openai.com | ~$0.01/institution | Rate extraction from HTML/PDF |

All deposit rate data is scraped from publicly accessible web pages. This is the same data available to any consumer visiting those sites — no data licensing agreements required.

---

## Troubleshooting

### "0 rates extracted" from an institution
1. Confirm you're using `gpt-4.1-mini` in `config.json`, NOT a reasoning model (`o1`, `o3`, `gpt-5.4-mini`)
2. Check that `raw_section` in the `institutions` table actually contains rate data (look for `%` symbols)
3. Run `python3 scrapers/jina_scraper.py --url https://example.com/rates` manually to inspect raw output

### Playwright timeout
Some sites are slow to render. Increase `wait_ms` in the `INST_CONFIG` dictionary in `playwright_scraper.py` for the affected institution.

### SQLite database locked
Only one process should write to `db/rates.db` at a time. If you see lock errors, check for zombie Python processes and kill them:
```bash
lsof db/rates.db
```

### Jina timeout on large pages
Jina occasionally times out on pages with large DOM trees. The scraper retries automatically. If it consistently fails, try the `playwright_scraper.py` as an alternative for that institution.

### Missing peer institutions in report
If institutions you expect don't appear in the peer group, check `branch_markets`:
```sql
SELECT * FROM branch_markets WHERE market_key = 'baltimore|md' LIMIT 20;
```
If the table is empty, re-run `branch_geography.py` and `cu_geography.py`.

---

## License

Internal tool — Strum Agency. Not for redistribution.
