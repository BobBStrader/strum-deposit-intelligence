"""
Orchestration Runner — deposit-intelligence pipeline
------------------------------------------------------
Runs pipeline phases for deposit, loan, and mortgage rate collection.

Phases:
  deposit-scrape    — Scrape deposit rate pages (tavily/jina/playwright)
  deposit-parse     — Parse deposit rates with LLM
  deposit-report    — Generate deposit ranking PDF/text report

  loan-scrape       — (Re)scrape loan rate pages (uses loan_rates_url if set)
  loan-parse        — Parse loan rates with LLM
  loan-report       — Generate loan ranking PDF/text report

  mortgage-scrape   — (Re)scrape mortgage rate pages (uses mortgage_rates_url if set)
  mortgage-parse    — Parse mortgage rates with LLM
  mortgage-report   — Generate mortgage ranking PDF/text report

  url-discovery     — Discover loan/mortgage URLs for institutions
  migrate           — Run schema migrations

Usage:
    python3 run.py --phase deposit-scrape
    python3 run.py --phase deposit-parse
    python3 run.py --phase deposit-report --client "Securityplus FCU" --market Baltimore MD
    python3 run.py --phase loan-parse --force
    python3 run.py --phase loan-report --client "Securityplus FCU" --market Baltimore MD --text
    python3 run.py --phase mortgage-report --client "Securityplus FCU" --cbsa 12580
    python3 run.py --phase url-discovery --url-type loan
    python3 run.py --phase migrate
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def phase_migrate():
    """Run schema migrations."""
    from scrapers.schema import init_db, migrate
    init_db()
    migrate()
    print("✅ Schema ready")


def phase_deposit_scrape(args):
    """Scrape deposit rate pages."""
    try:
        from scrapers.tavily_scraper import run_scraper as tavily_run
        print("Running Tavily scraper...")
        tavily_run()
    except Exception as e:
        print(f"Tavily scraper error: {e}")

    try:
        from scrapers.jina_scraper import run_scraper as jina_run
        print("Running Jina scraper (fallback)...")
        jina_run()
    except Exception as e:
        print(f"Jina scraper error: {e}")


def phase_deposit_parse(args):
    """Parse deposit rates with LLM."""
    from scrapers.llm_parser import run_parser
    inst_ids = [args.id] if args.id else None
    run_parser(inst_ids=inst_ids, force=args.force,
               model=args.model, backend=args.backend)


def phase_deposit_report(args):
    """Generate deposit ranking report."""
    from jobs.deposit_ranking_report import render_text_report, render_pdf_report
    from scrapers.schema import get_conn
    from datetime import date

    if not args.client:
        print("Error: --client required for deposit-report")
        sys.exit(1)
    if not args.market and not args.cbsa:
        print("Error: --market CITY STATE or --cbsa CODE required")
        sys.exit(1)

    city = state = ""
    cbsa_code = None
    if args.cbsa:
        cbsa_code = str(args.cbsa).strip()
    if args.market:
        city, state = args.market

    conn = get_conn()

    if args.text or not args.output:
        report = render_text_report(conn, city, state, args.client, cbsa_code=cbsa_code)
        print(report)

    if args.output or not args.text:
        out = args.output or f"/tmp/{args.client.replace(' ','_')}_DepositRanking_{date.today()}.pdf"
        render_pdf_report(conn, city, state, args.client, out, cbsa_code=cbsa_code)
        print(f"✅ PDF saved: {out}")


def phase_loan_scrape(args):
    """Scrape loan rate pages (uses loan_rates_url if set, else falls back to rates_url)."""
    try:
        from scrapers.tavily_scraper import run_scraper as tavily_run
        print("Running Tavily scraper for loan URLs...")
        tavily_run(url_field='loan_rates_url')
    except TypeError:
        # Tavily scraper may not support url_field param — run standard scrape
        from scrapers.tavily_scraper import run_scraper as tavily_run
        print("Running Tavily scraper (standard, loan_rates_url not supported — update tavily_scraper.py)...")
        tavily_run()
    except Exception as e:
        print(f"Loan scrape error: {e}")


def phase_loan_parse(args):
    """Parse loan rates with LLM."""
    from scrapers.llm_parser import run_loan_parser
    inst_ids = [args.id] if args.id else None
    run_loan_parser(inst_ids=inst_ids, force=args.force,
                    model=args.model, backend=args.backend)


def phase_loan_report(args):
    """Generate loan ranking report."""
    from jobs.loan_ranking_report import render_text_report, render_pdf_report
    from scrapers.schema import get_conn
    from datetime import date

    if not args.client:
        print("Error: --client required for loan-report")
        sys.exit(1)
    if not args.market and not args.cbsa:
        print("Error: --market CITY STATE or --cbsa CODE required")
        sys.exit(1)

    city = state = ""
    cbsa_code = None
    if args.cbsa:
        cbsa_code = str(args.cbsa).strip()
    if args.market:
        city, state = args.market

    conn = get_conn()

    if args.text or not args.output:
        report = render_text_report(conn, city, state, args.client, cbsa_code=cbsa_code)
        print(report)

    if args.output or not args.text:
        out = args.output or f"/tmp/{args.client.replace(' ','_')}_LoanRanking_{date.today()}.pdf"
        render_pdf_report(conn, city, state, args.client, out, cbsa_code=cbsa_code)
        print(f"✅ PDF saved: {out}")


def phase_mortgage_scrape(args):
    """Scrape mortgage rate pages."""
    try:
        from scrapers.tavily_scraper import run_scraper as tavily_run
        print("Running Tavily scraper for mortgage URLs...")
        tavily_run(url_field='mortgage_rates_url')
    except TypeError:
        from scrapers.tavily_scraper import run_scraper as tavily_run
        print("Running Tavily scraper (standard, mortgage_rates_url not supported — update tavily_scraper.py)...")
        tavily_run()
    except Exception as e:
        print(f"Mortgage scrape error: {e}")


def phase_mortgage_parse(args):
    """Parse mortgage rates with LLM."""
    from scrapers.llm_parser import run_mortgage_parser
    inst_ids = [args.id] if args.id else None
    run_mortgage_parser(inst_ids=inst_ids, force=args.force,
                        model=args.model, backend=args.backend)


def phase_mortgage_report(args):
    """Generate mortgage ranking report."""
    from jobs.mortgage_ranking_report import render_text_report, render_pdf_report
    from scrapers.schema import get_conn
    from datetime import date

    if not args.client:
        print("Error: --client required for mortgage-report")
        sys.exit(1)
    if not args.market and not args.cbsa:
        print("Error: --market CITY STATE or --cbsa CODE required")
        sys.exit(1)

    city = state = ""
    cbsa_code = None
    if args.cbsa:
        cbsa_code = str(args.cbsa).strip()
    if args.market:
        city, state = args.market

    conn = get_conn()

    if args.text or not args.output:
        report = render_text_report(conn, city, state, args.client, cbsa_code=cbsa_code)
        print(report)

    if args.output or not args.text:
        out = args.output or f"/tmp/{args.client.replace(' ','_')}_MortgageRanking_{date.today()}.pdf"
        render_pdf_report(conn, city, state, args.client, out, cbsa_code=cbsa_code)
        print(f"✅ PDF saved: {out}")


def phase_url_discovery(args):
    """Discover loan/mortgage URLs for institutions."""
    from scrapers.url_discovery import run_discovery
    inst_ids  = [args.id] if args.id else None
    url_type  = getattr(args, 'url_type', 'both') or 'both'
    run_discovery(inst_ids=inst_ids, force=args.force, url_type=url_type)


PHASE_MAP = {
    "deposit-scrape":   phase_deposit_scrape,
    "deposit-parse":    phase_deposit_parse,
    "deposit-report":   phase_deposit_report,
    "loan-scrape":      phase_loan_scrape,
    "loan-parse":       phase_loan_parse,
    "loan-report":      phase_loan_report,
    "mortgage-scrape":  phase_mortgage_scrape,
    "mortgage-parse":   phase_mortgage_parse,
    "mortgage-report":  phase_mortgage_report,
    "url-discovery":    phase_url_discovery,
    "migrate":          lambda _: phase_migrate(),
}


def main():
    parser = argparse.ArgumentParser(description="Deposit-Intelligence Pipeline Runner")
    parser.add_argument("--phase",   required=True, choices=list(PHASE_MAP.keys()),
                        help="Pipeline phase to run")
    # Scraping / parsing options
    parser.add_argument("--id",      metavar="INST_ID",
                        help="Single institution DB id (e.g. ncua:67790)")
    parser.add_argument("--force",   action="store_true",
                        help="Force re-processing even if already done")
    parser.add_argument("--model",   default=None, help="Override LLM model name")
    parser.add_argument("--backend", choices=["openai", "ollama"], default=None,
                        help="LLM backend (default: openai)")
    # Report options
    parser.add_argument("--client",  default=None, help="Client institution name for reports")
    parser.add_argument("--market",  nargs=2, metavar=("CITY", "STATE"),
                        help="Market by city + state (e.g. --market Baltimore MD)")
    parser.add_argument("--cbsa",    metavar="CODE",
                        help="Market by MSA/CBSA code")
    parser.add_argument("--output",  default=None, help="PDF output path")
    parser.add_argument("--text",    action="store_true", help="Print text report to stdout")
    # URL discovery options
    parser.add_argument("--url-type", choices=["loan", "mortgage", "both"], default="both",
                        help="URL type for url-discovery phase (default: both)")
    args = parser.parse_args()

    fn = PHASE_MAP[args.phase]
    fn(args)


if __name__ == "__main__":
    main()
