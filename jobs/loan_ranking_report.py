"""
Loan Ranking Report Generator
------------------------------
Generates a competitive auto/personal loan rate ranking report for a client
institution, showing how their rates compare to local market peers.

Modeled after S&P Global's Loan Ranking Report format.
Sorted lowest-rate-first (best loan rate = lowest APR).

Usage:
    python3 loan_ranking_report.py --client "Securityplus FCU" --market Baltimore MD
    python3 loan_ranking_report.py --client "Securityplus FCU" --market Baltimore MD --text
    python3 loan_ranking_report.py --client "Securityplus FCU" --market Baltimore MD --output /tmp/report.pdf
    python3 loan_ranking_report.py --client "Securityplus FCU" --cbsa 12580
"""

import argparse
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from scrapers.schema import get_conn
from scrapers.peer_group import get_peers

# ── Auto loan configurations ──────────────────────────────────────────────────
# (label, product, term_months, vehicle_age_years, loan_amount_k)
AUTO_CONFIGS = [
    ("36Mo New Auto 25k",       "new_auto_loan",  36, 0, 25),
    ("48Mo New Auto 25k",       "new_auto_loan",  48, 0, 25),
    ("60Mo New Auto 25k",       "new_auto_loan",  60, 0, 25),
    ("72Mo New Auto 25k",       "new_auto_loan",  72, 0, 25),
    ("36Mo 2 Yr Used Auto 15k", "used_auto_loan", 36, 2, 15),
    ("48Mo 2 Yr Used Auto 15k", "used_auto_loan", 48, 2, 15),
    ("60Mo 2 Yr Used Auto 15k", "used_auto_loan", 60, 2, 15),
    ("72Mo 2 Yr Used Auto 15k", "used_auto_loan", 72, 2, 15),
    ("36Mo 4 Yr Used Auto 9k",  "used_auto_loan", 36, 4,  9),
    ("48Mo 4 Yr Used Auto 9k",  "used_auto_loan", 48, 4,  9),
    ("60Mo 4 Yr Used Auto 9k",  "used_auto_loan", 60, 4,  9),
    ("72Mo 4 Yr Used Auto 9k",  "used_auto_loan", 72, 4,  9),
]


# ── Name normalization (shared with deposit report) ───────────────────────────

def normalize_name(name: str) -> str:
    """Normalize institution name for fuzzy client matching."""
    drop = ["federal credit union", "credit union", "national association",
            "national bank", "bank", "fcu", "cu", "inc", "na", "n.a."]
    n = name.lower().strip()
    for d in drop:
        n = n.replace(d, "")
    return n.strip().rstrip(",.")


def find_client(peers: list, client_name: str) -> dict | None:
    """Find the client institution in the peer list by fuzzy name match."""
    target = normalize_name(client_name)
    best = None
    best_score = 0
    for p in peers:
        nm = normalize_name(p.get("name") or "")
        score = 0
        if target in nm or nm in target:
            score = min(len(target), len(nm))
        if score > best_score:
            best_score = score
            best = p
    return best if best_score > 0 else None


# ── CBSA helpers ─────────────────────────────────────────────────────────────

def get_peers_by_cbsa(conn, cbsa_code: str) -> list:
    """Return all institutions in a given CBSA."""
    rows = conn.execute("""
        SELECT DISTINCT i.id AS institution_id, i.name, i.type AS inst_type,
               i.assets_k, i.cbsa_code, i.cbsa_name, i.website_url
        FROM institutions i
        WHERE i.cbsa_code = ?
        ORDER BY i.assets_k DESC, i.name
    """, (str(cbsa_code),)).fetchall()
    return [dict(r) for r in rows]


def get_cbsa_name(conn, cbsa_code: str) -> str:
    """Look up MSA name from cbsa_code."""
    row = conn.execute(
        "SELECT cbsa_name FROM institutions WHERE cbsa_code=? AND cbsa_name IS NOT NULL LIMIT 1",
        (str(cbsa_code),)
    ).fetchone()
    if row:
        return row[0]
    try:
        row = conn.execute(
            "SELECT cbsa_name FROM branch_markets WHERE cbsa_code=? AND cbsa_name IS NOT NULL LIMIT 1",
            (str(cbsa_code),)
        ).fetchone()
        return row[0] if row else f"MSA {cbsa_code}"
    except Exception:
        return f"MSA {cbsa_code}"


# ── Data Engine ───────────────────────────────────────────────────────────────

def _inst_meta_for_ids(conn, inst_ids: list) -> dict:
    """Fetch institution metadata by ID list."""
    if not inst_ids:
        return {}
    placeholders = ",".join("?" * len(inst_ids))
    rows = conn.execute(
        f"SELECT id, name, type, assets_k FROM institutions WHERE id IN ({placeholders})",
        inst_ids
    ).fetchall()
    return {r["id"]: {"name": r["name"], "type": r["type"], "assets_k": r["assets_k"]}
            for r in rows}


def get_loan_rates(conn, peer_ids: list, product: str, term_months: int,
                   vehicle_age_years: int, loan_amount_k: int) -> tuple:
    """
    Pull latest auto/personal loan rates for peers matching product + term + vehicle age.
    Returns (rates_dict, inst_meta_dict).
    rates_dict: { institution_id: { 'apy': float, 'apr': float|None, 'prior_apy': float|None, 'week': str } }
    """
    if not peer_ids:
        return {}, {}
    placeholders = ",".join("?" * len(peer_ids))

    # Latest week per institution for this product+term
    latest = conn.execute(f"""
        SELECT institution_id, MAX(scraped_week) AS max_week
        FROM rates
        WHERE institution_id IN ({placeholders})
          AND product = ?
          AND term_months = ?
          AND vehicle_age_years IS ?
        GROUP BY institution_id
    """, peer_ids + [product, term_months, vehicle_age_years]).fetchall()
    latest_map = {r["institution_id"]: r["max_week"] for r in latest}

    if not latest_map:
        return {}, _inst_meta_for_ids(conn, peer_ids)

    # Prior week for week-over-week change
    prior = conn.execute(f"""
        SELECT institution_id, MAX(scraped_week) AS prior_week
        FROM rates
        WHERE institution_id IN ({placeholders})
          AND product = ?
          AND term_months = ?
          AND vehicle_age_years IS ?
          AND scraped_week < COALESCE((
              SELECT MAX(r2.scraped_week) FROM rates r2
              WHERE r2.institution_id = rates.institution_id
                AND r2.product = ?
                AND r2.term_months = ?
          ), '9999')
        GROUP BY institution_id
    """, peer_ids + [product, term_months, vehicle_age_years, product, term_months]).fetchall()
    prior_map = {r["institution_id"]: r["prior_week"] for r in prior}

    # Fetch all rates (include apr column)
    rows = conn.execute(f"""
        SELECT institution_id, apy, apr, scraped_week
        FROM rates
        WHERE institution_id IN ({placeholders})
          AND product = ?
          AND term_months = ?
          AND vehicle_age_years IS ?
          AND apy IS NOT NULL
        ORDER BY apy ASC
    """, peer_ids + [product, term_months, vehicle_age_years]).fetchall()

    result = {}
    prior_rates = {r["institution_id"]: r["apy"] for r in rows
                   if prior_map.get(r["institution_id"]) == r["scraped_week"]}
    for r in rows:
        iid = r["institution_id"]
        if latest_map.get(iid) == r["scraped_week"]:
            # Keep lowest APR (best rate for loans) if multiple entries
            if iid not in result or r["apy"] < result[iid]["apy"]:
                result[iid] = {
                    "apy":       r["apy"],
                    "apr":       r["apr"] if r["apr"] is not None else None,
                    "prior_apy": prior_rates.get(iid),
                    "week":      r["scraped_week"],
                }

    inst_meta = _inst_meta_for_ids(conn, peer_ids)
    return result, inst_meta


def _get_peer_ids_by_market(conn, city: str, state: str) -> list:
    """Get peer institution IDs by city/state market."""
    mkey = f"{city.strip().lower()}|{state.strip().lower()}"
    rows = conn.execute("""
        SELECT DISTINCT COALESCE(i.id,'') AS id
        FROM branch_markets bm
        LEFT JOIN institutions i ON (i.id='fdic:'||bm.cert OR i.id='ncua:'||bm.cert)
        WHERE bm.market_key=? AND i.id IS NOT NULL
    """, (mkey,)).fetchall()
    return [r["id"] for r in rows]


def build_loan_table(inst_meta: dict, rates: dict, client_id: str | None) -> list:
    """
    Build a ranked table for a single loan product config.
    Sorted lowest APR first (best loan rate).
    """
    rows = []
    for iid, meta in inst_meta.items():
        rate_data = rates.get(iid)
        rows.append({
            "name":      meta["name"] or "Unknown",
            "type":      meta["type"] or "bank",
            "assets_k":  meta["assets_k"],
            "is_client": (iid == client_id),
            "inst_id":   iid,
            "apy":       rate_data["apy"]       if rate_data else None,
            "apr":       rate_data.get("apr")   if rate_data else None,
            "prior_apy": rate_data["prior_apy"] if rate_data else None,
            "week":      rate_data["week"]      if rate_data else None,
        })

    def sort_key(r):
        # Lowest APR first (best loan rate), then N/O last
        if r["apy"] is not None:
            return (0, r["apy"])
        return (1, 0)

    rows.sort(key=sort_key)
    return rows


def compute_average(rows: list) -> float | None:
    """Compute average APY/APR from rows that have a rate."""
    apys = [r["apy"] for r in rows if r["apy"] is not None]
    return sum(apys) / len(apys) if apys else None


# ── Text Report ───────────────────────────────────────────────────────────────

def render_text_report(conn, city: str, state: str, client_name: str,
                       cbsa_code: str = None) -> str:
    """Render a plain-text loan ranking report."""
    if cbsa_code:
        peers        = get_peers_by_cbsa(conn, cbsa_code)
        market_label = get_cbsa_name(conn, cbsa_code)
        peer_ids     = [p["institution_id"] for p in peers]
    else:
        peers        = get_peers(conn, city, state)
        market_label = f"{city.title()}, {state.upper()}"
        peer_ids     = _get_peer_ids_by_market(conn, city, state)

    inst_meta = _inst_meta_for_ids(conn, peer_ids)
    client    = find_client(peers, client_name)
    client_id = client["institution_id"] if client else None

    today = date.today().strftime("%m/%d/%Y")
    lines = []
    lines.append("=" * 70)
    lines.append(f"  {client_name}")
    lines.append(f"  Loan Ranking Report — Powered by Strum Platform")
    lines.append(f"  Market: {market_label}")
    if cbsa_code:
        lines.append(f"  MSA/CBSA Code: {cbsa_code}")
    lines.append(f"  Generated: {today}")
    lines.append("=" * 70)

    # Group by product category
    sections = [
        ("NEW AUTO LOANS — $25k",   [c for c in AUTO_CONFIGS if c[1] == "new_auto_loan"]),
        ("USED AUTO (2 YR) — $15k", [c for c in AUTO_CONFIGS if c[1] == "used_auto_loan" and c[3] == 2]),
        ("USED AUTO (4 YR) — $9k",  [c for c in AUTO_CONFIGS if c[1] == "used_auto_loan" and c[3] == 4]),
    ]

    for sect_label, configs in sections:
        tables_in_sect = 0
        for label, product, term_months, vehicle_age_years, loan_amount_k in configs:
            rates, _ = get_loan_rates(conn, peer_ids, product, term_months,
                                      vehicle_age_years, loan_amount_k)
            rows = build_loan_table(inst_meta, rates, client_id)
            rows_with_rates = [r for r in rows if r["apy"] is not None]
            if not rows_with_rates:
                continue

            if tables_in_sect == 0:
                lines.append(f"\n{'─'*70}")
                lines.append(f"  {sect_label}")
                lines.append(f"{'─'*70}")

            lines.append(f"\n  {label}")
            lines.append(f"  {'Institution':<42} {'Rate':>6}  {'APR':>6}  {'Chg':>7}")
            lines.append(f"  {'-'*62}")

            for r in rows:
                marker = "►" if r["is_client"] else " "
                name   = r["name"][:40]
                if r["apy"] is not None:
                    rate_str = f"{r['apy']*100:.2f}%"
                    apr_str  = f"{r['apr']*100:.2f}%" if r.get('apr') else "  —"
                    chg_str  = "    —"
                    if r["prior_apy"] is not None:
                        chg = (r["apy"] - r["prior_apy"]) * 100
                        chg_str = f"{chg:+.2f}" if abs(chg) > 0.001 else "    —"
                    lines.append(f"  {marker} {name:<42} {rate_str:>6}  {apr_str:>6}  {chg_str:>7}")
                else:
                    lines.append(f"  {marker} {name:<42}    N/O")

            avg = compute_average(rows)
            if avg:
                lines.append(f"  {'─'*62}")
                lines.append(f"  {'Rate Group Average':<42} {avg*100:.2f}%")
            tables_in_sect += 1

    lines.append(f"\n{'='*70}")
    lines.append(f"  © {date.today().year}, Powered by Strum Platform")
    lines.append(f"  Data sourced from public institution websites")
    lines.append("=" * 70)
    return "\n".join(lines)


# ── PDF Report ────────────────────────────────────────────────────────────────

def render_pdf_report(conn, city: str, state: str, client_name: str,
                      output_path: str, cbsa_code: str = None) -> str:
    """Render a PDF loan ranking report matching S&P format."""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
    )
    from reportlab.lib.enums import TA_CENTER

    if cbsa_code:
        peers        = get_peers_by_cbsa(conn, cbsa_code)
        market_label = get_cbsa_name(conn, cbsa_code)
        peer_ids     = [p["institution_id"] for p in peers]
    else:
        peers        = get_peers(conn, city, state)
        market_label = f"{city.title()}, {state.upper()}"
        peer_ids     = _get_peer_ids_by_market(conn, city, state)

    inst_meta = _inst_meta_for_ids(conn, peer_ids)
    client    = find_client(peers, client_name)
    client_id = client["institution_id"] if client else None

    # Colors
    NAVY       = colors.HexColor("#1B3A6B")
    GOLD       = colors.HexColor("#C8A84B")
    LIGHT_BLUE = colors.HexColor("#E8EEF7")
    CLIENT_BG  = colors.HexColor("#FFF8E1")
    WHITE      = colors.white
    GRAY       = colors.HexColor("#666666")
    MED_GRAY   = colors.HexColor("#CCCCCC")
    AVG_BG     = colors.HexColor("#F0F0F0")

    styles  = getSampleStyleSheet()
    title_style = ParagraphStyle("title", fontSize=16, textColor=NAVY,
                                  fontName="Helvetica-Bold", spaceAfter=4)
    sub_style   = ParagraphStyle("sub",   fontSize=10, textColor=GRAY,
                                  fontName="Helvetica", spaceAfter=2)
    tbl_style   = ParagraphStyle("tbl",   fontSize=9,  textColor=NAVY,
                                  fontName="Helvetica-Bold", spaceAfter=4, spaceBefore=8)

    doc = SimpleDocTemplate(
        output_path, pagesize=letter,
        leftMargin=0.65*inch, rightMargin=0.65*inch,
        topMargin=0.75*inch,  bottomMargin=0.65*inch,
    )

    story = []
    today = date.today().strftime("%B %d, %Y")

    # Cover header
    story.append(Paragraph(client_name, title_style))
    story.append(Paragraph("Loan Ranking Report", title_style))
    story.append(HRFlowable(width="100%", thickness=2, color=GOLD, spaceAfter=6))
    msa_suffix = f"  |  MSA/CBSA: {cbsa_code}" if cbsa_code else ""
    story.append(Paragraph(
        f"Market: {market_label}{msa_suffix}  |  Generated: {today}  |  Powered by Strum Platform",
        sub_style))
    story.append(Spacer(1, 0.15*inch))

    def _make_section_header(label: str):
        hdr = Table([[f"  {label}"]], colWidths=[7.2*inch])
        hdr.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,-1), NAVY),
            ("TEXTCOLOR",     (0,0), (-1,-1), WHITE),
            ("FONTNAME",      (0,0), (-1,-1), "Helvetica-Bold"),
            ("FONTSIZE",      (0,0), (-1,-1), 11),
            ("TOPPADDING",    (0,0), (-1,-1), 6),
            ("BOTTOMPADDING", (0,0), (-1,-1), 6),
        ]))
        return hdr

    def _make_loan_table(label, product, term_months, vehicle_age_years, loan_amount_k):
        rates, _ = get_loan_rates(conn, peer_ids, product, term_months,
                                  vehicle_age_years, loan_amount_k)
        rows = build_loan_table(inst_meta, rates, client_id)
        if not any(r["apy"] is not None for r in rows):
            return None, None

        data = [["Institution", "Rate (%)", "APR (%)", "Change"]]
        tstyles = [
            ("BACKGROUND",     (0,0), (-1,0), NAVY),
            ("TEXTCOLOR",      (0,0), (-1,0), WHITE),
            ("FONTNAME",       (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE",       (0,0), (-1,-1), 8),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [WHITE, LIGHT_BLUE]),
            ("GRID",           (0,0), (-1,-1), 0.25, MED_GRAY),
            ("ALIGN",          (1,0), (-1,-1), "CENTER"),
            ("VALIGN",         (0,0), (-1,-1), "MIDDLE"),
            ("LEFTPADDING",    (0,0), (-1,-1), 5),
            ("RIGHTPADDING",   (0,0), (-1,-1), 5),
            ("TOPPADDING",     (0,0), (-1,-1), 3),
            ("BOTTOMPADDING",  (0,0), (-1,-1), 3),
        ]

        for i, r in enumerate(rows, start=1):
            name = ("► " if r["is_client"] else "") + r["name"]
            if r["apy"] is not None:
                rate_str = f"{r['apy']*100:.2f}"
                apr_str  = f"{r['apr']*100:.2f}" if r.get('apr') else "—"
                chg_str  = ""
                if r["prior_apy"] is not None:
                    chg = (r["apy"] - r["prior_apy"]) * 100
                    chg_str = f"({chg:+.2f})" if abs(chg) > 0.001 else ""
            else:
                rate_str = apr_str = chg_str = "N/O"
            data.append([name, rate_str, apr_str, chg_str])
            if r["is_client"]:
                tstyles.append(("BACKGROUND", (0,i), (-1,i), CLIENT_BG))
                tstyles.append(("FONTNAME",   (0,i), (-1,i), "Helvetica-Bold"))
                tstyles.append(("TEXTCOLOR",  (0,i), (-1,i), NAVY))

        avg = compute_average(rows)
        if avg:
            ai = len(data)
            data.append(["Rate Group Average **", f"{avg*100:.2f}", "—", ""])
            tstyles.append(("BACKGROUND", (0,ai), (-1,ai), AVG_BG))
            tstyles.append(("FONTNAME",   (0,ai), (-1,ai), "Helvetica-Bold"))

        tbl = Table(data, colWidths=[3.2*inch, 1.0*inch, 1.0*inch, 1.0*inch], repeatRows=1)
        tbl.setStyle(TableStyle(tstyles))
        return label, tbl

    # Sections
    sections = [
        ("NEW AUTO LOANS — $25,000",        [c for c in AUTO_CONFIGS if c[1] == "new_auto_loan"]),
        ("USED AUTO LOANS (2 YR) — $15,000",[c for c in AUTO_CONFIGS if c[1] == "used_auto_loan" and c[3] == 2]),
        ("USED AUTO LOANS (4 YR) — $9,000", [c for c in AUTO_CONFIGS if c[1] == "used_auto_loan" and c[3] == 4]),
    ]

    for sect_label, configs in sections:
        tables_in_sect = 0
        for cfg in configs:
            label, tbl = _make_loan_table(*cfg)
            if tbl is None:
                continue
            if tables_in_sect == 0:
                story.append(Spacer(1, 0.1*inch))
                story.append(_make_section_header(sect_label))
                story.append(Spacer(1, 0.05*inch))
            story.append(Paragraph(label, tbl_style))
            story.append(tbl)
            story.append(Spacer(1, 0.08*inch))
            tables_in_sect += 1

    # Footer
    story.append(Spacer(1, 0.2*inch))
    story.append(HRFlowable(width="100%", thickness=1, color=GOLD))
    story.append(Paragraph(
        f"© {date.today().year}, Powered by Strum Platform  |  "
        f"Data sourced from public institution websites  |  Generated {today}",
        ParagraphStyle("footer", fontSize=7, textColor=GRAY,
                       fontName="Helvetica", alignment=TA_CENTER, spaceBefore=4)
    ))

    doc.build(story)
    return output_path


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Loan Ranking Report Generator")
    parser.add_argument("--client", required=True, help="Client institution name")
    parser.add_argument("--market", nargs=2, metavar=("CITY", "STATE"),
                        help="Market by city + state (e.g. --market Baltimore MD)")
    parser.add_argument("--cbsa",   metavar="CODE",
                        help="Market by MSA/CBSA code (e.g. --cbsa 12580)")
    parser.add_argument("--output", default=None, help="PDF output path")
    parser.add_argument("--text",   action="store_true", help="Print text report to stdout")
    args = parser.parse_args()

    if not args.market and not args.cbsa:
        parser.error("Specify either --market CITY STATE or --cbsa CODE")

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
        print(f"Generating PDF → {out}")
        render_pdf_report(conn, city, state, args.client, out, cbsa_code=cbsa_code)
        print(f"✅ PDF saved: {out}")


if __name__ == "__main__":
    main()
