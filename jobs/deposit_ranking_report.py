"""
Deposit Ranking Report Generator — Phase 3
-------------------------------------------
Generates a competitive deposit rate ranking report for a client
institution, showing how their rates compare to local market peers.

Modeled after S&P Global's Deposit Ranking Report format.

Usage:
    python3 deposit_ranking_report.py --client "Securityplus FCU" --market "Baltimore" MD
    python3 deposit_ranking_report.py --client "Securityplus FCU" --market "Baltimore" MD --output /tmp/report.pdf
    python3 deposit_ranking_report.py --client "Securityplus FCU" --market "Baltimore" MD --text
"""

import argparse
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from scrapers.schema import get_conn
from scrapers.peer_group import get_peers

# ── CD terms to include in report (months) ────────────────────────────────────
CD_TERMS = [1, 3, 6, 9, 12, 18, 24, 36, 48, 60]
CD_LABELS = {
    1: "1 Mo CD", 3: "3 Mo CD", 6: "6 Mo CD", 9: "9 Mo CD",
    12: "1 Yr CD", 18: "18 Mo CD", 24: "2 Yr CD", 36: "3 Yr CD",
    48: "4 Yr CD", 60: "5 Yr CD"
}
MIN_BALANCES = [10_000, 100_000]
MIN_BAL_LABELS = {10_000: "$10k", 100_000: "$100k"}

# ── Liquid products to include in report ──────────────────────────────────────
LIQUID_PRODUCTS = ["savings", "money_market", "checking"]
LIQUID_LABELS   = {"savings": "Savings", "money_market": "Money Market", "checking": "Checking"}


# ── Data Engine ───────────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    """Normalize institution name for fuzzy client matching."""
    drop = ["federal credit union", "credit union", "national association",
            "national bank", "bank", "fcu", "cu", "inc", "na", "n.a."]
    n = name.lower().strip()
    for d in drop:
        n = n.replace(d, "")
    return n.strip().rstrip(",.")


def find_client(peers: list[dict], client_name: str) -> dict | None:
    """Find the client institution in the peer list by fuzzy name match."""
    target = normalize_name(client_name)
    best = None
    best_score = 0
    for p in peers:
        score = 0
        nm = normalize_name(p.get("name") or "")
        # Simple substring match score
        if target in nm or nm in target:
            score = min(len(target), len(nm))
        if score > best_score:
            best_score = score
            best = p
    return best if best_score > 0 else None


def get_market_rates(conn, city: str, state: str) -> dict:
    """
    Pull latest CD rates for all peers in a market.
    Returns: { institution_id: { (term_months, min_balance): {apy, rate, week} } }
    """
    mkey = f"{city.strip().lower()}|{state.strip().lower()}"

    # Get peer IDs for market
    peer_ids = [r[0] for r in conn.execute("""
        SELECT DISTINCT COALESCE(i.id,'') FROM branch_markets bm
        LEFT JOIN institutions i ON (i.id='fdic:'||bm.cert OR i.id='ncua:'||bm.cert)
        WHERE bm.market_key=? AND i.id IS NOT NULL
    """, (mkey,)).fetchall()]

    if not peer_ids:
        return {}, {}

    placeholders = ",".join("?" * len(peer_ids))

    # Latest CD week per institution
    latest = conn.execute(f"""
        SELECT institution_id, MAX(scraped_week) AS max_week
        FROM rates WHERE institution_id IN ({placeholders}) AND product='cd'
        GROUP BY institution_id
    """, peer_ids).fetchall()
    latest_map = {r["institution_id"]: r["max_week"] for r in latest}

    # Prior CD week per institution
    prior = conn.execute(f"""
        SELECT institution_id, MAX(scraped_week) AS prior_week
        FROM rates WHERE institution_id IN ({placeholders}) AND product='cd'
        AND scraped_week < COALESCE((
            SELECT MAX(r2.scraped_week) FROM rates r2
            WHERE r2.institution_id=rates.institution_id AND r2.product='cd'
        ),'9999')
        GROUP BY institution_id
    """, peer_ids).fetchall()
    prior_map = {r["institution_id"]: r["prior_week"] for r in prior}

    # Fetch current + prior rates together
    rows = conn.execute(f"""
        SELECT
            i.id AS inst_id,
            COALESCE(i.name, bm.inst_name) AS inst_name,
            COALESCE(i.type, CASE WHEN bm.id LIKE 'ncua:%' THEN 'cu' ELSE 'bank' END) AS inst_type,
            i.assets_k, r.term_months, r.apy, r.min_balance, r.scraped_week
        FROM branch_markets bm
        JOIN institutions i ON (i.id='fdic:'||bm.cert OR i.id='ncua:'||bm.cert)
        JOIN rates r ON r.institution_id=i.id AND r.product='cd'
        WHERE bm.market_key=? AND r.apy IS NOT NULL
        ORDER BY r.term_months, r.apy DESC
    """, (mkey,)).fetchall()

    # Filter to latest week
    current_rows = [r for r in rows if latest_map.get(r["inst_id"]) == r["scraped_week"]]
    prior_rows   = {(r["inst_id"], r["term_months"], r["min_balance"] or 0): r["apy"]
                    for r in rows if prior_map.get(r["inst_id"]) == r["scraped_week"]}

    result = {}
    inst_meta = {}
    for r in current_rows:
        iid = r["inst_id"]
        if iid not in result:
            result[iid] = {}
            inst_meta[iid] = {
                "name":     r["inst_name"],
                "type":     r["inst_type"],
                "assets_k": r["assets_k"],
            }
        if r["term_months"] is not None and r["apy"] is not None:
            bal = r["min_balance"] or 0
            key = (r["term_months"], bal)
            result[iid][key] = {
                "apy":       r["apy"],
                "prior_apy": prior_rows.get((iid, r["term_months"], bal)),
                "week":      r["scraped_week"],
            }
    return result, inst_meta


def get_market_liquid_rates(conn, city: str, state: str) -> tuple[dict, dict]:
    """
    Pull latest savings/MM/checking rates for all peers in a market.
    Returns same (rates, inst_meta) structure as get_market_rates.
    """
    mkey = f"{city.strip().lower()}|{state.strip().lower()}"
    # Get peer institution IDs for this market first (fast)
    peer_ids = [r[0] for r in conn.execute("""
        SELECT DISTINCT COALESCE(i.id, '') FROM branch_markets bm
        LEFT JOIN institutions i ON (i.id = 'fdic:' || bm.cert OR i.id = 'ncua:' || bm.cert)
        WHERE bm.market_key = ? AND i.id IS NOT NULL
    """, (mkey,)).fetchall()]

    if not peer_ids:
        return {}, {}

    placeholders = ",".join("?" * len(peer_ids))

    # Latest week per institution+product (pre-aggregated, fast)
    latest = conn.execute(f"""
        SELECT institution_id, product, MAX(scraped_week) AS max_week
        FROM rates
        WHERE institution_id IN ({placeholders})
          AND product IN ('savings','money_market','checking')
        GROUP BY institution_id, product
    """, peer_ids).fetchall()

    # Build lookup: (inst_id, product) → max_week
    latest_map = {(r["institution_id"], r["product"]): r["max_week"] for r in latest}
    if not latest_map:
        return {}, {}

    # Fetch actual rates for those weeks
    rows = conn.execute(f"""
        SELECT
            i.id AS inst_id,
            COALESCE(i.name, bm.inst_name) AS inst_name,
            COALESCE(i.type, CASE WHEN bm.id LIKE 'ncua:%' THEN 'cu' ELSE 'bank' END) AS inst_type,
            i.assets_k,
            r.product, r.apy, r.min_balance, r.scraped_week
        FROM branch_markets bm
        JOIN institutions i ON (i.id = 'fdic:' || bm.cert OR i.id = 'ncua:' || bm.cert)
        JOIN rates r ON r.institution_id = i.id
            AND r.product IN ('savings','money_market','checking')
        WHERE bm.market_key = ? AND r.apy IS NOT NULL
        ORDER BY r.product, r.apy DESC
    """, (mkey,)).fetchall()

    # Filter to latest week per inst+product
    filtered = [r for r in rows if latest_map.get((r["inst_id"], r["product"])) == r["scraped_week"]]

    result = {}
    inst_meta = {}
    for r in filtered:
        iid = r["inst_id"]
        if iid not in result:
            result[iid] = {}
            inst_meta[iid] = {
                "name":     r["inst_name"],
                "type":     r["inst_type"],
                "assets_k": r["assets_k"],
            }
        if r["product"] and r["apy"] is not None:
            key = (r["product"], r["min_balance"] or 0)
            result[iid][key] = {
                "apy":       r["apy"],
                "prior_apy": None,  # prior week lookup omitted for performance
                "week":      r["scraped_week"],
            }
    return result, inst_meta


def build_liquid_table(inst_meta: dict, rates: dict, product: str,
                       client_id: str | None) -> list[dict]:
    """Build a ranked table for a single liquid product (savings/MM/checking)."""
    rows = []
    for iid, meta in inst_meta.items():
        rate_data = rates.get(iid, {})
        # Find best (highest APY) entry for this product
        entries = [(bal, data) for (prod, bal), data in rate_data.items() if prod == product]
        best = max(entries, key=lambda x: x[1]["apy"], default=None)
        rows.append({
            "name":       meta["name"] or "Unknown",
            "type":       meta["type"] or "bank",
            "assets_k":   meta["assets_k"],
            "is_client":  (iid == client_id),
            "inst_id":    iid,
            "apy":        best[1]["apy"]       if best else None,
            "prior_apy":  best[1]["prior_apy"] if best else None,
            "week":       best[1]["week"]      if best else None,
            "min_balance":best[0]              if best else None,
        })

    def sort_key(r):
        return (0, -(r["apy"])) if r["apy"] is not None else (1, 0)

    rows.sort(key=sort_key)
    return rows


def build_term_table(inst_meta: dict, rates: dict, term_months: int,
                     min_balance: float, client_id: str | None) -> list[dict]:
    """
    Build a ranked table for a single CD term + min_balance combo.
    Returns rows sorted by APY desc, with N/O and blank handling.
    """
    rows = []
    for iid, meta in inst_meta.items():
        rate_data = rates.get(iid, {})
        # Find best matching min_balance key
        entry = None
        for (term, bal), data in rate_data.items():
            if term == term_months:
                if entry is None or abs(bal - min_balance) < abs(
                        (entry.get("_bal") or 0) - min_balance):
                    entry = {**data, "_bal": bal}

        is_client = (iid == client_id)
        rows.append({
            "name":       meta["name"] or "Unknown",
            "type":       meta["type"] or "bank",
            "assets_k":   meta["assets_k"],
            "is_client":  is_client,
            "inst_id":    iid,
            "apy":        entry["apy"]       if entry else None,
            "rate":       entry["apy"]       if entry else None,  # same as apy for now
            "prior_apy":  entry["prior_apy"] if entry else None,
            "week":       entry["week"]      if entry else None,
            "status":     "rate"             if entry else ("no_data"),
        })

    # Sort: rates desc, then N/O, then no data
    def sort_key(r):
        if r["apy"] is not None:
            return (0, -(r["apy"]))
        return (1, 0)

    rows.sort(key=sort_key)
    return rows


def compute_average(rows: list[dict]) -> float | None:
    apys = [r["apy"] for r in rows if r["apy"] is not None]
    return sum(apys) / len(apys) if apys else None


# ── Text Report ───────────────────────────────────────────────────────────────

def render_text_report(conn, city: str, state: str, client_name: str) -> str:
    peers = get_peers(conn, city, state)
    rates, inst_meta = get_market_rates(conn, city, state)
    liq_rates, liq_meta = get_market_liquid_rates(conn, city, state)
    # Merge inst_meta so liquid-only institutions appear
    merged_meta = {**liq_meta, **inst_meta}

    client = find_client(peers, client_name)
    client_id = client["institution_id"] if client else None

    today = date.today().strftime("%m/%d/%Y")
    lines = []
    lines.append("=" * 70)
    lines.append(f"  {client_name}")
    lines.append(f"  Deposit Ranking Report — Powered by Strum Platform")
    lines.append(f"  Market: {city.title()}, {state.upper()}")
    lines.append(f"  Generated: {today}")
    lines.append("=" * 70)

    # ── Liquid products ──
    for product in LIQUID_PRODUCTS:
        label = LIQUID_LABELS[product]
        rows = build_liquid_table(merged_meta, liq_rates, product, client_id)
        rows_with_rates = [r for r in rows if r["apy"] is not None]
        if not rows_with_rates:
            continue
        lines.append(f"\n{'─'*70}")
        lines.append(f"  {label.upper()}")
        lines.append(f"{'─'*70}")
        lines.append(f"  {'Institution':<42} {'APY%':>6}  {'Chg':>7}")
        lines.append(f"  {'-'*58}")
        for r in rows:
            marker = "►" if r["is_client"] else " "
            name = r["name"][:40]
            if r["apy"] is not None:
                apy_str = f"{r['apy']*100:.2f}%"
                chg_str = "    —"
                if r["prior_apy"] is not None:
                    chg = (r["apy"] - r["prior_apy"]) * 100
                    chg_str = f"{chg:+.2f}" if abs(chg) > 0.001 else "    —"
                lines.append(f"  {marker} {name:<42} {apy_str:>6}  {chg_str:>7}")
            else:
                lines.append(f"  {marker} {name:<42}    N/O")
        avg = compute_average(rows)
        if avg:
            lines.append(f"  {'─'*58}")
            lines.append(f"  {'Rate Group Average':<42} {avg*100:.2f}%")

    # ── CDs ──
    for min_bal in MIN_BALANCES:
        lines.append(f"\n{'─'*70}")
        lines.append(f"  CERTIFICATES OF DEPOSIT — {MIN_BAL_LABELS[min_bal]} minimum")
        lines.append(f"{'─'*70}")

        for term in CD_TERMS:
            label = CD_LABELS[term]
            rows = build_term_table(inst_meta, rates, term, min_bal, client_id)
            rows_with_rates = [r for r in rows if r["apy"] is not None]
            if not rows_with_rates:
                continue

            lines.append(f"\n  {label} — {MIN_BAL_LABELS[min_bal]}")
            lines.append(f"  {'Institution':<42} {'APY%':>6}  {'Chg':>7}")
            lines.append(f"  {'-'*58}")

            for r in rows:
                marker = "►" if r["is_client"] else " "
                name   = r["name"][:40]
                if r["apy"] is not None:
                    apy_str = f"{r['apy']*100:.2f}%"
                    if r["prior_apy"] is not None:
                        chg = (r["apy"] - r["prior_apy"]) * 100
                        chg_str = f"{chg:+.2f}" if abs(chg) > 0.001 else "    —"
                    else:
                        chg_str = "    —"
                    lines.append(f"  {marker} {name:<42} {apy_str:>6}  {chg_str:>7}")
                else:
                    lines.append(f"  {marker} {name:<42}    N/O")

            avg = compute_average(rows)
            if avg:
                lines.append(f"  {'─'*58}")
                lines.append(f"  {'Rate Group Average':<42} {avg*100:.2f}%")

    lines.append(f"\n{'='*70}")
    lines.append(f"  © {date.today().year}, Powered by Strum Platform")
    lines.append(f"  Data sourced from public institution websites")
    lines.append("=" * 70)
    return "\n".join(lines)


# ── PDF Report ────────────────────────────────────────────────────────────────

def render_pdf_report(conn, city: str, state: str, client_name: str,
                      output_path: str) -> str:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        SimpleDocTemplate, Table, TableStyle, Paragraph,
        Spacer, HRFlowable, PageBreak
    )
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

    peers = get_peers(conn, city, state)
    rates, inst_meta = get_market_rates(conn, city, state)
    liq_rates, liq_meta = get_market_liquid_rates(conn, city, state)
    merged_meta = {**liq_meta, **inst_meta}
    client = find_client(peers, client_name)
    client_id = client["institution_id"] if client else None

    # Colors
    NAVY      = colors.HexColor("#1B3A6B")
    GOLD      = colors.HexColor("#C8A84B")
    LIGHT_BLUE= colors.HexColor("#E8EEF7")
    CLIENT_BG = colors.HexColor("#FFF8E1")
    WHITE     = colors.white
    GRAY      = colors.HexColor("#666666")
    MED_GRAY  = colors.HexColor("#CCCCCC")
    AVG_BG    = colors.HexColor("#F0F0F0")

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("title", fontSize=16, textColor=NAVY,
                                  fontName="Helvetica-Bold", spaceAfter=4)
    sub_style   = ParagraphStyle("sub",   fontSize=10, textColor=GRAY,
                                  fontName="Helvetica", spaceAfter=2)
    sect_style  = ParagraphStyle("sect",  fontSize=11, textColor=WHITE,
                                  fontName="Helvetica-Bold", spaceAfter=0,
                                  spaceBefore=12)
    tbl_style   = ParagraphStyle("tbl",   fontSize=9,  textColor=NAVY,
                                  fontName="Helvetica-Bold", spaceAfter=4,
                                  spaceBefore=8)

    doc = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        leftMargin=0.65*inch, rightMargin=0.65*inch,
        topMargin=0.75*inch,  bottomMargin=0.65*inch,
    )

    story = []
    today = date.today().strftime("%B %d, %Y")

    # ── Cover header ──
    story.append(Paragraph(client_name, title_style))
    story.append(Paragraph("Deposit Ranking Report", title_style))
    story.append(HRFlowable(width="100%", thickness=2, color=GOLD, spaceAfter=6))
    story.append(Paragraph(
        f"Market: {city.title()}, {state.upper()}  |  Generated: {today}  |  Powered by Strum Platform",
        sub_style))
    story.append(Spacer(1, 0.15*inch))

    def make_term_table(term, min_bal):
        label = f"{CD_LABELS[term]} — {MIN_BAL_LABELS[min_bal]}"
        rows  = build_term_table(inst_meta, rates, term, min_bal, client_id)
        rows_with_rates = [r for r in rows if r["apy"] is not None]
        if not rows_with_rates:
            return None, None

        # Table header
        data = [["Institution", "Rate (%)", "APY (%)", "Change"]]
        styles_list = [
            ("BACKGROUND",  (0,0), (-1,0), NAVY),
            ("TEXTCOLOR",   (0,0), (-1,0), WHITE),
            ("FONTNAME",    (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE",    (0,0), (-1,-1), 8),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [WHITE, LIGHT_BLUE]),
            ("GRID",        (0,0), (-1,-1), 0.25, MED_GRAY),
            ("ALIGN",       (1,0), (-1,-1), "CENTER"),
            ("VALIGN",      (0,0), (-1,-1), "MIDDLE"),
            ("LEFTPADDING",  (0,0), (-1,-1), 5),
            ("RIGHTPADDING", (0,0), (-1,-1), 5),
            ("TOPPADDING",   (0,0), (-1,-1), 3),
            ("BOTTOMPADDING",(0,0), (-1,-1), 3),
        ]

        for i, r in enumerate(rows, start=1):
            name = r["name"]
            if r["is_client"]:
                name = f"► {name}"

            if r["apy"] is not None:
                apy_str  = f"{r['apy']*100:.2f}"
                rate_str = f"{r['apy']*100:.2f}"
                if r["prior_apy"] is not None:
                    chg = (r["apy"] - r["prior_apy"]) * 100
                    chg_str = f"({chg:+.2f})" if abs(chg) > 0.001 else ""
                else:
                    chg_str = ""
            else:
                apy_str = rate_str = chg_str = "N/O"

            data.append([name, rate_str, apy_str, chg_str])

            if r["is_client"]:
                styles_list.append(("BACKGROUND", (0,i), (-1,i), CLIENT_BG))
                styles_list.append(("FONTNAME",   (0,i), (-1,i), "Helvetica-Bold"))
                styles_list.append(("TEXTCOLOR",  (0,i), (-1,i), NAVY))

        # Average row
        avg = compute_average(rows)
        if avg:
            avg_row_idx = len(data)
            data.append(["Rate Group Average **", f"{avg*100:.2f}", f"{avg*100:.2f}", ""])
            styles_list.append(("BACKGROUND", (0, avg_row_idx), (-1, avg_row_idx), AVG_BG))
            styles_list.append(("FONTNAME",   (0, avg_row_idx), (-1, avg_row_idx), "Helvetica-Bold"))

        col_widths = [3.2*inch, 1.0*inch, 1.0*inch, 1.0*inch]
        tbl = Table(data, colWidths=col_widths, repeatRows=1)
        tbl.setStyle(TableStyle(styles_list))
        return label, tbl

    # ── Liquid products section ──
    liquid_tables_added = 0
    for product in LIQUID_PRODUCTS:
        prod_label = LIQUID_LABELS[product]
        rows = build_liquid_table(merged_meta, liq_rates, product, client_id)
        rows_with_rates = [r for r in rows if r["apy"] is not None]
        if not rows_with_rates:
            continue

        if liquid_tables_added == 0:
            # Section header
            liq_hdr = Table([[f"  SAVINGS, CHECKING & MONEY MARKET"]], colWidths=[7.2*inch])
            liq_hdr.setStyle(TableStyle([
                ("BACKGROUND",   (0,0), (-1,-1), NAVY),
                ("TEXTCOLOR",    (0,0), (-1,-1), WHITE),
                ("FONTNAME",     (0,0), (-1,-1), "Helvetica-Bold"),
                ("FONTSIZE",     (0,0), (-1,-1), 11),
                ("TOPPADDING",   (0,0), (-1,-1), 6),
                ("BOTTOMPADDING",(0,0), (-1,-1), 6),
            ]))
            story.append(Spacer(1, 0.1*inch))
            story.append(liq_hdr)
            story.append(Spacer(1, 0.05*inch))

        # Build table data
        data = [[prod_label, "Rate (%)", "APY (%)", "Change"]]
        tstyles = [
            ("BACKGROUND",  (0,0), (-1,0), NAVY),
            ("TEXTCOLOR",   (0,0), (-1,0), WHITE),
            ("FONTNAME",    (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE",    (0,0), (-1,-1), 8),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [WHITE, LIGHT_BLUE]),
            ("GRID",        (0,0), (-1,-1), 0.25, MED_GRAY),
            ("ALIGN",       (1,0), (-1,-1), "CENTER"),
            ("VALIGN",      (0,0), (-1,-1), "MIDDLE"),
            ("LEFTPADDING",  (0,0), (-1,-1), 5),
            ("RIGHTPADDING", (0,0), (-1,-1), 5),
            ("TOPPADDING",   (0,0), (-1,-1), 3),
            ("BOTTOMPADDING",(0,0), (-1,-1), 3),
        ]
        for i, r in enumerate(rows, start=1):
            name = ("► " if r["is_client"] else "") + r["name"]
            if r["apy"] is not None:
                apy_str = f"{r['apy']*100:.2f}"
                chg_str = ""
                if r["prior_apy"] is not None:
                    chg = (r["apy"] - r["prior_apy"]) * 100
                    chg_str = f"({chg:+.2f})" if abs(chg) > 0.001 else ""
            else:
                apy_str = chg_str = "N/O"
            data.append([name, apy_str, apy_str, chg_str])
            if r["is_client"]:
                tstyles.append(("BACKGROUND", (0,i), (-1,i), CLIENT_BG))
                tstyles.append(("FONTNAME",   (0,i), (-1,i), "Helvetica-Bold"))
                tstyles.append(("TEXTCOLOR",  (0,i), (-1,i), NAVY))

        avg = compute_average(rows)
        if avg:
            ai = len(data)
            data.append(["Rate Group Average **", f"{avg*100:.2f}", f"{avg*100:.2f}", ""])
            tstyles.append(("BACKGROUND", (0,ai), (-1,ai), AVG_BG))
            tstyles.append(("FONTNAME",   (0,ai), (-1,ai), "Helvetica-Bold"))

        tbl = Table(data, colWidths=[3.2*inch, 1.0*inch, 1.0*inch, 1.0*inch], repeatRows=1)
        tbl.setStyle(TableStyle(tstyles))
        story.append(Paragraph(prod_label, tbl_style))
        story.append(tbl)
        story.append(Spacer(1, 0.08*inch))
        liquid_tables_added += 1

    # ── Generate tables for each min_balance tier ──
    for min_bal in MIN_BALANCES:
        # Section header
        sect_hdr_data = [[f"  CERTIFICATES OF DEPOSIT — {MIN_BAL_LABELS[min_bal]} Minimum"]]
        sect_hdr = Table(sect_hdr_data, colWidths=[7.2*inch])
        sect_hdr.setStyle(TableStyle([
            ("BACKGROUND",   (0,0), (-1,-1), NAVY),
            ("TEXTCOLOR",    (0,0), (-1,-1), WHITE),
            ("FONTNAME",     (0,0), (-1,-1), "Helvetica-Bold"),
            ("FONTSIZE",     (0,0), (-1,-1), 11),
            ("TOPPADDING",   (0,0), (-1,-1), 6),
            ("BOTTOMPADDING",(0,0), (-1,-1), 6),
        ]))
        story.append(Spacer(1, 0.1*inch))
        story.append(sect_hdr)
        story.append(Spacer(1, 0.05*inch))

        tables_added = 0
        for term in CD_TERMS:
            label, tbl = make_term_table(term, min_bal)
            if tbl is None:
                continue
            story.append(Paragraph(label, tbl_style))
            story.append(tbl)
            story.append(Spacer(1, 0.08*inch))
            tables_added += 1

        if tables_added == 0:
            story.append(Paragraph("  No rate data available for this tier.", sub_style))

    # Footer
    story.append(Spacer(1, 0.2*inch))
    story.append(HRFlowable(width="100%", thickness=1, color=GOLD))
    story.append(Paragraph(
        f"© {date.today().year}, Powered by Strum Platform  |  "
        f"Data sourced from public institution websites  |  "
        f"Generated {today}",
        ParagraphStyle("footer", fontSize=7, textColor=GRAY,
                       fontName="Helvetica", alignment=TA_CENTER, spaceBefore=4)
    ))

    doc.build(story)
    return output_path


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Deposit Ranking Report Generator")
    parser.add_argument("--client",  required=True, help="Client institution name")
    parser.add_argument("--market",  nargs=2, metavar=("CITY", "STATE"), required=True)
    parser.add_argument("--output",  default=None, help="PDF output path")
    parser.add_argument("--text",    action="store_true", help="Print text report to stdout")
    args = parser.parse_args()

    city, state = args.market
    conn = get_conn()

    if args.text or not args.output:
        report = render_text_report(conn, city, state, args.client)
        print(report)

    if args.output or not args.text:
        out = args.output or f"/tmp/{args.client.replace(' ','_')}_DepositRanking_{date.today()}.pdf"
        print(f"Generating PDF → {out}")
        render_pdf_report(conn, city, state, args.client, out)
        print(f"✅ PDF saved: {out}")


if __name__ == "__main__":
    main()
