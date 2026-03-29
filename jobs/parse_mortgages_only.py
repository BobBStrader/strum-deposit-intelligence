"""
Parse mortgage rates for Baltimore market institutions that haven't been processed yet.
Uses mortgage_raw_section column.
"""
import sys, os, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scrapers'))

from schema import get_conn
import llm_parser as lp
from datetime import datetime, timezone, date


def current_week():
    iso = date.today().isocalendar()
    return f"{iso[0]}-{iso[1]:02d}"


def parse_mortgage_rates():
    """Parse mortgage rates for all Baltimore institutions not yet done."""
    conn = get_conn()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    c = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    week = current_week()

    # Get Baltimore institutions with mortgage_raw_section not yet parsed this week
    rows = conn.execute("""
        SELECT DISTINCT i.id, i.name, i.mortgage_raw_section
        FROM branch_markets bm
        LEFT JOIN institutions i ON (i.id='fdic:'||bm.cert OR i.id='ncua:'||bm.cert)
        WHERE bm.market_key='baltimore|md'
          AND i.id IS NOT NULL
          AND i.mortgage_raw_section IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM rates r
              WHERE r.institution_id = i.id
                AND r.product IN ('mortgage_fixed','mortgage_arm')
                AND r.scraped_week = ?
          )
        ORDER BY i.name
    """, (week,)).fetchall()

    total_extracted = total_verified = total_rejected = 0
    print(f"\n=== MORTGAGE PARSER: {len(rows)} institutions to process ===\n")

    for idx, row in enumerate(rows):
        inst_id = row['id']
        name = row['name']
        section = row['mortgage_raw_section']

        print(f"  [{idx+1}/{len(rows)}] {name[:55]}", flush=True)

        # Extract
        prompt = lp.MORTGAGE_EXTRACT_PROMPT.format(institution=name, page_text=section)
        extracted, used_model = lp._llm_extract(prompt, name)

        if not extracted:
            print(f"    → 0 mortgage rates extracted")
            continue

        # Normalize + filter
        clean = []
        for r in extracted:
            if not isinstance(r, dict) or 'product' not in r:
                total_rejected += 1
                continue
            apy = lp.normalize_apy(r.get('apy'))
            product = r.get('product', 'unknown')
            term = r.get('term_months')
            ok, reason = lp.passes_rules(product, apy, term)
            if ok:
                clean.append({**r, 'apy': apy})
            else:
                print(f"    REJECTED: {product} {apy} — {reason}")
                total_rejected += 1

        if not clean:
            print(f"    → {len(extracted)} extracted, all failed rules")
            continue

        print(f"    → {len(clean)} passed rules, verifying...", flush=True)

        # Verify
        rates_summary = [
            {'product': r['product'], 'term_months': r.get('term_months'),
             'apy_pct': round(r['apy'] * 100, 3)}
            for r in clean
        ]
        prompt2 = lp.MORTGAGE_VERIFY_PROMPT.format(
            institution=name,
            rates_json=json.dumps(rates_summary, indent=2),
            page_text=section
        )
        raw_resp2 = lp.call_openai(prompt2, timeout=120, model=used_model)
        verified_list = lp.parse_json(raw_resp2) or []

        verify_map = {}
        for v in verified_list:
            key = (v.get('product'), v.get('term_months'), v.get('apy_pct'))
            verify_map[key] = v

        # Delete old mortgage rates for this inst this week
        c.execute("""DELETE FROM rates
                     WHERE institution_id=? AND scraped_week=?
                       AND product IN ('mortgage_fixed','mortgage_arm')""",
                  (inst_id, week))

        saved = 0
        for r in clean:
            apy_pct = round(r['apy'] * 100, 3)
            key = (r['product'], r.get('term_months'), apy_pct)
            vresult = verify_map.get(key, {})
            is_verified = vresult.get('verified', False)
            snippet = vresult.get('snippet')
            confidence = 'verified' if is_verified else 'unverified'
            group_id = lp.PRODUCT_GROUP_MAP.get(r['product'])

            rate_type = r.get('rate_type', 'fixed' if r['product'] == 'mortgage_fixed' else 'arm')
            arm_init = r.get('arm_initial_years')
            arm_adj = r.get('arm_adjust_months')
            conforming_val = r.get('conforming', 1)
            term_mo = r.get('term_months')

            if rate_type == 'arm' and arm_init:
                adj_str = f"/{arm_adj//12 if arm_adj and arm_adj >= 12 else arm_adj}" if arm_adj else ""
                label = f"{arm_init}{adj_str} ARM Conforming" if conforming_val else f"{arm_init}{adj_str} ARM Jumbo"
            elif rate_type == 'fixed' and term_mo:
                yr = term_mo // 12
                label = f"{yr}Yr Fixed Conforming" if conforming_val else f"{yr}Yr Fixed Jumbo"
            else:
                label = r['product'].replace('_', ' ').title()

            c.execute("""INSERT INTO rates
                         (institution_id, scraped_at, scraped_week, product, group_id,
                          term_months, apy, min_balance, notes, confidence, verified_snippet,
                          loan_term_label, rate_type, arm_initial_years, arm_adjust_months, conforming)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (inst_id, now, week, r['product'], group_id, term_mo,
                       r['apy'], r.get('min_balance'), r.get('notes'), confidence, snippet,
                       label, rate_type, arm_init, arm_adj, conforming_val))
            saved += 1
            if is_verified:
                total_verified += 1
            total_extracted += 1

        conn.commit()
        vc = sum(1 for r in clean
                 if verify_map.get((r['product'], r.get('term_months'),
                                    round(r['apy']*100, 3)), {}).get('verified'))
        print(f"    ✅ {vc} verified | ❓ {saved - vc} unverified | saved={saved}")
        time.sleep(lp.CALL_DELAY)

    conn.close()
    print(f"\n═══ Mortgage Parse Complete ═══\n  Extracted: {total_extracted}\n  Verified: {total_verified}\n  Rejected: {total_rejected}\n")


if __name__ == '__main__':
    parse_mortgage_rates()

    print("\n=== SUMMARY BY PRODUCT ===")
    from schema import get_conn
    conn = get_conn()
    rows = conn.execute("""
        SELECT product, COUNT(*) as cnt, MIN(apy*100), MAX(apy*100)
        FROM rates GROUP BY product ORDER BY product
    """).fetchall()
    for r in rows:
        print(f"  {r[0]:<22} count={r[1]:3d}  min={r[2]:.3f}%  max={r[3]:.3f}%")

    print("\n=== MORTGAGE RATES BY INSTITUTION ===")
    rows = conn.execute("""
        SELECT i.name, r.product, r.term_months, r.apy*100, r.arm_initial_years, r.arm_adjust_months, r.conforming
        FROM rates r JOIN institutions i ON r.institution_id=i.id
        WHERE r.product IN ('mortgage_fixed','mortgage_arm')
        ORDER BY r.product, r.term_months, r.apy
    """).fetchall()
    for r in rows:
        print(f"  {r[0]:<45} {r[1]:<15} term={r[2]} rate={r[3]:.3f}% arm_init={r[4]} arm_adj={r[5]} conf={r[6]}")
    conn.close()
