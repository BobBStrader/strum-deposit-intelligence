"""
Parse loan and mortgage rates for Baltimore market institutions.
Uses loan_raw_section and mortgage_raw_section columns (not raw_section).
Calls run_loan_parser() and run_mortgage_parser() with custom logic.
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

def get_baltimore_institutions():
    """Get all institution IDs in Baltimore market."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT DISTINCT i.id, i.name, i.loan_raw_section, i.mortgage_raw_section
        FROM branch_markets bm
        LEFT JOIN institutions i ON (i.id='fdic:'||bm.cert OR i.id='ncua:'||bm.cert)
        WHERE bm.market_key='baltimore|md' AND i.id IS NOT NULL
        ORDER BY i.name
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def parse_loan_rates(institutions):
    """Parse loan rates using LOAN_EXTRACT_PROMPT on loan_raw_section."""
    conn = get_conn()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    c = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    week = current_week()

    total_extracted = total_verified = total_rejected = 0
    insts_with_loans = [i for i in institutions if i.get('loan_raw_section')]
    print(f"\n=== LOAN PARSER: {len(insts_with_loans)} institutions with loan_raw_section ===\n")

    for idx, inst in enumerate(insts_with_loans):
        inst_id = inst['id']
        name = inst['name']
        section = inst['loan_raw_section']

        print(f"  [{idx+1}/{len(insts_with_loans)}] {name[:55]}", flush=True)

        # Extract
        prompt = lp.LOAN_EXTRACT_PROMPT.format(institution=name, page_text=section)
        extracted, used_model = lp._llm_extract(prompt, name)

        if not extracted:
            print(f"    → 0 loan rates extracted")
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
        prompt2 = lp.LOAN_VERIFY_PROMPT.format(
            institution=name,
            rates_json=json.dumps(rates_summary, indent=2),
            page_text=section
        )
        raw_resp2 = lp.call_openai(prompt2, timeout=120, model=used_model)
        verified = lp.parse_json(raw_resp2) or []

        verify_map = {}
        for v in verified:
            key = (v.get('product'), v.get('term_months'), v.get('apy_pct'))
            verify_map[key] = v

        # Delete old loan rates for this institution this week
        c.execute("""DELETE FROM rates
                     WHERE institution_id=? AND scraped_week=?
                       AND product IN ('new_auto_loan','used_auto_loan','personal_loan','home_equity')""",
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

            term_mo = r.get('term_months')
            vage = r.get('vehicle_age_years')
            amtk = r.get('loan_amount_k')
            if r['product'] == 'new_auto_loan':
                label = f"{term_mo}Mo New Auto {amtk}k" if term_mo and amtk else f"{term_mo}Mo New Auto"
            elif r['product'] == 'used_auto_loan':
                yr_str = f"{vage} Yr " if vage is not None else ""
                label = f"{term_mo}Mo {yr_str}Used Auto {amtk}k" if term_mo and amtk else f"{term_mo}Mo Used Auto"
            else:
                label = f"{term_mo}Mo {r['product'].replace('_',' ').title()}" if term_mo else r['product']

            notes = r.get('notes') or 'APR'
            if 'APR' not in (notes or ''):
                notes = ('APR; ' + notes).strip('; ') if notes else 'APR'

            c.execute("""INSERT INTO rates
                         (institution_id, scraped_at, scraped_week, product, group_id,
                          term_months, apy, min_balance, notes, confidence, verified_snippet,
                          loan_term_label, vehicle_age_years, loan_amount_k, rate_type)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (inst_id, now, week, r['product'], group_id, term_mo,
                       r['apy'], r.get('min_balance'), notes, confidence, snippet,
                       label, vage, amtk, 'apr'))
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
    print(f"\n═══ Loan Parse Complete ═══\n  Extracted: {total_extracted}\n  Verified: {total_verified}\n  Rejected: {total_rejected}\n")

def parse_mortgage_rates(institutions):
    """Parse mortgage rates using MORTGAGE_EXTRACT_PROMPT on mortgage_raw_section."""
    conn = get_conn()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    c = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    week = current_week()

    total_extracted = total_verified = total_rejected = 0
    insts_with_mtg = [i for i in institutions if i.get('mortgage_raw_section')]
    print(f"\n=== MORTGAGE PARSER: {len(insts_with_mtg)} institutions with mortgage_raw_section ===\n")

    for idx, inst in enumerate(insts_with_mtg):
        inst_id = inst['id']
        name = inst['name']
        section = inst['mortgage_raw_section']

        print(f"  [{idx+1}/{len(insts_with_mtg)}] {name[:55]}", flush=True)

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
        verified = lp.parse_json(raw_resp2) or []

        verify_map = {}
        for v in verified:
            key = (v.get('product'), v.get('term_months'), v.get('apy_pct'))
            verify_map[key] = v

        # Delete old mortgage rates for this institution this week
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
    institutions = get_baltimore_institutions()
    print(f"Baltimore market: {len(institutions)} institutions total")
    loan_count = sum(1 for i in institutions if i.get('loan_raw_section'))
    mtg_count = sum(1 for i in institutions if i.get('mortgage_raw_section'))
    print(f"  With loan_raw_section: {loan_count}")
    print(f"  With mortgage_raw_section: {mtg_count}")

    parse_loan_rates(institutions)
    parse_mortgage_rates(institutions)

    print("\n=== SUMMARY BY PRODUCT ===")
    conn = get_conn()
    rows = conn.execute("""
        SELECT product, COUNT(*) as cnt, MIN(apy*100), MAX(apy*100)
        FROM rates GROUP BY product ORDER BY product
    """).fetchall()
    for r in rows:
        print(f"  {r[0]:<22} count={r[1]:3d}  min={r[2]:.3f}%  max={r[3]:.3f}%")
    conn.close()
