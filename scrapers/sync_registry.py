#!/usr/bin/env python3
"""
Registry Sync — keeps institutions table current with FDIC and NCUA APIs.

Handles:
- New institutions (add with status pending)
- Name changes (update name, log change)
- Closures / mergers (mark active=0, log reason)
- Asset size changes (update assets_k)
- Website URL changes

Usage:
    python3 scrapers/sync_registry.py              # full sync
    python3 scrapers/sync_registry.py --fdic-only
    python3 scrapers/sync_registry.py --ncua-only
    python3 scrapers/sync_registry.py --dry-run    # show changes without writing
    python3 scrapers/sync_registry.py --stats      # show current registry stats

Cron (weekly, Sunday 2 AM):
    0 2 * * 0 cd /Users/bob/.openclaw/workspace/deposit-intelligence && python3 scrapers/sync_registry.py >> /tmp/registry_sync.log 2>&1
"""

import argparse, sqlite3, json, time, datetime, requests, sys, os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'rates.db')

FDIC_API  = 'https://banks.data.fdic.gov/api/institutions'
NCUA_API  = 'https://mapping.ncua.gov/api/ResearchCreditUnion/GetQuickSearch'
NOW       = datetime.datetime.now().isoformat()
TODAY     = datetime.date.today().isoformat()

# FDIC field mapping
FDIC_FIELDS = 'CERT,NAME,ACTIVE,STALP,ASSET,WEBADDR,NAMEHCR,INSTCAT,SPECGRP'


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.row_factory = sqlite3.Row
    return conn


# ── FDIC ──────────────────────────────────────────────────────────────────────

def fetch_fdic_page(offset, limit=10000):
    try:
        r = requests.get(FDIC_API, params={
            'fields':  FDIC_FIELDS,
            'limit':   limit,
            'offset':  offset,
            'output':  'json',
        }, timeout=30)
        data = r.json()
        return data.get('data', []), data.get('meta', {}).get('total', 0)
    except Exception as e:
        print(f'  FDIC fetch error at offset {offset}: {e}')
        return [], 0


def fetch_all_fdic():
    """Fetch all FDIC institutions. Returns list of dicts."""
    print('[FDIC] Fetching institution registry...')
    all_insts = []
    offset = 0
    limit  = 10000
    total  = None

    while True:
        page, total = fetch_fdic_page(offset, limit)
        if not page:
            break
        all_insts.extend(page)
        offset += len(page)
        print(f'  {offset:,} / {total:,}', end='\r', flush=True)
        if offset >= total:
            break
        time.sleep(0.1)

    print(f'  Fetched {len(all_insts):,} FDIC institutions')
    return all_insts


# ── NCUA ──────────────────────────────────────────────────────────────────────

def fetch_all_ncua():
    """Fetch all active NCUA credit unions. Returns list of dicts."""
    print('[NCUA] Fetching credit union registry...')
    all_cus = []
    skip = 0
    take = 100

    while True:
        try:
            r = requests.post(NCUA_API,
                json={'skip': skip, 'take': take},
                timeout=20)
            data = r.json()
            results = data.get('results', data) if isinstance(data, dict) else data
            if not results:
                break
            # Filter active, non-corporate CUs client-side
            active = [cu for cu in results
                      if cu.get('isActive', True) and not cu.get('isCorporate', False)]
            all_cus.extend(active)
            skip += take
            print(f'  {len(all_cus):,} active CUs so far...', end='\r', flush=True)
            if len(results) < take:
                break
            time.sleep(0.05)
        except Exception as e:
            print(f'  NCUA fetch error at skip {skip}: {e}')
            break

    print(f'  Fetched {len(all_cus):,} active NCUA credit unions')
    return all_cus


# ── NCUA detail (for website URL) ─────────────────────────────────────────────

def fetch_ncua_detail(charter):
    try:
        r = requests.get(
            f'https://mapping.ncua.gov/api/CreditUnionDetails/GetCreditUnionDetails/{charter}',
            timeout=10)
        return r.json()
    except:
        return {}


# ── Sync logic ────────────────────────────────────────────────────────────────

def sync_fdic(conn, dry_run=False):
    """Sync FDIC banks into institutions table."""
    fdic_insts = fetch_all_fdic()

    added = renamed = deactivated = asset_updated = 0
    changes = []

    for inst in fdic_insts:
        data = inst.get('data', inst)
        cert    = str(data.get('CERT', '')).strip()
        name    = (data.get('NAME', '') or '').strip().upper()
        active  = int(data.get('ACTIVE', 1)) == 1
        state   = (data.get('STALP', '') or '').strip()
        assets  = int(data.get('ASSET', 0) or 0)  # already in thousands
        website = (data.get('WEBADDR', '') or '').strip()

        if not cert or not name:
            continue

        inst_id = f'fdic:{cert}'

        # Clean up website URL
        if website and not website.startswith('http'):
            website = f'https://{website}'

        existing = conn.execute(
            'SELECT id, name, active, assets_k, website_url FROM institutions WHERE id=?',
            (inst_id,)).fetchone()

        if existing is None:
            # New institution
            if not dry_run:
                conn.execute("""
                    INSERT OR IGNORE INTO institutions
                    (id, type, name, charter, state, assets_k, website_url, active)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (inst_id, 'bank', name, int(cert), state, assets,
                      website or None, 1 if active else 0))
            added += 1
            changes.append(f'NEW FDIC  {inst_id}: {name} ({state})')

        else:
            updates = []
            vals    = []

            # Name change
            if existing['name'] != name:
                updates.append('name=?')
                vals.append(name)
                changes.append(f'RENAME FDIC {inst_id}: "{existing["name"]}" → "{name}"')
                renamed += 1

            # Active status change (closure/reopening)
            was_active = bool(existing['active'])
            if was_active != active:
                updates.append('active=?')
                vals.append(1 if active else 0)
                if not active:
                    updates.append('scrape_status=?')
                    vals.append('closed')
                    changes.append(f'CLOSED FDIC {inst_id}: {name}')
                    deactivated += 1
                else:
                    changes.append(f'REOPENED FDIC {inst_id}: {name}')

            # Asset size change (>10% difference)
            old_assets = existing['assets_k'] or 0
            if assets > 0 and old_assets > 0:
                delta_pct = abs(assets - old_assets) / old_assets
                if delta_pct > 0.10:
                    updates.append('assets_k=?')
                    vals.append(assets)
                    asset_updated += 1

            # Website URL (only update if currently empty)
            if website and not existing['website_url']:
                updates.append('website_url=?')
                vals.append(website)

            if updates and not dry_run:
                vals.append(inst_id)
                conn.execute(
                    f"UPDATE institutions SET {', '.join(updates)}, last_scraped_at=NULL WHERE id=?",
                    vals)

    if not dry_run:
        conn.commit()

    print(f'[FDIC] added={added} renamed={renamed} deactivated={deactivated} asset_updated={asset_updated}')
    return changes


def sync_ncua(conn, dry_run=False):
    """Sync NCUA credit unions into institutions table."""
    ncua_cus = fetch_all_ncua()

    # Build set of active NCUA charters from API
    api_charters = {str(cu.get('charterNumber', cu.get('charter', ''))) for cu in ncua_cus}

    added = renamed = deactivated = asset_updated = 0
    changes = []

    for cu in ncua_cus:
        charter = str(cu.get('charterNumber', cu.get('charter', ''))).strip()
        name    = (cu.get('creditUnionName', cu.get('name', '')) or '').strip().upper()
        state   = (cu.get('state', cu.get('stateName', '')) or '')[:2].upper()
        assets  = int(cu.get('totalAssets', cu.get('assets', 0)) or 0)  # in dollars
        assets_k = assets // 1000

        if not charter or not name:
            continue

        inst_id = f'ncua:{charter}'

        existing = conn.execute(
            'SELECT id, name, active, assets_k, website_url FROM institutions WHERE id=?',
            (inst_id,)).fetchone()

        if existing is None:
            # New CU — fetch detail for website URL
            detail = fetch_ncua_detail(charter)
            website = (detail.get('siteUrl', '') or '').strip()
            if website and not website.startswith('http'):
                website = f'https://{website}'

            if not dry_run:
                conn.execute("""
                    INSERT OR IGNORE INTO institutions
                    (id, type, name, charter, state, assets_k, website_url, active)
                    VALUES (?,?,?,?,?,?,?,1)
                """, (inst_id, 'cu', name, int(charter), state, assets_k, website or None))
            added += 1
            changes.append(f'NEW NCUA  {inst_id}: {name} ({state})')
            time.sleep(0.1)  # rate limit on detail calls

        else:
            updates = []
            vals    = []

            if existing['name'] != name:
                updates.append('name=?')
                vals.append(name)
                changes.append(f'RENAME NCUA {inst_id}: "{existing["name"]}" → "{name}"')
                renamed += 1

            # Check asset size change
            old_k = existing['assets_k'] or 0
            if assets_k > 0 and old_k > 0:
                delta_pct = abs(assets_k - old_k) / old_k
                if delta_pct > 0.10:
                    updates.append('assets_k=?')
                    vals.append(assets_k)
                    asset_updated += 1

            if updates and not dry_run:
                vals.append(inst_id)
                conn.execute(
                    f"UPDATE institutions SET {', '.join(updates)} WHERE id=?", vals)

    # Mark CUs not in API response as inactive (closed/merged)
    existing_ncua = conn.execute(
        "SELECT id, name FROM institutions WHERE type='cu' AND active=1").fetchall()

    for row in existing_ncua:
        charter = row['id'].replace('ncua:', '')
        if charter not in api_charters:
            changes.append(f'CLOSED NCUA {row["id"]}: {row["name"]}')
            deactivated += 1
            if not dry_run:
                conn.execute(
                    "UPDATE institutions SET active=0, scrape_status='closed' WHERE id=?",
                    (row['id'],))

    if not dry_run:
        conn.commit()

    print(f'[NCUA] added={added} renamed={renamed} deactivated={deactivated} asset_updated={asset_updated}')
    return changes


def show_stats(conn):
    total      = conn.execute('SELECT COUNT(*) FROM institutions').fetchone()[0]
    active     = conn.execute('SELECT COUNT(*) FROM institutions WHERE active=1').fetchone()[0]
    inactive   = conn.execute('SELECT COUNT(*) FROM institutions WHERE active=0').fetchone()[0]
    banks      = conn.execute("SELECT COUNT(*) FROM institutions WHERE type='bank' AND active=1").fetchone()[0]
    cus        = conn.execute("SELECT COUNT(*) FROM institutions WHERE type='cu' AND active=1").fetchone()[0]
    pending    = conn.execute("SELECT COUNT(*) FROM institutions WHERE active=1 AND scrape_status='pending'").fetchone()[0]
    with_rates = conn.execute('SELECT COUNT(DISTINCT institution_id) FROM rates').fetchone()[0]
    print(f"""
=== Registry Stats ===
Total institutions:  {total:,}
  Active:            {active:,} ({banks:,} banks + {cus:,} CUs)
  Inactive/closed:   {inactive:,}
Pending scrape:      {pending:,}
With any rates:      {with_rates:,}
Last sync:           {TODAY}
""")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--fdic-only', action='store_true')
    parser.add_argument('--ncua-only', action='store_true')
    parser.add_argument('--dry-run',   action='store_true', help='Show changes without writing')
    parser.add_argument('--stats',     action='store_true')
    parser.add_argument('--log-changes', action='store_true', help='Print all changes')
    args = parser.parse_args()

    conn = get_db()

    if args.stats:
        show_stats(conn)
        return

    print(f'[{NOW}] Registry sync starting (dry_run={args.dry_run})')
    all_changes = []

    if not args.ncua_only:
        changes = sync_fdic(conn, dry_run=args.dry_run)
        all_changes.extend(changes)

    if not args.fdic_only:
        changes = sync_ncua(conn, dry_run=args.dry_run)
        all_changes.extend(changes)

    # Log significant changes
    print(f'\n[DONE] Total changes: {len(all_changes)}')
    if args.log_changes or args.dry_run:
        for c in all_changes:
            print(f'  {c}')

    # Write change log
    if all_changes and not args.dry_run:
        log_path = os.path.join(os.path.dirname(__file__), '..', 'logs', 'registry_changes.log')
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, 'a') as f:
            f.write(f'\n=== {NOW} ===\n')
            for c in all_changes:
                f.write(f'{c}\n')
        print(f'  Changes logged to {log_path}')

    show_stats(conn)
    conn.close()


if __name__ == '__main__':
    main()
