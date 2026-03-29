"""
Load All Institutions — copy full FDIC + NCUA registry from rate-scraper DB.
Run once to bootstrap, then re-run to pick up new institutions.

Usage:
    python3 load_all_institutions.py          # copy all
    python3 load_all_institutions.py --state MD  # one state only
"""
import argparse
import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from schema import get_conn

RATE_SCRAPER_DB = os.path.expanduser(
    "~/.openclaw/workspace/rate-scraper/db/rates.db"
)

def load_all(state_filter=None):
    if not os.path.exists(RATE_SCRAPER_DB):
        print(f"❌ rate-scraper DB not found: {RATE_SCRAPER_DB}")
        return

    src = sqlite3.connect(RATE_SCRAPER_DB)
    src.row_factory = sqlite3.Row
    dst = get_conn()
    c = dst.cursor()

    for inst_type in ("bank", "cu"):
        query = """SELECT id, type, name, charter, state, assets_k, website_url
                   FROM institutions WHERE type=?"""
        params = [inst_type]
        if state_filter:
            query += " AND state=?"
            params.append(state_filter.upper())

        rows = src.execute(query, params).fetchall()
        scope = state_filter.upper() if state_filter else "all states"
        print(f"Copying {len(rows)} {inst_type}s ({scope})...")

        inserted = updated = 0
        for r in rows:
            existing = c.execute(
                "SELECT id FROM institutions WHERE id=?", (r["id"],)
            ).fetchone()
            if existing:
                # Update website_url if we have one and they don't
                if r["website_url"]:
                    c.execute(
                        "UPDATE institutions SET name=?, assets_k=?, website_url=COALESCE(website_url,?) WHERE id=?",
                        (r["name"], r["assets_k"], r["website_url"], r["id"])
                    )
                updated += 1
            else:
                c.execute(
                    """INSERT INTO institutions
                       (id, type, name, charter, state, assets_k, website_url)
                       VALUES (?,?,?,?,?,?,?)""",
                    (r["id"], r["type"], r["name"], r["charter"],
                     r["state"], r["assets_k"], r["website_url"])
                )
                inserted += 1

        dst.commit()
        print(f"  ✅ {inserted} new, {updated} updated")

    src.close()
    dst.close()
    print("\nDone.")


def main():
    parser = argparse.ArgumentParser(description="Load all institutions from rate-scraper DB")
    parser.add_argument("--state", help="Filter by state (e.g. MD)")
    args = parser.parse_args()
    load_all(state_filter=args.state)


if __name__ == "__main__":
    main()
