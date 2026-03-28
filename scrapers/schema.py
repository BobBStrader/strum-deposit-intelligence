"""
Rate Scraper — SQLite Schema
"""
import sqlite3, os

DB_PATH = os.path.join(os.path.dirname(__file__), '../db/rates.db')

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.executescript("""
    -- Product group lookup
    CREATE TABLE IF NOT EXISTS product_groups (
        group_id    TEXT PRIMARY KEY,
        group_name  TEXT NOT NULL,
        category    TEXT NOT NULL   -- 'deposit' | 'loan'
    );
    INSERT OR IGNORE INTO product_groups VALUES
        ('deposit_liquid', 'Liquid Deposits',    'deposit'),
        ('deposit_term',   'Term Deposits (CDs)', 'deposit'),
        ('loan_secured',   'Secured Loans',       'loan'),
        ('loan_unsecured', 'Unsecured Loans',     'loan');

    -- Product → group mapping
    CREATE TABLE IF NOT EXISTS product_group_map (
        product  TEXT PRIMARY KEY,
        group_id TEXT NOT NULL REFERENCES product_groups(group_id)
    );
    INSERT OR IGNORE INTO product_group_map VALUES
        ('savings',         'deposit_liquid'),
        ('checking',        'deposit_liquid'),
        ('money_market',    'deposit_liquid'),
        ('cd',              'deposit_term'),
        ('ira_cd',          'deposit_term'),
        ('mortgage',        'loan_secured'),
        ('home_equity',     'loan_secured'),
        ('auto_loan',       'loan_secured'),
        ('personal_loan',   'loan_unsecured'),
        ('new_auto_loan',   'loan_secured'),
        ('used_auto_loan',  'loan_secured'),
        ('mortgage_fixed',  'loan_secured'),
        ('mortgage_arm',    'loan_secured');

    -- Institutions registry
    CREATE TABLE IF NOT EXISTS institutions (
        id                      TEXT PRIMARY KEY,      -- 'fdic:{cert}' or 'ncua:{charter}'
        type                    TEXT NOT NULL,         -- 'bank' | 'cu'
        name                    TEXT NOT NULL,
        charter                 INTEGER,
        state                   TEXT,
        assets_k                INTEGER,               -- assets in thousands
        cbsa_code               TEXT,                  -- Census CBSA code (e.g. '42660')
        cbsa_name               TEXT,                  -- Metro name (e.g. 'Seattle-Tacoma-Bellevue, WA')
        website_url             TEXT,
        rates_url               TEXT,
        loan_rates_url          TEXT,   -- separate URL for loan rates page
        mortgage_rates_url      TEXT,   -- separate URL for mortgage rates page
        url_found_at            TEXT,
        last_scraped_at         TEXT,
        scrape_status           TEXT DEFAULT 'pending',
        raw_section             TEXT,                  -- rate-dense page section stored for LLM
        active                  INTEGER DEFAULT 1,
        discovery_attempted_at  TEXT,                  -- ISO timestamp of last discovery attempt
        discovery_pass          INTEGER DEFAULT 0      -- highest pass completed: 0=never, 1=paths, 2=brave, 3=llm
    );
    CREATE INDEX IF NOT EXISTS idx_inst_type   ON institutions(type);
    CREATE INDEX IF NOT EXISTS idx_inst_state  ON institutions(state);
    CREATE INDEX IF NOT EXISTS idx_inst_assets ON institutions(assets_k DESC);
    CREATE INDEX IF NOT EXISTS idx_inst_cbsa   ON institutions(cbsa_code);

    -- All rates — historical records kept (never overwritten)
    -- Each scrape week creates new rows; old rows stay for trend tracking
    CREATE TABLE IF NOT EXISTS rates (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        institution_id   TEXT NOT NULL REFERENCES institutions(id),
        scraped_at       TEXT NOT NULL,           -- ISO timestamp of this scrape
        scraped_week     TEXT NOT NULL,           -- ISO week 'YYYY-WW' for weekly comparisons
        product          TEXT NOT NULL,
        group_id         TEXT REFERENCES product_groups(group_id),
        term_months      INTEGER,                 -- null for liquid products
        apy              REAL,                    -- decimal (0.05 = 5.00% APY)
        min_balance      REAL,
        notes            TEXT,
        confidence       TEXT DEFAULT 'unverified', -- verified|unverified|rejected
        verified_snippet TEXT,
        -- Loan / mortgage specific fields
        loan_term_label  TEXT,    -- human label: "36Mo New Auto 25k", "5/1 ARM Conforming"
        vehicle_age_years INTEGER, -- auto loans: 0=new, 2=used 2yr, 4=used 4yr
        loan_amount_k    INTEGER,  -- reference loan amount in thousands
        rate_type        TEXT,     -- 'fixed' | 'arm' | 'apr'
        arm_initial_years INTEGER, -- ARMs: initial fixed period in years (3,5,7,10)
        arm_adjust_months INTEGER, -- ARMs: adjustment period in months (12=yearly, 6=6mo)
        conforming       INTEGER   -- 1 if conforming loan
    );
    CREATE INDEX IF NOT EXISTS idx_rates_inst    ON rates(institution_id);
    CREATE INDEX IF NOT EXISTS idx_rates_week    ON rates(scraped_week);
    CREATE INDEX IF NOT EXISTS idx_rates_product ON rates(product);
    CREATE INDEX IF NOT EXISTS idx_rates_group   ON rates(group_id);
    CREATE INDEX IF NOT EXISTS idx_rates_conf    ON rates(confidence);

    -- Scrape log
    CREATE TABLE IF NOT EXISTS scrape_log (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        institution_id TEXT,
        scraped_at     TEXT,
        status         TEXT,
        rates_found    INTEGER DEFAULT 0,
        error_msg      TEXT,
        duration_ms    INTEGER
    );

    -- Week-over-week change view
    -- Compares current week verified rates vs previous week
    CREATE VIEW IF NOT EXISTS rate_changes AS
    SELECT
        curr.institution_id,
        i.name,
        i.type,
        i.state,
        i.assets_k,
        curr.product,
        curr.group_id,
        curr.term_months,
        curr.scraped_week                          AS week,
        curr.apy                                   AS apy_current,
        prev.apy                                   AS apy_previous,
        round((curr.apy - prev.apy) * 100, 4)     AS change_pct_points,
        CASE
            WHEN prev.apy IS NULL            THEN 'new'
            WHEN curr.apy > prev.apy         THEN 'up'
            WHEN curr.apy < prev.apy         THEN 'down'
            ELSE                                  'unchanged'
        END                                        AS direction
    FROM rates curr
    JOIN institutions i ON curr.institution_id = i.id
    LEFT JOIN rates prev
        ON  prev.institution_id = curr.institution_id
        AND prev.product        = curr.product
        AND prev.term_months    IS curr.term_months
        AND prev.scraped_week   = (
            SELECT MAX(r2.scraped_week)
            FROM rates r2
            WHERE r2.institution_id = curr.institution_id
              AND r2.product        = curr.product
              AND r2.term_months    IS curr.term_months
              AND r2.scraped_week   < curr.scraped_week
              AND r2.confidence     = 'verified'
        )
        AND prev.confidence = 'verified'
    WHERE curr.confidence = 'verified';
    """)
    conn.commit()
    conn.close()

def migrate():
    """Add new columns to existing DB without losing data."""
    conn = get_conn()
    c = conn.cursor()
    migrations = [
        "ALTER TABLE institutions ADD COLUMN raw_section TEXT",
        "ALTER TABLE rates ADD COLUMN confidence TEXT DEFAULT 'unverified'",
        "ALTER TABLE rates ADD COLUMN verified_snippet TEXT",
        "ALTER TABLE institutions ADD COLUMN discovery_attempted_at TEXT",
        "ALTER TABLE institutions ADD COLUMN discovery_pass INTEGER DEFAULT 0",
    ]
    for sql in migrations:
        try:
            c.execute(sql)
            conn.commit()
        except Exception:
            pass  # column already exists
    # Add MSA/CBSA columns (added 2026-03-28)
    for sql in [
        "ALTER TABLE institutions ADD COLUMN cbsa_code TEXT",
        "ALTER TABLE institutions ADD COLUMN cbsa_name TEXT",
        "CREATE INDEX IF NOT EXISTS idx_inst_cbsa ON institutions(cbsa_code)",
    ]:
        try:
            c.execute(sql)
            conn.commit()
        except Exception:
            pass  # already exists

    # Add loan/mortgage URL columns to institutions (added 2026-03-28)
    for sql in [
        "ALTER TABLE institutions ADD COLUMN loan_rates_url TEXT",
        "ALTER TABLE institutions ADD COLUMN mortgage_rates_url TEXT",
    ]:
        try:
            c.execute(sql)
            conn.commit()
        except Exception:
            pass  # already exists

    # Add loan/mortgage rate fields to rates table (added 2026-03-28)
    for sql in [
        "ALTER TABLE rates ADD COLUMN loan_term_label TEXT",
        "ALTER TABLE rates ADD COLUMN vehicle_age_years INTEGER",
        "ALTER TABLE rates ADD COLUMN loan_amount_k INTEGER",
        "ALTER TABLE rates ADD COLUMN rate_type TEXT",
        "ALTER TABLE rates ADD COLUMN arm_initial_years INTEGER",
        "ALTER TABLE rates ADD COLUMN arm_adjust_months INTEGER",
        "ALTER TABLE rates ADD COLUMN conforming INTEGER",
        "ALTER TABLE rates ADD COLUMN apr REAL",  # APR as decimal (added 2026-03-28)
    ]:
        try:
            c.execute(sql)
            conn.commit()
        except Exception:
            pass  # already exists

    # Add new loan product types to product_group_map
    for product, group_id in [
        ('new_auto_loan',  'loan_secured'),
        ('used_auto_loan', 'loan_secured'),
        ('mortgage_fixed', 'loan_secured'),
        ('mortgage_arm',   'loan_secured'),
    ]:
        try:
            c.execute("INSERT OR IGNORE INTO product_group_map VALUES (?,?)", (product, group_id))
            conn.commit()
        except Exception:
            pass

    # Remove old columns we no longer use (SQLite: recreate isn't worth it, just ignore)
    conn.close()

if __name__ == '__main__':
    init_db()
    migrate()
    print("✅ Schema ready")
