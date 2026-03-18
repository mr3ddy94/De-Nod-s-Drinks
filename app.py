"""
De-Nod's Wholesale Drinks Manager
A complete POS + Inventory + Reports app for a Ghanaian drinks wholesaler.
"""

import streamlit as st
import sqlite3
import json
import os
import re
import html as _html
from datetime import datetime, date, timedelta
import pandas as pd
import uuid

# ── Cloud DB detection (auto: Supabase on Streamlit Cloud, SQLite offline) ────
try:
    _PG_CFG = st.secrets["supabase"]
    _USE_PG = True
except Exception:
    _PG_CFG = None
    _USE_PG = False

if _USE_PG:
    try:
        import psycopg2
        import psycopg2.extras
        import psycopg2.pool
    except ImportError:
        _USE_PG = False
        _PG_CFG = None


class _CursorWrap:
    """psycopg2 cursor that speaks the sqlite3 cursor API."""
    def __init__(self, cur):
        self._cur = cur
    def execute(self, sql, params=()):
        self._cur.execute(sql.replace('?','%s'), params or ())
        return self
    def executemany(self, sql, params_list):
        self._cur.executemany(sql.replace('?','%s'), params_list)
        return self
    def fetchone(self):
        return self._cur.fetchone()
    def fetchall(self):
        return self._cur.fetchall()
    @property
    def rowcount(self):
        return self._cur.rowcount


class _ConnWrap:
    """psycopg2 connection that speaks the sqlite3 connection API.
    When closed, returns the connection to the pool instead of destroying it."""
    def __init__(self, raw, pool=None):
        self._c   = raw
        self._pool = pool
    def execute(self, sql, params=()):
        cur = self._c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(sql.replace('?','%s'), params or ())
        except Exception:
            self._c.rollback(); raise
        return _CursorWrap(cur)
    def executemany(self, sql, params_list):
        cur = self._c.cursor()
        try:
            cur.executemany(sql.replace('?','%s'), params_list)
        except Exception:
            self._c.rollback(); raise
    def cursor(self):
        return _CursorWrap(self._c.cursor(cursor_factory=psycopg2.extras.RealDictCursor))
    def commit(self):
        self._c.commit()
    def close(self):
        """Return connection to pool (not destroyed — reused next call)."""
        try:
            self._c.commit()
        except Exception:
            try: self._c.rollback()
            except: pass
        if self._pool:
            try:
                self._pool.putconn(self._c)
            except Exception:
                try: self._c.close()
                except: pass
        else:
            try: self._c.close()
            except: pass

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH = "denods.db"

# ── VAT constants (Ghana VAT Act 2025, effective 1 Jan 2026) ─────────────────
VAT_RATE      = 0.15   # 15%
NHIL_RATE     = 0.025  # 2.5%
GETFUND_RATE  = 0.025  # 2.5%
TOTAL_TAX_RATE = VAT_RATE + NHIL_RATE + GETFUND_RATE  # 20%

# ── Security helpers ──────────────────────────────────────────────────────────
SESSION_TIMEOUT_MINS = 60  # auto-logout after 60 minutes of inactivity

def sanitize(text, max_len=500):
    """Strip HTML tags and limit length for safe display."""
    if not isinstance(text, str):
        text = str(text)
    text = _html.escape(text.strip())
    return text[:max_len]

def sanitize_pin(pin):
    """Only digits, 4-6 chars."""
    return re.sub(r'[^0-9]', '', str(pin).strip())[:6]

st.set_page_config(
    page_title="De-Nod's Drinks Manager",
    page_icon="🍺",
    layout="wide",
    initial_sidebar_state="auto"
)

# ── Elder-Friendly CSS ────────────────────────────────────────────────────────
# ── Security headers injected via Streamlit meta (best available in Streamlit) ─
st.markdown("""
<meta http-equiv="X-Content-Type-Options" content="nosniff">
<meta http-equiv="X-Frame-Options" content="DENY">
<meta http-equiv="Referrer-Policy" content="strict-origin-when-cross-origin">
<meta name="robots" content="noindex,nofollow">
""", unsafe_allow_html=True)

st.markdown("""
<style>
  /* No external font imports — fully offline compatible */

  html, body, [class*="css"] {
      font-family: 'Segoe UI', 'Ubuntu', 'Helvetica Neue', Arial, sans-serif !important;
  }

  /* Big readable text throughout */
  .stApp { background: #F5F7FA; }

  h1 { font-size: 2.2rem !important; font-weight: 900 !important; color: #1a2035; }
  h2 { font-size: 1.7rem !important; font-weight: 800 !important; color: #1a2035; }
  h3 { font-size: 1.3rem !important; font-weight: 700 !important; color: #1a2035; }
  p, li, label, .stMarkdown { font-size: 1.1rem !important; }

  /* Sidebar navigation */
  [data-testid="stSidebar"] { min-width: 220px !important; max-width: 240px !important; }
  [data-testid="stSidebar"] .stButton > button {
      font-size: 1rem !important;
      font-weight: 700 !important;
      padding: 0.55rem 0.75rem !important;
      text-align: left !important;
      margin-bottom: 4px !important;
  }

  /* Big buttons */
  .stButton > button {
      font-size: 1.15rem !important;
      font-weight: 800 !important;
      padding: 0.65rem 1.5rem !important;
      border-radius: 10px !important;
      transition: all 0.15s ease !important;
  }
  .stButton > button:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(0,0,0,0.15) !important; }

  /* Inputs */
  .stTextInput > div > div > input,
  .stNumberInput > div > div > input,
  .stSelectbox > div > div { font-size: 1.1rem !important; padding: 0.5rem 0.75rem !important; }

  /* Metric cards */
  [data-testid="stMetric"] { background: white; border-radius: 14px; padding: 1rem 1.25rem; box-shadow: 0 2px 12px rgba(0,0,0,0.07); }
  [data-testid="stMetricLabel"] { font-size: 0.95rem !important; color: #666 !important; font-weight: 700 !important; }
  [data-testid="stMetricValue"] { font-size: 2rem !important; font-weight: 900 !important; color: #1a2035 !important; }

  /* Dataframe */
  [data-testid="stDataFrame"] { font-size: 1rem !important; }

  /* Card boxes */
  .card {
      background: white;
      border-radius: 16px;
      padding: 1.4rem;
      margin-bottom: 1rem;
      box-shadow: 0 2px 14px rgba(0,0,0,0.07);
  }
  .cart-item {
      background: #F0F4FF;
      border-radius: 10px;
      padding: 0.8rem 1rem;
      margin-bottom: 0.5rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 1.05rem;
      font-weight: 700;
  }
  .total-bar {
      background: #1a2035;
      color: white;
      border-radius: 14px;
      padding: 1.2rem 1.5rem;
      font-size: 1.6rem;
      font-weight: 900;
      text-align: center;
      margin: 1rem 0;
  }
  .change-bar {
      background: #2ECC71;
      color: white;
      border-radius: 14px;
      padding: 1rem 1.5rem;
      font-size: 1.4rem;
      font-weight: 900;
      text-align: center;
      margin: 0.5rem 0;
  }
  .change-bar.owed {
      background: #E74C3C;
  }
  .low-stock { background: #FFF3CD; border-left: 5px solid #FFC107; padding: 0.8rem 1rem; border-radius: 8px; margin-bottom: 0.5rem; font-weight: 700; }
  .success-box { background: #D4EDDA; border-left: 5px solid #28A745; padding: 0.8rem 1rem; border-radius: 8px; margin: 0.5rem 0; font-weight: 700; }

  /* Header banner */
  .app-header {
      background: linear-gradient(135deg, #1a2035 0%, #2d3a5e 100%);
      color: white;
      padding: 1.2rem 2rem;
      border-radius: 18px;
      margin-bottom: 1.5rem;
      display: flex;
      align-items: center;
      gap: 1rem;
  }
  .app-header h1 { color: white !important; margin: 0; font-size: 1.9rem !important; }
  .app-header .sub { color: #aabfe8; font-size: 1rem; font-weight: 600; margin: 0; }

  /* Section headings */
  .section-title {
      font-size: 1.4rem;
      font-weight: 800;
      color: #1a2035;
      border-bottom: 3px solid #FF6B2B;
      padding-bottom: 0.4rem;
      margin-bottom: 1rem;
  }
  
  /* Receipt */
  .receipt-container {
      font-family: 'Courier New', monospace;
      max-width: 400px;
      margin: 0 auto;
      background: white;
      padding: 1.5rem;
      border-radius: 14px;
      box-shadow: 0 4px 20px rgba(0,0,0,0.12);
      font-size: 0.95rem;
  }
  
  /* Hide streamlit watermark */
  footer { visibility: hidden; }
  #MainMenu { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ── Database ───────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def _get_pg_pool():
    """Create a persistent connection pool — lives for the app's lifetime.
    min=2 connections always ready, max=5 handles bursts. Reused every call."""
    return psycopg2.pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=5,
        host=str(_PG_CFG["host"]),
        port=int(_PG_CFG.get("port", 5432)),
        dbname=str(_PG_CFG.get("dbname", "postgres")),
        user=str(_PG_CFG["user"]),
        password=str(_PG_CFG["password"]),
        sslmode="require",
        connect_timeout=15,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
    )


def get_db():
    if _USE_PG:
        pool = _get_pg_pool()
        raw  = pool.getconn()
        # Health-check: if the connection went stale, reset it
        try:
            raw.isolation_level  # cheap attribute read
            if raw.closed:
                raise Exception("closed")
        except Exception:
            # Connection is dead — close it and get a fresh one
            try: pool.putconn(raw, close=True)
            except: pass
            raw = pool.getconn()
        raw.autocommit = False
        return _ConnWrap(raw, pool=pool)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-8000")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


# ── Permissions ───────────────────────────────────────────────────────────────
ALL_PERMISSIONS = {
    "can_view_inventory":  "View Inventory & Stock",
    "can_add_product":     "Add New Products",
    "can_change_price":    "Change Prices",
    "can_log_damage":      "Log Damaged Goods",
    "can_view_reports":    "View Reports",
    "can_void_sale":       "Void / Delete a Sale",
    "can_manage_users":    "Manage Users & PINs",
}
DEFAULT_STAFF_PERMISSIONS = {
    "can_view_inventory": True,
    "can_add_product":    False,
    "can_change_price":   False,
    "can_log_damage":     True,
    "can_view_reports":   False,
    "can_void_sale":      False,
    "can_manage_users":   False,
}
ADMIN_PERMISSIONS = {k: True for k in ALL_PERMISSIONS}


def _run_migrations(c):
    """Idempotent DB migrations — safe on both SQLite and PostgreSQL."""
    # Check existing columns (different syntax per backend)
    if _USE_PG:
        cols = [r['column_name'] for r in c.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='users'"
        ).fetchall()]
    else:
        cols = [r[1] for r in c.execute("PRAGMA table_info(users)").fetchall()]

    if "role" not in cols:
        c.execute("ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'staff'")
    if "permissions" not in cols:
        c.execute("ALTER TABLE users ADD COLUMN permissions TEXT DEFAULT '{}'")

    # Promote first user to admin
    c.execute("""UPDATE users SET role='admin', permissions=?
                 WHERE id=(SELECT MIN(id) FROM users)
                 AND (role IS NULL OR role='staff' OR role='')""",
              (json.dumps(ADMIN_PERMISSIONS),))

    # Default permissions for other users with empty perms
    for row in c.execute(
        "SELECT id, permissions FROM users WHERE id!=(SELECT MIN(id) FROM users)"
    ).fetchall():
        uid  = row['id']
        praw = row['permissions']
        try:
            existing = json.loads(praw) if praw and praw.strip() not in ("", "{}") else {}
        except Exception:
            existing = {}
        if not existing:
            c.execute("UPDATE users SET permissions=? WHERE id=?",
                      (json.dumps(DEFAULT_STAFF_PERMISSIONS), uid))

    # Upsert defaults (ON CONFLICT syntax works in both SQLite 3.24+ and PostgreSQL)
    c.execute("INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT (key) DO NOTHING",
              ("revenue_reset_date", ""))
    c.execute("INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT (key) DO NOTHING",
              ("pin_attempts", "0"))
    c.execute("INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT (key) DO NOTHING",
              ("lockout_until", ""))

    # Indexes (same syntax in both)
    c.execute("CREATE INDEX IF NOT EXISTS idx_products_name   ON products(name)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_products_active ON products(active)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sales_date      ON sales(sale_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_damage_date     ON damaged_goods(date_logged)")

    # Audit log table
    _pk = "SERIAL PRIMARY KEY" if _USE_PG else "INTEGER PRIMARY KEY AUTOINCREMENT"
    c.execute(f"""CREATE TABLE IF NOT EXISTS audit_log (
        id {_pk},
        log_time TEXT,
        user_name TEXT,
        action TEXT,
        detail TEXT
    )""")


def init_db():
    conn = get_db()
    c = conn.cursor()
    _pk = "SERIAL PRIMARY KEY" if _USE_PG else "INTEGER PRIMARY KEY AUTOINCREMENT"

    c.execute(f'''CREATE TABLE IF NOT EXISTS products (
        id {_pk},
        name TEXT NOT NULL,
        category TEXT DEFAULT '',
        size TEXT DEFAULT '',
        buy_price REAL DEFAULT 0,
        sell_price REAL DEFAULT 0,
        stock_qty INTEGER DEFAULT 0,
        low_stock_alert INTEGER DEFAULT 10,
        active INTEGER DEFAULT 1
    )''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS sales (
        id {_pk},
        receipt_no TEXT UNIQUE,
        sale_date TEXT,
        sale_time TEXT,
        items_json TEXT,
        subtotal REAL DEFAULT 0,
        amount_paid REAL DEFAULT 0,
        change_given REAL DEFAULT 0,
        customer_name TEXT DEFAULT '',
        notes TEXT DEFAULT ''
    )''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS damaged_goods (
        id {_pk},
        product_id INTEGER DEFAULT 0,
        product_name TEXT,
        size TEXT DEFAULT '',
        qty INTEGER DEFAULT 1,
        reason TEXT DEFAULT '',
        date_logged TEXT,
        unit_cost REAL DEFAULT 0,
        total_loss REAL DEFAULT 0
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS users (
        id {_pk},
        name TEXT NOT NULL,
        pin TEXT NOT NULL,
        active INTEGER DEFAULT 1,
        role TEXT DEFAULT 'staff',
        permissions TEXT DEFAULT \'{{}}\'
    )''')

    # ON CONFLICT syntax works in both SQLite 3.24+ and PostgreSQL
    defaults = {
        'shop_name':          "De-Nod's Wholesale Drinks",
        'shop_address':       'Ghana',
        'shop_phone':         '',
        'shop_email':         '',
        'receipt_footer':     'Thank you for your business! Come again soon.',
        'currency':           'GHS',
        'revenue_reset_date': '',
    }
    for k, v in defaults.items():
        c.execute("INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT (key) DO NOTHING", (k, v))

    row = c.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()
    existing_users = row['cnt'] if isinstance(row, dict) else row[0]
    if existing_users == 0:
        c.execute("INSERT INTO users (name, pin, role, permissions) VALUES (?,?,?,?)",
                  ("Stephen Acquah", "1234", "admin", json.dumps(ADMIN_PERMISSIONS)))
        c.execute("INSERT INTO users (name, pin, role, permissions) VALUES (?,?,?,?)",
                  ("Staff", "5678", "staff", json.dumps(DEFAULT_STAFF_PERMISSIONS)))

    _run_migrations(c)
    conn.commit()

    row2 = c.execute("SELECT COUNT(*) AS cnt FROM products").fetchone()
    count = row2['cnt'] if isinstance(row2, dict) else row2[0]
    if count == 0:
        _seed_products(c)
        conn.commit()

    conn.close()


@st.cache_data(ttl=600, show_spinner=False)
def get_setting(key, default=''):
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    if row is None: return default
    return row['value'] if isinstance(row, dict) else row[0]


def set_setting(key, value):
    conn = get_db()
    conn.execute(
        "INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
        (key, value)
    )
    conn.commit()
    conn.close()
    get_setting.clear()   # bust the cache so next read is fresh


# ── Auth helpers ───────────────────────────────────────────────────────────────
def check_pin(pin_entered):
    """Return full user dict if PIN matches an active user, else None."""
    conn = get_db()
    row = conn.execute(
        "SELECT id, name, role, permissions FROM users WHERE pin=? AND active=1",
        (pin_entered.strip(),)
    ).fetchone()
    conn.close()
    if not row:
        return None
    try:
        perms = json.loads(row['permissions']) if row['permissions'] else {}
    except Exception:
        perms = {}
    if row['role'] == "admin":
        perms = ADMIN_PERMISSIONS.copy()
    return {"id": row['id'], "name": row['name'], "role": row['role'], "permissions": perms}


def get_all_users():
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, pin, active, role, permissions FROM users ORDER BY id"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        try:
            perms = json.loads(r['permissions']) if r['permissions'] else {}
        except Exception:
            perms = {}
        result.append({"id": r['id'], "name": r['name'], "pin": r['pin'],
                        "active": r['active'], "role": r['role'], "permissions": perms})
    return result


def has_perm(perm_key):
    """True if logged-in user has permission or is admin."""
    if st.session_state.get("is_admin", False):
        return True
    return st.session_state.get("permissions", {}).get(perm_key, False)


def _row_val(row, idx=0):
    """Get first column value from a DB row — works for both SQLite and PG."""
    if row is None: return 0
    if isinstance(row, dict): return list(row.values())[idx]
    return row[idx]


def audit(action, detail=""):
    """Write a timestamped audit entry. Never crashes the app."""
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO audit_log (log_time, user_name, action, detail) VALUES (?,?,?,?)",
            (datetime.now().isoformat(timespec='seconds'),
             st.session_state.get("logged_in_user", "unknown"),
             sanitize(action, 100), sanitize(detail, 300))
        )
        conn.commit()
    except Exception:
        pass
    finally:
        try: conn.close()
        except: pass


def sync_new_products():
    """Insert any seed products that don't yet exist in the DB (matched by name+size)."""
    conn = get_db()
    c = conn.cursor()
    existing = set(
        (r['name'].strip().lower(), r['size'].strip().lower())
        for r in c.execute("SELECT name, size FROM products").fetchall()
    )
    inserted = 0
    for row in _get_seed_list():
        key = (row[0].strip().lower(), row[2].strip().lower())
        if key not in existing:
            c.execute(
                "INSERT INTO products (name, category, size, buy_price, sell_price, stock_qty, low_stock_alert) VALUES (?,?,?,?,?,?,?)",
                (row[0], row[1], row[2], 0, row[4], row[5], row[6])
            )
            inserted += 1
    conn.commit()
    conn.close()
    return inserted


def _get_seed_list():
    """Master product list. Size = what is sold as one unit (carton/crate/box).
       Price = wholesale price per carton/crate/box in GHS."""
    return [
        # (name, category, size, buy_price_unused, wholesale_price_per_carton, stock_qty, low_stock_alert)

        # ── BEERS ─────────────────────────────────────────────────────────────
        ("Club Beer",                    "Beer", "Crate of 24 x 330ml Bottles",  0, 185,  10, 2),
        ("Club Beer",                    "Beer", "Crate of 12 x 500ml Bottles",  0, 145,   8, 2),
        ("Star Beer",                    "Beer", "Crate of 24 x 330ml Bottles",  0, 185,  10, 2),
        ("Star Beer",                    "Beer", "Crate of 12 x 500ml Bottles",  0, 145,   8, 2),
        ("Guinness Foreign Extra Stout", "Beer", "Crate of 24 x 330ml Bottles",  0, 220,   8, 2),
        ("Guinness Foreign Extra Stout", "Beer", "Pack of 12 x 500ml Cans",      0, 180,   6, 2),
        ("Stone Strong Lager",           "Beer", "Crate of 24 x 330ml Bottles",  0, 160,  10, 2),
        ("Eagle Lager",                  "Beer", "Crate of 24 x 330ml Bottles",  0, 150,  10, 2),
        ("Trophy Lager",                 "Beer", "Crate of 12 x 600ml Bottles",  0, 150,   8, 2),
        ("Club Shandy",                  "Beer", "Pack of 24 x 330ml Cans",      0, 200,   6, 2),
        ("Orijin Beer",                  "Beer", "Pack of 24 x 330ml Cans",      0, 230,   6, 2),
        ("Legend Extra Stout",           "Beer", "Crate of 12 x 600ml Bottles",  0, 150,   6, 2),
        ("Heineken",                     "Beer", "Pack of 24 x 330ml Bottles",   0, 380,   4, 1),
        ("Corona Extra",                 "Beer", "Pack of 24 x 330ml Bottles",   0, 480,   4, 1),
        ("Stella Artois",                "Beer", "Pack of 24 x 330ml Bottles",   0, 420,   4, 1),
        ("Budweiser",                    "Beer", "Pack of 24 x 330ml Cans",      0, 420,   4, 1),

        # ── MALTA GUINNESS RANGE ──────────────────────────────────────────────
        ("Malta Guinness",               "Malt Drink", "Crate of 24 x 330ml Bottles", 0, 175,  10, 2),
        ("Malta Guinness",               "Malt Drink", "Pack of 24 x 330ml Cans",     0, 175,  10, 2),
        ("Malta Guinness",               "Malt Drink", "Crate of 12 x 500ml Bottles", 0, 130,   6, 2),
        ("Malta Guinness Cocoa",         "Malt Drink", "Pack of 24 x 330ml Cans",     0, 190,   6, 2),
        ("Malta Guinness Extra",         "Malt Drink", "Crate of 24 x 330ml Bottles", 0, 190,   6, 2),
        ("Beta Malt",                    "Malt Drink", "Pack of 24 x 330ml Cans",     0, 150,  10, 2),
        ("Alvaro Pear",                  "Malt Drink", "Crate of 24 x 330ml Bottles", 0, 190,   8, 2),
        ("Alvaro Apple",                 "Malt Drink", "Crate of 24 x 330ml Bottles", 0, 190,   8, 2),
        ("Schweppes Malt",               "Malt Drink", "Pack of 24 x 330ml Cans",     0, 170,   6, 2),
        ("Hollandia Malt",               "Malt Drink", "Pack of 24 x 330ml Cans",     0, 170,   6, 2),

        # ── COCA-COLA RANGE ───────────────────────────────────────────────────
        ("Coca-Cola",             "Soft Drink", "Crate of 24 x 200ml Bottles",  0,  80,  10, 2),
        ("Coca-Cola",             "Soft Drink", "Crate of 24 x 300ml Bottles",  0, 100,  15, 3),
        ("Coca-Cola",             "Soft Drink", "Crate of 24 x 500ml Bottles",  0, 170,  10, 2),
        ("Coca-Cola",             "Soft Drink", "Pack of 12 x 1L Bottles",      0, 145,   8, 2),
        ("Coca-Cola",             "Soft Drink", "Pack of 12 x 1.5L Bottles",    0, 185,   6, 2),
        ("Coca-Cola",             "Soft Drink", "Pack of 6 x 2L Bottles",       0, 110,   4, 1),
        ("Coca-Cola",             "Soft Drink", "Pack of 24 x 330ml Cans",      0, 175,   8, 2),
        ("Coca-Cola Zero Sugar",  "Soft Drink", "Pack of 24 x 330ml Cans",      0, 175,   6, 2),
        ("Coca-Cola Zero Sugar",  "Soft Drink", "Crate of 24 x 500ml Bottles",  0, 185,   6, 2),
        ("Diet Coke",             "Soft Drink", "Pack of 24 x 330ml Cans",      0, 175,   4, 1),
        ("Fanta Orange",          "Soft Drink", "Crate of 24 x 200ml Bottles",  0,  80,  10, 2),
        ("Fanta Orange",          "Soft Drink", "Crate of 24 x 300ml Bottles",  0, 100,  15, 3),
        ("Fanta Orange",          "Soft Drink", "Crate of 24 x 500ml Bottles",  0, 170,  10, 2),
        ("Fanta Orange",          "Soft Drink", "Pack of 12 x 1.5L Bottles",    0, 185,   6, 2),
        ("Fanta Orange",          "Soft Drink", "Pack of 24 x 330ml Cans",      0, 175,   8, 2),
        ("Fanta Pineapple",       "Soft Drink", "Crate of 24 x 300ml Bottles",  0, 100,  10, 2),
        ("Fanta Pineapple",       "Soft Drink", "Crate of 24 x 500ml Bottles",  0, 170,   8, 2),
        ("Fanta Pineapple",       "Soft Drink", "Pack of 24 x 330ml Cans",      0, 175,   6, 2),
        ("Fanta Strawberry",      "Soft Drink", "Pack of 24 x 330ml Cans",      0, 175,   6, 2),
        ("Fanta Grape",           "Soft Drink", "Pack of 24 x 330ml Cans",      0, 175,   6, 2),
        ("Fanta Lemon",           "Soft Drink", "Pack of 24 x 330ml Cans",      0, 175,   6, 2),
        ("Sprite",                "Soft Drink", "Crate of 24 x 200ml Bottles",  0,  80,  10, 2),
        ("Sprite",                "Soft Drink", "Crate of 24 x 300ml Bottles",  0, 100,  10, 2),
        ("Sprite",                "Soft Drink", "Crate of 24 x 500ml Bottles",  0, 170,  10, 2),
        ("Sprite",                "Soft Drink", "Pack of 12 x 1.5L Bottles",    0, 185,   6, 2),
        ("Sprite",                "Soft Drink", "Pack of 24 x 330ml Cans",      0, 175,   8, 2),
        ("Schweppes Ginger Ale",  "Soft Drink", "Pack of 24 x 330ml Cans",      0, 155,   6, 2),
        ("Schweppes Tonic Water", "Soft Drink", "Pack of 24 x 330ml Cans",      0, 155,   6, 2),
        ("Schweppes Lemon",       "Soft Drink", "Pack of 24 x 330ml Cans",      0, 155,   6, 2),
        ("Schweppes Chapman",     "Soft Drink", "Pack of 24 x 330ml Cans",      0, 155,   6, 2),
        ("Bonaqua Still Water",   "Water",      "Crate of 24 x 500ml Bottles",  0,  85,   8, 2),
        ("Bonaqua Still Water",   "Water",      "Pack of 12 x 1.5L Bottles",    0,  90,   6, 2),

        # ── BEL-COLA RANGE ────────────────────────────────────────────────────
        ("Bel-Cola Classic",   "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  90,  10, 2),
        ("Bel-Cola Classic",   "Soft Drink", "Pack of 24 x 330ml Cans",      0,  95,   8, 2),
        ("Bel-Cola Orange",    "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  90,   8, 2),
        ("Bel-Cola Pineapple", "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  90,   8, 2),
        ("Bel-Cola Diet",      "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  90,   6, 2),
        ("Bel-Cola Classic",   "Soft Drink", "Crate of 24 x 500ml Bottles",  0, 130,   8, 2),
        ("Bel-Aqua Water",     "Water",      "Crate of 24 x 500ml Bottles",  0,  70,  10, 2),
        ("Bel-Aqua Water",     "Water",      "Pack of 12 x 1.5L Bottles",    0,  75,   8, 2),

        # ── OTHER SOFT DRINKS ─────────────────────────────────────────────────
        ("Pepsi",              "Soft Drink", "Crate of 24 x 300ml Bottles",  0,  90,   8, 2),
        ("Pepsi",              "Soft Drink", "Pack of 24 x 330ml Cans",      0, 155,   6, 2),
        ("7UP",                "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  95,   8, 2),
        ("7UP",                "Soft Drink", "Pack of 24 x 330ml Cans",      0, 155,   6, 2),
        ("Mirinda Orange",     "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  90,   8, 2),
        ("Mirinda Strawberry", "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  90,   6, 2),
        ("Vimto",              "Soft Drink", "Pack of 24 x 250ml Bottles",   0,  95,   6, 2),
        ("Tropical Splash",    "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  75,   8, 2),
        ("Bigoo Cola",         "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  70,   8, 2),
        ("Bigoo Orange",       "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  70,   8, 2),
        ("Bigoo Lime",         "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  70,   6, 2),
        ("Bigoo Cocktail",     "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  70,   6, 2),

        # ── SPECIAL ICE RANGE ─────────────────────────────────────────────────
        ("Special Ice Cola",       "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  75,  10, 2),
        ("Special Ice Orange",     "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  75,  10, 2),
        ("Special Ice Pineapple",  "Soft Drink", "Crate of 24 x 350ml Bottles",  0,  75,  10, 2),
        ("Special Ice Cola",       "Soft Drink", "Crate of 24 x 500ml Bottles",  0, 105,   8, 2),
        ("Special Ice Orange",     "Soft Drink", "Crate of 24 x 500ml Bottles",  0, 105,   8, 2),
        ("Special Ice Water",      "Water",      "Crate of 24 x 500ml Bottles",  0,  70,  10, 2),
        ("Special Ice Water",      "Water",      "Pack of 12 x 1.5L Bottles",    0,  72,   8, 2),
        ("Special Ice Water",      "Water",      "Pack of 6 x 5L Bottles",       0,  80,   6, 2),
        ("Special Ice Water",      "Water",      "18.5L Refill Jar",             0,  45,  10, 3),

        # ── KALYPPO RANGE (Aquafresh) ─────────────────────────────────────────
        ("Kalyppo Pineapple",  "Juice", "Carton of 40 x 250ml Packs",  0,  65,  20, 5),
        ("Kalyppo Orange",     "Juice", "Carton of 40 x 250ml Packs",  0,  65,  20, 5),
        ("Kalyppo Multifruit", "Juice", "Carton of 40 x 250ml Packs",  0,  65,  20, 5),
        ("Kalyppo Apple",      "Juice", "Carton of 40 x 250ml Packs",  0,  65,  20, 5),
        ("Kalyppo Fruitimix",  "Juice", "Carton of 40 x 250ml Packs",  0,  65,  15, 5),
        ("Kalyppo Cocopine",   "Juice", "Carton of 40 x 250ml Packs",  0,  65,  15, 5),
        ("Kalyppo Oranpine",   "Juice", "Carton of 40 x 250ml Packs",  0,  65,  15, 5),
        ("Kalyppo Tangerine",  "Juice", "Carton of 40 x 250ml Packs",  0,  65,  15, 5),

        # ── FRUTELLI RANGE (Aquafresh) ────────────────────────────────────────
        ("Frutelli Mango",      "Juice", "Carton of 12 x 1L Packs",  0, 120,  10, 3),
        ("Frutelli Multifruit", "Juice", "Carton of 12 x 1L Packs",  0, 120,  10, 3),
        ("Frutelli Tropic Mix", "Juice", "Carton of 12 x 1L Packs",  0, 120,  10, 3),
        ("Frutelli Pineapple",  "Juice", "Carton of 12 x 1L Packs",  0, 120,  10, 3),
        ("Frutelli Orange",     "Juice", "Carton of 12 x 1L Packs",  0, 120,  10, 3),

        # ── FANICE / FANMILK RANGE ────────────────────────────────────────────
        ("FanIce Vanilla",               "Dairy Drink", "Box of 60 x 120ml Pouches",  0, 120,  5, 2),
        ("FanIce Strawberry",            "Dairy Drink", "Box of 60 x 120ml Pouches",  0, 120,  5, 2),
        ("FanIce Chocolate",             "Dairy Drink", "Box of 60 x 120ml Pouches",  0, 120,  5, 2),
        ("FanIce American Vanilla",      "Dairy Drink", "Box of 60 x 120ml Pouches",  0, 120,  5, 2),
        ("FanIce Banana",                "Dairy Drink", "Box of 60 x 120ml Pouches",  0, 120,  5, 2),
        ("FanIce Vanilla",               "Dairy Drink", "Box of 12 x 500ml Tubs",     0, 280,  3, 1),
        ("FanIce Strawberry",            "Dairy Drink", "Box of 12 x 500ml Tubs",     0, 280,  3, 1),
        ("FanIce Chocolate",             "Dairy Drink", "Box of 12 x 500ml Tubs",     0, 280,  3, 1),
        ("FanIce 2-in-1 Straw/Vanilla",  "Dairy Drink", "Box of 6 x 2L Tubs",        0, 260,  2, 1),
        ("FanIce 2-in-1 Choc/Vanilla",   "Dairy Drink", "Box of 6 x 2L Tubs",        0, 260,  2, 1),
        ("FanYogo Strawberry",           "Dairy Drink", "Box of 60 x 120ml Pouches",  0, 110,  5, 2),
        ("FanYogo Vanilla",              "Dairy Drink", "Box of 60 x 120ml Pouches",  0, 110,  5, 2),
        ("FanChoco Chocolate Drink",     "Dairy Drink", "Box of 60 x 120ml Pouches",  0, 110,  5, 2),
        ("FanDango Pineapple",           "Juice",       "Box of 60 x 120ml Pouches",  0, 105,  5, 2),
        ("FanDango Orange",              "Juice",       "Box of 60 x 120ml Pouches",  0, 105,  5, 2),
        ("FanPop Lolly",                 "Juice",       "Box of 60 Sticks",           0,  90,  5, 2),
        ("FanMaxx Vanilla",              "Dairy Drink", "Box of 60 x 120ml Pouches",  0, 150,  5, 2),

        # ── OTHER JUICES ──────────────────────────────────────────────────────
        ("Tampico Orange",           "Juice", "Carton of 12 x 500ml Bottles",  0,  80,  8, 2),
        ("Tampico Citrus Punch",     "Juice", "Carton of 12 x 500ml Bottles",  0,  80,  8, 2),
        ("Tampico Pineapple",        "Juice", "Carton of 12 x 500ml Bottles",  0,  80,  8, 2),
        ("Chivita Active Orange",    "Juice", "Carton of 24 x 350ml Packs",    0, 170,  8, 2),
        ("Chivita Active Pineapple", "Juice", "Carton of 24 x 350ml Packs",    0, 170,  6, 2),
        ("Chivita Active Mango",     "Juice", "Carton of 24 x 350ml Packs",    0, 170,  6, 2),
        ("Five Alive Citrus",        "Juice", "Carton of 24 x 350ml Packs",    0, 170,  8, 2),
        ("Five Alive Berry",         "Juice", "Carton of 24 x 350ml Packs",    0, 170,  6, 2),
        ("Minute Maid Pulpy Orange", "Juice", "Pack of 24 x 330ml Cans",       0, 175,  6, 2),
        ("Minute Maid Orange",       "Juice", "Carton of 12 x 1L Packs",       0, 145,  6, 2),
        ("Rani Float Orange",        "Juice", "Pack of 24 x 240ml Cans",       0, 185,  6, 2),
        ("Rani Float Mango",         "Juice", "Pack of 24 x 240ml Cans",       0, 185,  6, 2),
        ("Blue Skies Pineapple",     "Juice", "Carton of 12 x 1L Packs",       0, 195,  4, 1),
        ("Blue Skies Mango",         "Juice", "Carton of 12 x 1L Packs",       0, 195,  4, 1),
        ("Juice Up Orange",          "Juice", "Carton of 12 x 500ml Bottles",  0,  90,  6, 2),
        ("Juice Up Mango",           "Juice", "Carton of 12 x 500ml Bottles",  0,  90,  6, 2),
        ("Hollandia Yoghurt Drink",  "Dairy Drink", "Carton of 12 x 500ml Packs",  0, 115,  6, 2),
        ("Hollandia Yoghurt Drink",  "Dairy Drink", "Carton of 12 x 1L Packs",     0, 200,  4, 1),

        # ── WATER ─────────────────────────────────────────────────────────────
        ("Voltic Water",  "Water", "Crate of 24 x 500ml Bottles",  0,  75,  20, 4),
        ("Voltic Water",  "Water", "Pack of 12 x 1.5L Bottles",    0,  80,  10, 2),
        ("Voltic Water",  "Water", "Pack of 6 x 5L Bottles",       0,  80,   6, 2),
        ("Voltic Water",  "Water", "Pack of 24 x 330ml Cans",      0,  90,   8, 2),
        ("Verna Water",   "Water", "Crate of 24 x 500ml Bottles",  0,  85,  20, 4),
        ("Verna Water",   "Water", "Pack of 12 x 1.5L Bottles",    0,  90,  10, 2),
        ("Verna Water",   "Water", "Pack of 6 x 5L Bottles",       0,  90,   6, 2),
        ("Sky Water",     "Water", "Crate of 24 x 500ml Bottles",  0,  70,  10, 2),
        ("Sky Water",     "Water", "Pack of 12 x 1.5L Bottles",    0,  72,   8, 2),
        ("Sachet Water",  "Water", "Bag of 30 Sachets",            0,   5, 100, 20),

        # ── ENERGY DRINKS ─────────────────────────────────────────────────────
        ("Lucozade Energy Original",    "Energy Drink", "Carton of 12 x 380ml Bottles",  0, 120,  8, 2),
        ("Lucozade Energy Orange",      "Energy Drink", "Carton of 12 x 380ml Bottles",  0, 120,  8, 2),
        ("Lucozade Energy Watermelon",  "Energy Drink", "Carton of 12 x 380ml Bottles",  0, 120,  6, 2),
        ("Lucozade Sport Orange",       "Energy Drink", "Carton of 12 x 500ml Bottles",  0, 130,  6, 2),
        ("Lucozade Sport Mango",        "Energy Drink", "Carton of 12 x 500ml Bottles",  0, 130,  6, 2),
        ("Rush Energy Drink",           "Energy Drink", "Pack of 24 x 250ml Cans",       0, 175,  6, 2),
        ("Rush Energy Drink",           "Energy Drink", "Pack of 12 x 500ml Cans",       0, 150,  6, 2),
        ("Predator Energy",             "Energy Drink", "Pack of 24 x 250ml Cans",       0, 170,  6, 2),
        ("Power Horse",                 "Energy Drink", "Pack of 24 x 250ml Cans",       0, 200,  4, 1),
        ("Burn Energy",                 "Energy Drink", "Pack of 24 x 250ml Cans",       0, 240,  4, 1),
        ("Monster Energy Original",     "Energy Drink", "Pack of 12 x 500ml Cans",       0, 260,  3, 1),
        ("Monster Energy Green",        "Energy Drink", "Pack of 12 x 500ml Cans",       0, 260,  3, 1),

        # ── SPIRITS ───────────────────────────────────────────────────────────
        ("Smirnoff Vodka",              "Spirit", "Carton of 24 x 200ml Bottles",  0,  400,  3, 1),
        ("Smirnoff Vodka",              "Spirit", "Carton of 12 x 750ml Bottles",  0,  650,  2, 1),
        ("Smirnoff Vodka",              "Spirit", "Carton of 12 x 1L Bottles",     0,  850,  2, 1),
        ("Alomo Bitters (Kasapreko)",   "Spirit", "Carton of 24 x 200ml Bottles",  0,  300,  4, 1),
        ("Alomo Bitters (Kasapreko)",   "Spirit", "Carton of 12 x 750ml Bottles",  0,  460,  3, 1),
        ("Orijin Spirit",               "Spirit", "Carton of 24 x 200ml Bottles",  0,  340,  4, 1),
        ("Orijin Spirit",               "Spirit", "Carton of 12 x 750ml Bottles",  0,  530,  3, 1),
        ("Akpeteshie (Local Gin)",      "Spirit", "Carton of 24 x 200ml Bottles",  0,  190,  6, 2),
        ("Akpeteshie (Local Gin)",      "Spirit", "Carton of 12 x 750ml Bottles",  0,  290,  4, 1),
        ("GIHOC Akpeteshie",            "Spirit", "Carton of 12 x 750ml Bottles",  0,  340,  4, 1),
        ("Zola Bitters",                "Spirit", "Carton of 24 x 200ml Bottles",  0,  240,  4, 1),
        ("Kasapreko Herb Liqueur",      "Spirit", "Carton of 24 x 200ml Bottles",  0,  280,  4, 1),
        ("Kasapreko Herb Liqueur",      "Spirit", "Carton of 12 x 750ml Bottles",  0,  420,  3, 1),
        ("Richot Brandy",               "Spirit", "Carton of 12 x 750ml Bottles",  0,  780,  2, 1),
        ("Johnnie Walker Red Label",    "Whisky", "Carton of 12 x 750ml Bottles",  0, 1650,  1, 1),
        ("Johnnie Walker Black Label",  "Whisky", "Carton of 12 x 750ml Bottles",  0, 2600,  1, 1),
        ("Jameson Whiskey",             "Whisky", "Carton of 12 x 750ml Bottles",  0, 2100,  1, 1),
        ("Baileys Irish Cream",         "Spirit", "Carton of 12 x 750ml Bottles",  0, 1700,  1, 1),
        ("Hennessy VS",                 "Spirit", "Carton of 12 x 750ml Bottles",  0, 4200,  1, 1),
        ("Jack Daniel's",               "Whisky", "Carton of 12 x 750ml Bottles",  0, 2800,  1, 1),
        ("Gordon's Gin",                "Spirit", "Carton of 12 x 750ml Bottles",  0, 1200,  2, 1),
        ("Gordon's Pink Gin",           "Spirit", "Carton of 12 x 750ml Bottles",  0, 1250,  2, 1),

        # ── WINE ──────────────────────────────────────────────────────────────
        ("Four Cousins Sweet Red",   "Wine",         "Carton of 6 x 750ml Bottles",  0,  260,  4, 1),
        ("Four Cousins Sweet White", "Wine",         "Carton of 6 x 750ml Bottles",  0,  260,  4, 1),
        ("Four Cousins Sweet Rose",  "Wine",         "Carton of 6 x 750ml Bottles",  0,  260,  4, 1),
        ("Robertson Winery Red",     "Wine",         "Carton of 6 x 750ml Bottles",  0,  300,  3, 1),
        ("Robertson Winery White",   "Wine",         "Carton of 6 x 750ml Bottles",  0,  300,  3, 1),
        ("Amarula Cream",            "Wine/Liqueur", "Carton of 6 x 750ml Bottles",  0,  580,  2, 1),

        # ── LOCAL / TRADITIONAL DRINKS ────────────────────────────────────────
        ("Sobolo (Hibiscus Drink)", "Local Drink", "Carton of 12 x 500ml Bottles",  0,  70,  6, 2),
        ("Sobolo (Hibiscus Drink)", "Local Drink", "Carton of 12 x 1L Bottles",     0, 110,  4, 1),
        ("Asaana (Corn Drink)",     "Local Drink", "Carton of 12 x 500ml Bottles",  0,  65,  4, 1),
        ("Ginger Beer (Local)",     "Local Drink", "Carton of 12 x 500ml Bottles",  0,  65,  4, 1),
        ("Palm Wine",               "Local Drink", "Carton of 12 x 750ml Bottles",  0, 130,  2, 1),
    ]


def _seed_products(c):
    c.executemany(
        "INSERT INTO products (name, category, size, buy_price, sell_price, stock_qty, low_stock_alert) VALUES (?,?,?,?,?,?,?)",
        _get_seed_list()
    )

# ── Helper Functions ───────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner=False)
def search_products(query):
    conn = get_db()
    q = f"%{query.lower()}%"
    rows = conn.execute(
        "SELECT * FROM products WHERE active=1 AND (LOWER(name) LIKE ? OR LOWER(category) LIKE ? OR LOWER(size) LIKE ?) ORDER BY name, size",
        (q, q, q)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@st.cache_data(ttl=600, show_spinner=False)
def get_all_products():
    conn = get_db()
    rows = conn.execute("SELECT * FROM products WHERE active=1 ORDER BY category, name, size").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_product_by_id(pid):
    conn = get_db()
    row = conn.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    conn.close()
    return dict(row) if row else None


def update_stock(pid, delta):
    conn = get_db()
    conn.execute("UPDATE products SET stock_qty = stock_qty + ? WHERE id=?", (delta, pid))
    conn.commit()
    conn.close()


@st.cache_data(ttl=600, show_spinner=False)
def get_low_stock_products():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM products WHERE active=1 AND stock_qty <= low_stock_alert ORDER BY stock_qty ASC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def record_sale(items, subtotal, amount_paid, change_given, customer_name='', notes=''):
    receipt_no = f"DN-{datetime.now().strftime('%Y%m%d')}-{str(uuid.uuid4())[:4].upper()}"
    now = datetime.now()
    conn = get_db()
    conn.execute(
        "INSERT INTO sales (receipt_no, sale_date, sale_time, items_json, subtotal, amount_paid, change_given, customer_name, notes) VALUES (?,?,?,?,?,?,?,?,?)",
        (
            receipt_no,
            now.strftime("%Y-%m-%d"),
            now.strftime("%H:%M:%S"),
            json.dumps(items),
            subtotal, amount_paid, change_given,
            customer_name, notes
        )
    )
    conn.commit()
    conn.close()
    # Deduct stock
    for item in items:
        update_stock(item['product_id'], -item['qty'])
    return receipt_no


def log_damage(product_id, product_name, size, qty, reason, unit_cost):
    total_loss = unit_cost * qty
    conn = get_db()
    conn.execute(
        "INSERT INTO damaged_goods (product_id, product_name, size, qty, reason, date_logged, unit_cost, total_loss) VALUES (?,?,?,?,?,?,?,?)",
        (product_id, product_name, size, qty, reason, date.today().isoformat(), unit_cost, total_loss)
    )
    conn.commit()
    conn.close()
    get_damaged_in_range.clear()
    # Deduct from stock
    if product_id:
        update_stock(product_id, -qty)


@st.cache_data(ttl=600, show_spinner=False)
def get_sales_in_range(start_date, end_date):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM sales WHERE sale_date BETWEEN ? AND ? ORDER BY sale_date DESC, sale_time DESC",
        (start_date.isoformat(), end_date.isoformat())
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@st.cache_data(ttl=600, show_spinner=False)
def get_damaged_in_range(start_date, end_date):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM damaged_goods WHERE date_logged BETWEEN ? AND ? ORDER BY date_logged DESC",
        (start_date.isoformat(), end_date.isoformat())
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def generate_receipt_html(receipt_no, items, subtotal, amount_paid, change_given,
                          customer_name='', auto_print=False):
    """Generate a thermal-printer-ready receipt (optimised for 80mm roll paper).
    Set auto_print=True to fire the print dialog automatically on load."""
    shop_name = get_setting('shop_name', "De-Nod's Wholesale Drinks")
    shop_address = get_setting('shop_address', 'Ghana')
    shop_phone = get_setting('shop_phone', '')
    footer = get_setting('receipt_footer', 'Thank you for your business!')
    currency = get_setting('currency', 'GHS')
    now = datetime.now()

    # Build item rows — wrap long names so they fit 80mm paper
    lines = ""
    for item in items:
        # Truncate name+size to avoid overflow on narrow paper
        label = f"{item['name']}"
        size_label = f"  ({item['size']})"
        lines += f"""
        <tr class="item-row">
          <td class="item-name">{label}<br><span class="item-size">{size_label}</span></td>
          <td class="item-qty">{item['qty']}</td>
          <td class="item-price">{item['unit_price']:.2f}</td>
          <td class="item-total">{item['total']:.2f}</td>
        </tr>"""

    phone_line = f"<div>{shop_phone}</div>" if shop_phone else ""
    customer_line = f"<div class='meta-row'><span>Customer:</span><span>{customer_name}</span></div>" if customer_name else ""
    auto_print_js = "window.onload = function(){ setTimeout(function(){ window.print(); }, 400); };" if auto_print else ""

    # ── VAT breakdown (prices are VAT-inclusive at 20%) ──
    pre_tax     = subtotal / (1 + TOTAL_TAX_RATE)
    vat_amt     = pre_tax * VAT_RATE
    nhil_amt    = pre_tax * NHIL_RATE
    getfund_amt = pre_tax * GETFUND_RATE

    # Dashed separator helper
    sep = "-" * 42

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Receipt {receipt_no}</title>
<style>
  /* ── Thermal printer page setup ── */
  @page {{
    size: 80mm auto;   /* 80mm roll width, auto height */
    margin: 2mm 1mm;
  }}

  /* ── Screen preview matches paper width ── */
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}

  body {{
    font-family: 'Courier New', Courier, monospace;
    font-size: 11px;
    color: #000;
    background: #fff;
    width: 76mm;
    margin: 0 auto;
    padding: 2mm 1mm;
  }}

  /* ── Header ── */
  .hdr {{ text-align: center; margin-bottom: 3mm; }}
  .hdr .shop-name {{
    font-size: 15px;
    font-weight: bold;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    margin-bottom: 1mm;
  }}
  .hdr .shop-sub {{ font-size: 10px; color: #333; line-height: 1.5; }}

  .sep {{ color: #000; font-size: 10px; letter-spacing: 0; word-break: break-all; margin: 2mm 0; }}

  /* ── Receipt meta ── */
  .meta-row {{ display: flex; justify-content: space-between; font-size: 10px; margin: 0.8mm 0; }}
  .meta-row span:last-child {{ text-align: right; }}

  /* ── Items table ── */
  table {{ width: 100%; border-collapse: collapse; margin: 2mm 0; }}
  thead th {{
    font-size: 10px;
    font-weight: bold;
    border-top: 1px solid #000;
    border-bottom: 1px solid #000;
    padding: 1mm 0.5mm;
    text-align: left;
  }}
  th.item-qty, th.item-price, th.item-total {{ text-align: right; }}

  td {{ padding: 1mm 0.5mm; vertical-align: top; font-size: 10px; border-bottom: 1px dotted #aaa; }}
  .item-name {{ width: 44%; font-weight: bold; font-size: 10px; line-height: 1.35; }}
  .item-size {{ font-weight: normal; font-size: 9px; color: #444; }}
  .item-qty  {{ width: 8%;  text-align: right; }}
  .item-price{{ width: 22%; text-align: right; }}
  .item-total{{ width: 26%; text-align: right; font-weight: bold; }}

  /* ── Totals ── */
  .totals {{ margin: 2mm 0 1mm 0; }}
  .total-row {{ display: flex; justify-content: space-between; font-size: 11px; padding: 0.8mm 0; }}
  .total-row.grand {{
    font-size: 14px;
    font-weight: bold;
    border-top: 2px solid #000;
    border-bottom: 2px solid #000;
    padding: 1.5mm 0;
    margin: 1mm 0;
  }}
  .total-row.change-row {{ font-size: 12px; font-weight: bold; }}

  /* ── Footer ── */
  .ftr {{ text-align: center; font-size: 10px; color: #333; margin-top: 3mm; line-height: 1.6; }}

  /* ── Screen-only print button (hidden when printing) ── */
  .print-btn {{
    display: block;
    width: 100%;
    margin-top: 5mm;
    padding: 3mm;
    font-size: 13px;
    font-weight: bold;
    background: #1a2035;
    color: white;
    border: none;
    border-radius: 2mm;
    cursor: pointer;
    letter-spacing: 0.5px;
  }}
  .print-btn:hover {{ background: #FF6B2B; }}

  @media print {{
    .print-btn {{ display: none !important; }}
    body {{ width: 76mm; margin: 0; padding: 1mm; }}
  }}
</style>
<script>{auto_print_js}</script>
</head>
<body>

  <div class="hdr">
    <div class="shop-name">{shop_name}</div>
    <div class="shop-sub">
      <div>{shop_address}</div>
      {phone_line}
    </div>
  </div>

  <div class="sep">{sep}</div>

  <div class="meta-row"><span>Receipt #</span><span>{receipt_no}</span></div>
  <div class="meta-row"><span>Date</span><span>{now.strftime("%d/%m/%Y  %H:%M")}</span></div>
  {customer_line}

  <div class="sep">{sep}</div>

  <table>
    <thead>
      <tr>
        <th class="item-name">ITEM</th>
        <th class="item-qty">QTY</th>
        <th class="item-price">PRICE</th>
        <th class="item-total">TOTAL</th>
      </tr>
    </thead>
    <tbody>{lines}</tbody>
  </table>

  <div class="totals">
    <div class="total-row" style="font-size:10px;">
      <span>Sub-total (excl. tax)</span>
      <span>{currency} {pre_tax:.2f}</span>
    </div>
    <div class="total-row" style="font-size:10px;">
      <span>VAT (15%)</span>
      <span>{currency} {vat_amt:.2f}</span>
    </div>
    <div class="total-row" style="font-size:10px;">
      <span>NHIL (2.5%)</span>
      <span>{currency} {nhil_amt:.2f}</span>
    </div>
    <div class="total-row" style="font-size:10px;">
      <span>GETFund (2.5%)</span>
      <span>{currency} {getfund_amt:.2f}</span>
    </div>
    <div class="total-row grand">
      <span>TOTAL (incl. 20% tax)</span>
      <span>{currency} {subtotal:.2f}</span>
    </div>
    <div class="total-row">
      <span>Amount Paid</span>
      <span>{currency} {amount_paid:.2f}</span>
    </div>
    <div class="total-row change-row">
      <span>Change</span>
      <span>{currency} {change_given:.2f}</span>
    </div>
  </div>

  <div class="sep">{sep}</div>

  <div class="ftr">
    <div>{footer}</div>
    <div style="font-size:9px;margin-top:1.5mm;border-top:1px solid #bbb;padding-top:1mm;">VAT Reg: Prices incl. VAT 15% + NHIL 2.5% + GETFund 2.5% = 20%</div>
    <div style="margin-top:0.5mm;font-size:9px;">Printed: {now.strftime("%d/%m/%Y %H:%M:%S")}</div>
  </div>

  <button class="print-btn" onclick="window.print()">🖨️  PRINT RECEIPT</button>

</body>
</html>"""
    return html


# ── Cart Session State ─────────────────────────────────────────────────────────
if 'cart' not in st.session_state:
    st.session_state.cart = []
if 'last_receipt' not in st.session_state:
    st.session_state.last_receipt = None
if 'search_results' not in st.session_state:
    st.session_state.search_results = []
if 'print_now' not in st.session_state:
    st.session_state.print_now = False
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'logged_in_user' not in st.session_state:
    st.session_state.logged_in_user = ""
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'permissions' not in st.session_state:
    st.session_state.permissions = {}
if 'last_activity' not in st.session_state:
    st.session_state.last_activity = datetime.now()
if 'restock_product_id' not in st.session_state:
    st.session_state.restock_product_id = None
if 'active_page' not in st.session_state:
    st.session_state.active_page = "🛒  New Sale"
if 'active_inv_tab' not in st.session_state:
    st.session_state.active_inv_tab = "📋 View Stock" 
# ── Brute-force protection (DB-backed so it survives app restarts) ────────────
MAX_ATTEMPTS = 5
LOCKOUT_MINS = 10

# Check if st.fragment is available (Streamlit >= 1.33)
_HAS_FRAGMENT = hasattr(st, 'fragment')


# ── Init DB ────────────────────────────────────────────────────────────────────
init_db()

# ── Session timeout: auto-logout after SESSION_TIMEOUT_MINS of inactivity ─────
if st.session_state.logged_in:
    idle = (datetime.now() - st.session_state.last_activity).total_seconds() / 60
    if idle > SESSION_TIMEOUT_MINS:
        st.session_state.logged_in      = False
        st.session_state.logged_in_user = ""
        st.session_state.is_admin       = False
        st.session_state.permissions    = {}
        st.session_state.cart           = []
        st.session_state.last_receipt   = None
        st.info("⏱️ You were logged out after inactivity. Please sign in again.")
        st.rerun()
    else:
        # Only write to session state if more than 60s have passed
        # — avoids forcing a re-render on every single widget interaction
        if (datetime.now() - st.session_state.last_activity).total_seconds() > 60:
            st.session_state.last_activity = datetime.now()

# ══════════════════════════════════════════════════════════════════════════════
#  PIN LOGIN GATE  —  nothing below renders until the user is authenticated
# ══════════════════════════════════════════════════════════════════════════════
if not st.session_state.logged_in:
    st.markdown("""
    <style>
      .login-wrap {
          max-width: 380px;
          margin: 80px auto 0 auto;
          background: white;
          border-radius: 22px;
          padding: 2.5rem 2rem 2rem 2rem;
          box-shadow: 0 8px 40px rgba(0,0,0,0.13);
          text-align: center;
      }
      .login-logo  { font-size: 3.5rem; margin-bottom: 0.3rem; }
      .login-title { font-size: 1.7rem; font-weight: 900; color: #1a2035; margin-bottom: 0.2rem; }
      .login-sub   { font-size: 1rem; color: #666; margin-bottom: 1.5rem; }
    </style>
    <div class="login-wrap">
      <div class="login-logo">🍺</div>
      <div class="login-title">De-Nod's</div>
      <div class="login-sub">Wholesale Drinks Manager</div>
    </div>
    """, unsafe_allow_html=True)

    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        st.markdown("<br>", unsafe_allow_html=True)

        # ── DB-backed lockout (survives app restarts / Streamlit Cloud sleep) ────
        now_dt       = datetime.now()
        lockout_str  = get_setting("lockout_until", "")
        pin_attempts = int(get_setting("pin_attempts", "0") or "0")

        # Parse lockout timestamp
        lockout_until = None
        if lockout_str:
            try:
                lockout_until = datetime.fromisoformat(lockout_str)
            except Exception:
                lockout_until = None

        locked = lockout_until is not None and now_dt < lockout_until

        if locked:
            remaining = int((lockout_until - now_dt).total_seconds())
            mins, secs = divmod(remaining, 60)
            st.error(
                f"🔒 Too many wrong attempts.\n\n"
                f"Please wait **{mins}m {secs:02d}s** before trying again."
            )
            st.caption("The app will unlock automatically. Do not close this page.")
            st.markdown('<meta http-equiv="refresh" content="5">', unsafe_allow_html=True)

        else:
            # Lockout expired — clear it
            if lockout_str:
                set_setting("lockout_until", "")
                set_setting("pin_attempts", "0")
                pin_attempts = 0

            attempts_left = MAX_ATTEMPTS - pin_attempts
            if pin_attempts > 0:
                st.warning(
                    f"⚠️ {pin_attempts} wrong attempt(s). "
                    f"{attempts_left} remaining before a {LOCKOUT_MINS}-minute lockout."
                )

            pin_input = st.text_input(
                "🔐  Enter your PIN",
                type="password",
                placeholder="e.g. 1234",
                key="pin_input",
                help="Ask the owner for your PIN"
            )
            login_btn = st.button("➡️  Enter App", use_container_width=True, type="primary")

            if login_btn or (pin_input and len(pin_input) >= 4):
                if pin_input:
                    user = check_pin(sanitize_pin(pin_input))
                    if user:
                        # ✅ Success — clear lockout state
                        set_setting("pin_attempts", "0")
                        set_setting("lockout_until", "")
                        st.session_state.logged_in      = True
                        st.session_state.logged_in_user = user["name"]
                        st.session_state.is_admin       = (user["role"] == "admin")
                        st.session_state.permissions    = user["permissions"]
                        st.session_state.last_activity  = datetime.now()
                        audit("LOGIN", f"Role: {user['role']}")
                        st.rerun()
                    else:
                        # ❌ Wrong PIN — increment DB counter
                        new_attempts = pin_attempts + 1
                        set_setting("pin_attempts", str(new_attempts))
                        if new_attempts >= MAX_ATTEMPTS:
                            lockout_ts = (datetime.now() + timedelta(minutes=LOCKOUT_MINS)).isoformat()
                            set_setting("lockout_until", lockout_ts)
                            st.error(f"🔒 Too many wrong attempts. App locked for {LOCKOUT_MINS} minutes.")
                            audit("LOCKOUT", f"After {new_attempts} failed attempts")
                        else:
                            remaining_tries = MAX_ATTEMPTS - new_attempts
                            st.error(f"❌ Wrong PIN. {remaining_tries} attempt(s) left before lockout.")
                        st.rerun()

    st.stop()   # ← nothing below runs until logged in
# ══════════════════════════════════════════════════════════════════════════════

# ── Sidebar navigation ────────────────────────────────────────────────────────
low_stock = get_low_stock_products()
alert_text = f"⚠️ {len(low_stock)} item(s) low on stock" if low_stock else "✅ All stock levels OK"
currency = get_setting('currency', 'GHS')

NAV_PAGES = ["🛒  New Sale", "📦  Inventory", "⚠️  Damaged Goods", "📊  Reports", "⚙️  Settings"]

with st.sidebar:
    st.markdown(f"""
    <div style="text-align:center;padding:1rem 0 0.5rem 0;">
      <div style="font-size:2.5rem;">🍺</div>
      <div style="font-weight:900;font-size:1.1rem;color:#1a2035;">De-Nod's</div>
      <div style="font-size:0.85rem;color:#666;">Wholesale Drinks</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("---")
    for page in NAV_PAGES:
        is_active = st.session_state.active_page == page
        btn_style = "primary" if is_active else "secondary"
        if st.button(page, key=f"nav_{page}", use_container_width=True, type=btn_style):
            st.session_state.active_page = page
            st.rerun()
    st.markdown("---")
    st.markdown(f"👤 **{st.session_state.logged_in_user}**")
    if st.session_state.is_admin:
        st.caption("👑 Administrator")
    st.caption(f"{alert_text}")
    st.markdown("")
    if st.button("🔒 Sign Out", use_container_width=True):
        st.session_state.logged_in      = False
        st.session_state.logged_in_user = ""
        st.session_state.is_admin       = False
        st.session_state.permissions    = {}
        st.session_state.cart           = []
        st.session_state.last_receipt   = None
        st.rerun()

# ── Page header ────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="app-header">
  <div style="font-size:2.5rem;">{'🛒' if st.session_state.active_page == '🛒  New Sale' else
                                   '📦' if st.session_state.active_page == '📦  Inventory' else
                                   '⚠️' if st.session_state.active_page == '⚠️  Damaged Goods' else
                                   '📊' if st.session_state.active_page == '📊  Reports' else '⚙️'}</div>
  <div>
    <h1>De-Nod's Wholesale Drinks</h1>
    <p class="sub">📅 {datetime.now().strftime("%A, %d %B %Y")} &nbsp;|&nbsp; {alert_text}</p>
  </div>
</div>
""", unsafe_allow_html=True)

_page = st.session_state.active_page


# ═══════════════════════════════════════════════════════════════════════════════
#  TAB 1: NEW SALE
# ═══════════════════════════════════════════════════════════════════════════════
if _page == '🛒  New Sale':
    col_left, col_right = st.columns([3, 2], gap="large")

    with col_left:
        st.markdown('<div class="section-title">🔍 Search & Add Items</div>', unsafe_allow_html=True)

        search_query = st.text_input(
            "Type drink name or category (e.g. 'Club', 'Beer', 'Water')",
            placeholder="Start typing...",
            key="sale_search"
        )

        if search_query and len(search_query) >= 2:
            results = search_products(search_query)
            if results:
                st.markdown(f"**{len(results)} result(s) found:**")
                for prod in results[:20]:
                    stock_color = "🔴" if prod['stock_qty'] <= prod['low_stock_alert'] else "🟢"
                    with st.container():
                        c1, c2, c3, c4 = st.columns([3, 2, 2, 1])
                        c1.write(f"**{prod['name']}** — {prod['size']}")
                        c2.write(f"**{currency} {prod['sell_price']:.2f}** (wholesale)")
                        c3.write(f"{stock_color} Stock: **{prod['stock_qty']}**")
                        _avail = max(1, prod['stock_qty'])
                        add_qty = c4.number_input("", min_value=1, max_value=_avail,
                                                   value=1, key=f"qty_{prod['id']}", label_visibility="collapsed")
                        if st.button(f"➕ Add to Cart", key=f"add_{prod['id']}"):
                            # Check if already in cart
                            existing = next((i for i in st.session_state.cart if i['product_id'] == prod['id']), None)
                            if existing:
                                existing['qty'] += add_qty
                                existing['total'] = existing['qty'] * existing['unit_price']
                            else:
                                st.session_state.cart.append({
                                    'product_id': prod['id'],
                                    'name': prod['name'],
                                    'size': prod['size'],
                                    'unit_price': prod['sell_price'],
                                    'qty': add_qty,
                                    'total': round(prod['sell_price'] * add_qty, 2)
                                })
                            st.success(f"Added {add_qty}x {prod['name']} ({prod['size']}) to cart!")
                            st.rerun()
                        st.divider()
            else:
                st.info("No products found. Check the Inventory tab to add new products.")

        # Last receipt quick access
        if st.session_state.last_receipt:
            with st.expander("🧾 View Last Receipt"):
                rno, items, subtotal, paid, change, cust = st.session_state.last_receipt
                html = generate_receipt_html(rno, items, subtotal, paid, change, cust)
                st.components.v1.html(html, height=600, scrolling=True)
                st.download_button("💾 Download Receipt", html, file_name=f"{rno}.html", mime="text/html")

    with col_right:
        st.markdown('<div class="section-title">🧺 Cart</div>', unsafe_allow_html=True)

        if not st.session_state.cart:
            st.info("🛒 Cart is empty. Search for drinks and add them.")
        else:
            # Display cart items
            for i, item in enumerate(st.session_state.cart):
                cols = st.columns([4, 2, 1])
                cols[0].write(f"**{item['name']}** ({item['size']})")
                cols[0].write(f"{currency} {item['unit_price']:.2f} × {item['qty']} = **{currency} {item['total']:.2f}**")
                new_qty = cols[1].number_input("", min_value=0, value=item['qty'],
                                               key=f"cart_qty_{i}", label_visibility="collapsed")
                if new_qty != item['qty']:
                    if new_qty == 0:
                        st.session_state.cart.pop(i)
                    else:
                        st.session_state.cart[i]['qty'] = new_qty
                        st.session_state.cart[i]['total'] = round(new_qty * item['unit_price'], 2)
                    st.rerun()
                if cols[2].button("🗑️", key=f"del_{i}"):
                    st.session_state.cart.pop(i)
                    st.rerun()

            subtotal = sum(item['total'] for item in st.session_state.cart)
            st.markdown(f'<div class="total-bar">TOTAL: {currency} {subtotal:.2f}</div>', unsafe_allow_html=True)

            st.markdown("---")
            st.markdown("**💵 Payment**")
            customer_name_raw = st.text_input("Customer Name (optional)", placeholder="e.g. Kofi Mensah", key="cust_name")
            customer_name = sanitize(customer_name_raw, 100)
            amount_paid = st.number_input(f"Amount Paid ({currency})", min_value=0.0, value=float(round(subtotal)), step=1.0, key="amt_paid")

            change = amount_paid - subtotal
            if change >= 0:
                st.markdown(f'<div class="change-bar">✅ Change: {currency} {change:.2f}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="change-bar owed">❌ Short by: {currency} {abs(change):.2f}</div>', unsafe_allow_html=True)

            notes = st.text_input("Notes (optional)", placeholder="e.g. Bulk order, Credit arrangement", key="sale_notes")

            col_a, col_b = st.columns(2)
            if col_a.button("✅ COMPLETE SALE", use_container_width=True, type="primary", disabled=(change < 0)):
                if st.session_state.cart:
                    receipt_no = record_sale(
                        st.session_state.cart, subtotal, amount_paid, max(0, change),
                        customer_name, notes
                    )
                    st.session_state.last_receipt = (
                        receipt_no, list(st.session_state.cart),
                        subtotal, amount_paid, max(0, change), customer_name
                    )
                    audit("SALE_COMPLETE",
                          f"Receipt #{receipt_no} — {currency} {subtotal:.2f}"
                          + (f" — {customer_name}" if customer_name else ""))
                    st.session_state.cart = []
                    st.session_state.print_now = True
                    st.rerun()

            if col_b.button("🗑️ Clear Cart", use_container_width=True):
                st.session_state.cart = []
                st.rerun()

        # ── Receipt panel — shown whenever a last receipt exists ──────────────
        if st.session_state.last_receipt:
            rno, items, stotal, paid, chg, cust = st.session_state.last_receipt

            st.markdown("---")
            st.markdown('<div class="section-title">🧾 Last Receipt</div>', unsafe_allow_html=True)

            # Big prominent print button at the top
            p1, p2 = st.columns([3, 1])
            p1.markdown(f"**Receipt:** `{rno}`")

            if p2.button("🖨️  PRINT", use_container_width=True, type="primary", key="top_print_btn"):
                st.session_state.print_now = True
                st.rerun()

            # Render receipt — auto_print fires once after sale, then turns off
            auto = st.session_state.print_now
            receipt_html = generate_receipt_html(rno, items, stotal, paid, chg, cust, auto_print=auto)

            if auto:
                st.session_state.print_now = False   # reset so it doesn't loop

            # Show the receipt preview inside Streamlit
            st.components.v1.html(receipt_html, height=620, scrolling=True)

            # Save a copy to disk so it can also be downloaded
            st.download_button(
                "💾  Save Receipt (HTML)",
                data=receipt_html,
                file_name=f"receipt_{rno}.html",
                mime="text/html",
                use_container_width=True,
            )


# ═══════════════════════════════════════════════════════════════════════════════
#  TAB 2: INVENTORY
# ═══════════════════════════════════════════════════════════════════════════════
#  TAB 2 – INVENTORY
# ═══════════════════════════════════════════════════════════════════════════════
if _page == '📦  Inventory':
    if not has_perm("can_view_inventory"):
        st.warning("🚫 You do not have permission to view inventory. Ask the administrator.")
    else:
        inv_tab1, inv_tab2, inv_tab3 = st.tabs(
            ["📋  View Stock", "➕  Add Product", "✏️  Update Stock"]
        )

        # ── VIEW STOCK ────────────────────────────────────────────────────────
        with inv_tab1:
            st.markdown('<div class="section-title">📋 Current Inventory</div>', unsafe_allow_html=True)

            low = get_low_stock_products()
            all_prods = get_all_products()

            if low:
                st.error(f"⚠️ **{len(low)} item(s) low on stock or out of stock!**")

                # If a restock panel is open, show it here (no tab-switch needed)
                restock_id = st.session_state.get("restock_product_id")

                for item in low:
                    col_warn, col_btn = st.columns([5, 2])
                    stock_label = "OUT OF STOCK" if item["stock_qty"] <= 0 else f"Only {item['stock_qty']} left"
                    col_warn.markdown(
                        f'<div class="low-stock">🔴 <strong>{item["name"]}</strong> ({item["size"]}) — {stock_label} (Alert: ≤{item["low_stock_alert"]})</div>',
                        unsafe_allow_html=True
                    )
                    if col_btn.button("➕ Restock", key=f"goto_restock_{item['id']}",
                                      use_container_width=True, type="primary"):
                        # Toggle: click again to close
                        if st.session_state.get("restock_product_id") == item["id"]:
                            st.session_state.restock_product_id = None
                        else:
                            st.session_state.restock_product_id = item["id"]
                        st.rerun()

                    # ── Inline restock panel ──────────────────────────────────
                    if st.session_state.get("restock_product_id") == item["id"]:
                        with st.container():
                            st.markdown(
                                f'''<div style="background:#e8f5e9;border:2px solid #2e7d32;
                                border-radius:10px;padding:14px 18px;margin:6px 0 12px 0;">
                                <strong>📦 Restocking: {item["name"]} ({item["size"]})</strong>
                                </div>''', unsafe_allow_html=True
                            )
                            rc1, rc2 = st.columns([2, 3])
                            topup_qty = rc1.number_input(
                                "Units to add", min_value=1, value=24,
                                key=f"inline_topup_qty_{item['id']}"
                            )
                            topup_note = rc2.text_input(
                                "Supplier / delivery note (optional)",
                                key=f"inline_topup_note_{item['id']}",
                                placeholder="e.g. Delivered by Kofi"
                            )
                            rb1, rb2 = st.columns(2)
                            if rb1.button("✅ Add to Stock", key=f"inline_add_{item['id']}",
                                          use_container_width=True, type="primary"):
                                update_stock(item["id"], topup_qty)
                                audit("STOCK_TOPUP",
                                      f"{item['name']} ({item['size']}) +{topup_qty} — {topup_note}")
                                st.session_state.restock_product_id = None
                                st.success(f"✅ Added {topup_qty} units to {item['name']}. "
                                           f"New stock: {item['stock_qty'] + topup_qty}")
                                st.rerun()
                            if rb2.button("✖ Cancel", key=f"inline_cancel_{item['id']}",
                                         use_container_width=True):
                                st.session_state.restock_product_id = None
                                st.rerun()

                st.markdown("---")

            if all_prods:
                categories = ["All"] + sorted(list(set(p['category'] for p in all_prods)))
                cat_filter = st.selectbox("Filter by Category", categories, key="inv_cat_filter")
                filtered = all_prods if cat_filter == "All" else [p for p in all_prods if p['category'] == cat_filter]

                df = pd.DataFrame(filtered)
                df = df.rename(columns={
                    'name': 'Name', 'category': 'Category', 'size': 'Size',
                    'sell_price': f'Wholesale Price ({currency})',
                    'stock_qty': 'In Stock', 'low_stock_alert': 'Alert Level'
                })
                display_cols = ['Name', 'Category', 'Size', f'Wholesale Price ({currency})', 'In Stock', 'Alert Level']

                def highlight_low(row):
                    if row['In Stock'] <= row['Alert Level']:
                        return ['background-color: #ffe0e0'] * len(row)
                    return [''] * len(row)

                st.dataframe(
                    df[display_cols].style.apply(highlight_low, axis=1),
                    use_container_width=True, height=500
                )

                c1, c2, c3 = st.columns(3)
                c1.metric("Total Products", len(all_prods))
                total_value = sum(p['sell_price'] * p['stock_qty'] for p in all_prods)
                c2.metric(f"Total Stock Value ({currency})", f"{total_value:,.0f}")
                c3.metric("Low Stock Items", len(low))
            else:
                st.info("No products yet. Add some in the ➕ Add Product tab.")

        # ── ADD PRODUCT ───────────────────────────────────────────────────────
        with inv_tab2:
            if not has_perm("can_add_product"):
                st.warning("🚫 You do not have permission to add products.")
            else:
                st.markdown('<div class="section-title">➕ Add New Product</div>', unsafe_allow_html=True)
                with st.form("add_product_form", clear_on_submit=True):
                    c1, c2 = st.columns(2)
                    prod_name = c1.text_input("Product Name *", placeholder="e.g. Guinness Stout")
                    prod_cat  = c2.selectbox("Category *", [
                        "Beer","Malt Drink","Soft Drink","Water","Spirit","Whisky",
                        "Wine","Energy Drink","Juice","Dairy Drink","Local Drink","Other"
                    ])
                    c3, c4 = st.columns(2)
                    prod_size  = c3.text_input("Size *", placeholder="e.g. 330ml Bottle")
                    prod_alert = c4.number_input("Low Stock Alert (units)", min_value=1, value=12)
                    c5, c6 = st.columns(2)
                    prod_sell  = c5.number_input(f"Wholesale Price ({currency})", min_value=0.0, value=0.0, step=0.5)
                    prod_stock = c6.number_input("Opening Stock (units)", min_value=0, value=0)
                    if st.form_submit_button("✅ Add Product", use_container_width=True, type="primary"):
                        if prod_name and prod_size:
                            conn = get_db()
                            conn.execute(
                                "INSERT INTO products (name,category,size,buy_price,sell_price,stock_qty,low_stock_alert) VALUES (?,?,?,?,?,?,?)",
                                (sanitize(prod_name,100), prod_cat, sanitize(prod_size,100), 0, prod_sell, prod_stock, prod_alert)
                            )
                            conn.commit(); conn.close()
                            get_all_products.clear(); get_low_stock_products.clear()
                            st.success(f"✅ '{prod_name} ({prod_size})' added!")
                        else:
                            st.error("Please fill in Product Name and Size.")

        # ── UPDATE STOCK ──────────────────────────────────────────────────────
        with inv_tab3:
            if not has_perm("can_view_inventory"):
                st.warning("🚫 You do not have permission to update stock.")
            else:
                st.markdown('<div class="section-title">✏️ Restock & Update Products</div>', unsafe_allow_html=True)

                all_prods_upd = get_all_products()
                if not all_prods_upd:
                    st.info("No products yet.")
                else:
                    if "selected_restock_id" not in st.session_state:
                        st.session_state["selected_restock_id"] = None

                    # Search bar
                    st.markdown("**🔍 Search product to restock:**")
                    search_text = st.text_input(
                        "search_label", label_visibility="collapsed",
                        placeholder="Type name, size or category — e.g. Club Beer, 500ml, Water...",
                        key="restock_search"
                    )

                    if search_text.strip():
                        q = search_text.strip().lower()
                        filtered_prods = [p for p in all_prods_upd
                                          if q in p['name'].lower() or q in p['size'].lower()
                                          or q in p['category'].lower()]
                        if not filtered_prods:
                            st.warning("No products match — try a different search term.")
                        else:
                            st.caption(f"{len(filtered_prods)} match(es) — click a product to select it:")
                            for p in filtered_prods[:12]:
                                if p['stock_qty'] <= 0:
                                    bc = "#c62828"; bt = "OUT OF STOCK"; bg = "#fff3f3"; bdr = "#c62828"
                                elif p['stock_qty'] <= p['low_stock_alert']:
                                    bc = "#e65100"; bt = f"LOW — {p['stock_qty']} left"; bg = "#fff8f0"; bdr = "#e65100"
                                else:
                                    bc = "#2e7d32"; bt = f"✅ {p['stock_qty']} in stock"; bg = "#f9f9f9"; bdr = "#e0e0e0"

                                st.markdown(f"""
                                <div style="border:2px solid {bdr};border-radius:10px;padding:10px 14px;
                                margin-bottom:4px;background:{bg};display:flex;
                                justify-content:space-between;align-items:center;">
                                  <div>
                                    <div style="font-size:1rem;font-weight:700;color:#1a2035;">{p['name']}</div>
                                    <div style="font-size:0.82rem;color:#666;margin-top:2px;">{p['size']} &nbsp;·&nbsp; {p['category']}</div>
                                  </div>
                                  <div style="background:{bc};color:white;padding:4px 10px;
                                              border-radius:20px;font-size:0.78rem;font-weight:700;white-space:nowrap;">
                                    {bt}
                                  </div>
                                </div>""", unsafe_allow_html=True)

                                if st.button(f"Select  ›  {p['name']} ({p['size']})",
                                             key=f"sel_prod_{p['id']}",
                                             use_container_width=True):
                                    st.session_state["selected_restock_id"] = p['id']
                                    st.rerun()

                            if len(filtered_prods) > 12:
                                st.caption(f"Showing first 12 of {len(filtered_prods)} — refine search to narrow down.")

                    selected_id_upd = st.session_state.get("selected_restock_id")
                    if selected_id_upd:
                        prod = get_product_by_id(selected_id_upd)
                        if prod:
                            st.markdown("---")
                            top_left, top_right = st.columns([5, 2])
                            top_left.markdown(
                                f"**Editing:** &nbsp;<span style='font-size:1.1rem;font-weight:800;"
                                f"color:#1a2035'>{prod['name']}</span> &nbsp;"
                                f"<span style='color:#888;font-size:0.9rem'>({prod['size']})</span>",
                                unsafe_allow_html=True
                            )
                            if top_right.button("🔄 Change Product", use_container_width=True, key="change_prod_btn"):
                                st.session_state["selected_restock_id"] = None
                                st.rerun()

                            sc = prod['stock_qty']
                            scolor = "#e53935" if sc<=0 else "#f57c00" if sc<=prod['low_stock_alert'] else "#2e7d32"
                            slabel = "🚫 OUT OF STOCK" if sc<=0 else "⚠️ LOW STOCK" if sc<=prod['low_stock_alert'] else "✅ OK"
                            st.markdown(
                                f'''<div style="background:{scolor};color:white;padding:12px 18px;
                                border-radius:10px;margin:10px 0;font-size:1.1rem;font-weight:bold;">
                                📦 Current Stock: {sc} units &nbsp; {slabel}</div>''',
                                unsafe_allow_html=True
                            )

                            st.markdown("**⚡ Quick Top-Up**")
                            qa, qb = st.columns(2)
                            topup_qty  = qa.number_input("Units to Add", min_value=1, value=24, key="topup_qty_new")
                            topup_note = qb.text_input("Supplier Note (optional)", key="topup_note",
                                                       placeholder="e.g. Delivered by Kofi, Inv#123")
                            ta, tb = st.columns(2)
                            if ta.button("➕ Add to Stock", use_container_width=True, type="primary", key="quick_topup_btn"):
                                update_stock(selected_id_upd, topup_qty)
                                audit("STOCK_TOPUP", f"{prod['name']} ({prod['size']}) +{topup_qty} — {topup_note}")
                                st.success(f"✅ Added {topup_qty} units. New stock: {sc + topup_qty}")
                                st.rerun()
                            if tb.button("🔢 Set Exact Count", use_container_width=True, key="set_exact_btn"):
                                st.session_state[f"show_exact_{selected_id_upd}"] = True

                            if st.session_state.get(f"show_exact_{selected_id_upd}"):
                                _cur = int(prod['stock_qty'])
                                exact_val = st.number_input("Set stock to exactly:", min_value=min(0, _cur),
                                                            value=_cur, key="exact_stock_val")
                                if st.button("✅ Confirm", key="confirm_exact"):
                                    update_stock(selected_id_upd, exact_val - _cur)
                                    audit("STOCK_SET", f"{prod['name']} ({prod['size']}) → {exact_val}")
                                    st.session_state[f"show_exact_{selected_id_upd}"] = False
                                    st.success(f"✅ Stock set to {exact_val} units.")
                                    st.rerun()

                            st.markdown("---")
                            with st.expander("✏️ Edit Product Details (name, price, size, alert level)", expanded=False):
                                can_price = has_perm("can_change_price")
                                with st.form("update_product_form"):
                                    c1, c2 = st.columns(2)
                                    new_name = c1.text_input("Name", value=prod['name'])
                                    cats = ["Beer","Malt Drink","Soft Drink","Water","Spirit","Whisky",
                                            "Wine","Energy Drink","Juice","Dairy Drink","Local Drink","Other"]
                                    new_cat = c2.selectbox("Category", cats,
                                        index=cats.index(prod['category']) if prod['category'] in cats else 0)
                                    c3, c4 = st.columns(2)
                                    new_size  = c3.text_input("Size", value=prod['size'])
                                    new_alert = c4.number_input("Low Stock Alert", min_value=1, value=prod['low_stock_alert'])
                                    c5, c6 = st.columns(2)
                                    new_sell = c5.number_input(
                                        f"Wholesale Price ({currency})" + ("" if can_price else " 🔒 Read-only"),
                                        min_value=0.0, value=float(prod['sell_price']), step=0.5,
                                        disabled=not can_price
                                    )
                                    _cs = int(prod['stock_qty'])
                                    new_stock_val = c6.number_input("Stock Quantity",
                                        min_value=min(0, _cs), value=_cs)
                                    col_save, col_del = st.columns(2)
                                    save_btn   = col_save.form_submit_button("💾 Save Changes",
                                                    use_container_width=True, type="primary")
                                    delete_btn = col_del.form_submit_button("🗑️ Deactivate",
                                                    use_container_width=True)
                                    if save_btn:
                                        fp = new_sell if can_price else prod['sell_price']
                                        conn = get_db()
                                        conn.execute(
                                            "UPDATE products SET name=?,category=?,size=?,sell_price=?,stock_qty=?,low_stock_alert=? WHERE id=?",
                                            (sanitize(new_name), new_cat, sanitize(new_size), fp, new_stock_val, new_alert, selected_id_upd)
                                        )
                                        conn.commit(); conn.close()
                                        get_all_products.clear(); get_low_stock_products.clear(); get_product_by_id.clear()
                                        audit("PRODUCT_EDIT", f"{new_name} ({new_size})")
                                        st.success("✅ Product updated!")
                                        st.rerun()
                                    if delete_btn:
                                        conn = get_db()
                                        conn.execute("UPDATE products SET active=0 WHERE id=?", (selected_id_upd,))
                                        conn.commit(); conn.close()
                                        get_all_products.clear(); get_low_stock_products.clear(); get_product_by_id.clear()
                                        audit("PRODUCT_DEACTIVATE", f"{prod['name']} ({prod['size']})")
                                        st.session_state["selected_restock_id"] = None
                                        st.success("Product deactivated.")
                                        st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
#  TAB 3: DAMAGED GOODS
# ═══════════════════════════════════════════════════════════════════════════════
if _page == '⚠️  Damaged Goods':
    if not has_perm("can_log_damage"):
        st.warning("🚫 You do not have permission to log damaged goods. Ask the administrator.")
    else:
        col_d1, col_d2 = st.columns([2, 3], gap="large")

    with col_d1:
        st.markdown('<div class="section-title">⚠️ Log Damaged Goods</div>', unsafe_allow_html=True)

        all_prods = get_all_products()
        with st.form("damage_form", clear_on_submit=True):
            damage_type = st.radio("Log damage for:", ["Existing Product", "Custom Item"], horizontal=True)

            if damage_type == "Existing Product" and all_prods:
                prod_options = {f"{p['name']} ({p['size']}) — Stock: {p['stock_qty']}": p for p in all_prods}
                selected_label = st.selectbox("Select Product", list(prod_options.keys()))
                selected_prod = prod_options[selected_label]
                dmg_name = selected_prod['name']
                dmg_size = selected_prod['size']
                dmg_pid = selected_prod['id']
                dmg_cost = st.number_input(f"Unit Wholesale Price ({currency})", min_value=0.0,
                                            value=float(selected_prod['sell_price']), step=0.5)
            else:
                dmg_name = st.text_input("Product Name", placeholder="e.g. Unknown brand")
                dmg_size = st.text_input("Size", placeholder="e.g. 500ml")
                dmg_pid = 0
                dmg_cost = st.number_input(f"Unit Wholesale Price ({currency})", min_value=0.0, value=0.0, step=0.5)

            dmg_qty = st.number_input("Quantity Damaged", min_value=1, value=1)
            dmg_reason = st.selectbox("Reason", [
                "Broken / Cracked", "Expired", "Leaking", "Stolen",
                "Flood / Water Damage", "Fire Damage", "Supplier Damage", "Other"
            ])
            dmg_notes = st.text_input("Additional Notes", placeholder="Optional details")

            total_loss = dmg_cost * dmg_qty
            st.markdown(f"**Estimated Loss: {currency} {total_loss:.2f}**")

            submitted = st.form_submit_button("⚠️ Log Damage", use_container_width=True, type="primary")
            if submitted:
                if dmg_name:
                    log_damage(dmg_pid, dmg_name, dmg_size if 'dmg_size' in dir() else '', dmg_qty,
                               f"{dmg_reason}. {dmg_notes}".strip('. '), dmg_cost)
                    audit("DAMAGE_LOG",
                          f"{dmg_name} x{dmg_qty} — {dmg_reason} — Loss: {currency} {total_loss:.2f}")
                    st.success(f"✅ Logged {dmg_qty}x {dmg_name} as damaged. Loss: {currency} {total_loss:.2f}")
                    st.rerun()
                else:
                    st.error("Please enter a product name.")

    with col_d2:
        st.markdown('<div class="section-title">📋 Damage History</div>', unsafe_allow_html=True)
        dmg_range = st.selectbox("Show records from:", ["Today", "Last 7 Days", "This Month", "All Time"], key="dmg_range")

        today = date.today()
        if dmg_range == "Today":
            start, end = today, today
        elif dmg_range == "Last 7 Days":
            start, end = today - timedelta(days=7), today
        elif dmg_range == "This Month":
            start = today.replace(day=1)
            end = today
        else:
            start = date(2020, 1, 1)
            end = today

        damage_records = get_damaged_in_range(start, end)
        if damage_records:
            df_dmg = pd.DataFrame(damage_records)
            df_dmg = df_dmg[['date_logged', 'product_name', 'size', 'qty', 'unit_cost', 'total_loss', 'reason']]
            df_dmg.columns = ['Date', 'Product', 'Size', 'Qty', f'Wholesale Price', 'Total Loss', 'Reason']
            st.dataframe(df_dmg, use_container_width=True, height=350)
            total_dmg_loss = sum(r['total_loss'] for r in damage_records)
            st.error(f"💸 Total Loss ({dmg_range}): **{currency} {total_dmg_loss:.2f}**")
        else:
            st.info("No damage records found for this period. That's great! 👍")


# ═══════════════════════════════════════════════════════════════════════════════
#  TAB 4: REPORTS
# ═══════════════════════════════════════════════════════════════════════════════
if _page == '📊  Reports':
    if not has_perm("can_view_reports"):
        st.warning("🚫 You do not have permission to view reports. Ask the administrator.")
    else:
        st.markdown('<div class="section-title">📊 Sales Reports</div>', unsafe_allow_html=True)

    report_type = st.radio("Report Period:", ["📅 Today", "📅 This Week", "📅 This Month", "📆 Custom Range"],
                           horizontal=True, key="report_type")

    today = date.today()
    if report_type == "📅 Today":
        start_d, end_d = today, today
        period_label = f"Today ({today.strftime('%d %b %Y')})"
    elif report_type == "📅 This Week":
        start_d = today - timedelta(days=today.weekday())
        end_d = today
        period_label = f"This Week ({start_d.strftime('%d %b')} – {end_d.strftime('%d %b %Y')})"
    elif report_type == "📅 This Month":
        start_d = today.replace(day=1)
        end_d = today
        period_label = f"This Month ({start_d.strftime('%B %Y')})"
    else:
        col_s, col_e = st.columns(2)
        start_d = col_s.date_input("From", value=today - timedelta(days=30))
        end_d = col_e.date_input("To", value=today)
        period_label = f"{start_d.strftime('%d %b')} – {end_d.strftime('%d %b %Y')}"

    sales = get_sales_in_range(start_d, end_d)
    damage = get_damaged_in_range(start_d, end_d)

    # ── Summary Metrics ──
    total_revenue = sum(s['subtotal'] for s in sales)
    num_transactions = len(sales)
    total_loss = sum(d['total_loss'] for d in damage)
    net_revenue = total_revenue - total_loss
    avg_sale = total_revenue / num_transactions if num_transactions > 0 else 0

    st.markdown(f"### 📋 Report: {period_label}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"💰 Total Revenue ({currency})", f"{total_revenue:,.2f}")
    c2.metric("🧾 Transactions", num_transactions)
    c3.metric(f"💸 Damage Loss ({currency})", f"{total_loss:,.2f}")
    c4.metric(f"✅ Net Revenue ({currency})", f"{net_revenue:,.2f}")

    if sales:
        # ── Sales Table ──
        st.markdown("---")
        st.markdown("**🧾 Sales Transactions**")
        rows = []
        for s in sales:
            items = json.loads(s['items_json'])
            item_summary = ", ".join([f"{i['qty']}x {i['name']}" for i in items[:3]])
            if len(items) > 3:
                item_summary += f" +{len(items)-3} more"
            rows.append({
                'Date': s['sale_date'],
                'Time': s['sale_time'],
                'Receipt': s['receipt_no'],
                'Customer': s['customer_name'] or '—',
                'Items': item_summary,
                f'Total ({currency})': f"{s['subtotal']:.2f}",
                f'Paid ({currency})': f"{s['amount_paid']:.2f}",
                f'Change ({currency})': f"{s['change_given']:.2f}",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=300)

        # ── Top Selling Products ──
        st.markdown("---")
        st.markdown("**🏆 Top Selling Products**")
        product_totals = {}
        for s in sales:
            for item in json.loads(s['items_json']):
                key = f"{item['name']} ({item['size']})"
                if key not in product_totals:
                    product_totals[key] = {'qty': 0, 'revenue': 0}
                product_totals[key]['qty'] += item['qty']
                product_totals[key]['revenue'] += item['total']
        if product_totals:
            top_df = pd.DataFrame([
                {'Product': k, 'Qty Sold': v['qty'], f'Revenue ({currency})': round(v['revenue'], 2)}
                for k, v in product_totals.items()
            ]).sort_values(f'Revenue ({currency})', ascending=False).head(10)
            st.dataframe(top_df, use_container_width=True)

        # ── Daily breakdown (for week/month) ──
        if report_type != "📅 Today":
            st.markdown("---")
            st.markdown("**📈 Daily Revenue Breakdown**")
            daily = {}
            for s in sales:
                d = s['sale_date']
                daily[d] = daily.get(d, 0) + s['subtotal']
            if daily:
                daily_df = pd.DataFrame(list(daily.items()), columns=['Date', f'Revenue ({currency})']).sort_values('Date')
                st.bar_chart(daily_df.set_index('Date'))

        # ── Export ──
        st.markdown("---")
        if st.button("📥 Download Report as CSV", use_container_width=True):
            csv_df = pd.DataFrame(rows)
            csv_str = csv_df.to_csv(index=False)
            st.download_button(
                "💾 Click to Download CSV",
                data=csv_str,
                file_name=f"denods_report_{start_d}_{end_d}.csv",
                mime="text/csv"
            )
    else:
        st.info("📭 No sales recorded for this period yet.")

    # ── Damage Summary ──
    if damage:
        st.markdown("---")
        st.markdown("**⚠️ Damage Summary**")
        dmg_df = pd.DataFrame(damage)[['date_logged', 'product_name', 'qty', 'total_loss', 'reason']]
        dmg_df.columns = ['Date', 'Product', 'Qty', f'Loss ({currency})', 'Reason']
        st.dataframe(dmg_df, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  TAB 5: SETTINGS
# ═══════════════════════════════════════════════════════════════════════════════
if _page == '⚙️  Settings':
    st.markdown('<div class="section-title">⚙️ Shop Settings</div>', unsafe_allow_html=True)

    col_s1, col_s2 = st.columns(2, gap="large")

    with col_s1:
        st.markdown("**🏪 Shop Information**")
        with st.form("settings_form"):
            shop_name = st.text_input("Shop Name", value=get_setting('shop_name', "De-Nod's Wholesale Drinks"))
            shop_address = st.text_input("Address", value=get_setting('shop_address', 'Ghana'))
            shop_phone = st.text_input("Phone Number", value=get_setting('shop_phone', ''))
            shop_email = st.text_input("Email (optional)", value=get_setting('shop_email', ''))
            receipt_footer = st.text_area("Receipt Footer Message",
                                          value=get_setting('receipt_footer', 'Thank you for your business! Come again soon.'))
            currency = st.selectbox("Currency", ["GHS", "USD", "EUR", "GBP", "NGN"],
                                    index=["GHS","USD","EUR","GBP","NGN"].index(get_setting('currency','GHS')))
            if st.form_submit_button("💾 Save Settings", use_container_width=True, type="primary"):
                set_setting('shop_name', shop_name)
                set_setting('shop_address', shop_address)
                set_setting('shop_phone', shop_phone)
                set_setting('shop_email', shop_email)
                set_setting('receipt_footer', receipt_footer)
                set_setting('currency', currency)
                st.success("✅ Settings saved!")
                st.rerun()

    with col_s2:
        st.markdown("**📊 Database Info**")
        conn = get_db()
        def _n(row): return list(row.values())[0] if isinstance(row, dict) else row[0]
        total_products = _n(conn.execute("SELECT COUNT(*) FROM products WHERE active=1").fetchone())
        total_sales    = _n(conn.execute("SELECT COUNT(*) FROM sales").fetchone())
        reset_date     = get_setting("revenue_reset_date", "")
        if reset_date:
            total_rev = _n(conn.execute(
                "SELECT COALESCE(SUM(subtotal),0) FROM sales WHERE sale_date >= ?",
                (reset_date,)
            ).fetchone())
        else:
            total_rev = _n(conn.execute("SELECT COALESCE(SUM(subtotal),0) FROM sales").fetchone())
        total_damage = _n(conn.execute("SELECT COALESCE(SUM(total_loss),0) FROM damaged_goods").fetchone())
        conn.close()

        st.metric("Products in Catalogue", total_products)
        st.metric("Total Transactions", total_sales)
        rev_label = f"Revenue ({get_setting('currency','GHS')})"
        if reset_date:
            rev_label += f"  since {reset_date}"
        st.metric(rev_label, f"{total_rev:,.2f}")
        st.metric(f"Damage Losses ({get_setting('currency','GHS')})", f"{total_damage:,.2f}")

        if st.session_state.is_admin:
            st.markdown("---")
            st.markdown("**🔄 Reset Revenue Counter**")
            st.caption("Resets the display only — all sales data is kept safely.")
            if st.button("🔄 Reset Revenue Counter to Today", use_container_width=True):
                set_setting("revenue_reset_date", date.today().isoformat())
                st.success(f"✅ Revenue counter reset from {date.today().strftime('%d %b %Y')}.")
                st.rerun()
            if reset_date:
                if st.button("↩️ Show All-Time Revenue", use_container_width=True):
                    set_setting("revenue_reset_date", "")
                    st.success("✅ Showing all-time revenue again.")
                    st.rerun()

        st.markdown("---")
        st.markdown("**💾 Backup Database**")
        if st.button("📥 Download Database Backup", use_container_width=True):
            with open(DB_PATH, 'rb') as f:
                db_bytes = f.read()
            st.download_button(
                "💾 Download denods.db",
                data=db_bytes,
                file_name=f"denods_backup_{date.today()}.db",
                mime="application/octet-stream"
            )

        st.markdown("---")
        st.markdown("**🔄 Sync Product Catalogue**")
        st.caption("Loads new drinks added in an app update. Existing data is never changed.")
        if st.button("🔄 Sync New Products from Latest Update", use_container_width=True):
            added = sync_new_products()
            if added > 0:
                st.success(f"✅ {added} new product(s) added to your catalogue!")
            else:
                st.info("✅ Catalogue is up to date.")
            st.rerun()

        st.markdown("---")
        st.markdown("**ℹ️ App Info**")
        st.info("De-Nod's Drinks Manager v1.0\nBuilt for De-Nod's Wholesale Drinks, Ghana.\nData stored locally in denods.db")

        if st.session_state.is_admin:
            st.markdown("---")
            st.markdown("**🔍 Activity Audit Log** *(Admin only)*")
            st.caption("Every sale, stock update, login, and change is recorded here.")
            log_limit = st.selectbox("Show last:", [50, 100, 200, 500], key="audit_limit")
            conn = get_db()
            log_rows = conn.execute(
                "SELECT log_time, user_name, action, detail FROM audit_log ORDER BY id DESC LIMIT ?",
                (log_limit,)
            ).fetchall()
            log_rows = [dict(r) if isinstance(r, dict) else dict(zip(['Time','User','Action','Detail'], r)) for r in log_rows]
            conn.close()
            if log_rows:
                log_df = pd.DataFrame(log_rows, columns=["Time", "User", "Action", "Detail"])
                st.dataframe(log_df, use_container_width=True, height=300)
            else:
                st.info("No activity recorded yet.")

# ── User Management — Admin Only ───────────────────────────────────────────────
if _page == '⚙️  Settings':
    st.markdown("---")
    st.markdown('<div class="section-title">👥 User Management</div>', unsafe_allow_html=True)

    if not st.session_state.is_admin:
        st.warning("🚫 Only the administrator (Stephen Acquah) can manage users and permissions.")
    else:
        st.caption("Manage who can access the app and what each person is allowed to do.")
        users = get_all_users()

        for u in users:
            is_self     = (u["name"] == st.session_state.logged_in_user)
            role_badge  = "👑 Admin" if u["role"] == "admin" else "👤 Staff"
            status_icon = "✅" if u["active"] else "🚫"
            with st.expander(f"{status_icon}  {u['name']}  —  {role_badge}", expanded=False):
                with st.form(f"user_form_{u['id']}"):
                    c1, c2, c3 = st.columns([3, 2, 2])
                    new_uname  = c1.text_input("Name", value=u["name"], key=f"uname_{u['id']}")
                    new_pin    = c2.text_input("PIN", value=u["pin"], key=f"upin_{u['id']}", max_chars=6)
                    new_active = c3.selectbox("Status", ["Active", "Disabled"],
                                              index=0 if u["active"] else 1,
                                              key=f"uact_{u['id']}")
                    role_opts  = ["admin", "staff"]
                    new_role   = st.selectbox("Role", role_opts,
                                              index=role_opts.index(u["role"] if u["role"] in role_opts else "staff"),
                                              key=f"urole_{u['id']}",
                                              disabled=is_self,
                                              help="You cannot change your own role.")
                    st.markdown("**Permissions:**")
                    new_perms = {}
                    if new_role == "admin":
                        st.info("Admins have all permissions automatically.")
                        new_perms = ADMIN_PERMISSIONS.copy()
                    else:
                        pcols = st.columns(2)
                        for idx2, (pkey, plabel) in enumerate(ALL_PERMISSIONS.items()):
                            if pkey == "can_manage_users":
                                continue
                            cur = u["permissions"].get(pkey, False)
                            new_perms[pkey] = pcols[idx2 % 2].checkbox(
                                plabel, value=cur, key=f"perm_{u['id']}_{pkey}")
                        new_perms["can_manage_users"] = False

                    if st.form_submit_button("💾 Save Changes", use_container_width=True, type="primary"):
                        if new_pin.strip().isdigit() and len(new_pin.strip()) >= 4:
                            fr = "admin" if new_role == "admin" else "staff"
                            fp = ADMIN_PERMISSIONS.copy() if fr == "admin" else new_perms
                            conn = get_db()
                            conn.execute(
                                "UPDATE users SET name=?, pin=?, active=?, role=?, permissions=? WHERE id=?",
                                (new_uname.strip(), new_pin.strip(),
                                 1 if new_active == "Active" else 0, fr, json.dumps(fp), u["id"])
                            )
                            conn.commit()
                            conn.close()
                            st.success(f"✅ {new_uname} updated.")
                            st.rerun()
                        else:
                            st.error("PIN must be 4–6 digits.")

        st.markdown("---")
        st.markdown("**➕ Add New User**")
        with st.form("add_user_form", clear_on_submit=True):
            ca, cb, cc = st.columns([3, 2, 2])
            add_name = ca.text_input("Name", placeholder="e.g. Abena Mensah")
            add_pin  = cb.text_input("PIN", placeholder="e.g. 4321", max_chars=6)
            add_role = cc.selectbox("Role", ["staff", "admin"])
            st.markdown("**Starting Permissions** (for staff):")
            nup = {}
            if add_role == "admin":
                st.info("Admins have all permissions automatically.")
                nup = ADMIN_PERMISSIONS.copy()
            else:
                pc2 = st.columns(2)
                for idx3, (pkey, plabel) in enumerate(ALL_PERMISSIONS.items()):
                    if pkey == "can_manage_users":
                        continue
                    dv = DEFAULT_STAFF_PERMISSIONS.get(pkey, False)
                    nup[pkey] = pc2[idx3 % 2].checkbox(plabel, value=dv, key=f"np_{pkey}")
                nup["can_manage_users"] = False
            if st.form_submit_button("➕ Add User", use_container_width=True, type="primary"):
                if add_name.strip() and add_pin.strip().isdigit() and len(add_pin.strip()) >= 4:
                    conn = get_db()
                    clash = conn.execute("SELECT name FROM users WHERE pin=? AND active=1",
                                         (add_pin.strip(),)).fetchone()
                    if clash:
                        st.error(f"PIN already used by {clash[0]}. Choose another.")
                    else:
                        fr2 = add_role
                        fp2 = ADMIN_PERMISSIONS.copy() if fr2 == "admin" else nup
                        conn.execute(
                            "INSERT INTO users (name, pin, role, permissions) VALUES (?,?,?,?)",
                            (add_name.strip(), add_pin.strip(), fr2, json.dumps(fp2))
                        )
                        conn.commit()
                        st.success(f"✅ {add_name} added with PIN {add_pin}.")
                        st.rerun()
                    conn.close()
                else:
                    st.error("Please enter a name and a PIN of at least 4 digits.")
