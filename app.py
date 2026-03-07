"""
De-Nod's Wholesale Drinks Manager
A complete POS + Inventory + Reports app for a Ghanaian drinks wholesaler.
"""

import streamlit as st
import sqlite3
import json
import os
from datetime import datetime, date, timedelta
import pandas as pd
import uuid

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH = "denods.db"

st.set_page_config(
    page_title="De-Nod's Drinks Manager",
    page_icon="🍺",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── Elder-Friendly CSS ────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800;900&display=swap');

  html, body, [class*="css"] {
      font-family: 'Nunito', sans-serif !important;
  }

  /* Big readable text throughout */
  .stApp { background: #F5F7FA; }

  h1 { font-size: 2.2rem !important; font-weight: 900 !important; color: #1a2035; }
  h2 { font-size: 1.7rem !important; font-weight: 800 !important; color: #1a2035; }
  h3 { font-size: 1.3rem !important; font-weight: 700 !important; color: #1a2035; }
  p, li, label, .stMarkdown { font-size: 1.1rem !important; }

  /* Bigger tab labels */
  .stTabs [data-baseweb="tab"] { font-size: 1.1rem !important; font-weight: 700 !important; padding: 12px 22px !important; }
  .stTabs [aria-selected="true"] { background: #FF6B2B !important; color: white !important; border-radius: 8px 8px 0 0 !important; }

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
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        category TEXT DEFAULT '',
        size TEXT DEFAULT '',
        buy_price REAL DEFAULT 0,
        sell_price REAL DEFAULT 0,
        stock_qty INTEGER DEFAULT 0,
        low_stock_alert INTEGER DEFAULT 10,
        active INTEGER DEFAULT 1
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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

    c.execute('''CREATE TABLE IF NOT EXISTS damaged_goods (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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

    defaults = {
        'shop_name': "De-Nod's Wholesale Drinks",
        'shop_address': 'Ghana',
        'shop_phone': '',
        'shop_email': '',
        'receipt_footer': 'Thank you for your business! Come again soon.',
        'currency': 'GHS',
    }
    for k, v in defaults.items():
        c.execute("INSERT OR IGNORE INTO settings VALUES (?,?)", (k, v))

    conn.commit()

    count = c.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    if count == 0:
        _seed_products(c)
        conn.commit()

    conn.close()


def get_setting(key, default=''):
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row[0] if row else default


def set_setting(key, value):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, value))
    conn.commit()
    conn.close()


def sync_new_products():
    """Insert any seed products that don't yet exist in the DB (matched by name+size)."""
    conn = get_db()
    c = conn.cursor()
    existing = set(
        (r[0].strip().lower(), r[1].strip().lower())
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
def search_products(query):
    conn = get_db()
    q = f"%{query.lower()}%"
    rows = conn.execute(
        "SELECT * FROM products WHERE active=1 AND (LOWER(name) LIKE ? OR LOWER(category) LIKE ? OR LOWER(size) LIKE ?) ORDER BY name, size",
        (q, q, q)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


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
    # Deduct from stock
    if product_id:
        update_stock(product_id, -qty)


def get_sales_in_range(start_date, end_date):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM sales WHERE sale_date BETWEEN ? AND ? ORDER BY sale_date DESC, sale_time DESC",
        (start_date.isoformat(), end_date.isoformat())
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


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
    <div class="total-row grand">
      <span>TOTAL</span>
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
    <div style="margin-top:1mm;font-size:9px;">Printed: {now.strftime("%d/%m/%Y %H:%M:%S")}</div>
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


# ── Init DB ────────────────────────────────────────────────────────────────────
init_db()

# ── Header ─────────────────────────────────────────────────────────────────────
low_stock = get_low_stock_products()
alert_text = f"⚠️ {len(low_stock)} item(s) low on stock" if low_stock else "✅ All stock levels OK"
currency = get_setting('currency', 'GHS')

st.markdown(f"""
<div class="app-header">
  <div style="font-size:3rem;">🍺</div>
  <div>
    <h1>De-Nod's Wholesale Drinks</h1>
    <p class="sub">📍 Ghana &nbsp;|&nbsp; {datetime.now().strftime("%A, %d %B %Y")} &nbsp;|&nbsp; {alert_text}</p>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Main Tabs ──────────────────────────────────────────────────────────────────
tab_sale, tab_inv, tab_damage, tab_reports, tab_settings = st.tabs([
    "🛒  New Sale",
    "📦  Inventory",
    "⚠️  Damaged Goods",
    "📊  Reports",
    "⚙️  Settings"
])


# ═══════════════════════════════════════════════════════════════════════════════
#  TAB 1: NEW SALE
# ═══════════════════════════════════════════════════════════════════════════════
with tab_sale:
    col_left, col_right = st.columns([3, 2], gap="large")

    with col_left:
        st.markdown('<div class="section-title">🔍 Search & Add Items</div>', unsafe_allow_html=True)

        search_query = st.text_input(
            "Type drink name or category (e.g. 'Club', 'Beer', 'Water')",
            placeholder="Start typing...",
            key="sale_search"
        )

        if search_query and len(search_query) >= 1:
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
                        add_qty = c4.number_input("", min_value=1, max_value=max(1, prod['stock_qty']),
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
            customer_name = st.text_input("Customer Name (optional)", placeholder="e.g. Kofi Mensah", key="cust_name")
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
                    st.session_state.cart = []
                    st.session_state.print_now = True   # ← triggers auto-print
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
with tab_inv:
    inv_tab1, inv_tab2, inv_tab3 = st.tabs(["📋 View Stock", "➕ Add Product", "✏️ Update Stock"])

    # ── View Stock ──
    with inv_tab1:
        st.markdown('<div class="section-title">📋 Current Inventory</div>', unsafe_allow_html=True)

        # Low stock alert
        low = get_low_stock_products()
        if low:
            st.error(f"⚠️ **{len(low)} items are low on stock or out of stock!**")
            for item in low:
                st.markdown(f'<div class="low-stock">🔴 {item["name"]} ({item["size"]}) — Only <strong>{item["stock_qty"]}</strong> left (Alert: ≤{item["low_stock_alert"]})</div>', unsafe_allow_html=True)
            st.markdown("---")

        all_prods = get_all_products()
        if all_prods:
            # Category filter
            categories = ["All"] + sorted(list(set(p['category'] for p in all_prods)))
            cat_filter = st.selectbox("Filter by Category", categories, key="inv_cat_filter")

            filtered = all_prods if cat_filter == "All" else [p for p in all_prods if p['category'] == cat_filter]

            # Build dataframe
            df = pd.DataFrame(filtered)
            df = df.rename(columns={
                'name': 'Name', 'category': 'Category', 'size': 'Size',
                'sell_price': f'Wholesale Price ({currency})',
                'stock_qty': 'In Stock', 'low_stock_alert': 'Alert Level'
            })
            display_cols = ['Name', 'Category', 'Size', f'Wholesale Price ({currency})', 'In Stock', 'Alert Level']

            # Highlight low stock
            def highlight_low(row):
                if row['In Stock'] <= row['Alert Level']:
                    return ['background-color: #ffe0e0'] * len(row)
                return [''] * len(row)

            st.dataframe(
                df[display_cols].style.apply(highlight_low, axis=1),
                use_container_width=True,
                height=500
            )

            # Summary
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Products", len(all_prods))
            total_value = sum(p['sell_price'] * p['stock_qty'] for p in all_prods)
            c2.metric(f"Total Stock Value ({currency})", f"{total_value:,.0f}")
            c3.metric("Low Stock Items", len(low))

    # ── Add Product ──
    with inv_tab2:
        st.markdown('<div class="section-title">➕ Add New Product</div>', unsafe_allow_html=True)
        with st.form("add_product_form", clear_on_submit=True):
            c1, c2 = st.columns(2)
            prod_name = c1.text_input("Product Name *", placeholder="e.g. Guinness Stout")
            prod_cat = c2.selectbox("Category *", [
                "Beer", "Malt Drink", "Soft Drink", "Water", "Spirit", "Whisky",
                "Wine", "Energy Drink", "Juice", "Dairy Drink", "Local Drink", "Other"
            ])
            c3, c4 = st.columns(2)
            prod_size = c3.text_input("Size *", placeholder="e.g. 330ml Bottle")
            prod_alert = c4.number_input("Low Stock Alert (units)", min_value=1, value=12)

            c5, c6 = st.columns(2)
            prod_sell = c5.number_input(f"Wholesale Price ({currency})", min_value=0.0, value=0.0, step=0.5)
            prod_stock = c6.number_input("Opening Stock (units)", min_value=0, value=0)

            submitted = st.form_submit_button("✅ Add Product", use_container_width=True, type="primary")
            if submitted:
                if prod_name and prod_size:
                    conn = get_db()
                    conn.execute(
                        "INSERT INTO products (name, category, size, buy_price, sell_price, stock_qty, low_stock_alert) VALUES (?,?,?,?,?,?,?)",
                        (prod_name.strip(), prod_cat, prod_size.strip(), 0, prod_sell, prod_stock, prod_alert)
                    )
                    conn.commit()
                    conn.close()
                    st.success(f"✅ '{prod_name} ({prod_size})' added successfully!")
                else:
                    st.error("Please fill in Product Name and Size.")

    # ── Update Stock ──
    with inv_tab3:
        st.markdown('<div class="section-title">✏️ Update Existing Product</div>', unsafe_allow_html=True)

        all_prods = get_all_products()
        if all_prods:
            prod_options = {f"{p['name']} ({p['size']}) — Stock: {p['stock_qty']}": p['id'] for p in all_prods}
            selected_label = st.selectbox("Select Product to Update", list(prod_options.keys()), key="update_prod_sel")
            selected_id = prod_options[selected_label]
            prod = get_product_by_id(selected_id)

            if prod:
                with st.form("update_product_form"):
                    c1, c2 = st.columns(2)
                    new_name = c1.text_input("Name", value=prod['name'])
                    new_cat = c2.selectbox("Category", [
                        "Beer", "Malt Drink", "Soft Drink", "Water", "Spirit", "Whisky",
                        "Wine", "Energy Drink", "Juice", "Dairy Drink", "Local Drink", "Other"
                    ], index=["Beer","Malt Drink","Soft Drink","Water","Spirit","Whisky","Wine","Energy Drink","Juice","Dairy Drink","Local Drink","Other"].index(prod['category']) if prod['category'] in ["Beer","Malt Drink","Soft Drink","Water","Spirit","Whisky","Wine","Energy Drink","Juice","Dairy Drink","Local Drink","Other"] else 0)
                    c3, c4 = st.columns(2)
                    new_size = c3.text_input("Size", value=prod['size'])
                    new_alert = c4.number_input("Low Stock Alert", min_value=1, value=prod['low_stock_alert'])
                    c5, c6 = st.columns(2)
                    new_sell = c5.number_input(f"Wholesale Price ({currency})", min_value=0.0, value=float(prod['sell_price']), step=0.5)
                    new_stock = c6.number_input("Stock Quantity", min_value=0, value=int(prod['stock_qty']))

                    col_save, col_del = st.columns(2)
                    save = col_save.form_submit_button("💾 Save Changes", use_container_width=True, type="primary")
                    delete = col_del.form_submit_button("🗑️ Deactivate Product", use_container_width=True)

                    if save:
                        conn = get_db()
                        conn.execute(
                            "UPDATE products SET name=?, category=?, size=?, sell_price=?, stock_qty=?, low_stock_alert=? WHERE id=?",
                            (new_name, new_cat, new_size, new_sell, new_stock, new_alert, selected_id)
                        )
                        conn.commit()
                        conn.close()
                        st.success("✅ Product updated!")
                        st.rerun()

                    if delete:
                        conn = get_db()
                        conn.execute("UPDATE products SET active=0 WHERE id=?", (selected_id,))
                        conn.commit()
                        conn.close()
                        st.success("Product deactivated.")
                        st.rerun()

                # Quick stock top-up
                st.markdown("**⚡ Quick Stock Top-Up**")
                col_a, col_b = st.columns([2, 1])
                add_qty = col_a.number_input("Add Units to Stock", min_value=1, value=24, key="topup_qty")
                if col_b.button("➕ Add Stock", use_container_width=True, type="primary"):
                    update_stock(selected_id, add_qty)
                    st.success(f"✅ Added {add_qty} units to {prod['name']} ({prod['size']})")
                    st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
#  TAB 3: DAMAGED GOODS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_damage:
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
with tab_reports:
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
with tab_settings:
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
        total_products = conn.execute("SELECT COUNT(*) FROM products WHERE active=1").fetchone()[0]
        total_sales = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
        total_rev = conn.execute("SELECT COALESCE(SUM(subtotal),0) FROM sales").fetchone()[0]
        total_damage = conn.execute("SELECT COALESCE(SUM(total_loss),0) FROM damaged_goods").fetchone()[0]
        conn.close()

        st.metric("Products in Catalogue", total_products)
        st.metric("Total Transactions", total_sales)
        st.metric(f"Lifetime Revenue ({get_setting('currency','GHS')})", f"{total_rev:,.2f}")
        st.metric(f"Total Damage Losses ({get_setting('currency','GHS')})", f"{total_damage:,.2f}")

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
        st.caption("Use this to load new drinks added in an app update. Your existing products, prices and stock are never changed.")
        if st.button("🔄 Sync New Products from Latest Update", use_container_width=True):
            added = sync_new_products()
            if added > 0:
                st.success(f"✅ {added} new product(s) added to your catalogue!")
            else:
                st.info("✅ Your catalogue is already up to date — no new products to add.")
            st.rerun()

        st.markdown("---")
        st.markdown("**ℹ️ App Info**")
        st.info("De-Nod's Drinks Manager v1.0\nBuilt for De-Nod's Wholesale Drinks, Ghana.\nData stored locally in denods.db")
