"""
DataClerk OpenEnv — SQLite database setup and seeding.

Uses a fixed random seed (42) so every run produces identical data
and task graders remain deterministic.
"""

from __future__ import annotations

import os
import random
import sqlite3
from datetime import datetime, timedelta

DB_PATH: str = os.environ.get("DB_PATH", "/tmp/dataclerk.db")

# ─────────────────────────────────────────────
#  Schema
# ─────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS customers (
    id          INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL,
    email       TEXT    UNIQUE NOT NULL,
    city        TEXT,
    country     TEXT,
    tier        TEXT    DEFAULT 'standard',
    created_at  DATE    NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    id             INTEGER PRIMARY KEY,
    name           TEXT    NOT NULL,
    category       TEXT    NOT NULL,
    base_price     REAL    NOT NULL,
    stock_quantity INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS orders (
    id            INTEGER PRIMARY KEY,
    customer_id   INTEGER NOT NULL,
    status        TEXT    NOT NULL DEFAULT 'completed',
    total_amount  REAL    NOT NULL,
    created_at    DATE    NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE IF NOT EXISTS order_items (
    id          INTEGER PRIMARY KEY,
    order_id    INTEGER NOT NULL,
    product_id  INTEGER NOT NULL,
    quantity    INTEGER NOT NULL,
    unit_price  REAL    NOT NULL,
    FOREIGN KEY (order_id)   REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE IF NOT EXISTS support_tickets (
    id           INTEGER PRIMARY KEY,
    customer_id  INTEGER NOT NULL,
    category     TEXT    NOT NULL,
    priority     TEXT    NOT NULL,
    status       TEXT    NOT NULL,
    created_at   DATE    NOT NULL,
    resolved_at  DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);
"""

# ─────────────────────────────────────────────
#  Seed data
# ─────────────────────────────────────────────

_CITIES = [
    ("New York", "US"), ("Los Angeles", "US"), ("Chicago", "US"),
    ("London", "UK"), ("Manchester", "UK"),
    ("Berlin", "DE"), ("Munich", "DE"),
    ("Paris", "FR"),
    ("Tokyo", "JP"), ("Osaka", "JP"),
    ("Sydney", "AU"),
    ("Toronto", "CA"),
    ("Mumbai", "IN"), ("Bangalore", "IN"),
]

_TIERS = ["standard", "premium", "enterprise"]
_TIER_WEIGHTS = [0.60, 0.30, 0.10]

_CATEGORIES_PRODUCTS: dict[str, list[tuple[str, float]]] = {
    "Electronics": [
        ("Laptop Pro 15", 1299.99),
        ("Wireless Noise-Cancelling Headphones", 199.99),
        ("Smart Watch Series 5", 299.99),
        ("Tablet 10-inch", 499.99),
        ("Mechanical Keyboard", 149.99),
        ("Webcam 4K", 119.99),
        ("USB-C Hub 7-port", 59.99),
        ("Phone Case Premium", 29.99),
    ],
    "Clothing": [
        ("Running Shoes Pro", 89.99),
        ("Insulated Winter Jacket", 179.99),
        ("Slim-Fit Denim Jeans", 69.99),
        ("Organic Cotton T-Shirt 3-pack", 34.99),
        ("High-Support Sports Bra", 49.99),
        ("Lightweight Casual Sneakers", 74.99),
        ("Merino Wool Sweater", 99.99),
        ("Waterproof Hiking Boots", 139.99),
    ],
    "Food & Beverage": [
        ("Whey Protein Powder 2kg", 54.99),
        ("Single-Origin Coffee Beans 500g", 22.99),
        ("Premium Green Tea Set", 27.99),
        ("Protein Energy Bars 24-pack", 39.99),
        ("Daily Multivitamin Pack 90ct", 32.99),
        ("Raw Organic Honey 500g", 16.99),
        ("Collagen Supplement 60ct", 44.99),
    ],
    "Sports": [
        ("Premium Yoga Mat 6mm", 44.99),
        ("Adjustable Dumbbell Set 40kg", 129.99),
        ("Speed Jump Rope", 24.99),
        ("Resistance Bands Set 5-level", 29.99),
        ("Insulated Water Bottle 1L", 34.99),
        ("Durable Gym Bag 40L", 59.99),
        ("Foam Roller High-Density", 39.99),
    ],
    "Home & Garden": [
        ("HEPA Air Purifier Large Room", 249.99),
        ("Ceramic Plant Pot Set 3-pc", 39.99),
        ("LED Desk Lamp with USB", 54.99),
        ("Stackable Storage Organizer", 34.99),
        ("12-cup Programmable Coffee Maker", 89.99),
        ("High-Speed Blender 1200W", 79.99),
        ("Bamboo Cutting Board Set", 29.99),
    ],
}

_ORDER_STATUSES = ["completed"] * 8 + ["refunded"] * 1 + ["pending"] * 1
_TICKET_CATEGORIES = ["billing", "technical", "shipping", "returns", "general"]
_TICKET_PRIORITIES = ["low", "medium", "high", "urgent"]
_TICKET_PRI_WEIGHTS = [0.25, 0.40, 0.25, 0.10]
_TICKET_STATUSES = ["open", "in_progress", "resolved", "closed"]
_TICKET_STATUS_WEIGHTS = [0.15, 0.10, 0.45, 0.30]

# Resolution days per priority (used to seed resolved_at)
_RESOLUTION_DAYS: dict[str, tuple[int, int]] = {
    "urgent": (1, 3),
    "high": (2, 7),
    "medium": (4, 14),
    "low": (7, 21),
}

# ─────────────────────────────────────────────
#  Public API
# ─────────────────────────────────────────────

def seed_database(db_path: str = DB_PATH) -> None:
    """Create the database schema and insert deterministic seed data."""
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_SQL)

    # Skip if already populated
    if conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0] > 0:
        conn.close()
        return

    rng = random.Random(42)  # fixed seed → deterministic answers
    today = datetime(2025, 6, 15)  # fixed "today" for reproducibility

    def days_ago(n: int) -> str:
        return (today - timedelta(days=n)).strftime("%Y-%m-%d")

    # ── Customers (200) ──────────────────────────────────────────────────────
    customers = []
    for i in range(1, 201):
        city, country = rng.choice(_CITIES)
        tier = rng.choices(_TIERS, weights=_TIER_WEIGHTS)[0]
        created = days_ago(rng.randint(60, 900))
        customers.append(
            (i, f"Customer_{i:03d}", f"user{i}@example.com", city, country, tier, created)
        )
    conn.executemany(
        "INSERT OR IGNORE INTO customers VALUES (?,?,?,?,?,?,?)", customers
    )

    # ── Products (37) ────────────────────────────────────────────────────────
    products = []
    pid = 1
    for category, items in _CATEGORIES_PRODUCTS.items():
        for name, price in items:
            stock = rng.randint(0, 300)
            products.append((pid, name, category, price, stock))
            pid += 1
    conn.executemany("INSERT OR IGNORE INTO products VALUES (?,?,?,?,?)", products)

    # ── Orders + items (1 800 orders) ────────────────────────────────────────
    orders: list[tuple] = []
    order_items: list[tuple] = []
    oid = 1
    iid = 1

    # Spread orders over last 400 days; heavier in recent 180 days
    all_customer_ids = list(range(1, 201))

    for _ in range(1800):
        cid = rng.choice(all_customer_ids)
        # ~60 % of orders in last 180 days
        if rng.random() < 0.60:
            days_back = rng.randint(0, 179)
        else:
            days_back = rng.randint(180, 400)
        order_date = days_ago(days_back)
        status = rng.choice(_ORDER_STATUSES)

        n_items = rng.randint(1, 4)
        selected = rng.sample(products, n_items)

        total = 0.0
        for prod in selected:
            qty = rng.randint(1, 3)
            price = round(prod[3] * rng.uniform(0.92, 1.08), 2)
            total += qty * price
            order_items.append((iid, oid, prod[0], qty, price))
            iid += 1

        orders.append((oid, cid, status, round(total, 2), order_date))
        oid += 1

    conn.executemany(
        "INSERT OR IGNORE INTO orders VALUES (?,?,?,?,?)", orders
    )
    conn.executemany(
        "INSERT OR IGNORE INTO order_items VALUES (?,?,?,?,?)", order_items
    )

    # ── Support tickets (600) ────────────────────────────────────────────────
    tickets: list[tuple] = []
    for tid in range(1, 601):
        cid = rng.randint(1, 200)
        cat = rng.choice(_TICKET_CATEGORIES)
        pri = rng.choices(_TICKET_PRIORITIES, weights=_TICKET_PRI_WEIGHTS)[0]
        status = rng.choices(_TICKET_STATUSES, weights=_TICKET_STATUS_WEIGHTS)[0]
        created_days = rng.randint(0, 270)
        created_str = days_ago(created_days)

        resolved_str = None
        if status in ("resolved", "closed"):
            lo, hi = _RESOLUTION_DAYS[pri]
            res_days = rng.randint(lo, hi)
            resolved_dt = datetime.strptime(created_str, "%Y-%m-%d") + timedelta(days=res_days)
            resolved_str = resolved_dt.strftime("%Y-%m-%d")

        tickets.append((tid, cid, cat, pri, status, created_str, resolved_str))

    conn.executemany(
        "INSERT OR IGNORE INTO support_tickets VALUES (?,?,?,?,?,?,?)", tickets
    )

    conn.commit()
    conn.close()


def get_schema_summary(db_path: str = DB_PATH) -> dict[str, list[str]]:
    """Return {table: ["col (TYPE)", …]} for all tables."""
    conn = sqlite3.connect(db_path)
    tables = ["customers", "products", "orders", "order_items", "support_tickets"]
    summary: dict[str, list[str]] = {}
    for table in tables:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        summary[table] = [f"{r[1]} ({r[2]})" for r in rows]
    conn.close()
    return summary
