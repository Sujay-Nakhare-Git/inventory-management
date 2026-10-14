import sqlite3
from flask import g
from config import (
    DATABASE, IST, EXPENSE_BILL_UPLOAD_DIR, PRODUCT_IMAGE_UPLOAD_DIR,
    load_whatsapp_cloud_config
)


def get_db():
    """Get or create the per-request SQLite database connection."""
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


def close_db(exc):
    """Close the database connection at end of request."""
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    """Initialize the database schema and run migrations."""
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            sku_code TEXT
        );

        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category_id INTEGER,
            sku TEXT UNIQUE,
            size TEXT,
            color TEXT,
            cost_price REAL NOT NULL DEFAULT 0,
            selling_price REAL NOT NULL DEFAULT 0,
            quantity INTEGER NOT NULL DEFAULT 0,
            low_stock_threshold INTEGER NOT NULL DEFAULT 5,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes')),
            updated_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes')),
            FOREIGN KEY (category_id) REFERENCES categories(id)
        );

        CREATE TABLE IF NOT EXISTS bills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bill_number TEXT UNIQUE,
            bill_type TEXT NOT NULL DEFAULT 'sale',
            customer_name TEXT,
            customer_phone TEXT,
            subtotal REAL NOT NULL DEFAULT 0,
            rental_days INTEGER NOT NULL DEFAULT 1,
            discount_percent REAL NOT NULL DEFAULT 0,
            discount_amount REAL NOT NULL DEFAULT 0,
            tax_percent REAL NOT NULL DEFAULT 0,
            tax_amount REAL NOT NULL DEFAULT 0,
            total REAL NOT NULL DEFAULT 0,
            rent_amount REAL NOT NULL DEFAULT 0,
            deposit_amount REAL NOT NULL DEFAULT 0,
            deposit_returned REAL NOT NULL DEFAULT 0,
            deposit_returned_at TEXT,
            payment_method TEXT DEFAULT 'Cash',
            payment_breakdown_json TEXT,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes'))
        );

        CREATE TABLE IF NOT EXISTS bill_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bill_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            unit_price REAL NOT NULL,
            discount_percent REAL NOT NULL DEFAULT 0,
            discount_amount REAL NOT NULL DEFAULT 0,
            total_price REAL NOT NULL,
            FOREIGN KEY (bill_id) REFERENCES bills(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS updates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            type TEXT NOT NULL DEFAULT 'general',
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes'))
        );

        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            vendor TEXT,
            description TEXT,
            category TEXT NOT NULL DEFAULT 'General',
            amount REAL NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes'))
        );

        CREATE TABLE IF NOT EXISTS refunds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bill_id INTEGER,
            customer_name TEXT,
            type TEXT NOT NULL DEFAULT 'refund',
            reason TEXT,
            refund_amount REAL NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes')),
            FOREIGN KEY (bill_id) REFERENCES bills(id)
        );

        CREATE TABLE IF NOT EXISTS refund_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            refund_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            unit_price REAL NOT NULL,
            action TEXT NOT NULL DEFAULT 'refund',
            exchange_product_id INTEGER,
            exchange_product_name TEXT,
            FOREIGN KEY (refund_id) REFERENCES refunds(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS store_credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_name TEXT NOT NULL,
            customer_phone TEXT NOT NULL UNIQUE,
            balance REAL NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes')),
            updated_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes'))
        );

        CREATE TABLE IF NOT EXISTS credit_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            credit_id INTEGER NOT NULL,
            bill_id INTEGER,
            amount REAL NOT NULL DEFAULT 0,
            transaction_type TEXT NOT NULL,
            notes TEXT,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes')),
            FOREIGN KEY (credit_id) REFERENCES store_credits(id),
            FOREIGN KEY (bill_id) REFERENCES bills(id)
        );

        CREATE TABLE IF NOT EXISTS investments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT NOT NULL,
            amount REAL NOT NULL DEFAULT 0,
            investment_date TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes'))
        );

        CREATE TABLE IF NOT EXISTS counters (
            name TEXT PRIMARY KEY,
            value INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS vendors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            contact_person TEXT,
            phone TEXT,
            email TEXT,
            address TEXT,
            notes TEXT,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes')),
            updated_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes'))
        );

        CREATE TABLE IF NOT EXISTS low_stock_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL,
            size TEXT NOT NULL DEFAULT '',
            threshold INTEGER NOT NULL DEFAULT 5,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes')),
            updated_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes')),
            UNIQUE (category_id, size),
            FOREIGN KEY (category_id) REFERENCES categories(id)
        );

        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT NOT NULL UNIQUE,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes')),
            updated_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes'))
        );

        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            discount_percent REAL NOT NULL DEFAULT 0,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes')),
            updated_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes'))
        );

        CREATE TABLE IF NOT EXISTS sale_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes')),
            FOREIGN KEY (sale_id) REFERENCES sales(id),
            FOREIGN KEY (product_id) REFERENCES products(id),
            UNIQUE (sale_id, product_id)
        );

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            is_superadmin INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes')),
            last_login_at TEXT
        );

        CREATE TABLE IF NOT EXISTS user_permissions (
            user_id INTEGER NOT NULL,
            permission_key TEXT NOT NULL,
            PRIMARY KEY (user_id, permission_key),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS dashboard_notes (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            content TEXT NOT NULL DEFAULT '',
            updated_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes'))
        );

        CREATE TABLE IF NOT EXISTS whatsapp_webhook_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL,
            message_id TEXT,
            customer_phone TEXT,
            message_type TEXT,
            message_text TEXT,
            status TEXT,
            event_timestamp TEXT,
            payload_json TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now','+5 hours','+30 minutes'))
        );

        INSERT OR IGNORE INTO dashboard_notes (id, content) VALUES (1, '');
    """)

    count = db.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
    if count == 0:
        for cat in ["Sarees", "Kurtis", "Lehengas", "Suits", "Dupattas",
                     "Blouses", "Accessories", "Western Wear", "Kids Wear", "Others"]:
            db.execute("INSERT INTO categories (name) VALUES (?)", (cat,))

    category_columns = {
        row["name"] for row in db.execute("PRAGMA table_info(categories)").fetchall()
    }
    if "sku_code" not in category_columns:
        db.execute("ALTER TABLE categories ADD COLUMN sku_code TEXT")

    product_columns = {
        row["name"] for row in db.execute("PRAGMA table_info(products)").fetchall()
    }
    if "image_filename" not in product_columns:
        db.execute("ALTER TABLE products ADD COLUMN image_filename TEXT")
    if "product_group_id" not in product_columns:
        db.execute("ALTER TABLE products ADD COLUMN product_group_id INTEGER")
    if "vendor_id" not in product_columns:
        db.execute("ALTER TABLE products ADD COLUMN vendor_id INTEGER")

    expense_columns = {
        row["name"] for row in db.execute("PRAGMA table_info(expenses)").fetchall()
    }
    if "vendor" not in expense_columns:
        db.execute("ALTER TABLE expenses ADD COLUMN vendor TEXT")
    if "bill_image_path" not in expense_columns:
        db.execute("ALTER TABLE expenses ADD COLUMN bill_image_path TEXT")
    if "payment_mode" not in expense_columns:
        db.execute("ALTER TABLE expenses ADD COLUMN payment_mode TEXT NOT NULL DEFAULT 'Cash'")

    bills_columns = {
        row["name"] for row in db.execute("PRAGMA table_info(bills)").fetchall()
    }
    if "store_credit_used" not in bills_columns:
        db.execute("ALTER TABLE bills ADD COLUMN store_credit_used REAL DEFAULT 0")
    if "bill_number" not in bills_columns:
        db.execute("ALTER TABLE bills ADD COLUMN bill_number TEXT")
    if "payment_breakdown_json" not in bills_columns:
        db.execute("ALTER TABLE bills ADD COLUMN payment_breakdown_json TEXT")
    if "bill_type" not in bills_columns:
        db.execute("ALTER TABLE bills ADD COLUMN bill_type TEXT NOT NULL DEFAULT 'sale'")
    if "rent_amount" not in bills_columns:
        db.execute("ALTER TABLE bills ADD COLUMN rent_amount REAL NOT NULL DEFAULT 0")
    if "deposit_amount" not in bills_columns:
        db.execute("ALTER TABLE bills ADD COLUMN deposit_amount REAL NOT NULL DEFAULT 0")
    if "deposit_returned" not in bills_columns:
        db.execute("ALTER TABLE bills ADD COLUMN deposit_returned REAL NOT NULL DEFAULT 0")
    if "deposit_returned_at" not in bills_columns:
        db.execute("ALTER TABLE bills ADD COLUMN deposit_returned_at TEXT")
    if "rental_days" not in bills_columns:
        db.execute("ALTER TABLE bills ADD COLUMN rental_days INTEGER NOT NULL DEFAULT 1")

    bill_item_columns = {
        row["name"] for row in db.execute("PRAGMA table_info(bill_items)").fetchall()
    }
    if "discount_percent" not in bill_item_columns:
        db.execute("ALTER TABLE bill_items ADD COLUMN discount_percent REAL NOT NULL DEFAULT 0")
    if "discount_amount" not in bill_item_columns:
        db.execute("ALTER TABLE bill_items ADD COLUMN discount_amount REAL NOT NULL DEFAULT 0")

    db.execute("INSERT OR IGNORE INTO counters (name, value) VALUES ('bill_number', 0)")

    if "include_in_pl" not in expense_columns:
        db.execute("ALTER TABLE expenses ADD COLUMN include_in_pl INTEGER NOT NULL DEFAULT 1")

    customer_count = db.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    if customer_count == 0:
        bill_customers = db.execute(
            "SELECT TRIM(customer_phone) AS phone, "
            "(SELECT TRIM(b2.customer_name) FROM bills b2 "
            " WHERE TRIM(b2.customer_phone) = TRIM(b1.customer_phone) "
            " ORDER BY b2.created_at DESC LIMIT 1) AS name "
            "FROM bills b1 "
            "WHERE customer_phone IS NOT NULL AND TRIM(customer_phone) != '' "
            "GROUP BY TRIM(customer_phone)"
        ).fetchall()
        for row in bill_customers:
            db.execute(
                "INSERT OR IGNORE INTO customers (name, phone, created_at, updated_at) "
                "VALUES (?, ?, datetime('now','+5 hours','+30 minutes'), datetime('now','+5 hours','+30 minutes'))",
                (row["name"] or "Walk-in", row["phone"]),
            )
        credit_customers = db.execute(
            "SELECT customer_name, customer_phone FROM store_credits "
            "WHERE customer_phone IS NOT NULL AND TRIM(customer_phone) != ''"
        ).fetchall()
        for row in credit_customers:
            db.execute(
                "INSERT OR IGNORE INTO customers (name, phone, created_at, updated_at) "
                "VALUES (?, ?, datetime('now','+5 hours','+30 minutes'), datetime('now','+5 hours','+30 minutes'))",
                (row["customer_name"] or "Walk-in", row["customer_phone"]),
            )

    db.commit()


def upsert_customer(db, name, phone):
    """Create or update a customer record keyed by phone. No-op if phone is blank."""
    phone = (phone or "").strip()
    name = (name or "").strip()
    if not phone:
        return
    existing = db.execute("SELECT id FROM customers WHERE phone = ?", (phone,)).fetchone()
    if existing:
        if name:
            db.execute(
                "UPDATE customers SET name = ?, updated_at = datetime('now','+5 hours','+30 minutes') "
                "WHERE phone = ?",
                (name, phone),
            )
    else:
        db.execute(
            "INSERT INTO customers (name, phone, created_at, updated_at) "
            "VALUES (?, ?, datetime('now','+5 hours','+30 minutes'), datetime('now','+5 hours','+30 minutes'))",
            (name or "Walk-in", phone),
        )


def get_next_bill_number(db):
    """Atomically increment and return the next bill number."""
    db.execute("UPDATE counters SET value = value + 1 WHERE name = 'bill_number'")
    row = db.execute("SELECT value FROM counters WHERE name = 'bill_number'").fetchone()
    return f"G{row['value']:03d}"
