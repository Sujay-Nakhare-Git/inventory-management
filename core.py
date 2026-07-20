import os
import io
import csv
import sqlite3
import hashlib
import hmac
import json
import urllib.error
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify, g, session, Response
)
from werkzeug.utils import secure_filename

try:
    from PIL import Image, UnidentifiedImageError
except ImportError:
    Image = None
    UnidentifiedImageError = Exception

app = Flask(__name__)
app.secret_key = os.urandom(32)
IST = ZoneInfo("Asia/Kolkata")


def now_ist():
    return datetime.now(IST)


@app.template_filter("billdate")
def _format_bill_date(value):
    if not value:
        return ""
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[: len(fmt) + 4], fmt).strftime("%d-%b-%Y")
        except ValueError:
            continue
    return text


@app.template_filter("istdatetime")
def _format_ist_datetime(value):
    if not value:
        return ""

    dt = None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return ""
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"

        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            for fmt in (
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
            ):
                try:
                    dt = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue

    if dt is None:
        return str(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)

    return dt.astimezone(IST).strftime("%d-%b-%Y %I:%M %p IST")


@app.template_filter("istdate")
def _format_ist_date(value):
    if not value:
        return ""

    dt = None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return ""
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"

        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            for fmt in (
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
            ):
                try:
                    dt = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue

    if dt is None:
        return str(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)

    return dt.astimezone(IST).strftime("%d-%b-%Y")

DATABASE = os.path.join(app.root_path, "boutique.db")
EXPENSE_BILL_UPLOAD_DIR = os.path.join(app.root_path, "static", "expense_bills")
ALLOWED_BILL_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp"}
PRODUCT_IMAGE_UPLOAD_DIR = os.path.join(app.root_path, "static", "product_images")
ALLOWED_PRODUCT_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "gif"}
MAX_IMAGE_SIZE = (1600, 1600)
WHATSAPP_CONFIG_PATH = os.path.join(app.root_path, "instance", "whatsapp_config.json")


def load_whatsapp_cloud_config():
    token = os.getenv("WHATSAPP_CLOUD_API_TOKEN", "").strip()
    phone_number_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID", "").strip()
    graph_version = os.getenv("WHATSAPP_GRAPH_VERSION", "v22.0").strip() or "v22.0"
    if token and phone_number_id:
        return token, phone_number_id, graph_version

    try:
        with open(WHATSAPP_CONFIG_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh) or {}
    except (OSError, json.JSONDecodeError):
        data = {}

    file_token = str(data.get("token", "")).strip()
    file_phone_number_id = str(data.get("phone_number_id", "")).strip()
    file_graph_version = str(data.get("graph_version", "v22.0")).strip() or "v22.0"
    return file_token, file_phone_number_id, file_graph_version

os.makedirs(EXPENSE_BILL_UPLOAD_DIR, exist_ok=True)
os.makedirs(PRODUCT_IMAGE_UPLOAD_DIR, exist_ok=True)
os.makedirs(os.path.dirname(WHATSAPP_CONFIG_PATH), exist_ok=True)


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
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
            customer_name TEXT,
            customer_phone TEXT,
            subtotal REAL NOT NULL DEFAULT 0,
            discount_percent REAL NOT NULL DEFAULT 0,
            discount_amount REAL NOT NULL DEFAULT 0,
            tax_percent REAL NOT NULL DEFAULT 0,
            tax_amount REAL NOT NULL DEFAULT 0,
            total REAL NOT NULL DEFAULT 0,
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
    """)

    # Seed default categories if empty
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

    db.execute(
        "INSERT OR IGNORE INTO counters (name, value) VALUES ('bill_number', 0)"
    )

    if "include_in_pl" not in expense_columns:
        db.execute("ALTER TABLE expenses ADD COLUMN include_in_pl INTEGER NOT NULL DEFAULT 1")

    # Backfill customers table from existing bills + store_credits (one-time, only if empty)
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


with app.app_context():
    init_db()


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
    db.execute("UPDATE counters SET value = value + 1 WHERE name = 'bill_number'")
    row = db.execute(
        "SELECT value FROM counters WHERE name = 'bill_number'"
    ).fetchone()
    return f"G{row['value']:03d}"


ALLOWED_PAYMENT_METHODS = {"Cash", "UPI", "Card", "Bank Transfer"}


def _row_get(row, key, default=None):
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (TypeError, KeyError, IndexError):
        return default


def parse_bill_payment_breakdown(bill):
    breakdown_text = _row_get(bill, "payment_breakdown_json", "")
    parsed = []
    if breakdown_text:
        try:
            raw = json.loads(breakdown_text)
            if isinstance(raw, list):
                for entry in raw:
                    if not isinstance(entry, dict):
                        continue
                    method = str(entry.get("method", "")).strip()
                    if method not in ALLOWED_PAYMENT_METHODS:
                        continue
                    try:
                        amount = round(float(entry.get("amount", 0)), 2)
                    except (TypeError, ValueError):
                        continue
                    if amount <= 0:
                        continue
                    parsed.append({"method": method, "amount": amount})
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = []

    if parsed:
        return parsed

    total = round(float(_row_get(bill, "total", 0) or 0), 2)
    payment_method = str(_row_get(bill, "payment_method", "") or "").strip()
    if total > 0 and payment_method:
        return [{"method": payment_method, "amount": total}]
    return []


def _insert_exchange_bill(db, source_bill, exchange_items):
    exchange_subtotal = round(
        sum(item["exchange_line_total"] for item in exchange_items),
        2,
    )
    exchange_bill_number = get_next_bill_number(db)
    cursor = db.execute(
        "INSERT INTO bills (bill_number, customer_name, customer_phone, subtotal, "
        "discount_percent, discount_amount, tax_percent, tax_amount, total, "
        "payment_method, payment_breakdown_json, store_credit_used, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','+5 hours','+30 minutes'))",
        (
            exchange_bill_number,
            source_bill["customer_name"],
            source_bill["customer_phone"],
            exchange_subtotal,
            0,
            0,
            0,
            0,
            exchange_subtotal,
            "Exchange",
            None,
            0,
        ),
    )
    exchange_bill_id = cursor.lastrowid
    upsert_customer(db, source_bill["customer_name"], source_bill["customer_phone"])

    for item in exchange_items:
        db.execute(
            "INSERT INTO bill_items (bill_id, product_id, product_name, "
            "quantity, unit_price, total_price) VALUES (?, ?, ?, ?, ?, ?)",
            (
                exchange_bill_id,
                item["exchange_product_id"],
                item["exchange_product_name"],
                item["quantity"],
                item["exchange_unit_price"],
                item["exchange_line_total"],
            ),
        )

    return exchange_bill_id, exchange_bill_number, exchange_subtotal


def display_bill_ref(bill):
    bill_number = None
    bill_id = None

    if isinstance(bill, dict):
        bill_number = bill.get("bill_number")
        bill_id = bill.get("id")
    else:
        try:
            bill_number = bill["bill_number"]
        except (TypeError, KeyError, IndexError):
            bill_number = None
        try:
            bill_id = bill["id"]
        except (TypeError, KeyError, IndexError):
            bill_id = None

    if bill_number:
        return bill_number
    if bill_id is not None:
        return f"#{bill_id}"
    return "-"


def normalize_phone_for_whatsapp_cloud(raw_phone):
    digits = "".join(ch for ch in str(raw_phone or "") if ch.isdigit())
    if len(digits) == 10:
        return f"91{digits}"
    if len(digits) == 12 and digits.startswith("91"):
        return digits
    if len(digits) == 13 and digits.startswith("091"):
        return digits[1:]
    return None


def send_whatsapp_text_message(to_phone, body):
    cloud_api_token, phone_number_id, graph_version = load_whatsapp_cloud_config()
    if not (cloud_api_token and phone_number_id):
        return {"sent": False, "reason": "not_configured"}

    payload = json.dumps(
        {
            "messaging_product": "whatsapp",
            "to": to_phone,
            "type": "text",
            "text": {"preview_url": False, "body": body},
        }
    ).encode("utf-8")
    endpoint = (
        f"https://graph.facebook.com/{graph_version}/"
        f"{phone_number_id}/messages"
    )
    request_obj = urllib.request.Request(endpoint, data=payload, method="POST")
    request_obj.add_header(
        "Authorization",
        f"Bearer {cloud_api_token}",
    )
    request_obj.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(request_obj, timeout=12) as response:
            response_body = response.read().decode("utf-8", errors="replace")
            response_json = json.loads(response_body)
            message_id = ""
            messages = response_json.get("messages", [])
            if messages and isinstance(messages, list):
                message_id = messages[0].get("id", "")
            return {
                "sent": True,
                "reason": "sent",
                "sid": message_id,
            }
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        app.logger.warning("WhatsApp API HTTP error for %s: %s", to_phone, error_body)
        return {
            "sent": False,
            "reason": "send_failed",
            "error": error_body,
        }
    except Exception as exc:
        app.logger.warning("WhatsApp send failed for %s: %s", to_phone, exc)
        return {"sent": False, "reason": "send_failed", "error": str(exc)}


def send_whatsapp_bill_message(customer_phone, customer_name, bill_number, total):
    if not customer_phone:
        return {"sent": False, "reason": "missing_phone"}

    to_phone = normalize_phone_for_whatsapp_cloud(customer_phone)
    if not to_phone:
        return {"sent": False, "reason": "invalid_phone"}

    safe_name = (customer_name or "Customer").strip() or "Customer"
    body = (
        f"Namaste {safe_name}, your bill {bill_number} has been generated at "
        f"Gulmohar by Ankita. Total amount: Rs {total:.2f}. Thank you for shopping with us."
    )
    return send_whatsapp_text_message(to_phone, body)


@app.context_processor
def inject_bill_helpers():
    return {"display_bill_ref": display_bill_ref}


# ── Helpers ──────────────────────────────────────────────────────────────
def log_update(title, description, update_type="general"):
    db = get_db()
    db.execute(
        "INSERT INTO updates (title, description, type, created_at) "
        "VALUES (?, ?, ?, datetime('now','+5 hours','+30 minutes'))",
        (title, description, update_type),
    )
    db.commit()


def allowed_bill_image(filename):
    return (
        "." in filename and
        filename.rsplit(".", 1)[1].lower() in ALLOWED_BILL_IMAGE_EXTENSIONS
    )


def save_expense_bill_image(uploaded_file, title):
    if not uploaded_file or not uploaded_file.filename:
        return None

    if not allowed_bill_image(uploaded_file.filename):
        return None

    safe_title = secure_filename(title) or "expense"
    extension = uploaded_file.filename.rsplit(".", 1)[1].lower()
    timestamp = now_ist().strftime("%Y%m%d%H%M%S%f")
    filename = f"{safe_title}_{timestamp}.{extension}"
    saved_path = os.path.join(EXPENSE_BILL_UPLOAD_DIR, filename)
    save_optimized_image(uploaded_file, saved_path, extension)
    return f"expense_bills/{filename}"


def allowed_product_image(filename):
    return (
        "." in filename and
        filename.rsplit(".", 1)[1].lower() in ALLOWED_PRODUCT_IMAGE_EXTENSIONS
    )


def save_product_image(uploaded_file, product_name):
    if not uploaded_file or not uploaded_file.filename:
        return None

    if not allowed_product_image(uploaded_file.filename):
        return None

    safe_name = secure_filename(product_name) or "product"
    extension = uploaded_file.filename.rsplit(".", 1)[1].lower()
    timestamp = now_ist().strftime("%Y%m%d%H%M%S%f")
    filename = f"{safe_name}_{timestamp}.{extension}"
    saved_path = os.path.join(PRODUCT_IMAGE_UPLOAD_DIR, filename)
    save_optimized_image(uploaded_file, saved_path, extension)
    return filename


def save_optimized_image(uploaded_file, saved_path, extension):
    if Image is None:
        uploaded_file.save(saved_path)
        return

    format_map = {
        "jpg": "JPEG",
        "jpeg": "JPEG",
        "png": "PNG",
        "webp": "WEBP",
        "gif": "GIF",
    }
    image_format = format_map.get(extension, "JPEG")

    try:
        uploaded_file.stream.seek(0)
        with Image.open(uploaded_file.stream) as img:
            resample = Image.Resampling.LANCZOS if hasattr(Image, "Resampling") else Image.LANCZOS
            img.thumbnail(MAX_IMAGE_SIZE, resample)

            save_kwargs = {}
            if image_format == "JPEG":
                if img.mode not in ("RGB", "L"):
                    img = img.convert("RGB")
                save_kwargs = {"quality": 82, "optimize": True}
            elif image_format == "WEBP":
                if img.mode not in ("RGB", "RGBA"):
                    img = img.convert("RGBA" if "A" in img.getbands() else "RGB")
                save_kwargs = {"quality": 80, "method": 6}
            elif image_format == "PNG":
                save_kwargs = {"optimize": True, "compress_level": 7}
            elif image_format == "GIF" and img.mode not in ("P", "L"):
                img = img.convert("P", palette=Image.ADAPTIVE)
                save_kwargs = {"optimize": True}

            img.save(saved_path, format=image_format, **save_kwargs)
    except (UnidentifiedImageError, OSError, ValueError):
        uploaded_file.stream.seek(0)
        uploaded_file.save(saved_path)


ADMIN_PASSWORD_HASH = "d1215baec4cf39b5c9cc710527fbbfcb3d4290caaf9b0f095d32198c9d5e28aa"
ADMIN_IDLE_TIMEOUT_SECONDS = 20 * 60


def _current_ts():
    return int(now_ist().timestamp())


def mark_admin_session_active():
    session["admin_last_activity_ts"] = _current_ts()


def establish_admin_session():
    session["admin_authenticated"] = True
    mark_admin_session_active()


def clear_admin_session():
    session.pop("admin_authenticated", None)
    session.pop("admin_last_activity_ts", None)
    session.pop("pl_authenticated", None)


def admin_authenticated():
    if not session.get("admin_authenticated", False):
        return False

    last_active = session.get("admin_last_activity_ts")
    try:
        last_active = int(last_active)
    except (TypeError, ValueError):
        last_active = 0

    now_ts = _current_ts()
    if last_active <= 0 or (now_ts - last_active) > ADMIN_IDLE_TIMEOUT_SECONDS:
        clear_admin_session()
        session["admin_timeout_notice"] = True
        return False

    mark_admin_session_active()
    return True


def get_triggered_low_stock_alerts(db):
    """Return configured low-stock alerts whose current stock is at or below
    the configured threshold.

    Each alert is defined by category + size + threshold count. The current
    stock is the total quantity of products matching that category and size.
    """
    alerts = db.execute(
        "SELECT a.id, a.category_id, a.size, a.threshold, c.name AS category_name "
        "FROM low_stock_alerts a "
        "LEFT JOIN categories c ON c.id = a.category_id "
        "ORDER BY c.name, a.size"
    ).fetchall()

    triggered = []
    for alert in alerts:
        size = alert["size"] or ""
        if size:
            current = db.execute(
                "SELECT COALESCE(SUM(quantity), 0) FROM products "
                "WHERE category_id = ? AND size = ?",
                (alert["category_id"], size),
            ).fetchone()[0]
        else:
            current = db.execute(
                "SELECT COALESCE(SUM(quantity), 0) FROM products "
                "WHERE category_id = ? AND (size IS NULL OR TRIM(size) = '')",
                (alert["category_id"],),
            ).fetchone()[0]

        if current <= alert["threshold"]:
            triggered.append({
                "id": alert["id"],
                "category_name": alert["category_name"] or "Uncategorized",
                "size": size or "No Size",
                "threshold": alert["threshold"],
                "current": current,
            })
    return triggered


