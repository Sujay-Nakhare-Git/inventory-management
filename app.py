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

    db.commit()


with app.app_context():
    init_db()


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


def admin_authenticated():
    return session.get("admin_authenticated", False)


# ── Dashboard ────────────────────────────────────────────────────────────
@app.route("/")
def dashboard():
    db = get_db()
    total_products = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    total_stock = db.execute("SELECT COALESCE(SUM(quantity),0) FROM products").fetchone()[0]
    available_items = db.execute(
        "SELECT COALESCE(SUM(quantity),0) FROM products WHERE quantity > 0"
    ).fetchone()[0]
    low_stock = db.execute(
        "SELECT COUNT(*) FROM products WHERE quantity <= low_stock_threshold"
    ).fetchone()[0]
    today = now_ist().strftime("%Y-%m-%d")
    today_sales = db.execute(
        "SELECT COALESCE(SUM(total),0) FROM bills WHERE created_at LIKE ?",
        (f"{today}%",),
    ).fetchone()[0]
    total_bills = db.execute("SELECT COUNT(*) FROM bills").fetchone()[0]
    recent_updates = db.execute(
        "SELECT * FROM updates ORDER BY created_at DESC LIMIT 10"
    ).fetchall()
    low_stock_products = db.execute(
        "SELECT p.*, c.name as category_name FROM products p "
        "LEFT JOIN categories c ON p.category_id = c.id "
        "WHERE p.quantity <= p.low_stock_threshold ORDER BY p.quantity ASC LIMIT 10"
    ).fetchall()

    return render_template(
        "dashboard.html",
        total_products=total_products,
        total_stock=total_stock,
        available_items=available_items,
        low_stock=low_stock,
        today_sales=today_sales,
        total_bills=total_bills,
        recent_updates=recent_updates,
        low_stock_products=low_stock_products,
    )


# ── Inventory ────────────────────────────────────────────────────────────
@app.route("/inventory")
def inventory():
    db = get_db()
    search = request.args.get("search", "").strip()
    category_id = request.args.get("category", "")
    size_filter = request.args.get("size", "")
    vendor_filter = request.args.get("vendor", "")
    in_stock_only = request.args.get("in_stock", "")
    query = (
        "SELECT p.*, c.name as category_name, v.name as vendor_name FROM products p "
        "LEFT JOIN categories c ON p.category_id = c.id "
        "LEFT JOIN vendors v ON p.vendor_id = v.id WHERE 1=1"
    )
    params = []
    if search:
        query += " AND (p.name LIKE ? OR p.sku LIKE ?)"
        params += [f"%{search}%", f"%{search}%"]
    if category_id:
        query += " AND p.category_id = ?"
        params.append(category_id)
    if size_filter:
        if size_filter == "No Size":
            query += " AND (p.size IS NULL OR p.size = '')"
        else:
            query += " AND p.size = ?"
            params.append(size_filter)
    if vendor_filter:
        if vendor_filter == "No Vendor":
            query += " AND p.vendor_id IS NULL"
        else:
            query += " AND p.vendor_id = ?"
            params.append(vendor_filter)
    if in_stock_only:
        query += " AND p.quantity > 0"
    query += " ORDER BY p.updated_at DESC"
    products = db.execute(query, params).fetchall()
    categories = db.execute("SELECT * FROM categories ORDER BY name").fetchall()
    all_sizes = db.execute(
        "SELECT DISTINCT COALESCE(NULLIF(TRIM(size), ''), 'No Size') as name "
        "FROM products ORDER BY name"
    ).fetchall()
    vendors = db.execute("SELECT id, name FROM vendors ORDER BY name").fetchall()
    return render_template(
        "inventory.html", products=products, categories=categories,
        search=search, selected_category=category_id,
        all_sizes=all_sizes, vendors=vendors,
        selected_size=size_filter, selected_vendor=vendor_filter,
        in_stock_only=in_stock_only,
    )


@app.route("/inventory/add", methods=["GET", "POST"])
def add_product():
    db = get_db()
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        category_id = request.form.get("category_id") or None
        vendor_id = request.form.get("vendor_id") or None
        # Multi-size selection: if checkboxes selected, create one entry per size
        selected_sizes = request.form.getlist("sizes")
        custom_size = request.form.get("size", "").strip()
        color = request.form.get("color", "").strip()
        cost_price = float(request.form.get("cost_price", 0))
        selling_price = float(request.form.get("selling_price", 0))
        quantity = int(request.form.get("quantity", 1))
        low_stock_threshold = int(request.form.get("low_stock_threshold", 5))
        product_image = request.files.get("image")

        # Validate image once if provided
        image_bytes_cache = None
        if product_image and product_image.filename:
            if not allowed_product_image(product_image.filename):
                flash("Product image must be PNG, JPG, JPEG, WEBP, or GIF.", "error")
                return redirect(url_for("add_product"))

        # Decide list of sizes to create entries for
        if selected_sizes:
            sizes_to_create = selected_sizes
        else:
            sizes_to_create = [custom_size]  # may be empty string

        created_skus = []
        created_ids = []
        first_image_filename = None
        for idx, size in enumerate(sizes_to_create):
            sku = generate_sku(db, category_id) if category_id else None
            entry_name = name if name else (sku or "")

            # Save image for first entry; reuse same filename for subsequent
            image_filename = None
            if product_image and product_image.filename:
                if idx == 0:
                    # Reset stream pointer (Flask FileStorage)
                    try:
                        product_image.stream.seek(0)
                    except Exception:
                        pass
                    first_image_filename = save_product_image(product_image, entry_name)
                image_filename = first_image_filename

            cur = db.execute(
                "INSERT INTO products (name, category_id, sku, size, color, "
                "cost_price, selling_price, quantity, low_stock_threshold, image_filename, "
                "vendor_id, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "datetime('now','+5 hours','+30 minutes'), datetime('now','+5 hours','+30 minutes'))",
                (entry_name, category_id, sku, size, color, cost_price,
                 selling_price, quantity, low_stock_threshold, image_filename, vendor_id),
            )
            created_ids.append(cur.lastrowid)
            created_skus.append(sku or entry_name)

        # Auto-group all sizes together when multiple entries were created
        if len(created_ids) > 1:
            group_id = created_ids[0]
            for pid in created_ids:
                db.execute(
                    "UPDATE products SET product_group_id = ? WHERE id = ?",
                    (group_id, pid),
                )
        db.commit()

        if len(created_skus) > 1:
            log_update(
                "Products Added",
                f"Added {len(created_skus)} sized entries: {', '.join(created_skus)} — Qty each: {quantity}",
                "inventory",
            )
            flash(
                f"Created {len(created_skus)} inventory entries ({', '.join(created_skus)}).",
                "success",
            )
        else:
            log_update(
                "Product Added",
                f"Added '{created_skus[0]}' — Qty: {quantity}, Price: ₹{selling_price}",
                "inventory",
            )
            flash(f"Product '{created_skus[0]}' added successfully!", "success")
        return redirect(url_for("inventory"))

    categories = db.execute("SELECT * FROM categories ORDER BY name").fetchall()
    vendors = db.execute("SELECT * FROM vendors ORDER BY name").fetchall()
    return render_template("product_form.html", product=None, categories=categories, vendors=vendors)


@app.route("/inventory/edit/<int:product_id>", methods=["GET", "POST"])
def edit_product(product_id):
    db = get_db()
    product = db.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("inventory"))

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        category_id = request.form.get("category_id") or None
        vendor_id = request.form.get("vendor_id") or None
        # Re-generate SKU if category changed, or use manual SKU if edited
        manual_sku = request.form.get("sku", "").strip()
        old_category_id = str(product["category_id"]) if product["category_id"] else None
        if manual_sku and manual_sku != product["sku"]:
            sku = manual_sku
        elif category_id != old_category_id:
            sku = generate_sku(db, category_id) if category_id else None
        else:
            sku = product["sku"]
        size = request.form.get("size", "").strip()
        color = request.form.get("color", "").strip()
        cost_price = float(request.form.get("cost_price", 0))
        selling_price = float(request.form.get("selling_price", 0))
        quantity = int(request.form.get("quantity", 1))
        low_stock_threshold = int(request.form.get("low_stock_threshold", 5))
        remove_image = request.form.get("remove_image") == "1"
        product_image = request.files.get("image")

        # Use SKU as the display name if no name provided
        if not name and sku:
            name = sku
        elif not name:
            name = product["name"]

        image_filename = product["image_filename"]
        if remove_image and image_filename:
            old_path = os.path.join(PRODUCT_IMAGE_UPLOAD_DIR, image_filename)
            if os.path.exists(old_path):
                os.remove(old_path)
            image_filename = None

        if product_image and product_image.filename:
            if not allowed_product_image(product_image.filename):
                flash("Product image must be PNG, JPG, JPEG, WEBP, or GIF.", "error")
                return redirect(url_for("edit_product", product_id=product_id))
            if image_filename:
                old_path = os.path.join(PRODUCT_IMAGE_UPLOAD_DIR, image_filename)
                if os.path.exists(old_path):
                    os.remove(old_path)
            image_filename = save_product_image(product_image, name)

        db.execute(
            "UPDATE products SET name=?, category_id=?, sku=?, size=?, color=?, "
            "cost_price=?, selling_price=?, quantity=?, low_stock_threshold=?, image_filename=?, "
            "vendor_id=?, updated_at=datetime('now','+5 hours','+30 minutes') WHERE id=?",
            (name, category_id, sku, size, color, cost_price,
             selling_price, quantity, low_stock_threshold, image_filename, vendor_id, product_id),
        )
        db.commit()
        log_update("Product Updated", f"Updated '{name}'", "inventory")
        flash(f"Product '{name}' updated!", "success")
        return redirect(url_for("inventory"))

    categories = db.execute("SELECT * FROM categories ORDER BY name").fetchall()
    vendors = db.execute("SELECT * FROM vendors ORDER BY name").fetchall()
    # Fetch current size variants for display in edit form
    variants = []
    if product and product["product_group_id"]:
        variants = db.execute(
            "SELECT id, sku, name, size, color, quantity FROM products "
            "WHERE product_group_id = ? AND id != ? ORDER BY size",
            (product["product_group_id"], product_id),
        ).fetchall()
    return render_template("product_form.html", product=product, categories=categories,
                           variants=variants, vendors=vendors)


@app.route("/inventory/delete/<int:product_id>", methods=["POST"])
def delete_product(product_id):
    db = get_db()
    product = db.execute(
        "SELECT name, image_filename FROM products WHERE id = ?",
        (product_id,),
    ).fetchone()
    if product:
        if product["image_filename"]:
            image_path = os.path.join(PRODUCT_IMAGE_UPLOAD_DIR, product["image_filename"])
            if os.path.exists(image_path):
                os.remove(image_path)
        db.execute("DELETE FROM products WHERE id = ?", (product_id,))
        db.commit()
        log_update("Product Deleted", f"Deleted '{product['name']}'", "inventory")
        flash(f"Product '{product['name']}' deleted.", "success")
    return redirect(url_for("inventory"))


@app.route("/inventory/bulk-assign-vendor", methods=["POST"])
def bulk_assign_vendor():
    db = get_db()
    product_ids = request.form.getlist("product_ids")
    vendor_id = request.form.get("vendor_id", "").strip()

    if not product_ids:
        flash("No products selected.", "error")
        return redirect(url_for("inventory"))

    try:
        ids = [int(pid) for pid in product_ids]
    except ValueError:
        flash("Invalid product selection.", "error")
        return redirect(url_for("inventory"))

    vendor_value = None
    vendor_label = "No Vendor"
    if vendor_id:
        vendor = db.execute(
            "SELECT id, name FROM vendors WHERE id = ?", (vendor_id,)
        ).fetchone()
        if not vendor:
            flash("Selected vendor not found.", "error")
            return redirect(url_for("inventory"))
        vendor_value = vendor["id"]
        vendor_label = vendor["name"]

    placeholders = ",".join("?" for _ in ids)
    db.execute(
        f"UPDATE products SET vendor_id = ?, updated_at = datetime('now','+5 hours','+30 minutes') "
        f"WHERE id IN ({placeholders})",
        [vendor_value, *ids],
    )
    db.commit()
    log_update(
        "Vendor Assigned",
        f"Set vendor to '{vendor_label}' for {len(ids)} product(s)",
        "inventory",
    )
    flash(f"Assigned vendor '{vendor_label}' to {len(ids)} product(s).", "success")
    return redirect(url_for("inventory"))


# ── SKU Generation ────────────────────────────────────────────────────────
def generate_sku(db, category_id):
    """Generate next sequential unique SKU for a category."""
    cat = db.execute("SELECT sku_code FROM categories WHERE id = ?", (category_id,)).fetchone()
    if not cat or not cat["sku_code"]:
        return None
    prefix = cat["sku_code"]
    # Find the highest numeric suffix across all products with this prefix
    rows = db.execute(
        "SELECT sku FROM products WHERE sku LIKE ?", (f"{prefix}-%",)
    ).fetchall()
    max_num = 0
    for r in rows:
        try:
            num = int(r["sku"].split("-")[-1])
            if num > max_num:
                max_num = num
        except (ValueError, IndexError):
            pass
    next_num = max_num + 1
    sku = f"{prefix}-{next_num:03d}"
    # Ensure uniqueness (in case of manual entries or edge cases)
    while db.execute("SELECT 1 FROM products WHERE sku = ?", (sku,)).fetchone():
        next_num += 1
        sku = f"{prefix}-{next_num:03d}"
    return sku


@app.route("/api/next-sku/<int:category_id>")
def next_sku(category_id):
    db = get_db()
    sku = generate_sku(db, category_id)
    return jsonify({"sku": sku or ""})


@app.route("/api/product/<int:product_id>/variants")
def product_variants(product_id):
    db = get_db()
    product = db.execute(
        "SELECT product_group_id FROM products WHERE id = ?", (product_id,)
    ).fetchone()
    if not product or not product["product_group_id"]:
        return jsonify({"variants": []})
    variants = db.execute(
        "SELECT p.id, p.sku, p.name, p.size, p.color, p.quantity, p.low_stock_threshold "
        "FROM products p WHERE p.product_group_id = ? AND p.id != ? ORDER BY p.size",
        (product["product_group_id"], product_id),
    ).fetchall()
    return jsonify({"variants": [dict(v) for v in variants]})


@app.route("/inventory/<int:product_id>/link-variant", methods=["POST"])
def link_variant(product_id):
    db = get_db()
    target_id_raw = request.form.get("link_product_id", "").strip()
    if not target_id_raw:
        flash("No product selected to link.", "error")
        return redirect(url_for("edit_product", product_id=product_id))

    # Accept SKU or numeric ID
    if target_id_raw.isdigit():
        target = db.execute(
            "SELECT id, product_group_id FROM products WHERE id = ?", (int(target_id_raw),)
        ).fetchone()
    else:
        target = db.execute(
            "SELECT id, product_group_id FROM products WHERE sku = ?", (target_id_raw,)
        ).fetchone()

    if not target:
        flash("Product not found.", "error")
        return redirect(url_for("edit_product", product_id=product_id))
    if target["id"] == product_id:
        flash("Cannot link a product to itself.", "error")
        return redirect(url_for("edit_product", product_id=product_id))

    current = db.execute(
        "SELECT product_group_id FROM products WHERE id = ?", (product_id,)
    ).fetchone()

    # Determine the group_id to use: prefer existing group, else use current product's id
    existing_group = current["product_group_id"] or target["product_group_id"]
    new_group_id = existing_group if existing_group else product_id

    # If both already have groups, merge — move all members of target's group into current's group
    if current["product_group_id"] and target["product_group_id"] and \
            current["product_group_id"] != target["product_group_id"]:
        db.execute(
            "UPDATE products SET product_group_id = ? WHERE product_group_id = ?",
            (new_group_id, target["product_group_id"]),
        )

    # Set group on both products (and all current group members)
    if current["product_group_id"]:
        db.execute(
            "UPDATE products SET product_group_id = ? WHERE product_group_id = ?",
            (new_group_id, current["product_group_id"]),
        )
    db.execute(
        "UPDATE products SET product_group_id = ? WHERE id = ?",
        (new_group_id, product_id),
    )
    db.execute(
        "UPDATE products SET product_group_id = ? WHERE id = ?",
        (new_group_id, target["id"]),
    )
    db.commit()
    flash("Products linked as size variants.", "success")
    return redirect(url_for("edit_product", product_id=product_id))


@app.route("/inventory/<int:product_id>/unlink-variant", methods=["POST"])
def unlink_variant(product_id):
    db = get_db()
    product = db.execute(
        "SELECT product_group_id FROM products WHERE id = ?", (product_id,)
    ).fetchone()
    if not product or not product["product_group_id"]:
        flash("Product is not in a variant group.", "error")
        return redirect(url_for("edit_product", product_id=product_id))

    group_id = product["product_group_id"]
    db.execute(
        "UPDATE products SET product_group_id = NULL WHERE id = ?", (product_id,)
    )
    # If only one member remains, clear their group too
    remaining = db.execute(
        "SELECT id FROM products WHERE product_group_id = ?", (group_id,)
    ).fetchall()
    if len(remaining) == 1:
        db.execute(
            "UPDATE products SET product_group_id = NULL WHERE id = ?",
            (remaining[0]["id"],),
        )
    db.commit()
    flash("Product removed from variant group.", "success")
    return redirect(url_for("edit_product", product_id=product_id))



@app.route("/categories", methods=["GET", "POST"])
def categories():
    if not admin_authenticated():
        flash("Please unlock Admin to manage Categories.", "error")
        return redirect(url_for("admin", next=url_for("categories")))
    db = get_db()
    if request.method == "POST":
        name = request.form["name"].strip()
        sku_code = request.form.get("sku_code", "").strip().upper() or None
        if name:
            db.execute(
                "INSERT OR IGNORE INTO categories (name, sku_code) VALUES (?, ?)",
                (name, sku_code),
            )
            db.commit()
            flash(f"Category '{name}' added!", "success")
        return redirect(url_for("categories"))
    cats = db.execute(
        "SELECT c.*, COUNT(p.id) as product_count FROM categories c "
        "LEFT JOIN products p ON c.id = p.category_id GROUP BY c.id ORDER BY c.name"
    ).fetchall()
    return render_template("categories.html", categories=cats)


@app.route("/categories/edit/<int:cat_id>", methods=["POST"])
def edit_category(cat_id):
    db = get_db()
    name = request.form["name"].strip()
    sku_code = request.form.get("sku_code", "").strip().upper() or None

    if not name:
        flash("Category name is required.", "error")
        return redirect(url_for("categories"))

    old_cat = db.execute("SELECT sku_code FROM categories WHERE id = ?", (cat_id,)).fetchone()
    old_sku_code = old_cat["sku_code"] if old_cat else None

    db.execute(
        "UPDATE categories SET name = ?, sku_code = ? WHERE id = ?",
        (name, sku_code, cat_id),
    )

    # Update all product SKUs if category SKU code changed
    if sku_code != old_sku_code:
        products = db.execute(
            "SELECT id, sku FROM products WHERE category_id = ? ORDER BY id",
            (cat_id,),
        ).fetchall()
        for i, p in enumerate(products, 1):
            new_sku = f"{sku_code}-{i:03d}" if sku_code else None
            db.execute("UPDATE products SET sku = ? WHERE id = ?", (new_sku, p["id"]))

    db.commit()
    flash("Category updated.", "success")
    return redirect(url_for("categories"))


@app.route("/categories/delete/<int:cat_id>", methods=["POST"])
def delete_category(cat_id):
    db = get_db()
    db.execute("DELETE FROM categories WHERE id = ?", (cat_id,))
    db.commit()
    flash("Category deleted.", "success")
    return redirect(url_for("categories"))


# ── Billing ──────────────────────────────────────────────────────────────
@app.route("/billing")
def billing():
    db = get_db()
    products = db.execute(
        "SELECT p.*, c.name as category_name FROM products p "
        "LEFT JOIN categories c ON p.category_id = c.id "
        "WHERE p.quantity > 0 ORDER BY p.name"
    ).fetchall()
    return render_template("billing.html", products=products)


@app.route("/api/products")
def api_products():
    db = get_db()
    products = db.execute(
        "SELECT p.*, c.name as category_name FROM products p "
        "LEFT JOIN categories c ON p.category_id = c.id "
        "WHERE p.quantity > 0 ORDER BY p.name"
    ).fetchall()
    return jsonify([dict(p) for p in products])


@app.route("/api/customers/search")
def api_customers_search():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    db = get_db()
    like = f"%{q}%"
    rows = db.execute(
        """
        WITH all_customers AS (
            SELECT
                TRIM(COALESCE(customer_name, '')) AS name,
                TRIM(COALESCE(customer_phone, '')) AS phone,
                created_at AS last_seen
            FROM bills
            WHERE customer_phone IS NOT NULL AND TRIM(customer_phone) != ''
            UNION ALL
            SELECT
                TRIM(COALESCE(customer_name, '')) AS name,
                TRIM(COALESCE(customer_phone, '')) AS phone,
                updated_at AS last_seen
            FROM store_credits
            WHERE customer_phone IS NOT NULL AND TRIM(customer_phone) != ''
        ),
        agg AS (
            SELECT
                phone,
                MAX(name) AS name,
                MAX(last_seen) AS last_seen,
                COUNT(*) AS visit_count
            FROM all_customers
            GROUP BY phone
        )
        SELECT agg.phone, agg.name, agg.last_seen, agg.visit_count,
               sc.id AS credit_id, sc.balance AS credit_balance
        FROM agg
        LEFT JOIN store_credits sc ON sc.customer_phone = agg.phone
        WHERE agg.phone LIKE ? OR agg.name LIKE ?
        ORDER BY agg.last_seen DESC
        LIMIT 8
        """,
        (like, like),
    ).fetchall()

    return jsonify([
        {
            "name": r["name"] or "",
            "phone": r["phone"],
            "visit_count": r["visit_count"],
            "last_seen": r["last_seen"],
            "credit_id": r["credit_id"],
            "credit_balance": r["credit_balance"] or 0,
        }
        for r in rows
    ])


@app.route("/api/billing", methods=["POST"])
def create_bill():
    data = request.get_json()
    if not data or not data.get("items"):
        return jsonify({"error": "No items provided"}), 400

    db = get_db()
    customer_name = data.get("customer_name", "").strip()
    customer_phone = data.get("customer_phone", "").strip()
    discount_amount_input = data.get("discount_amount", None)
    discount_percent_input = data.get("discount_percent", 0)
    tax_percent = float(data.get("tax_percent", 0))
    payment_method = data.get("payment_method", "Cash")
    payment_breakdown_raw = data.get("payment_breakdown", [])
    store_credit_id = data.get("store_credit_id")
    store_credit_amount = float(data.get("store_credit_amount", 0))

    subtotal = 0
    validated_items = []
    for item in data["items"]:
        product = db.execute(
            "SELECT * FROM products WHERE id = ?", (item["product_id"],)
        ).fetchone()
        if not product:
            return jsonify({"error": f"Product ID {item['product_id']} not found"}), 400
        qty = int(item["quantity"])
        if qty > product["quantity"]:
            return jsonify({
                "error": f"Insufficient stock for '{product['name']}'. Available: {product['quantity']}"
            }), 400
        line_total = product["selling_price"] * qty
        subtotal += line_total
        validated_items.append({
            "product_id": product["id"],
            "product_name": product["name"],
            "quantity": qty,
            "unit_price": product["selling_price"],
            "total_price": line_total,
        })

    try:
        if discount_amount_input in (None, ""):
            discount_percent = float(discount_percent_input or 0)
            discount_amount = round(subtotal * discount_percent / 100, 2)
        else:
            discount_amount = round(float(discount_amount_input), 2)
            discount_amount = max(0, min(discount_amount, subtotal))
            discount_percent = round((discount_amount / subtotal) * 100, 2) if subtotal else 0
    except (TypeError, ValueError):
        discount_percent = 0
        discount_amount = 0

    after_discount = subtotal - discount_amount
    tax_amount = round(after_discount * tax_percent / 100, 2)
    total = round(after_discount + tax_amount, 2)

    # Validate store credit if provided
    if store_credit_id:
        try:
            store_credit_id = int(store_credit_id)
            credit = db.execute(
                "SELECT * FROM store_credits WHERE id = ?", (store_credit_id,)
            ).fetchone()
            if not credit:
                return jsonify({"error": "Store credit not found"}), 400
            if credit["balance"] < store_credit_amount:
                return jsonify({"error": f"Insufficient store credit. Available: ₹{credit['balance']}"}), 400
            # Store credit gets deducted from total after tax
            store_credit_amount = round(min(store_credit_amount, total), 2)
            total = round(total - store_credit_amount, 2)
        except (ValueError, TypeError):
            store_credit_id = None
            store_credit_amount = 0
    else:
        store_credit_id = None
        store_credit_amount = 0

    payment_breakdown = []
    normalized_payment_method = (
        str(payment_method).strip() if str(payment_method).strip() in ALLOWED_PAYMENT_METHODS else "Cash"
    )

    if total > 0 and isinstance(payment_breakdown_raw, list):
        method_totals = {}
        for row in payment_breakdown_raw:
            if not isinstance(row, dict):
                continue
            method = str(row.get("method", "")).strip()
            if method not in ALLOWED_PAYMENT_METHODS:
                continue
            try:
                amount = round(float(row.get("amount", 0)), 2)
            except (TypeError, ValueError):
                continue
            if amount <= 0:
                continue
            method_totals[method] = round(method_totals.get(method, 0) + amount, 2)

        payment_breakdown = [
            {"method": method, "amount": amount}
            for method, amount in method_totals.items()
            if amount > 0
        ]

    if total > 0 and not payment_breakdown:
        payment_breakdown = [{"method": normalized_payment_method, "amount": round(total, 2)}]

    if total > 0:
        breakdown_total = round(sum(item["amount"] for item in payment_breakdown), 2)
        if abs(breakdown_total - round(total, 2)) > 0.05:
            return jsonify({"error": "Payment breakup must match bill total."}), 400

        if len(payment_breakdown) == 1:
            normalized_payment_method = payment_breakdown[0]["method"]
        else:
            normalized_payment_method = "Mixed"
    else:
        normalized_payment_method = "Store Credit"
        payment_breakdown = []

    payment_breakdown_json = json.dumps(payment_breakdown) if payment_breakdown else None

    bill_number = get_next_bill_number(db)
    cursor = db.execute(
        "INSERT INTO bills (bill_number, customer_name, customer_phone, subtotal, "
        "discount_percent, discount_amount, tax_percent, tax_amount, "
        "total, payment_method, payment_breakdown_json, store_credit_used, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','+5 hours','+30 minutes'))",
        (bill_number, customer_name, customer_phone, subtotal, discount_percent,
         discount_amount, tax_percent, tax_amount, total, normalized_payment_method,
         payment_breakdown_json, store_credit_amount),
    )
    bill_id = cursor.lastrowid

    for it in validated_items:
        db.execute(
            "INSERT INTO bill_items (bill_id, product_id, product_name, "
            "quantity, unit_price, total_price) VALUES (?, ?, ?, ?, ?, ?)",
            (bill_id, it["product_id"], it["product_name"],
             it["quantity"], it["unit_price"], it["total_price"]),
        )
        db.execute(
            "UPDATE products SET quantity = quantity - ?, "
            "updated_at = datetime('now','+5 hours','+30 minutes') WHERE id = ?",
            (it["quantity"], it["product_id"]),
        )

    # Record store credit transaction if used
    if store_credit_id and store_credit_amount > 0:
        db.execute(
            "INSERT INTO credit_transactions (credit_id, bill_id, amount, transaction_type, notes, created_at) "
            "VALUES (?, ?, ?, ?, ?, datetime('now','+5 hours','+30 minutes'))",
            (store_credit_id, bill_id, store_credit_amount, "debit", f"Used in Bill {bill_number}"),
        )
        db.execute(
            "UPDATE store_credits SET balance = balance - ?, updated_at = datetime('now','+5 hours','+30 minutes') WHERE id = ?",
            (store_credit_amount, store_credit_id),
        )

    db.commit()
    log_update(
        "New Bill Created",
        f"Bill {bill_number} — ₹{after_discount + tax_amount} ({normalized_payment_method})" +
        (f" — Store Credit: ₹{store_credit_amount}" if store_credit_amount > 0 else "") +
        f" — {customer_name or 'Walk-in'}",
        "billing",
    )

    whatsapp_status = send_whatsapp_bill_message(
        customer_phone=customer_phone,
        customer_name=customer_name,
        bill_number=bill_number,
        total=total,
    )

    return jsonify(
        {
            "bill_id": bill_id,
            "bill_number": bill_number,
            "total": total,
            "message": "Bill created!",
            "whatsapp_sent": whatsapp_status.get("sent", False),
            "whatsapp_reason": whatsapp_status.get("reason", "unknown"),
            "whatsapp_error": (whatsapp_status.get("error", "") or "")[:220],
        }
    )


@app.route("/bills")
def bills_list():
    if not admin_authenticated():
        flash("Please unlock Admin to access Bill History.", "error")
        return redirect(url_for("admin", next=url_for("bills_list")))

    db = get_db()
    search = request.args.get("search", "").strip()
    search_type = request.args.get("search_type", "all")
    
    query = "SELECT * FROM bills WHERE 1=1"
    params = []
    
    if search:
        if search_type == "phone":
            query += " AND customer_phone LIKE ?"
            params.append(f"%{search}%")
        elif search_type == "name":
            query += " AND customer_name LIKE ?"
            params.append(f"%{search}%")
        else:  # search_type == "all"
            query += " AND (customer_name LIKE ? OR customer_phone LIKE ?)"
            params.append(f"%{search}%")
            params.append(f"%{search}%")
    
    query += " ORDER BY created_at DESC"
    bills = db.execute(query, params).fetchall()
    
    return render_template("bills.html", bills=bills, search=search, search_type=search_type)


@app.route("/bills/<int:bill_id>")
def bill_detail(bill_id):
    if not admin_authenticated():
        flash("Please unlock Admin to view bill details.", "error")
        return redirect(url_for("admin", next=url_for("bill_detail", bill_id=bill_id)))

    db = get_db()
    bill = db.execute("SELECT * FROM bills WHERE id = ?", (bill_id,)).fetchone()
    if not bill:
        flash("Bill not found.", "error")
        return redirect(url_for("bills_list"))
    items = db.execute(
        "SELECT * FROM bill_items WHERE bill_id = ?", (bill_id,)
    ).fetchall()
    refunds = db.execute(
        "SELECT * FROM refunds WHERE bill_id = ? ORDER BY created_at DESC", (bill_id,)
    ).fetchall()
    payment_breakdown = parse_bill_payment_breakdown(bill)
    return render_template(
        "bill_detail.html",
        bill=bill,
        items=items,
        refunds=refunds,
        payment_breakdown=payment_breakdown,
    )


@app.route("/bills/<int:bill_id>/edit", methods=["GET", "POST"])
def edit_bill(bill_id):
    if not admin_authenticated():
        flash("Please unlock Admin to edit bills.", "error")
        return redirect(url_for("admin", next=url_for("bill_detail", bill_id=bill_id)))

    db = get_db()
    bill = db.execute("SELECT * FROM bills WHERE id = ?", (bill_id,)).fetchone()
    if not bill:
        flash("Bill not found.", "error")
        return redirect(url_for("bills_list"))

    if request.method == "GET":
        payment_breakdown = parse_bill_payment_breakdown(bill)
        breakdown_map = {m: 0.0 for m in ALLOWED_PAYMENT_METHODS}
        for entry in payment_breakdown:
            breakdown_map[entry["method"]] = entry["amount"]
        return render_template(
            "bill_edit.html",
            bill=bill,
            payment_breakdown=payment_breakdown,
            breakdown_map=breakdown_map,
            allowed_methods=sorted(ALLOWED_PAYMENT_METHODS),
        )

    customer_name = request.form.get("customer_name", "").strip()
    customer_phone = request.form.get("customer_phone", "").strip()
    mode = request.form.get("payment_mode", "single")
    total = round(float(bill["total"] or 0), 2)

    payment_breakdown = []
    if total > 0:
        if mode == "split":
            method_totals = {}
            for method in ALLOWED_PAYMENT_METHODS:
                try:
                    amount = round(float(request.form.get(f"pay_{method}", 0) or 0), 2)
                except (TypeError, ValueError):
                    amount = 0
                if amount > 0:
                    method_totals[method] = round(method_totals.get(method, 0) + amount, 2)
            payment_breakdown = [
                {"method": m, "amount": a} for m, a in method_totals.items() if a > 0
            ]
            if not payment_breakdown:
                flash("Add at least one split payment amount.", "error")
                return redirect(url_for("edit_bill", bill_id=bill_id))
            breakdown_total = round(sum(r["amount"] for r in payment_breakdown), 2)
            if abs(breakdown_total - total) > 0.05:
                flash(
                    f"Split payment total (₹{breakdown_total:.2f}) must match bill total (₹{total:.2f}).",
                    "error",
                )
                return redirect(url_for("edit_bill", bill_id=bill_id))
            payment_method = payment_breakdown[0]["method"] if len(payment_breakdown) == 1 else "Mixed"
        else:
            payment_method = request.form.get("payment_method", "Cash").strip()
            if payment_method not in ALLOWED_PAYMENT_METHODS:
                payment_method = "Cash"
            payment_breakdown = [{"method": payment_method, "amount": total}]
    else:
        payment_method = "Store Credit"
        payment_breakdown = []

    payment_breakdown_json = json.dumps(payment_breakdown) if payment_breakdown else None

    db.execute(
        "UPDATE bills SET customer_name = ?, customer_phone = ?, "
        "payment_method = ?, payment_breakdown_json = ? WHERE id = ?",
        (customer_name, customer_phone, payment_method, payment_breakdown_json, bill_id),
    )
    db.commit()

    log_update(
        "Bill Edited",
        f"Bill {bill['bill_number'] or '#' + str(bill_id)} — payment updated to {payment_method}",
        "billing",
    )
    flash("Bill details updated.", "success")
    return redirect(url_for("bill_detail", bill_id=bill_id))


@app.route("/bills/<int:bill_id>/thermal")
def bill_thermal_print(bill_id):
    if not admin_authenticated():
        flash("Please unlock Admin to print bill history.", "error")
        return redirect(url_for("admin", next=url_for("bill_detail", bill_id=bill_id)))

    db = get_db()
    bill = db.execute("SELECT * FROM bills WHERE id = ?", (bill_id,)).fetchone()
    if not bill:
        flash("Bill not found.", "error")
        return redirect(url_for("bills_list"))

    items = db.execute(
        """
        SELECT bi.*, COALESCE(p.sku, bi.product_name) AS item_label
        FROM bill_items bi
        LEFT JOIN products p ON p.id = bi.product_id
        WHERE bi.bill_id = ?
        """,
        (bill_id,),
    ).fetchall()
    payment_breakdown = parse_bill_payment_breakdown(bill)
    return render_template(
        "bill_thermal.html",
        bill=bill,
        items=items,
        payment_breakdown=payment_breakdown,
    )


@app.route("/bills/delete/<int:bill_id>", methods=["POST"])
def delete_bill(bill_id):
    if not admin_authenticated():
        flash("Please unlock Admin to delete bills.", "error")
        return redirect(url_for("admin", next=url_for("bills_list")))

    db = get_db()
    bill = db.execute("SELECT * FROM bills WHERE id = ?", (bill_id,)).fetchone()
    if not bill:
        flash("Bill not found.", "error")
        return redirect(url_for("bills_list"))

    # Restore stock for all items in this bill
    items = db.execute("SELECT * FROM bill_items WHERE bill_id = ?", (bill_id,)).fetchall()
    for item in items:
        db.execute(
            "UPDATE products SET quantity = quantity + ?, "
            "updated_at = datetime('now','+5 hours','+30 minutes') WHERE id = ?",
            (item["quantity"], item["product_id"]),
        )

    # Restore store credit if it was used
    if bill["store_credit_used"] and bill["store_credit_used"] > 0:
        transaction = db.execute(
            "SELECT credit_id FROM credit_transactions WHERE bill_id = ? AND transaction_type = 'debit'",
            (bill_id,)
        ).fetchone()
        if transaction:
            credit_id = transaction["credit_id"]
            db.execute(
                "UPDATE store_credits SET balance = balance + ?, updated_at = datetime('now','+5 hours','+30 minutes') WHERE id = ?",
                (bill["store_credit_used"], credit_id),
            )
            db.execute(
                "INSERT INTO credit_transactions (credit_id, bill_id, amount, transaction_type, notes, created_at) "
                "VALUES (?, ?, ?, ?, ?, datetime('now','+5 hours','+30 minutes'))",
                (credit_id, bill_id, bill["store_credit_used"], "credit", f"Restored from deleted Bill #{bill_id}"),
            )

    db.execute("DELETE FROM bill_items WHERE bill_id = ?", (bill_id,))
    db.execute("DELETE FROM refund_items WHERE refund_id IN (SELECT id FROM refunds WHERE bill_id = ?)", (bill_id,))
    db.execute("DELETE FROM refunds WHERE bill_id = ?", (bill_id,))
    db.execute("DELETE FROM credit_transactions WHERE bill_id = ?", (bill_id,))
    db.execute("DELETE FROM bills WHERE id = ?", (bill_id,))
    db.commit()

    log_update(
        "Bill Deleted",
        f"Bill #{bill_id} — ₹{bill['total']} deleted. Stock restored." +
        (f" Store Credit restored: ₹{bill['store_credit_used']}" if bill["store_credit_used"] > 0 else ""),
        "billing",
    )
    flash(f"Bill #{bill_id} deleted and stock restored.", "success")
    return redirect(url_for("bills_list"))


# ── Store Credits ────────────────────────────────────────────────────────
@app.route("/store-credits")
def store_credits():
    if not admin_authenticated():
        flash("Please unlock Admin to access Store Credits.", "error")
        return redirect(url_for("admin", next=url_for("store_credits")))

    db = get_db()
    search = request.args.get("search", "").strip()
    query = "SELECT * FROM store_credits WHERE 1=1"
    params = []
    if search:
        query += " AND (customer_name LIKE ? OR customer_phone LIKE ?)"
        params += [f"%{search}%", f"%{search}%"]
    query += " ORDER BY updated_at DESC"
    all_credits = db.execute(query, params).fetchall()
    return render_template("store_credits.html", credits=all_credits, search=search)


@app.route("/store-credits/add", methods=["POST"])
def add_store_credit():
    if not admin_authenticated():
        flash("Please unlock Admin to add store credit.", "error")
        return redirect(url_for("admin", next=url_for("store_credits")))

    db = get_db()
    customer_name = request.form.get("customer_name", "").strip()
    customer_phone = request.form.get("customer_phone", "").strip()
    balance = float(request.form.get("balance", 0))

    if not customer_name or not customer_phone or balance <= 0:
        flash("Please provide customer name, phone, and balance.", "error")
        return redirect(url_for("store_credits"))

    # Check if phone already exists
    existing = db.execute(
        "SELECT id FROM store_credits WHERE customer_phone = ?", (customer_phone,)
    ).fetchone()
    if existing:
        flash(f"Store credit for phone {customer_phone} already exists.", "error")
        return redirect(url_for("store_credits"))

    cursor = db.execute(
        "INSERT INTO store_credits (customer_name, customer_phone, balance, created_at, updated_at) "
        "VALUES (?, ?, ?, datetime('now','+5 hours','+30 minutes'), datetime('now','+5 hours','+30 minutes'))",
        (customer_name, customer_phone, round(balance, 2)),
    )
    credit_id = cursor.lastrowid

    # Record initial transaction
    db.execute(
        "INSERT INTO credit_transactions (credit_id, amount, transaction_type, notes, created_at) "
        "VALUES (?, ?, ?, ?, datetime('now','+5 hours','+30 minutes'))",
        (credit_id, balance, "credit", "Initial credit added"),
    )
    db.commit()

    log_update(
        "Store Credit Added",
        f"{customer_name} ({customer_phone}) — ₹{balance}",
        "store_credit",
    )
    flash(f"Store credit for {customer_name} (₹{balance}) added!", "success")
    return redirect(url_for("store_credits"))


@app.route("/api/store-credit/lookup/<phone>")
def lookup_store_credit(phone):
    db = get_db()
    credit = db.execute(
        "SELECT * FROM store_credits WHERE customer_phone = ?", (phone,)
    ).fetchone()
    if not credit:
        return jsonify({"found": False})
    return jsonify({
        "found": True,
        "id": credit["id"],
        "customer_name": credit["customer_name"],
        "customer_phone": credit["customer_phone"],
        "balance": credit["balance"],
    })


@app.route("/store-credits/<int:credit_id>/add-balance", methods=["POST"])
def add_credit_balance(credit_id):
    if not admin_authenticated():
        flash("Please unlock Admin to modify store credits.", "error")
        return redirect(url_for("store_credits"))

    db = get_db()
    credit = db.execute(
        "SELECT * FROM store_credits WHERE id = ?", (credit_id,)
    ).fetchone()
    if not credit:
        flash("Store credit not found.", "error")
        return redirect(url_for("store_credits"))

    amount = float(request.form.get("amount", 0))
    notes = request.form.get("notes", "").strip()

    if amount <= 0:
        flash("Please provide a valid amount.", "error")
        return redirect(url_for("store_credits"))

    db.execute(
        "UPDATE store_credits SET balance = balance + ?, updated_at = datetime('now','+5 hours','+30 minutes') WHERE id = ?",
        (round(amount, 2), credit_id),
    )
    db.execute(
        "INSERT INTO credit_transactions (credit_id, amount, transaction_type, notes, created_at) "
        "VALUES (?, ?, ?, ?, datetime('now','+5 hours','+30 minutes'))",
        (credit_id, amount, "credit", notes or "Balance added"),
    )
    db.commit()

    log_update(
        "Store Credit Added",
        f"{credit['customer_name']} — ₹{amount}" + (f" ({notes})" if notes else ""),
        "store_credit",
    )
    flash(f"₹{amount} added to {credit['customer_name']}'s store credit!", "success")
    return redirect(url_for("store_credits"))


@app.route("/store-credits/<int:credit_id>/transactions")
def credit_transactions(credit_id):
    if not admin_authenticated():
        flash("Please unlock Admin to view store credit details.", "error")
        return redirect(url_for("admin", next=url_for("credit_transactions", credit_id=credit_id)))

    db = get_db()
    credit = db.execute(
        "SELECT * FROM store_credits WHERE id = ?", (credit_id,)
    ).fetchone()
    if not credit:
        flash("Store credit not found.", "error")
        return redirect(url_for("store_credits"))

    transactions = db.execute(
        "SELECT * FROM credit_transactions WHERE credit_id = ? ORDER BY created_at DESC",
        (credit_id,)
    ).fetchall()

    return render_template(
        "credit_transactions.html",
        credit=credit,
        transactions=transactions,
    )


@app.route("/store-credits/<int:credit_id>/delete", methods=["POST"])
def delete_store_credit(credit_id):
    if not admin_authenticated():
        flash("Please unlock Admin to delete store credits.", "error")
        return redirect(url_for("store_credits"))

    db = get_db()
    credit = db.execute(
        "SELECT * FROM store_credits WHERE id = ?", (credit_id,)
    ).fetchone()
    if not credit:
        flash("Store credit not found.", "error")
        return redirect(url_for("store_credits"))

    db.execute("DELETE FROM credit_transactions WHERE credit_id = ?", (credit_id,))
    db.execute("DELETE FROM store_credits WHERE id = ?", (credit_id,))
    db.commit()

    log_update(
        "Store Credit Deleted",
        f"{credit['customer_name']} ({credit['customer_phone']})",
        "store_credit",
    )
    flash(f"Store credit for {credit['customer_name']} deleted.", "success")
    return redirect(url_for("store_credits"))


# ── Refunds & Exchanges ──────────────────────────────────────────────────
@app.route("/refunds")
def refunds_list():
    if not admin_authenticated():
        flash("Please unlock Admin to access refund details.", "error")
        return redirect(url_for("admin", next=url_for("refunds_list")))

    db = get_db()
    all_refunds = db.execute(
        "SELECT r.*, b.bill_number, "
        "(SELECT GROUP_CONCAT(ri.product_name, ', ') FROM refund_items ri WHERE ri.refund_id = r.id) as products "
        "FROM refunds r "
        "LEFT JOIN bills b ON b.id = r.bill_id "
        "ORDER BY r.created_at DESC"
    ).fetchall()
    return render_template("refunds.html", refunds=all_refunds)


@app.route("/refunds/new/<int:bill_id>")
def new_refund(bill_id):
    if not admin_authenticated():
        flash("Please unlock Admin to process refunds.", "error")
        return redirect(url_for("admin", next=url_for("refunds_list")))

    db = get_db()
    bill = db.execute("SELECT * FROM bills WHERE id = ?", (bill_id,)).fetchone()
    if not bill:
        flash("Bill not found.", "error")
        return redirect(url_for("bills_list"))
    items = db.execute(
        "SELECT * FROM bill_items WHERE bill_id = ?", (bill_id,)
    ).fetchall()
    # Products available for exchange
    products = db.execute(
        "SELECT p.*, c.name as category_name FROM products p "
        "LEFT JOIN categories c ON p.category_id = c.id "
        "WHERE p.quantity > 0 ORDER BY p.name"
    ).fetchall()
    return render_template("refund_form.html", bill=bill, items=items, products=products)


@app.route("/refunds/process", methods=["POST"])
def process_refund():
    if not admin_authenticated():
        flash("Please unlock Admin to process refunds.", "error")
        return redirect(url_for("admin", next=url_for("refunds_list")))

    db = get_db()
    bill_id = int(request.form["bill_id"])
    reason = request.form.get("reason", "").strip()

    bill = db.execute("SELECT * FROM bills WHERE id = ?", (bill_id,)).fetchone()
    if not bill:
        flash("Bill not found.", "error")
        return redirect(url_for("bills_list"))

    bill_items = db.execute(
        "SELECT * FROM bill_items WHERE bill_id = ?", (bill_id,)
    ).fetchall()

    refund_amount = 0
    store_credit_refund = 0
    exchange_bill_id = None
    exchange_bill_number = None
    processed_items = []

    for bi in bill_items:
        action = request.form.get(f"action_{bi['id']}", "keep")
        if action == "keep":
            continue

        qty = int(request.form.get(f"qty_{bi['id']}", 0))
        if qty <= 0 or qty > bi["quantity"]:
            continue

        item_refund = bi["unit_price"] * qty
        exchange_product_id = None
        exchange_product_name = None
        exchange_unit_price = None
        exchange_line_total = 0

        if action == "refund":
            # Return stock
            db.execute(
                "UPDATE products SET quantity = quantity + ?, "
                "updated_at = datetime('now','+5 hours','+30 minutes') WHERE id = ?",
                (qty, bi["product_id"]),
            )
            refund_amount += item_refund

        elif action == "store_credit":
            # Return stock
            db.execute(
                "UPDATE products SET quantity = quantity + ?, "
                "updated_at = datetime('now','+5 hours','+30 minutes') WHERE id = ?",
                (qty, bi["product_id"]),
            )
            store_credit_refund += item_refund

        elif action == "exchange":
            exchange_product_id = request.form.get(f"exchange_{bi['id']}")
            if not exchange_product_id:
                continue
            exchange_product_id = int(exchange_product_id)
            exchange_product = db.execute(
                "SELECT * FROM products WHERE id = ?", (exchange_product_id,)
            ).fetchone()
            if not exchange_product or exchange_product["quantity"] < qty:
                flash(f"Insufficient stock for exchange product.", "error")
                continue

            # Return original product to stock
            db.execute(
                "UPDATE products SET quantity = quantity + ?, "
                "updated_at = datetime('now','+5 hours','+30 minutes') WHERE id = ?",
                (qty, bi["product_id"]),
            )
            # Deduct exchange product from stock
            db.execute(
                "UPDATE products SET quantity = quantity - ?, "
                "updated_at = datetime('now','+5 hours','+30 minutes') WHERE id = ?",
                (qty, exchange_product_id),
            )
            exchange_product_name = exchange_product["name"]
            exchange_unit_price = exchange_product["selling_price"]
            exchange_line_total = round(exchange_unit_price * qty, 2)

            # Cheaper exchanges become store credit; the replacement bill is created below.
            price_diff = bi["unit_price"] - exchange_unit_price
            if price_diff > 0:
                store_credit_refund += round(price_diff * qty, 2)

        processed_items.append({
            "product_id": bi["product_id"],
            "product_name": bi["product_name"],
            "quantity": qty,
            "unit_price": bi["unit_price"],
            "action": action,
            "exchange_product_id": exchange_product_id,
            "exchange_product_name": exchange_product_name,
            "exchange_unit_price": exchange_unit_price,
            "exchange_line_total": exchange_line_total,
        })

    if not processed_items:
        flash("No items selected for refund/exchange.", "error")
        return redirect(url_for("new_refund", bill_id=bill_id))

    # Handle store credit refund
    if store_credit_refund > 0:
        sc_phone = (bill["customer_phone"] or request.form.get("store_credit_phone", "")).strip()
        sc_name = request.form.get("store_credit_name", "").strip() or bill["customer_name"] or "Walk-in"
        if not sc_phone or len(sc_phone) != 10:
            flash("Please provide a valid 10-digit phone number for store credit.", "error")
            return redirect(url_for("new_refund", bill_id=bill_id))

        # Find or create store credit account
        credit = db.execute(
            "SELECT * FROM store_credits WHERE customer_phone = ?", (sc_phone,)
        ).fetchone()
        if credit:
            credit_id = credit["id"]
            db.execute(
                "UPDATE store_credits SET balance = balance + ?, updated_at = datetime('now','+5 hours','+30 minutes') WHERE id = ?",
                (round(store_credit_refund, 2), credit_id),
            )
        else:
            cursor2 = db.execute(
                "INSERT INTO store_credits (customer_name, customer_phone, balance, created_at, updated_at) "
                "VALUES (?, ?, ?, datetime('now','+5 hours','+30 minutes'), datetime('now','+5 hours','+30 minutes'))",
                (sc_name, sc_phone, round(store_credit_refund, 2)),
            )
            credit_id = cursor2.lastrowid

        db.execute(
            "INSERT INTO credit_transactions (credit_id, bill_id, amount, transaction_type, notes, created_at) "
            "VALUES (?, ?, ?, ?, ?, datetime('now','+5 hours','+30 minutes'))",
            (credit_id, bill_id, round(store_credit_refund, 2), "credit",
             f"Refund from Bill #{bill_id}"),
        )

    exchange_items = [item for item in processed_items if item["action"] == "exchange"]
    if exchange_items:
        exchange_bill_id, exchange_bill_number, _ = _insert_exchange_bill(db, bill, exchange_items)

    actions = set(i["action"] for i in processed_items)
    if actions == {"exchange"}:
        refund_type = "exchange"
    elif actions == {"refund"}:
        refund_type = "refund"
    elif actions == {"store_credit"}:
        refund_type = "store_credit"
    else:
        refund_type = "mixed"

    cursor = db.execute(
        "INSERT INTO refunds (bill_id, customer_name, type, reason, refund_amount, created_at) "
        "VALUES (?, ?, ?, ?, ?, datetime('now','+5 hours','+30 minutes'))",
        (bill_id, bill["customer_name"], refund_type, reason, round(refund_amount + store_credit_refund, 2)),
    )
    refund_id = cursor.lastrowid

    for it in processed_items:
        db.execute(
            "INSERT INTO refund_items (refund_id, product_id, product_name, "
            "quantity, unit_price, action, exchange_product_id, exchange_product_name) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (refund_id, it["product_id"], it["product_name"], it["quantity"],
             it["unit_price"], it["action"], it["exchange_product_id"],
             it["exchange_product_name"]),
        )

    db.commit()

    desc_parts = []
    for it in processed_items:
        if it["action"] == "refund":
            desc_parts.append(f"Refunded {it['quantity']}× {it['product_name']}")
        elif it["action"] == "store_credit":
            desc_parts.append(f"Store Credit {it['quantity']}× {it['product_name']}")
        else:
            desc_parts.append(f"Exchanged {it['quantity']}× {it['product_name']} → {it['exchange_product_name']}")

    type_label = {"refund": "Refund", "exchange": "Exchange", "store_credit": "Store Credit"}.get(refund_type, "Refund/Exchange")

    log_update(
        f"{type_label} Processed",
        f"Bill #{bill_id} — {'; '.join(desc_parts)}" +
        (f" — Cash Refund: ₹{round(refund_amount, 2)}" if refund_amount > 0 else "") +
        (f" — Store Credit: ₹{round(store_credit_refund, 2)}" if store_credit_refund > 0 else "") +
        (f" — Exchange Bill: {exchange_bill_number}" if exchange_bill_number else ""),
        "billing",
    )

    flash_msg = f"{type_label} processed!"
    if refund_amount > 0:
        flash_msg += f" Cash refund: ₹{round(refund_amount, 2)}"
    if store_credit_refund > 0:
        flash_msg += f" Store credit: ₹{round(store_credit_refund, 2)}"
    if exchange_bill_number:
        flash_msg += f" Exchange bill: {exchange_bill_number}"
    flash(flash_msg, "success")
    if exchange_bill_id and refund_type == "exchange":
        return redirect(url_for("bill_detail", bill_id=exchange_bill_id))
    return redirect(url_for("bill_detail", bill_id=bill_id))


# ── Updates ──────────────────────────────────────────────────────────────
@app.route("/updates")
def updates():
    db = get_db()
    all_updates = db.execute("SELECT * FROM updates ORDER BY created_at DESC").fetchall()
    return render_template("updates.html", updates=all_updates)


@app.route("/updates/add", methods=["POST"])
def add_update():
    title = request.form["title"].strip()
    description = request.form.get("description", "").strip()
    update_type = request.form.get("type", "general")
    if title:
        log_update(title, description, update_type)
        flash("Update added!", "success")
    return redirect(url_for("updates"))


# ── Expenses ─────────────────────────────────────────────────────────────
@app.route("/admin", methods=["GET", "POST"])
def admin():
    if request.method == "POST":
        password = request.form.get("password", "")
        entered_hash = hashlib.sha256(password.encode()).hexdigest()
        if hmac.compare_digest(entered_hash, ADMIN_PASSWORD_HASH):
            session["admin_authenticated"] = True
            flash("Admin access granted.", "success")
            next_url = request.form.get("next_url", "").strip()
            if next_url.startswith("/"):
                return redirect(next_url)
            return redirect(url_for("admin"))
        flash("Incorrect admin password.", "error")

    next_url = request.args.get("next", "")

    return render_template(
        "admin.html",
        locked=not admin_authenticated(),
        next_url=next_url,
    )


@app.route("/admin/inventory-overview")
def admin_inventory_overview():
    if not admin_authenticated():
        flash("Please unlock Admin to view Inventory Overview.", "error")
        return redirect(url_for("admin", next=url_for("admin_inventory_overview")))

    db = get_db()
    filter_category = request.args.get("filter_category", "")
    filter_size = request.args.get("filter_size", "")

    inventory_totals = db.execute(
        "SELECT COALESCE(SUM(cost_price * quantity), 0) as total_cost, "
        "COALESCE(SUM(selling_price * quantity), 0) as total_selling, "
        "COALESCE(SUM(quantity), 0) as total_items "
        "FROM products WHERE quantity > 0"
    ).fetchone()
    all_categories = db.execute(
        "SELECT DISTINCT COALESCE(c.name, 'Uncategorized') as name "
        "FROM products p LEFT JOIN categories c ON p.category_id = c.id ORDER BY name"
    ).fetchall()
    all_sizes = db.execute(
        "SELECT DISTINCT COALESCE(p.size, 'No Size') as name FROM products p ORDER BY name"
    ).fetchall()

    cat_where = ""
    cat_params = []
    if filter_category:
        if filter_category == "Uncategorized":
            cat_where = " WHERE c.name IS NULL"
        else:
            cat_where = " WHERE c.name = ?"
            cat_params = [filter_category]

    size_where = ""
    size_params = []
    if filter_size:
        if filter_size == "No Size":
            size_where = " WHERE p.size IS NULL OR p.size = ''"
        else:
            size_where = " WHERE p.size = ?"
            size_params = [filter_size]

    category_counts = db.execute(
        "SELECT COALESCE(c.name, 'Uncategorized') as category, "
        "COUNT(p.id) as product_count, COALESCE(SUM(p.quantity), 0) as total_stock "
        "FROM products p LEFT JOIN categories c ON p.category_id = c.id"
        + cat_where +
        " GROUP BY c.name ORDER BY total_stock DESC",
        cat_params,
    ).fetchall()
    size_counts = db.execute(
        "SELECT COALESCE(p.size, 'No Size') as size, "
        "COALESCE(c.name, 'Uncategorized') as category, "
        "COUNT(p.id) as product_count, COALESCE(SUM(p.quantity), 0) as total_stock "
        "FROM products p LEFT JOIN categories c ON p.category_id = c.id"
        + size_where +
        " GROUP BY p.size, c.name ORDER BY p.size, c.name",
        size_params,
    ).fetchall()

    return render_template(
        "inventory_overview.html",
        inventory_totals=inventory_totals,
        all_categories=all_categories,
        all_sizes=all_sizes,
        filter_category=filter_category,
        filter_size=filter_size,
        category_counts=category_counts,
        size_counts=size_counts,
    )


@app.route("/admin/sales-summary")
def admin_sales_summary():
    if not admin_authenticated():
        flash("Please unlock Admin to view Sales Summary.", "error")
        return redirect(url_for("admin", next=url_for("admin_sales_summary")))

    db = get_db()
    top_selling_category = db.execute(
        "SELECT COALESCE(c.name, 'Uncategorized') as name, "
        "COALESCE(SUM(bi.quantity), 0) as sold_qty, "
        "COALESCE(SUM(bi.total_price), 0) as sold_amount "
        "FROM bill_items bi "
        "LEFT JOIN products p ON p.id = bi.product_id "
        "LEFT JOIN categories c ON c.id = p.category_id "
        "GROUP BY c.name "
        "ORDER BY sold_qty DESC, sold_amount DESC "
        "LIMIT 1"
    ).fetchone()

    top_selling_size = db.execute(
        "SELECT COALESCE(NULLIF(TRIM(p.size), ''), 'No Size') as name, "
        "COALESCE(SUM(bi.quantity), 0) as sold_qty, "
        "COALESCE(SUM(bi.total_price), 0) as sold_amount "
        "FROM bill_items bi "
        "LEFT JOIN products p ON p.id = bi.product_id "
        "GROUP BY COALESCE(NULLIF(TRIM(p.size), ''), 'No Size') "
        "ORDER BY sold_qty DESC, sold_amount DESC "
        "LIMIT 1"
    ).fetchone()

    sales_breakdown = db.execute(
        "SELECT COALESCE(c.name, 'Uncategorized') as category, "
        "COALESCE(NULLIF(TRIM(p.size), ''), 'No Size') as size, "
        "COALESCE(SUM(bi.quantity), 0) as sold_qty, "
        "COALESCE(SUM(bi.total_price), 0) as sold_amount, "
        "COUNT(DISTINCT bi.bill_id) as bills_count "
        "FROM bill_items bi "
        "LEFT JOIN products p ON p.id = bi.product_id "
        "LEFT JOIN categories c ON c.id = p.category_id "
        "GROUP BY COALESCE(c.name, 'Uncategorized'), COALESCE(NULLIF(TRIM(p.size), ''), 'No Size') "
        "ORDER BY sold_qty DESC, sold_amount DESC, category, size"
    ).fetchall()

    return render_template(
        "sales_summary.html",
        top_selling_category=top_selling_category,
        top_selling_size=top_selling_size,
        sales_breakdown=sales_breakdown,
    )


@app.route("/admin/investments")
def admin_investments():
    if not admin_authenticated():
        flash("Please unlock Admin to manage Investments.", "error")
        return redirect(url_for("admin", next=url_for("admin_investments")))

    db = get_db()
    investments = db.execute(
        "SELECT * FROM investments ORDER BY investment_date DESC"
    ).fetchall()
    total_investment = sum(i["amount"] for i in investments)
    return render_template(
        "investments.html",
        investments=investments,
        total_investment=total_investment,
    )


@app.route("/admin/tools")
def admin_tools():
    if not admin_authenticated():
        flash("Please unlock Admin to access Tools.", "error")
        return redirect(url_for("admin", next=url_for("admin_tools")))

    return render_template("admin_tools.html")


# ── Vendors ──────────────────────────────────────────────────────────────
@app.route("/admin/vendors")
def vendors():
    if not admin_authenticated():
        flash("Please unlock Admin to manage Vendors.", "error")
        return redirect(url_for("admin", next=url_for("vendors")))

    db = get_db()
    search = request.args.get("search", "").strip()
    query = (
        "SELECT v.*, "
        "(SELECT COUNT(*) FROM products p WHERE p.vendor_id = v.id) as product_count "
        "FROM vendors v WHERE 1=1"
    )
    params = []
    if search:
        query += " AND (v.name LIKE ? OR v.contact_person LIKE ? OR v.phone LIKE ?)"
        params += [f"%{search}%", f"%{search}%", f"%{search}%"]
    query += " ORDER BY v.name"
    all_vendors = db.execute(query, params).fetchall()
    return render_template("vendors.html", vendors=all_vendors, search=search)


@app.route("/admin/vendors/add", methods=["POST"])
def add_vendor():
    if not admin_authenticated():
        flash("Please unlock Admin to add vendors.", "error")
        return redirect(url_for("admin", next=url_for("vendors")))

    db = get_db()
    name = request.form.get("name", "").strip()
    contact_person = request.form.get("contact_person", "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip()
    address = request.form.get("address", "").strip()
    notes = request.form.get("notes", "").strip()

    if not name:
        flash("Vendor name is required.", "error")
        return redirect(url_for("vendors"))

    db.execute(
        "INSERT INTO vendors (name, contact_person, phone, email, address, notes, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, datetime('now','+5 hours','+30 minutes'), datetime('now','+5 hours','+30 minutes'))",
        (name, contact_person or None, phone or None, email or None, address or None, notes or None),
    )
    db.commit()
    log_update("Vendor Added", f"{name}" + (f" ({phone})" if phone else ""), "vendor")
    flash(f"Vendor '{name}' added!", "success")
    return redirect(url_for("vendors"))


@app.route("/admin/vendors/edit/<int:vendor_id>", methods=["POST"])
def edit_vendor(vendor_id):
    if not admin_authenticated():
        flash("Please unlock Admin to manage vendors.", "error")
        return redirect(url_for("admin", next=url_for("vendors")))

    db = get_db()
    vendor = db.execute("SELECT * FROM vendors WHERE id = ?", (vendor_id,)).fetchone()
    if not vendor:
        flash("Vendor not found.", "error")
        return redirect(url_for("vendors"))

    name = request.form.get("name", "").strip()
    contact_person = request.form.get("contact_person", "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip()
    address = request.form.get("address", "").strip()
    notes = request.form.get("notes", "").strip()

    if not name:
        flash("Vendor name is required.", "error")
        return redirect(url_for("vendors"))

    db.execute(
        "UPDATE vendors SET name = ?, contact_person = ?, phone = ?, email = ?, "
        "address = ?, notes = ?, updated_at = datetime('now','+5 hours','+30 minutes') WHERE id = ?",
        (name, contact_person or None, phone or None, email or None, address or None, notes or None, vendor_id),
    )
    db.commit()
    log_update("Vendor Updated", f"{name}", "vendor")
    flash(f"Vendor '{name}' updated.", "success")
    return redirect(url_for("vendors"))


@app.route("/admin/vendors/delete/<int:vendor_id>", methods=["POST"])
def delete_vendor(vendor_id):
    if not admin_authenticated():
        flash("Please unlock Admin to manage vendors.", "error")
        return redirect(url_for("admin", next=url_for("vendors")))

    db = get_db()
    vendor = db.execute("SELECT * FROM vendors WHERE id = ?", (vendor_id,)).fetchone()
    if not vendor:
        flash("Vendor not found.", "error")
        return redirect(url_for("vendors"))

    # Detach products from this vendor, then delete the vendor.
    db.execute("UPDATE products SET vendor_id = NULL WHERE vendor_id = ?", (vendor_id,))
    db.execute("DELETE FROM vendors WHERE id = ?", (vendor_id,))
    db.commit()
    log_update("Vendor Deleted", f"{vendor['name']}", "vendor")
    flash(f"Vendor '{vendor['name']}' deleted.", "success")
    return redirect(url_for("vendors"))


@app.route("/admin/vendor-summary")
def vendor_summary():
    if not admin_authenticated():
        flash("Please unlock Admin to view Vendor Summary.", "error")
        return redirect(url_for("admin", next=url_for("vendor_summary")))

    db = get_db()
    # Current stock value per vendor.
    stock_rows = db.execute(
        "SELECT v.id as vendor_id, v.name as vendor_name, "
        "COUNT(p.id) as product_count, "
        "COALESCE(SUM(p.quantity), 0) as total_stock, "
        "COALESCE(SUM(p.cost_price * p.quantity), 0) as stock_cost_value, "
        "COALESCE(SUM(p.selling_price * p.quantity), 0) as stock_retail_value "
        "FROM vendors v "
        "LEFT JOIN products p ON p.vendor_id = v.id "
        "GROUP BY v.id, v.name "
        "ORDER BY v.name"
    ).fetchall()

    # Sales per vendor (units sold and revenue) derived from bill_items → products.
    sales_rows = db.execute(
        "SELECT v.id as vendor_id, "
        "COALESCE(SUM(bi.quantity), 0) as sold_qty, "
        "COALESCE(SUM(bi.total_price), 0) as sold_amount, "
        "COALESCE(SUM(bi.quantity * p.cost_price), 0) as sold_cost, "
        "COUNT(DISTINCT bi.bill_id) as bills_count "
        "FROM vendors v "
        "JOIN products p ON p.vendor_id = v.id "
        "JOIN bill_items bi ON bi.product_id = p.id "
        "GROUP BY v.id"
    ).fetchall()
    sales_map = {row["vendor_id"]: row for row in sales_rows}

    summary = []
    totals = {
        "product_count": 0,
        "total_stock": 0,
        "stock_cost_value": 0.0,
        "stock_retail_value": 0.0,
        "sold_qty": 0,
        "sold_amount": 0.0,
        "gross_profit": 0.0,
    }
    for row in stock_rows:
        sales = sales_map.get(row["vendor_id"])
        sold_qty = sales["sold_qty"] if sales else 0
        sold_amount = round(sales["sold_amount"], 2) if sales else 0.0
        sold_cost = round(sales["sold_cost"], 2) if sales else 0.0
        bills_count = sales["bills_count"] if sales else 0
        gross_profit = round(sold_amount - sold_cost, 2)
        summary.append({
            "vendor_id": row["vendor_id"],
            "vendor_name": row["vendor_name"],
            "product_count": row["product_count"],
            "total_stock": row["total_stock"],
            "stock_cost_value": round(row["stock_cost_value"], 2),
            "stock_retail_value": round(row["stock_retail_value"], 2),
            "sold_qty": sold_qty,
            "sold_amount": sold_amount,
            "bills_count": bills_count,
            "gross_profit": gross_profit,
        })
        totals["product_count"] += row["product_count"]
        totals["total_stock"] += row["total_stock"]
        totals["stock_cost_value"] += row["stock_cost_value"]
        totals["stock_retail_value"] += row["stock_retail_value"]
        totals["sold_qty"] += sold_qty
        totals["sold_amount"] += sold_amount
        totals["gross_profit"] += gross_profit

    summary.sort(key=lambda x: (-x["sold_amount"], x["vendor_name"]))
    totals["stock_cost_value"] = round(totals["stock_cost_value"], 2)
    totals["stock_retail_value"] = round(totals["stock_retail_value"], 2)
    totals["sold_amount"] = round(totals["sold_amount"], 2)
    totals["gross_profit"] = round(totals["gross_profit"], 2)

    return render_template("vendor_summary.html", summary=summary, totals=totals)



@app.route("/admin/logout")
def admin_logout():
    session.pop("admin_authenticated", None)
    session.pop("pl_authenticated", None)
    flash("Admin area locked.", "success")
    return redirect(url_for("dashboard"))


@app.route("/admin/whatsapp-test", methods=["POST"])
def admin_whatsapp_test():
    if not admin_authenticated():
        return jsonify({"sent": False, "reason": "forbidden", "error": "Admin access required."}), 403

    payload = request.get_json(silent=True) or {}
    customer_phone = str(payload.get("phone", "")).strip()
    if not customer_phone:
        return jsonify({"sent": False, "reason": "missing_phone", "error": "Enter a phone number."}), 400

    to_phone = normalize_phone_for_whatsapp_cloud(customer_phone)
    if not to_phone:
        return jsonify({"sent": False, "reason": "invalid_phone", "error": "Use a valid 10-digit Indian mobile number."}), 400

    test_message = (
        "Namaste! This is a WhatsApp test message from Gulmohar by Ankita billing system. "
        "If you received this, WhatsApp integration is working."
    )
    result = send_whatsapp_text_message(to_phone, test_message)
    status_code = 200 if result.get("sent") else 400
    return jsonify(result), status_code


@app.route("/admin/investments/add", methods=["POST"])
def add_investment():
    if not admin_authenticated():
        flash("Please unlock Admin to manage investments.", "error")
        return redirect(url_for("admin"))

    db = get_db()
    description = request.form.get("description", "").strip()
    amount = request.form.get("amount", "")
    investment_date = request.form.get("investment_date", "").strip()

    if not description or not amount or not investment_date:
        flash("Please provide description, amount, and date.", "error")
        return redirect(url_for("admin"))

    try:
        amount = float(amount)
        if amount <= 0:
            raise ValueError
    except ValueError:
        flash("Please provide a valid positive amount.", "error")
        return redirect(url_for("admin"))

    db.execute(
        "INSERT INTO investments (description, amount, investment_date, created_at) "
        "VALUES (?, ?, ?, datetime('now','+5 hours','+30 minutes'))",
        (description, amount, investment_date),
    )
    db.commit()
    log_update("Investment Added", f"{description} — ₹{amount} on {investment_date}", "investment")
    flash(f"Investment '₹{amount} — {description}' added!", "success")
    return redirect(url_for("admin"))


@app.route("/admin/investments/delete/<int:investment_id>", methods=["POST"])
def delete_investment(investment_id):
    if not admin_authenticated():
        flash("Please unlock Admin to manage investments.", "error")
        return redirect(url_for("admin"))

    db = get_db()
    inv = db.execute("SELECT * FROM investments WHERE id = ?", (investment_id,)).fetchone()
    if not inv:
        flash("Investment not found.", "error")
        return redirect(url_for("admin"))

    db.execute("DELETE FROM investments WHERE id = ?", (investment_id,))
    db.commit()
    log_update("Investment Deleted", f"{inv['description']} — ₹{inv['amount']}", "investment")
    flash("Investment deleted.", "success")
    return redirect(url_for("admin"))


@app.route("/admin/clean-all-data", methods=["POST"])
def clean_all_data():
    confirm = request.form.get("confirm", "").strip()
    password = request.form.get("password", "")

    if not admin_authenticated():
        flash("Please unlock Admin first.", "error")
        return redirect(url_for("admin"))

    if confirm != "DELETE ALL DATA":
        flash("Confirmation text did not match.", "error")
        return redirect(url_for("admin"))

    entered_hash = hashlib.sha256(password.encode()).hexdigest()
    if not hmac.compare_digest(entered_hash, ADMIN_PASSWORD_HASH):
        flash("Incorrect admin password.", "error")
        return redirect(url_for("admin"))

    db = get_db()
    expense_images = db.execute(
        "SELECT bill_image_path FROM expenses WHERE bill_image_path IS NOT NULL AND bill_image_path != ''"
    ).fetchall()
    for row in expense_images:
        image_path = os.path.join(app.root_path, "static", row["bill_image_path"])
        if os.path.exists(image_path):
            os.remove(image_path)

    db.execute("DELETE FROM bill_items")
    db.execute("DELETE FROM bills")
    db.execute("DELETE FROM refund_items")
    db.execute("DELETE FROM refunds")
    db.execute("DELETE FROM expenses")
    db.execute("DELETE FROM updates")
    db.execute("UPDATE counters SET value = 0 WHERE name = 'bill_number'")
    db.commit()

    log_update(
        "All Business Data Cleared",
        "Bills, refunds, expenses, and history were permanently deleted from Admin.",
        "announcement",
    )
    flash("All bills, refunds, expenses, and history were deleted.", "success")
    return redirect(url_for("admin"))


# ── Inventory Labels (2"x3" vertical) ────────────────────────────────────
@app.route("/admin/labels", methods=["GET"])
def inventory_labels():
    if not admin_authenticated():
        flash("Please unlock Admin to print labels.", "error")
        return redirect(url_for("admin", next=url_for("inventory_labels")))

    db = get_db()
    filter_category = request.args.get("filter_category", "").strip()
    filter_size = request.args.get("filter_size", "").strip()
    search = request.args.get("q", "").strip()

    query = (
        "SELECT p.id, p.name, p.sku, p.size, p.selling_price, p.quantity, "
        "COALESCE(c.name, 'Uncategorized') AS category "
        "FROM products p LEFT JOIN categories c ON p.category_id = c.id"
    )
    where = []
    params = []
    if filter_category:
        if filter_category == "Uncategorized":
            where.append("c.name IS NULL")
        else:
            where.append("c.name = ?")
            params.append(filter_category)
    if filter_size:
        if filter_size == "No Size":
            where.append("(p.size IS NULL OR p.size = '')")
        else:
            where.append("p.size = ?")
            params.append(filter_size)
    if search:
        where.append("(p.sku LIKE ? OR p.name LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY datetime(p.created_at) DESC, p.id DESC"

    products = db.execute(query, params).fetchall()
    all_categories = db.execute(
        "SELECT DISTINCT COALESCE(c.name, 'Uncategorized') as name "
        "FROM products p LEFT JOIN categories c ON p.category_id = c.id ORDER BY name"
    ).fetchall()
    all_sizes = db.execute(
        "SELECT DISTINCT COALESCE(p.size, 'No Size') as name FROM products p ORDER BY name"
    ).fetchall()

    return render_template(
        "labels_select.html",
        products=products,
        all_categories=all_categories,
        all_sizes=all_sizes,
        filter_category=filter_category,
        filter_size=filter_size,
        search=search,
    )


@app.route("/admin/labels/print", methods=["POST"])
def inventory_labels_print():
    if not admin_authenticated():
        flash("Please unlock Admin to print labels.", "error")
        return redirect(url_for("admin"))

    ids_raw = request.form.getlist("product_ids")
    try:
        product_ids = [int(x) for x in ids_raw if x.strip().isdigit()]
    except ValueError:
        product_ids = []

    try:
        copies = max(1, min(50, int(request.form.get("copies", "1"))))
    except (TypeError, ValueError):
        copies = 1

    if not product_ids:
        flash("Please select at least one product to print labels for.", "error")
        return redirect(url_for("inventory_labels"))

    db = get_db()
    placeholders = ",".join("?" for _ in product_ids)
    rows = db.execute(
        f"SELECT id, name, sku, size, selling_price FROM products WHERE id IN ({placeholders})",
        product_ids,
    ).fetchall()

    # Preserve the order of selection.
    rows_by_id = {r["id"]: r for r in rows}
    products = [rows_by_id[i] for i in product_ids if i in rows_by_id]
    labels = [p for p in products for _ in range(copies)]

    return render_template("labels_print.html", labels=labels, copies=copies)


@app.route("/daily-summary")
def daily_summary():
    if not admin_authenticated():
        flash("Please unlock Admin to access Daily Summary.", "error")
        return redirect(url_for("admin", next=url_for("daily_summary")))

    db = get_db()
    today_date = now_ist().strftime("%Y-%m-%d")
    selected_date = request.args.get("date", today_date).strip() or today_date
    try:
        datetime.strptime(selected_date, "%Y-%m-%d")
    except ValueError:
        selected_date = today_date
    date_filter = f"{selected_date}%"

    sales_total = db.execute(
        "SELECT COALESCE(SUM(total), 0) FROM bills WHERE created_at LIKE ?",
        (date_filter,),
    ).fetchone()[0]
    bill_count = db.execute(
        "SELECT COUNT(*) FROM bills WHERE created_at LIKE ?",
        (date_filter,),
    ).fetchone()[0]

    expense_total = db.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM expenses WHERE created_at LIKE ?",
        (date_filter,),
    ).fetchone()[0]
    expense_count = db.execute(
        "SELECT COUNT(*) FROM expenses WHERE created_at LIKE ?",
        (date_filter,),
    ).fetchone()[0]

    refund_total = db.execute(
        "SELECT COALESCE(SUM(refund_amount), 0) FROM refunds WHERE created_at LIKE ?",
        (date_filter,),
    ).fetchone()[0]
    refund_count = db.execute(
        "SELECT COUNT(*) FROM refunds WHERE created_at LIKE ?",
        (date_filter,),
    ).fetchone()[0]

    cogs_total = db.execute(
        "SELECT COALESCE(SUM(bi.quantity * p.cost_price), 0) "
        "FROM bill_items bi "
        "JOIN bills b ON bi.bill_id = b.id "
        "JOIN products p ON bi.product_id = p.id "
        "WHERE b.created_at LIKE ?",
        (date_filter,),
    ).fetchone()[0]

    gross_profit = sales_total - cogs_total
    net_after_expenses = gross_profit - expense_total - refund_total

    payment_rows = db.execute(
        "SELECT payment_method, payment_breakdown_json, total "
        "FROM bills WHERE created_at LIKE ?",
        (date_filter,),
    ).fetchall()
    payment_map = {}
    for row in payment_rows:
        breakdown = parse_bill_payment_breakdown(row)
        if not breakdown:
            continue
        methods_seen = set()
        for item in breakdown:
            method = item["method"]
            amount = item["amount"]
            if method not in payment_map:
                payment_map[method] = {
                    "payment_method": method,
                    "bill_count": 0,
                    "total": 0.0,
                }
            payment_map[method]["total"] = round(payment_map[method]["total"] + amount, 2)
            if method not in methods_seen:
                payment_map[method]["bill_count"] += 1
                methods_seen.add(method)

    payment_split = sorted(
        payment_map.values(),
        key=lambda x: (-x["total"], x["payment_method"]),
    )
    cash_total = round(
        sum(r["total"] for r in payment_split if r["payment_method"] == "Cash"),
        2,
    )
    digital_total = round(
        sum(r["total"] for r in payment_split if r["payment_method"] != "Cash"),
        2,
    )
    cash_total = round(sum(r["total"] for r in payment_split if r["payment_method"] == "Cash"), 2)
    digital_total = round(sum(r["total"] for r in payment_split if r["payment_method"] != "Cash"), 2)

    top_products = db.execute(
        "SELECT bi.product_name, SUM(bi.quantity) as qty, COALESCE(SUM(bi.total_price), 0) as revenue "
        "FROM bill_items bi "
        "JOIN bills b ON bi.bill_id = b.id "
        "WHERE b.created_at LIKE ? "
        "GROUP BY bi.product_name ORDER BY qty DESC, revenue DESC LIMIT 10",
        (date_filter,),
    ).fetchall()

    recent_bills = db.execute(
        "SELECT * FROM bills WHERE created_at LIKE ? ORDER BY created_at DESC LIMIT 10",
        (date_filter,),
    ).fetchall()
    recent_expenses = db.execute(
        "SELECT * FROM expenses WHERE created_at LIKE ? ORDER BY created_at DESC LIMIT 10",
        (date_filter,),
    ).fetchall()

    return render_template(
        "daily_summary.html",
        today=selected_date,
        selected_date=selected_date,
        today_date=today_date,
        sales_total=sales_total,
        bill_count=bill_count,
        expense_total=expense_total,
        expense_count=expense_count,
        refund_total=refund_total,
        refund_count=refund_count,
        cogs_total=cogs_total,
        gross_profit=gross_profit,
        net_after_expenses=net_after_expenses,
        payment_split=payment_split,
        top_products=top_products,
        recent_bills=recent_bills,
        recent_expenses=recent_expenses,
    )


@app.route("/expenses")
def expenses():
    if not admin_authenticated():
        flash("Please unlock Admin to access Expenses.", "error")
        return redirect(url_for("admin", next=url_for("expenses")))

    db = get_db()
    today_date = now_ist().strftime("%Y-%m-%d")
    search = request.args.get("search", "").strip()
    category = request.args.get("category", "")
    selected_date = request.args.get("date", "").strip()
    query = "SELECT * FROM expenses WHERE 1=1"
    params = []
    if search:
        query += " AND (title LIKE ? OR description LIKE ? OR vendor LIKE ?)"
        params += [f"%{search}%", f"%{search}%", f"%{search}%"]
    if category:
        query += " AND category = ?"
        params.append(category)
    if selected_date:
        query += " AND created_at LIKE ?"
        params.append(f"{selected_date}%")
    query += " ORDER BY created_at DESC"
    all_expenses = db.execute(query, params).fetchall()
    total = sum(e["amount"] for e in all_expenses)

    expense_count = len(all_expenses)
    pl_total = sum(e["amount"] for e in all_expenses if e["include_in_pl"])
    non_pl_total = total - pl_total

    category_totals = {}
    for expense in all_expenses:
        expense_category = expense["category"] or "Uncategorized"
        if expense_category not in category_totals:
            category_totals[expense_category] = {"amount": 0.0, "count": 0}
        category_totals[expense_category]["amount"] += expense["amount"]
        category_totals[expense_category]["count"] += 1

    category_breakdown = []
    for expense_category, values in category_totals.items():
        category_total = values["amount"]
        category_breakdown.append(
            {
                "category": expense_category,
                "amount": category_total,
                "count": values["count"],
                "share": (category_total / total * 100) if total else 0,
            }
        )
    category_breakdown.sort(key=lambda item: item["amount"], reverse=True)

    return render_template(
        "expenses.html", expenses=all_expenses, total=total,
        search=search, selected_category=category, selected_date=selected_date,
        today_date=today_date,
        expense_count=expense_count, pl_total=pl_total, non_pl_total=non_pl_total,
        category_breakdown=category_breakdown,
    )


@app.route("/expenses/add", methods=["POST"])
def add_expense():
    if not admin_authenticated():
        flash("Please unlock Admin to add expenses.", "error")
        return redirect(url_for("admin", next=url_for("expenses")))

    db = get_db()
    title = request.form["title"].strip()
    vendor = request.form.get("vendor", "").strip()
    description = request.form.get("description", "").strip()
    category = request.form.get("category", "General")
    amount = float(request.form.get("amount", 0))
    payment_mode = request.form.get("payment_mode", "Cash").strip()
    if payment_mode not in ALLOWED_PAYMENT_METHODS:
        payment_mode = "Cash"
    include_in_pl = 1 if request.form.get("include_in_pl") else 0
    bill_image = request.files.get("bill_image")
    if title and amount > 0:
        bill_image_path = None
        if bill_image and bill_image.filename:
            if not allowed_bill_image(bill_image.filename):
                flash("Bill image must be PNG, JPG, JPEG, or WEBP.", "error")
                return redirect(url_for("expenses"))
            bill_image_path = save_expense_bill_image(bill_image, title)

        db.execute(
            "INSERT INTO expenses (title, vendor, description, category, amount, payment_mode, bill_image_path, include_in_pl, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now','+5 hours','+30 minutes'))",
            (title, vendor or None, description, category, amount, payment_mode, bill_image_path, include_in_pl),
        )
        db.commit()
        log_update("Expense Added", f"{title} — ₹{amount} ({category})", "expense")
        flash(f"Expense '₹{amount} — {title}' added!", "success")
    else:
        flash("Please provide a title and valid amount.", "error")
    return redirect(url_for("expenses"))


@app.route("/expenses/edit/<int:expense_id>", methods=["GET", "POST"])
def edit_expense(expense_id):
    if not admin_authenticated():
        flash("Please unlock Admin to manage expenses.", "error")
        return redirect(url_for("admin", next=url_for("expenses")))

    db = get_db()
    expense = db.execute("SELECT * FROM expenses WHERE id = ?", (expense_id,)).fetchone()
    if not expense:
        flash("Expense not found.", "error")
        return redirect(url_for("expenses"))

    if request.method == "POST":
        title = request.form["title"].strip()
        vendor = request.form.get("vendor", "").strip()
        description = request.form.get("description", "").strip()
        category = request.form.get("category", "General")
        amount = float(request.form.get("amount", 0))
        payment_mode = request.form.get("payment_mode", "Cash").strip()
        if payment_mode not in ALLOWED_PAYMENT_METHODS:
            payment_mode = "Cash"
        include_in_pl = 1 if request.form.get("include_in_pl") else 0
        remove_image = request.form.get("remove_bill_image") == "1"
        bill_image = request.files.get("bill_image")

        if not title or amount <= 0:
            flash("Please provide a title and valid amount.", "error")
            return redirect(url_for("edit_expense", expense_id=expense_id))

        bill_image_path = expense["bill_image_path"]
        if remove_image and bill_image_path:
            old_path = os.path.join(app.root_path, "static", bill_image_path)
            if os.path.exists(old_path):
                os.remove(old_path)
            bill_image_path = None

        if bill_image and bill_image.filename:
            if not allowed_bill_image(bill_image.filename):
                flash("Bill image must be PNG, JPG, JPEG, or WEBP.", "error")
                return redirect(url_for("edit_expense", expense_id=expense_id))
            if bill_image_path:
                old_path = os.path.join(app.root_path, "static", bill_image_path)
                if os.path.exists(old_path):
                    os.remove(old_path)
            bill_image_path = save_expense_bill_image(bill_image, title)

        db.execute(
            "UPDATE expenses SET title = ?, vendor = ?, description = ?, category = ?, amount = ?, payment_mode = ?, bill_image_path = ?, include_in_pl = ? WHERE id = ?",
            (title, vendor or None, description, category, amount, payment_mode, bill_image_path, include_in_pl, expense_id),
        )
        db.commit()
        log_update("Expense Updated", f"{title} — ₹{amount} ({category})", "expense")
        flash("Expense updated.", "success")
        return redirect(url_for("expenses"))

    return render_template("expense_form.html", expense=expense)


@app.route("/expenses/delete/<int:expense_id>", methods=["POST"])
def delete_expense(expense_id):
    if not admin_authenticated():
        flash("Please unlock Admin to manage expenses.", "error")
        return redirect(url_for("admin", next=url_for("expenses")))

    db = get_db()
    expense = db.execute(
        "SELECT title, amount, bill_image_path FROM expenses WHERE id = ?",
        (expense_id,),
    ).fetchone()
    if expense:
        if expense["bill_image_path"]:
            image_path = os.path.join(app.root_path, "static", expense["bill_image_path"])
            if os.path.exists(image_path):
                os.remove(image_path)
        db.execute("DELETE FROM expenses WHERE id = ?", (expense_id,))
        db.commit()
        log_update("Expense Deleted", f"Deleted '{expense['title']}' — ₹{expense['amount']}", "expense")
        flash("Expense deleted.", "success")
    return redirect(url_for("expenses"))


# ── Profit & Loss (Admin Protected) ─────────────────────────────────────
@app.route("/profit-loss")
def profit_loss():
    if not admin_authenticated():
        flash("Please unlock Admin to access Profit & Loss.", "error")
        return redirect(url_for("admin", next=url_for("profit_loss")))

    db = get_db()
    period = request.args.get("period", "all")
    today = now_ist().strftime("%Y-%m-%d")
    month = now_ist().strftime("%Y-%m")

    if period == "today":
        date_filter = f"{today}%"
    elif period == "month":
        date_filter = f"{month}%"
    else:
        date_filter = "%"

    # Revenue from bills
    revenue = db.execute(
        "SELECT COALESCE(SUM(total), 0) FROM bills WHERE created_at LIKE ?",
        (date_filter,),
    ).fetchone()[0]

    # Cost of goods sold (from bill_items joined with products)
    cogs = db.execute(
        "SELECT COALESCE(SUM(bi.quantity * p.cost_price), 0) "
        "FROM bill_items bi "
        "JOIN bills b ON bi.bill_id = b.id "
        "JOIN products p ON bi.product_id = p.id "
        "WHERE b.created_at LIKE ?",
        (date_filter,),
    ).fetchone()[0]

    # Expenses (only those marked for P&L)
    total_expenses = db.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM expenses WHERE include_in_pl = 1 AND created_at LIKE ?",
        (date_filter,),
    ).fetchone()[0]

    gross_profit = revenue - cogs
    net_profit = gross_profit - total_expenses

    # Total investment
    total_investment = db.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM investments"
    ).fetchone()[0]

    # Expenses by category (only P&L expenses)
    expense_breakdown = db.execute(
        "SELECT category, SUM(amount) as total FROM expenses "
        "WHERE include_in_pl = 1 AND created_at LIKE ? GROUP BY category ORDER BY total DESC",
        (date_filter,),
    ).fetchall()

    # Recent bills for period
    recent_bills = db.execute(
        "SELECT * FROM bills WHERE created_at LIKE ? ORDER BY created_at DESC LIMIT 20",
        (date_filter,),
    ).fetchall()

    # Payment breakup for period (supports mixed/split payments)
    payment_rows = db.execute(
        "SELECT payment_method, payment_breakdown_json, total "
        "FROM bills WHERE created_at LIKE ?",
        (date_filter,),
    ).fetchall()
    payment_map = {}
    for row in payment_rows:
        breakdown = parse_bill_payment_breakdown(row)
        if not breakdown:
            continue
        methods_seen = set()
        for item in breakdown:
            method = item["method"]
            amount = item["amount"]
            if method not in payment_map:
                payment_map[method] = {
                    "payment_method": method,
                    "bill_count": 0,
                    "total": 0.0,
                }
            payment_map[method]["total"] = round(payment_map[method]["total"] + amount, 2)
            if method not in methods_seen:
                payment_map[method]["bill_count"] += 1
                methods_seen.add(method)

    payment_split = sorted(
        payment_map.values(),
        key=lambda x: (-x["total"], x["payment_method"]),
    )

    cash_total = round(
        sum(r["total"] for r in payment_split if r["payment_method"] == "Cash"),
        2,
    )
    digital_total = round(
        sum(r["total"] for r in payment_split if r["payment_method"] != "Cash"),
        2,
    )

    # Recent expenses for period
    recent_expenses = db.execute(
        "SELECT * FROM expenses WHERE created_at LIKE ? ORDER BY created_at DESC LIMIT 20",
        (date_filter,),
    ).fetchall()

    # Inventory value
    inventory_cost = db.execute(
        "SELECT COALESCE(SUM(cost_price * quantity), 0) FROM products"
    ).fetchone()[0]
    inventory_retail = db.execute(
        "SELECT COALESCE(SUM(selling_price * quantity), 0) FROM products"
    ).fetchone()[0]

    return render_template(
        "profit_loss.html",
        revenue=revenue,
        cogs=cogs,
        gross_profit=gross_profit,
        total_expenses=total_expenses,
        net_profit=net_profit,
        expense_breakdown=expense_breakdown,
        recent_bills=recent_bills,
        payment_split=payment_split,
        cash_total=cash_total,
        digital_total=digital_total,
        recent_expenses=recent_expenses,
        inventory_cost=inventory_cost,
        inventory_retail=inventory_retail,
        period=period,
        total_investment=total_investment,
    )


# ── Export: Expenses CSV ─────────────────────────────────────────────────
@app.route("/export/expenses")
def export_expenses():
    if not admin_authenticated():
        flash("Please unlock Admin to export data.", "error")
        return redirect(url_for("admin", next=url_for("expenses")))

    db = get_db()
    date = request.args.get("date", "").strip()
    category = request.args.get("category", "").strip()

    query = "SELECT * FROM expenses WHERE 1=1"
    params = []
    if date:
        query += " AND created_at LIKE ?"
        params.append(f"{date}%")
    if category:
        query += " AND category = ?"
        params.append(category)
    query += " ORDER BY created_at DESC"

    rows = db.execute(query, params).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Title", "Vendor", "Description", "Category", "Payment Mode", "Amount (₹)", "In P&L", "Date"])
    for r in rows:
        writer.writerow([r["id"], r["title"], r["vendor"] or "", r["description"] or "",
                         r["category"], r["payment_mode"] or "Cash", r["amount"],
                         "Yes" if r["include_in_pl"] else "No", r["created_at"]])

    filename = f"expenses_{date or 'all'}.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ── Export: Sales CSV ────────────────────────────────────────────────────
@app.route("/export/sales")
def export_sales():
    if not admin_authenticated():
        flash("Please unlock Admin to export data.", "error")
        return redirect(url_for("admin", next=url_for("bills_list")))

    db = get_db()
    date = request.args.get("date", "").strip()

    query = (
        "SELECT b.id, b.bill_number, b.customer_name, b.customer_phone, b.subtotal, "
        "b.discount_percent, b.discount_amount, b.tax_percent, b.tax_amount, "
        "b.total, b.payment_method, b.created_at "
        "FROM bills b WHERE 1=1"
    )
    params = []
    if date:
        query += " AND b.created_at LIKE ?"
        params.append(f"{date}%")
    query += " ORDER BY b.created_at DESC"

    bills = db.execute(query, params).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Bill #", "Customer", "Phone", "Subtotal (₹)",
                     "Discount (₹)", "Discount Rate %", "Tax %", "Tax (₹)",
                     "Total (₹)", "Payment Method", "Date", "Items"])

    for b in bills:
        items = db.execute(
            "SELECT product_name, quantity, unit_price, total_price "
            "FROM bill_items WHERE bill_id = ?", (b["id"],)
        ).fetchall()
        items_str = "; ".join(
            f"{it['product_name']} x{it['quantity']} @₹{it['unit_price']}"
            for it in items
        )
        writer.writerow([
            b["bill_number"] or f"#{b['id']}", b["customer_name"] or "Walk-in", b["customer_phone"] or "",
            b["subtotal"], b["discount_amount"], b["discount_percent"],
            b["tax_percent"], b["tax_amount"], b["total"],
            b["payment_method"], b["created_at"], items_str,
        ])

    filename = f"sales_{date or 'all'}.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
