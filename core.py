import os
from datetime import datetime
from flask import Flask, render_template
from flask_wtf.csrf import CSRFProtect

from config import SECRET_KEY, IST, EXPENSE_BILL_UPLOAD_DIR, PRODUCT_IMAGE_UPLOAD_DIR
from models import close_db, init_db
from services import inject_bill_helpers
from auth import _enforce_access_control, inject_auth_helpers

# Re-export everything from sub-modules for backward compat with `from core import *`
from config import *
from models import *
from services import *
from services import _insert_exchange_bill  # Private function, needs explicit import
from auth import *

app = Flask(__name__)
app.secret_key = SECRET_KEY
csrf = CSRFProtect(app)

# Register Flask lifecycle hooks
app.teardown_appcontext(close_db)
app.before_request(_enforce_access_control)
app.context_processor(inject_auth_helpers)
app.context_processor(inject_bill_helpers)

# Template filters (require app object, so must stay in core.py)
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
        return text

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    else:
        dt = dt.astimezone(IST)

    return dt.strftime("%d-%b-%Y %I:%M %p IST")


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
        return text

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    else:
        dt = dt.astimezone(IST)

    return dt.strftime("%d-%b-%Y")


# Initialize database on app startup
os.makedirs(EXPENSE_BILL_UPLOAD_DIR, exist_ok=True)
os.makedirs(PRODUCT_IMAGE_UPLOAD_DIR, exist_ok=True)

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
    replacement_discount = round(
        sum(item.get("replacement_discount_amount", 0) for item in exchange_items),
        2,
    )
    exchange_total = round(exchange_subtotal - replacement_discount, 2)
    discount_percent = round(
        replacement_discount / exchange_subtotal * 100, 2,
    ) if exchange_subtotal else 0
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
            discount_percent,
            replacement_discount,
            0,
            0,
            exchange_total,
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


def get_active_sale_discount(db, product_id):
    """Get the discount percent for a product if it's part of an active sale.
    
    Returns the discount percent (float) if product is in an active sale, 0 otherwise.
    """
    current_date = now_ist().strftime("%Y-%m-%d")
    sale = db.execute(
        """
        SELECT discount_percent FROM sales s
        JOIN sale_products sp ON s.id = sp.sale_id
        WHERE sp.product_id = ?
        AND s.start_date <= ?
        AND s.end_date >= ?
        ORDER BY s.discount_percent DESC
        LIMIT 1
        """,
        (product_id, current_date, current_date),
    ).fetchone()
    
    if sale:
        return float(sale["discount_percent"] or 0)
    return 0


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


# Legacy shared admin password hash. Kept only to let the very first login
# ("admin", before any users exist) migrate seamlessly into a real account.
LEGACY_ADMIN_PASSWORD_HASH = "d1215baec4cf39b5c9cc710527fbbfcb3d4290caaf9b0f095d32198c9d5e28aa"
SESSION_IDLE_TIMEOUT_SECONDS = 20 * 60

# Permission registry: (key, label) pairs, grouped for the checkbox UI used
# when creating/editing a user. Keys map to the tabs/sections of the app.
MAIN_PERMISSIONS = [
    ("dashboard", "Dashboard"),
    ("sku_size_checker", "Size Availability"),
    ("billing", "New Bill / Billing"),
]

ADMIN_PERMISSIONS = [
    ("daily_summary", "Daily Summary"),
    ("bills", "Bill History"),
    ("refunds", "Refund Details"),
    ("expenses", "Expenses"),
    ("profit_loss", "Profit & Loss"),
    ("store_credits", "Store Credits"),
    ("categories", "Categories"),
    ("vendors", "Vendors"),
    ("customers", "Customers"),
    ("vendor_summary", "Vendor Summary"),
    ("inventory", "Inventory"),
    ("dead_stock", "Backlog / Dead Stock"),
    ("inventory_overview", "Inventory Overview"),
    ("sales_summary", "Sales Summary"),
    ("sales_exhibition", "Sales / Exhibition"),
    ("investments", "Initial Investments"),
    ("labels", "Print Labels"),
    ("low_stock_alerts", "Low Stock Alerts"),
    ("admin_tools", "Tools & Danger Zone"),
]

ALL_PERMISSION_KEYS = {key for key, _ in MAIN_PERMISSIONS + ADMIN_PERMISSIONS}
ADMIN_PERMISSION_KEYS = [key for key, _ in ADMIN_PERMISSIONS]

# Sentinel required-permission value for routes only a superadmin may access.
SUPERADMIN_ONLY = object()

# endpoint -> required permission key, a tuple of keys (any one grants
# access), or SUPERADMIN_ONLY. Endpoints missing from this map default to
# SUPERADMIN_ONLY (fail closed) unless listed in LOGIN_ONLY_ENDPOINTS.
ENDPOINT_PERMISSIONS = {
    "dashboard": "dashboard",
    "get_dashboard_notes": "dashboard",
    "save_dashboard_notes": "dashboard",
    "sku_size_checker": "sku_size_checker",
    "billing": "billing",
    "api_products": "billing",
    "api_customers_search": "billing",
    "create_bill": "billing",
    "lookup_store_credit": "billing",
    "bill_detail": ("billing", "bills"),
    "bill_thermal_print": ("billing", "bills"),
    "rental_deposit_return_receipt": ("billing", "bills"),
    "bills_list": "bills",
    "edit_bill": "bills",
    "delete_bill": "bills",
    "return_rental_deposit": "bills",
    "export_sales": "bills",
    "daily_summary": "daily_summary",
    "refunds_list": "refunds",
    "new_refund": "refunds",
    "process_refund": "refunds",
    "expenses": "expenses",
    "add_expense": "expenses",
    "edit_expense": "expenses",
    "delete_expense": "expenses",
    "export_expenses": "expenses",
    "profit_loss": "profit_loss",
    "store_credits": "store_credits",
    "add_store_credit": "store_credits",
    "add_credit_balance": "store_credits",
    "credit_transactions": "store_credits",
    "store_credit_receipt": "store_credits",
    "delete_store_credit": "store_credits",
    "delete_credit_transaction": "store_credits",
    "edit_credit_transaction": "store_credits",
    "categories": "categories",
    "edit_category": "categories",
    "delete_category": "categories",
    "vendors": "vendors",
    "add_vendor": "vendors",
    "edit_vendor": "vendors",
    "delete_vendor": "vendors",
    "customers": "customers",
    "edit_customer": "customers",
    "customer_detail": "customers",
    "vendor_summary": "vendor_summary",
    "inventory": "inventory",
    "admin_dead_stock": "dead_stock",
    "add_product": "inventory",
    "edit_product": "inventory",
    "delete_product": "inventory",
    "bulk_assign_vendor": "inventory",
    "next_sku": "inventory",
    "product_variants": "inventory",
    "product_bills": "inventory",
    "link_variant": "inventory",
    "unlink_variant": "inventory",
    "admin_inventory_overview": "inventory_overview",
    "admin_sales_summary": "sales_summary",
    "sales_list": "sales_exhibition",
    "sales_create_form": "sales_exhibition",
    "sales_edit_form": "sales_exhibition",
    "sales_add": "sales_exhibition",
    "sales_delete": "sales_exhibition",
    "sales_summary": "sales_exhibition",
    "admin_investments": "investments",
    "add_investment": "investments",
    "delete_investment": "investments",
    "inventory_labels": "labels",
    "inventory_labels_print": "labels",
    "low_stock_alerts": "low_stock_alerts",
    "add_low_stock_alert": "low_stock_alerts",
    "edit_low_stock_alert": "low_stock_alerts",
    "delete_low_stock_alert": "low_stock_alerts",
    "admin_tools": "admin_tools",
    "admin_whatsapp_send": "admin_tools",
    "clean_all_data": "admin_tools",
    "manage_users": SUPERADMIN_ONLY,
    "add_user": SUPERADMIN_ONLY,
    "edit_user": SUPERADMIN_ONLY,
    "delete_user": SUPERADMIN_ONLY,
}

# Endpoints that only require a logged-in session (no specific permission).
# Shared dashboard notes are global to the app, so any logged-in user can access
# the same note content instead of seeing a user-specific version.
LOGIN_ONLY_ENDPOINTS = {
    "admin",
    "logout",
    "no_access",
    "change_password",
    "updates",
    "add_update",
    "get_dashboard_notes",
    "save_dashboard_notes",
}

# Endpoints reachable without logging in at all.
PUBLIC_ENDPOINTS = {"login", "static", "whatsapp_webhook"}


def current_ts():
    return int(now_ist().timestamp())


def hash_password(password):
    return generate_password_hash(password)


def verify_password(password_hash, password):
    try:
        return check_password_hash(password_hash, password)
    except ValueError:
        return False


def get_user_permissions(db, user_id):
    rows = db.execute(
        "SELECT permission_key FROM user_permissions WHERE user_id = ?", (user_id,)
    ).fetchall()
    return {row["permission_key"] for row in rows}


def get_current_user():
    """Return the logged-in user dict (with permissions) for this request, or None.

    Cached on ``g`` so repeated calls within one request don't re-query.
    """
    if "user" in g:
        return g.user

    g.user = None
    user_id = session.get("user_id")
    if not user_id:
        return None

    last_active = session.get("last_activity_ts")
    try:
        last_active = int(last_active)
    except (TypeError, ValueError):
        last_active = 0

    if last_active <= 0 or (current_ts() - last_active) > SESSION_IDLE_TIMEOUT_SECONDS:
        session.clear()
        session["session_timeout_notice"] = True
        return None

    db = get_db()
    row = db.execute(
        "SELECT id, username, is_superadmin, is_active FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if not row or not row["is_active"]:
        session.clear()
        return None

    session["last_activity_ts"] = current_ts()
    permissions = set() if row["is_superadmin"] else get_user_permissions(db, row["id"])
    g.user = {
        "id": row["id"],
        "username": row["username"],
        "is_superadmin": bool(row["is_superadmin"]),
        "permissions": permissions,
    }
    return g.user


def user_can(user, *keys):
    if not user:
        return False
    if user["is_superadmin"]:
        return True
    return any(key in user["permissions"] for key in keys)


def user_can_access_endpoint(user, endpoint):
    if not user or not endpoint:
        return False
    if endpoint in PUBLIC_ENDPOINTS or endpoint in LOGIN_ONLY_ENDPOINTS:
        return True

    required = ENDPOINT_PERMISSIONS.get(endpoint, SUPERADMIN_ONLY)
    if required is SUPERADMIN_ONLY:
        return user["is_superadmin"]

    keys = required if isinstance(required, tuple) else (required,)
    return user_can(user, *keys)


def default_landing_url(user):
    """Best page to send a user to right after login."""
    if user_can(user, "dashboard"):
        return url_for("dashboard")
    if user_can(user, "billing"):
        return url_for("billing")
    if user_can(user, "sku_size_checker"):
        return url_for("sku_size_checker")
    if user_can(user, *ADMIN_PERMISSION_KEYS):
        return url_for("admin")
    return url_for("no_access")


@app.before_request
def _enforce_access_control():
    endpoint = request.endpoint
    if endpoint is None or endpoint in PUBLIC_ENDPOINTS:
        return None

    user = get_current_user()
    if not user:
        timed_out = session.pop("session_timeout_notice", False)
        if request.path.startswith("/api/") or request.is_json:
            return jsonify({"error": "Please log in to continue."}), 401
        if timed_out:
            flash("You were logged out after 20 minutes of inactivity. Please log in again.", "error")
        else:
            flash("Please log in to continue.", "error")
        next_path = request.path if request.method == "GET" else ""
        return redirect(url_for("login", next=next_path))

    if not user_can_access_endpoint(user, endpoint):
        if request.path.startswith("/api/") or request.is_json:
            return jsonify({"error": "You do not have permission to access this."}), 403
        flash("You don't have permission to access that section.", "error")
        return redirect(url_for("no_access"))
    return None


@app.context_processor
def inject_auth_helpers():
    user = get_current_user()
    return {
        "current_user": user,
        "can": lambda *keys: user_can(user, *keys),
        "main_permissions": MAIN_PERMISSIONS,
        "admin_permissions": ADMIN_PERMISSIONS,
        "admin_permission_keys": ADMIN_PERMISSION_KEYS,
        "landing_url": default_landing_url(user) if user else url_for("login"),
    }


def admin_authenticated():
    """Legacy in-view guard kept as defense-in-depth.

    Real access control now happens centrally in ``_enforce_access_control``
    (matched per-endpoint to a specific permission), so by the time a view
    function runs, the caller has already been verified. This simply reflects
    whether someone is logged in at all.
    """
    return get_current_user() is not None


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


