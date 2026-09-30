import os
import json
import urllib.error
import urllib.request
from datetime import datetime
from flask import current_app
from config import (
    IST, ALLOWED_BILL_IMAGE_EXTENSIONS, PRODUCT_IMAGE_UPLOAD_DIR,
    EXPENSE_BILL_UPLOAD_DIR, MAX_IMAGE_SIZE, load_whatsapp_cloud_config
)
from models import get_db, get_next_bill_number, upsert_customer

try:
    from PIL import Image, UnidentifiedImageError
except ImportError:
    Image = None
    UnidentifiedImageError = Exception


def now_ist():
    return datetime.now(IST)


def now_ist_db():
    """Return ISO format datetime string in IST timezone for database DEFAULT clauses.

    Used as: INSERT INTO table (..., created_at) VALUES (..., ?), (now_ist_db(),)
    Replaces hardcoded: datetime('now','+5 hours','+30 minutes')
    """
    return now_ist().isoformat()


def profit_percent_on_cost(profit, cost):
    try:
        cost_value = float(cost or 0)
        profit_value = float(profit or 0)
    except (TypeError, ValueError):
        return None

    if cost_value <= 0:
        return None

    return round((profit_value / cost_value) * 100, 2)




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


def inject_bill_helpers():
    """Return helpers to be injected into templates by core.py."""
    return {"display_bill_ref": display_bill_ref}




# ── Helpers ──────────────────────────────────────────────────────────────
def parse_amount(value, default=0.0, max_val=None):
    """Parse and validate a currency amount from form input.

    Args:
        value: form input (string, int, float, or None)
        default: fallback if invalid (default: 0.0)
        max_val: optional maximum allowed value

    Returns:
        float rounded to 2 decimals, or default on error

    Replaces: float(request.form.get(...))  scattered across routes
    """
    try:
        amount = round(float(value or default), 2)
        if amount < 0:
            return default
        if max_val is not None and amount > max_val:
            return default
        return amount
    except (TypeError, ValueError):
        return default


def parse_int(value, default=0, min_val=None, max_val=None):
    """Parse and validate an integer from form input.

    Args:
        value: form input (string, int, or None)
        default: fallback if invalid (default: 0)
        min_val: optional minimum allowed value
        max_val: optional maximum allowed value

    Returns:
        int, or default on error or out of bounds

    Replaces: int(request.form.get(...)) or int(request.form["..."]) scattered across routes
    """
    try:
        num = int(value or default)
        if min_val is not None and num < min_val:
            return default
        if max_val is not None and num > max_val:
            return default
        return num
    except (TypeError, ValueError):
        return default


def apply_store_credit(db, credit_id, bill_id, amount, txn_type, notes=""):
    """Add or deduct store credit and record transaction.

    Args:
        db: database connection
        credit_id: store_credits.id
        bill_id: bills.id (or None for non-bill transactions)
        amount: amount in ₹ (positive for credit, negative for debit)
        txn_type: 'credit' or 'debit'
        notes: optional transaction notes/remarks

    Replaces all copy-pasted store credit mutations across routes.
    """
    db.execute(
        "UPDATE store_credits SET balance = balance + ?, updated_at = ? WHERE id = ?",
        (amount, now_ist_db(), credit_id),
    )
    db.execute(
        "INSERT INTO credit_transactions (credit_id, bill_id, amount, transaction_type, notes, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (credit_id, bill_id, amount, txn_type, notes, now_ist_db()),
    )


def log_update(title, description, update_type="general"):
    db = get_db()
    db.execute(
        "INSERT INTO updates (title, description, type, created_at) "
        "VALUES (?, ?, ?, ?)",
        (title, description, update_type, now_ist_db()),
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



