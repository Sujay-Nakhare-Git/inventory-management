import os
import io
import csv
import sqlite3
import hashlib
import hmac
from datetime import datetime
from werkzeug.utils import secure_filename
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify, g, session, Response
)

app = Flask(__name__)
app.secret_key = os.urandom(32)

DATABASE = os.path.join(app.root_path, "boutique.db")
UPLOAD_FOLDER = os.path.join(app.root_path, "static", "product_images")
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "gif"}
MAX_IMAGE_SIZE = 5 * 1024 * 1024  # 5 MB
app.config["MAX_CONTENT_LENGTH"] = MAX_IMAGE_SIZE
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


def allowed_file(filename):
    return (
        "." in filename
        and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS
    )


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
            name TEXT NOT NULL UNIQUE
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
            image_filename TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (category_id) REFERENCES categories(id)
        );

        CREATE TABLE IF NOT EXISTS bills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_name TEXT,
            customer_phone TEXT,
            subtotal REAL NOT NULL DEFAULT 0,
            discount_percent REAL NOT NULL DEFAULT 0,
            discount_amount REAL NOT NULL DEFAULT 0,
            tax_percent REAL NOT NULL DEFAULT 0,
            tax_amount REAL NOT NULL DEFAULT 0,
            total REAL NOT NULL DEFAULT 0,
            payment_method TEXT DEFAULT 'Cash',
            created_at TEXT DEFAULT (datetime('now','localtime'))
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
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            category TEXT NOT NULL DEFAULT 'General',
            amount REAL NOT NULL DEFAULT 0,
            payment_mode TEXT NOT NULL DEFAULT 'Cash',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS refunds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bill_id INTEGER,
            customer_name TEXT,
            type TEXT NOT NULL DEFAULT 'refund',
            reason TEXT,
            refund_amount REAL NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now','localtime')),
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
    """)

    # Add image_filename column if upgrading an existing DB
    existing_cols = [r[1] for r in db.execute("PRAGMA table_info(products)").fetchall()]
    if "image_filename" not in existing_cols:
        db.execute("ALTER TABLE products ADD COLUMN image_filename TEXT")
        db.commit()

    # Add payment_mode column to expenses if upgrading
    expense_cols = [r[1] for r in db.execute("PRAGMA table_info(expenses)").fetchall()]
    if "payment_mode" not in expense_cols:
        db.execute("ALTER TABLE expenses ADD COLUMN payment_mode TEXT NOT NULL DEFAULT 'Cash'")
        db.commit()

    # Seed default categories if empty
    count = db.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
    if count == 0:
        for cat in ["Sarees", "Kurtis", "Lehengas", "Suits", "Dupattas",
                     "Blouses", "Accessories", "Western Wear", "Kids Wear", "Others"]:
            db.execute("INSERT INTO categories (name) VALUES (?)", (cat,))

    db.commit()


with app.app_context():
    init_db()


# ── Helpers ──────────────────────────────────────────────────────────────
def log_update(title, description, update_type="general"):
    db = get_db()
    db.execute(
        "INSERT INTO updates (title, description, type) VALUES (?, ?, ?)",
        (title, description, update_type),
    )
    db.commit()


ADMIN_PASSWORD_HASH = "d1215baec4cf39b5c9cc710527fbbfcb3d4290caaf9b0f095d32198c9d5e28aa"


def admin_authenticated():
    return session.get("admin_authenticated", False)


# ── Dashboard ────────────────────────────────────────────────────────────
@app.route("/")
def dashboard():
    db = get_db()
    total_products = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    total_stock = db.execute("SELECT COALESCE(SUM(quantity),0) FROM products").fetchone()[0]
    low_stock = db.execute(
        "SELECT COUNT(*) FROM products WHERE quantity <= low_stock_threshold"
    ).fetchone()[0]
    today = datetime.now().strftime("%Y-%m-%d")
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
    query = (
        "SELECT p.*, c.name as category_name FROM products p "
        "LEFT JOIN categories c ON p.category_id = c.id WHERE 1=1"
    )
    params = []
    if search:
        query += " AND (p.name LIKE ? OR p.sku LIKE ?)"
        params += [f"%{search}%", f"%{search}%"]
    if category_id:
        query += " AND p.category_id = ?"
        params.append(category_id)
    query += " ORDER BY p.updated_at DESC"
    products = db.execute(query, params).fetchall()
    categories = db.execute("SELECT * FROM categories ORDER BY name").fetchall()
    return render_template(
        "inventory.html", products=products, categories=categories,
        search=search, selected_category=category_id,
    )


@app.route("/inventory/add", methods=["GET", "POST"])
def add_product():
    db = get_db()
    if request.method == "POST":
        name = request.form["name"].strip()
        category_id = request.form.get("category_id") or None
        sku = request.form.get("sku", "").strip() or None
        size = request.form.get("size", "").strip()
        color = request.form.get("color", "").strip()
        cost_price = float(request.form.get("cost_price", 0))
        selling_price = float(request.form.get("selling_price", 0))
        quantity = int(request.form.get("quantity", 0))
        low_stock_threshold = int(request.form.get("low_stock_threshold", 5))

        image_filename = None
        file = request.files.get("image")
        if file and file.filename:
            if not allowed_file(file.filename):
                flash("Invalid image type. Allowed: PNG, JPG, JPEG, WEBP, GIF.", "error")
                categories = db.execute("SELECT * FROM categories ORDER BY name").fetchall()
                return render_template("product_form.html", product=None, categories=categories)
            filename = secure_filename(file.filename)
            # Prefix with timestamp to avoid name collisions
            filename = f"{int(datetime.now().timestamp())}_{filename}"
            file.save(os.path.join(UPLOAD_FOLDER, filename))
            image_filename = filename

        db.execute(
            "INSERT INTO products (name, category_id, sku, size, color, "
            "cost_price, selling_price, quantity, low_stock_threshold, image_filename) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (name, category_id, sku, size, color, cost_price,
             selling_price, quantity, low_stock_threshold, image_filename),
        )
        db.commit()
        log_update(
            "Product Added",
            f"Added '{name}' — Qty: {quantity}, Price: ₹{selling_price}",
            "inventory",
        )
        flash(f"Product '{name}' added successfully!", "success")
        return redirect(url_for("inventory"))

    categories = db.execute("SELECT * FROM categories ORDER BY name").fetchall()
    return render_template("product_form.html", product=None, categories=categories)


@app.route("/inventory/edit/<int:product_id>", methods=["GET", "POST"])
def edit_product(product_id):
    db = get_db()
    product = db.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
    if not product:
        flash("Product not found.", "error")
        return redirect(url_for("inventory"))

    if request.method == "POST":
        name = request.form["name"].strip()
        category_id = request.form.get("category_id") or None
        sku = request.form.get("sku", "").strip() or None
        size = request.form.get("size", "").strip()
        color = request.form.get("color", "").strip()
        cost_price = float(request.form.get("cost_price", 0))
        selling_price = float(request.form.get("selling_price", 0))
        quantity = int(request.form.get("quantity", 0))
        low_stock_threshold = int(request.form.get("low_stock_threshold", 5))

        image_filename = product["image_filename"]  # keep existing by default

        # Remove image if requested
        if request.form.get("remove_image") == "1" and image_filename:
            old_path = os.path.join(UPLOAD_FOLDER, image_filename)
            if os.path.isfile(old_path):
                os.remove(old_path)
            image_filename = None

        # Replace with new upload
        file = request.files.get("image")
        if file and file.filename:
            if not allowed_file(file.filename):
                flash("Invalid image type. Allowed: PNG, JPG, JPEG, WEBP, GIF.", "error")
                categories = db.execute("SELECT * FROM categories ORDER BY name").fetchall()
                return render_template("product_form.html", product=product, categories=categories)
            # Remove old file
            if image_filename:
                old_path = os.path.join(UPLOAD_FOLDER, image_filename)
                if os.path.isfile(old_path):
                    os.remove(old_path)
            filename = secure_filename(file.filename)
            filename = f"{int(datetime.now().timestamp())}_{filename}"
            file.save(os.path.join(UPLOAD_FOLDER, filename))
            image_filename = filename

        db.execute(
            "UPDATE products SET name=?, category_id=?, sku=?, size=?, color=?, "
            "cost_price=?, selling_price=?, quantity=?, low_stock_threshold=?, "
            "image_filename=?, updated_at=datetime('now','localtime') WHERE id=?",
            (name, category_id, sku, size, color, cost_price,
             selling_price, quantity, low_stock_threshold, image_filename, product_id),
        )
        db.commit()
        log_update("Product Updated", f"Updated '{name}'", "inventory")
        flash(f"Product '{name}' updated!", "success")
        return redirect(url_for("inventory"))

    categories = db.execute("SELECT * FROM categories ORDER BY name").fetchall()
    return render_template("product_form.html", product=product, categories=categories)


@app.route("/inventory/delete/<int:product_id>", methods=["POST"])
def delete_product(product_id):
    db = get_db()
    product = db.execute("SELECT name, image_filename FROM products WHERE id = ?", (product_id,)).fetchone()
    if product:
        if product["image_filename"]:
            img_path = os.path.join(UPLOAD_FOLDER, product["image_filename"])
            if os.path.isfile(img_path):
                os.remove(img_path)
        db.execute("DELETE FROM products WHERE id = ?", (product_id,))
        db.commit()
        log_update("Product Deleted", f"Deleted '{product['name']}'", "inventory")
        flash(f"Product '{product['name']}' deleted.", "success")
    return redirect(url_for("inventory"))


# ── Categories ───────────────────────────────────────────────────────────
@app.route("/categories", methods=["GET", "POST"])
def categories():
    db = get_db()
    if request.method == "POST":
        name = request.form["name"].strip()
        if name:
            db.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (name,))
            db.commit()
            flash(f"Category '{name}' added!", "success")
        return redirect(url_for("categories"))
    cats = db.execute(
        "SELECT c.*, COUNT(p.id) as product_count FROM categories c "
        "LEFT JOIN products p ON c.id = p.category_id GROUP BY c.id ORDER BY c.name"
    ).fetchall()
    return render_template("categories.html", categories=cats)


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


@app.route("/api/billing", methods=["POST"])
def create_bill():
    data = request.get_json()
    if not data or not data.get("items"):
        return jsonify({"error": "No items provided"}), 400

    db = get_db()
    customer_name = data.get("customer_name", "").strip()
    customer_phone = data.get("customer_phone", "").strip()
    discount_percent = float(data.get("discount_percent", 0))
    tax_percent = float(data.get("tax_percent", 0))
    payment_method = data.get("payment_method", "Cash")

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

    discount_amount = round(subtotal * discount_percent / 100, 2)
    after_discount = subtotal - discount_amount
    tax_amount = round(after_discount * tax_percent / 100, 2)
    total = round(after_discount + tax_amount, 2)

    cursor = db.execute(
        "INSERT INTO bills (customer_name, customer_phone, subtotal, "
        "discount_percent, discount_amount, tax_percent, tax_amount, "
        "total, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (customer_name, customer_phone, subtotal, discount_percent,
         discount_amount, tax_percent, tax_amount, total, payment_method),
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
            "updated_at = datetime('now','localtime') WHERE id = ?",
            (it["quantity"], it["product_id"]),
        )

    db.commit()
    log_update(
        "New Bill Created",
        f"Bill #{bill_id} — ₹{total} ({payment_method}) — {customer_name or 'Walk-in'}",
        "billing",
    )

    return jsonify({"bill_id": bill_id, "total": total, "message": "Bill created!"})


@app.route("/bills")
def bills_list():
    db = get_db()
    bills = db.execute("SELECT * FROM bills ORDER BY created_at DESC").fetchall()
    return render_template("bills.html", bills=bills)


@app.route("/bills/<int:bill_id>")
def bill_detail(bill_id):
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
    return render_template("bill_detail.html", bill=bill, items=items, refunds=refunds)


@app.route("/bills/delete/<int:bill_id>", methods=["POST"])
def delete_bill(bill_id):
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
            "updated_at = datetime('now','localtime') WHERE id = ?",
            (item["quantity"], item["product_id"]),
        )

    db.execute("DELETE FROM bill_items WHERE bill_id = ?", (bill_id,))
    db.execute("DELETE FROM refund_items WHERE refund_id IN (SELECT id FROM refunds WHERE bill_id = ?)", (bill_id,))
    db.execute("DELETE FROM refunds WHERE bill_id = ?", (bill_id,))
    db.execute("DELETE FROM bills WHERE id = ?", (bill_id,))
    db.commit()

    log_update(
        "Bill Deleted",
        f"Bill #{bill_id} — ₹{bill['total']} deleted. Stock restored.",
        "billing",
    )
    flash(f"Bill #{bill_id} deleted and stock restored.", "success")
    return redirect(url_for("bills_list"))


# ── Refunds & Exchanges ──────────────────────────────────────────────────
@app.route("/refunds")
def refunds_list():
    db = get_db()
    all_refunds = db.execute(
        "SELECT r.*, "
        "(SELECT GROUP_CONCAT(ri.product_name, ', ') FROM refund_items ri WHERE ri.refund_id = r.id) as products "
        "FROM refunds r ORDER BY r.created_at DESC"
    ).fetchall()
    return render_template("refunds.html", refunds=all_refunds)


@app.route("/refunds/new/<int:bill_id>")
def new_refund(bill_id):
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

        if action == "refund":
            # Return stock
            db.execute(
                "UPDATE products SET quantity = quantity + ?, "
                "updated_at = datetime('now','localtime') WHERE id = ?",
                (qty, bi["product_id"]),
            )
            refund_amount += item_refund

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
                "updated_at = datetime('now','localtime') WHERE id = ?",
                (qty, bi["product_id"]),
            )
            # Deduct exchange product from stock
            db.execute(
                "UPDATE products SET quantity = quantity - ?, "
                "updated_at = datetime('now','localtime') WHERE id = ?",
                (qty, exchange_product_id),
            )
            exchange_product_name = exchange_product["name"]

            # Calculate price difference for refund/charge
            price_diff = bi["unit_price"] - exchange_product["selling_price"]
            if price_diff > 0:
                refund_amount += price_diff * qty  # Customer gets money back
            # If exchange product costs more, we note it but don't auto-charge

        processed_items.append({
            "product_id": bi["product_id"],
            "product_name": bi["product_name"],
            "quantity": qty,
            "unit_price": bi["unit_price"],
            "action": action,
            "exchange_product_id": exchange_product_id,
            "exchange_product_name": exchange_product_name,
        })

    if not processed_items:
        flash("No items selected for refund/exchange.", "error")
        return redirect(url_for("new_refund", bill_id=bill_id))

    refund_type = "exchange" if all(i["action"] == "exchange" for i in processed_items) else \
                  "refund" if all(i["action"] == "refund" for i in processed_items) else "mixed"

    cursor = db.execute(
        "INSERT INTO refunds (bill_id, customer_name, type, reason, refund_amount) "
        "VALUES (?, ?, ?, ?, ?)",
        (bill_id, bill["customer_name"], refund_type, reason, round(refund_amount, 2)),
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
        else:
            desc_parts.append(f"Exchanged {it['quantity']}× {it['product_name']} → {it['exchange_product_name']}")

    log_update(
        f"{'Refund' if refund_type == 'refund' else 'Exchange' if refund_type == 'exchange' else 'Refund/Exchange'} Processed",
        f"Bill #{bill_id} — {'; '.join(desc_parts)}" +
        (f" — Refund: ₹{round(refund_amount, 2)}" if refund_amount > 0 else ""),
        "billing",
    )

    flash(
        f"{'Refund' if refund_type == 'refund' else 'Exchange' if refund_type == 'exchange' else 'Refund/Exchange'} processed! "
        + (f"Refund amount: ₹{round(refund_amount, 2)}" if refund_amount > 0 else ""),
        "success",
    )
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


@app.route("/admin/logout")
def admin_logout():
    session.pop("admin_authenticated", None)
    session.pop("pl_authenticated", None)
    flash("Admin area locked.", "success")
    return redirect(url_for("dashboard"))


@app.route("/admin/clean-all-data", methods=["POST"])
def clean_all_data():
    if not admin_authenticated():
        flash("Admin access required.", "error")
        return redirect(url_for("admin"))

    password = request.form.get("password", "")
    entered_hash = hashlib.sha256(password.encode()).hexdigest()
    if not hmac.compare_digest(entered_hash, ADMIN_PASSWORD_HASH):
        flash("Incorrect admin password. Data was NOT cleared.", "error")
        return redirect(url_for("admin"))

    confirm = request.form.get("confirm", "")
    if confirm != "DELETE ALL DATA":
        flash("Confirmation text did not match. Data was NOT cleared.", "error")
        return redirect(url_for("admin"))

    db = get_db()
    db.executescript("""
        DELETE FROM refund_items;
        DELETE FROM refunds;
        DELETE FROM bill_items;
        DELETE FROM bills;
        DELETE FROM expenses;
        DELETE FROM updates;
    """)
    db.commit()

    log_update("All Data Cleared", "Admin cleared all history and data.", "admin")
    flash("All history and data have been permanently deleted.", "success")
    return redirect(url_for("admin"))


@app.route("/daily-summary")
def daily_summary():
    if not admin_authenticated():
        flash("Please unlock Admin to access Daily Summary.", "error")
        return redirect(url_for("admin", next=url_for("daily_summary")))

    db = get_db()
    today_date = datetime.now().strftime("%Y-%m-%d")
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

    payment_split = db.execute(
        "SELECT payment_method, COUNT(*) as bill_count, COALESCE(SUM(total), 0) as total "
        "FROM bills WHERE created_at LIKE ? "
        "GROUP BY payment_method ORDER BY total DESC",
        (date_filter,),
    ).fetchall()

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
    today_date = datetime.now().strftime("%Y-%m-%d")
    search = request.args.get("search", "").strip()
    category = request.args.get("category", "")
    selected_date = request.args.get("date", "").strip()
    query = "SELECT * FROM expenses WHERE 1=1"
    params = []
    if search:
        query += " AND (title LIKE ? OR description LIKE ?)"
        params += [f"%{search}%", f"%{search}%"]
    if category:
        query += " AND category = ?"
        params.append(category)
    if selected_date:
        query += " AND created_at LIKE ?"
        params.append(f"{selected_date}%")
    query += " ORDER BY created_at DESC"
    all_expenses = db.execute(query, params).fetchall()
    total = sum(e["amount"] for e in all_expenses)
    return render_template(
        "expenses.html", expenses=all_expenses, total=total,
        search=search, selected_category=category, selected_date=selected_date,
        today_date=today_date,
    )


@app.route("/expenses/add", methods=["POST"])
def add_expense():
    if not admin_authenticated():
        flash("Please unlock Admin to add expenses.", "error")
        return redirect(url_for("admin", next=url_for("expenses")))

    db = get_db()
    title = request.form["title"].strip()
    description = request.form.get("description", "").strip()
    category = request.form.get("category", "General")
    payment_mode = request.form.get("payment_mode", "Cash")
    amount = float(request.form.get("amount", 0))
    if title and amount > 0:
        db.execute(
            "INSERT INTO expenses (title, description, category, amount, payment_mode) "
            "VALUES (?, ?, ?, ?, ?)",
            (title, description, category, amount, payment_mode),
        )
        db.commit()
        log_update("Expense Added", f"{title} — ₹{amount} ({category}, {payment_mode})", "expense")
        flash(f"Expense '₹{amount} — {title}' added!", "success")
    else:
        flash("Please provide a title and valid amount.", "error")
    return redirect(url_for("expenses"))


@app.route("/expenses/delete/<int:expense_id>", methods=["POST"])
def delete_expense(expense_id):
    if not admin_authenticated():
        flash("Please unlock Admin to manage expenses.", "error")
        return redirect(url_for("admin", next=url_for("expenses")))

    db = get_db()
    expense = db.execute("SELECT title, amount FROM expenses WHERE id = ?", (expense_id,)).fetchone()
    if expense:
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
    today = datetime.now().strftime("%Y-%m-%d")
    month = datetime.now().strftime("%Y-%m")

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

    # Expenses
    total_expenses = db.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM expenses WHERE created_at LIKE ?",
        (date_filter,),
    ).fetchone()[0]

    gross_profit = revenue - cogs
    net_profit = gross_profit - total_expenses

    # Expenses by category
    expense_breakdown = db.execute(
        "SELECT category, SUM(amount) as total FROM expenses "
        "WHERE created_at LIKE ? GROUP BY category ORDER BY total DESC",
        (date_filter,),
    ).fetchall()

    # Recent bills for period
    recent_bills = db.execute(
        "SELECT * FROM bills WHERE created_at LIKE ? ORDER BY created_at DESC LIMIT 20",
        (date_filter,),
    ).fetchall()

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
        recent_expenses=recent_expenses,
        inventory_cost=inventory_cost,
        inventory_retail=inventory_retail,
        period=period,
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
    writer.writerow(["ID", "Title", "Description", "Category", "Amount (₹)", "Date"])
    for r in rows:
        writer.writerow([r["id"], r["title"], r["description"] or "",
                         r["category"], r["amount"], r["created_at"]])

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
        "SELECT b.id, b.customer_name, b.customer_phone, b.subtotal, "
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
                     "Discount %", "Discount (₹)", "Tax %", "Tax (₹)",
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
            b["id"], b["customer_name"] or "Walk-in", b["customer_phone"] or "",
            b["subtotal"], b["discount_percent"], b["discount_amount"],
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
