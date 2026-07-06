from core import *  # noqa: F401,F403


# ── Dashboard ────────────────────────────────────────────────────────────
@app.route("/")
def dashboard():
    db = get_db()
    total_products = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    total_stock = db.execute("SELECT COALESCE(SUM(quantity),0) FROM products").fetchone()[0]
    items_sold = db.execute(
        "SELECT COALESCE(SUM(quantity),0) FROM bill_items"
    ).fetchone()[0]
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

    low_stock_alerts = get_triggered_low_stock_alerts(db)

    return render_template(
        "dashboard.html",
        total_products=total_products,
        total_stock=total_stock,
        items_sold=items_sold,
        available_items=available_items,
        low_stock=low_stock,
        today_sales=today_sales,
        total_bills=total_bills,
        recent_updates=recent_updates,
        low_stock_products=low_stock_products,
        low_stock_alerts=low_stock_alerts,
    )


# ── Inventory ────────────────────────────────────────────────────────────
@app.route("/inventory")
def inventory():
    db = get_db()
    search = request.args.get("search", "").strip()
    category_id = request.args.get("category", "")
    size_filter = request.args.get("size", "")
    vendor_filter = request.args.get("vendor", "")
    # Default "available stock only" to checked on a fresh load; respect the
    # user's choice once the filter form has been submitted (marked by 'filtered').
    if request.args.get("filtered"):
        in_stock_only = request.args.get("in_stock", "")
    else:
        in_stock_only = "1"
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


@app.route("/api/product/<int:product_id>/bills")
def product_bills(product_id):
    db = get_db()
    rows = db.execute(
        "SELECT b.id, b.bill_number, b.created_at, b.customer_name, "
        "bi.quantity, bi.unit_price, bi.total_price "
        "FROM bill_items bi JOIN bills b ON bi.bill_id = b.id "
        "WHERE bi.product_id = ? ORDER BY b.created_at DESC",
        (product_id,),
    ).fetchall()
    return jsonify({"bills": [dict(r) for r in rows]})


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
            "product_name": product["sku"] or product["name"],
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


