from core import *  # noqa: F401,F403


# ── Admin ────────────────────────────────────────────────────────────────
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


