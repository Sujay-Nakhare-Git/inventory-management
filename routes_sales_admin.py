"""Sales/Exhibition management routes for admin."""

from core import *  # noqa: F401,F403


# ── Sales/Exhibition ─────────────────────────────────────────────────────────
@app.route("/admin/sales", methods=["GET"])
def sales_list():
    if not admin_authenticated():
        flash("Please unlock Admin to manage Sales.", "error")
        return redirect(url_for("admin", next=url_for("sales_list")))

    db = get_db()
    current_date = now_ist().strftime("%Y-%m-%d")
    
    sales = db.execute(
        """
        SELECT 
            s.id, s.name, s.description, s.discount_percent, 
            s.start_date, s.end_date, s.created_at,
            COUNT(sp.product_id) as product_count,
            CASE 
                WHEN s.start_date <= ? AND s.end_date >= ? THEN 'active'
                WHEN s.end_date < ? THEN 'expired'
                WHEN s.start_date > ? THEN 'upcoming'
            END as status
        FROM sales s
        LEFT JOIN sale_products sp ON s.id = sp.sale_id
        GROUP BY s.id
        ORDER BY s.start_date DESC
        """,
        (current_date, current_date, current_date, current_date),
    ).fetchall()

    return render_template(
        "sales.html",
        sales=sales,
        current_date=current_date,
    )


@app.route("/admin/sales/create", methods=["GET"])
def sales_create_form():
    if not admin_authenticated():
        flash("Please unlock Admin to create Sales.", "error")
        return redirect(url_for("admin", next=url_for("sales_create_form")))

    db = get_db()
    categories = db.execute("SELECT id, name FROM categories ORDER BY name").fetchall()
    products = db.execute(
        "SELECT id, name, category_id, sku, size, quantity FROM products ORDER BY name"
    ).fetchall()

    return render_template(
        "sale_form.html",
        sale=None,
        categories=categories,
        products=products,
    )


@app.route("/admin/sales/<int:sale_id>/edit", methods=["GET"])
def sales_edit_form(sale_id):
    if not admin_authenticated():
        flash("Please unlock Admin to edit Sales.", "error")
        return redirect(url_for("admin", next=url_for("sales_edit_form", sale_id=sale_id)))

    db = get_db()
    sale = db.execute("SELECT * FROM sales WHERE id = ?", (sale_id,)).fetchone()
    
    if not sale:
        flash("Sale not found.", "error")
        return redirect(url_for("sales_list"))

    sale_product_ids = [
        row["product_id"]
        for row in db.execute(
            "SELECT product_id FROM sale_products WHERE sale_id = ?", (sale_id,)
        ).fetchall()
    ]

    categories = db.execute("SELECT id, name FROM categories ORDER BY name").fetchall()
    products = db.execute(
        "SELECT id, name, category_id, sku, size, quantity FROM products ORDER BY name"
    ).fetchall()

    return render_template(
        "sale_form.html",
        sale=sale,
        selected_product_ids=sale_product_ids,
        categories=categories,
        products=products,
    )


@app.route("/admin/sales/add", methods=["POST"])
def sales_add():
    if not admin_authenticated():
        return jsonify({"error": "Not authenticated"}), 401

    name = request.form.get("name", "").strip()
    description = request.form.get("description", "").strip()
    discount_percent = request.form.get("discount_percent", "0").strip()
    start_date = request.form.get("start_date", "").strip()
    end_date = request.form.get("end_date", "").strip()
    selected_products = request.form.getlist("products")

    if not name:
        flash("Sale name is required.", "error")
        return redirect(url_for("sales_list"))

    if not start_date or not end_date:
        flash("Start date and end date are required.", "error")
        return redirect(url_for("sales_list"))

    try:
        discount_percent = float(discount_percent)
        if discount_percent < 0 or discount_percent > 100:
            raise ValueError("Invalid discount")
    except (TypeError, ValueError):
        flash("Discount percent must be between 0 and 100.", "error")
        return redirect(url_for("sales_list"))

    # Validate dates
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
        if end_date < start_date:
            flash("End date must be after start date.", "error")
            return redirect(url_for("sales_list"))
    except ValueError:
        flash("Invalid date format.", "error")
        return redirect(url_for("sales_list"))

    db = get_db()
    sale_id = request.form.get("sale_id")

    try:
        if sale_id:
            # Update existing sale
            db.execute(
                """
                UPDATE sales 
                SET name = ?, description = ?, discount_percent = ?, 
                    start_date = ?, end_date = ?,
                    updated_at = datetime('now','+5 hours','+30 minutes')
                WHERE id = ?
                """,
                (name, description, discount_percent, start_date, end_date, int(sale_id)),
            )
            # Delete existing sale products and re-add
            db.execute("DELETE FROM sale_products WHERE sale_id = ?", (int(sale_id),))
            sale_id = int(sale_id)
        else:
            # Create new sale
            cursor = db.execute(
                """
                INSERT INTO sales (name, description, discount_percent, start_date, end_date, 
                                   created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, datetime('now','+5 hours','+30 minutes'), 
                        datetime('now','+5 hours','+30 minutes'))
                """,
                (name, description, discount_percent, start_date, end_date),
            )
            sale_id = cursor.lastrowid

        # Add selected products to sale
        if selected_products:
            for product_id in selected_products:
                try:
                    pid = int(product_id)
                    db.execute(
                        "INSERT OR IGNORE INTO sale_products (sale_id, product_id, created_at) "
                        "VALUES (?, ?, datetime('now','+5 hours','+30 minutes'))",
                        (sale_id, pid),
                    )
                except (TypeError, ValueError):
                    continue

        db.commit()
        action = "updated" if sale_id else "created"
        flash(f"Sale {action} successfully.", "success")
        log_update(
            f"Sale {action.capitalize()}",
            f"Sale '{name}' ({discount_percent}% discount) from {start_date} to {end_date} with {len(selected_products)} products",
            "sales",
        )
        return redirect(url_for("sales_list"))

    except Exception as e:
        db.rollback()
        flash(f"Error saving sale: {str(e)}", "error")
        return redirect(url_for("sales_list"))


@app.route("/admin/sales/<int:sale_id>/delete", methods=["POST"])
def sales_delete(sale_id):
    if not admin_authenticated():
        return jsonify({"error": "Not authenticated"}), 401

    db = get_db()
    sale = db.execute("SELECT name FROM sales WHERE id = ?", (sale_id,)).fetchone()

    if not sale:
        flash("Sale not found.", "error")
        return redirect(url_for("sales_list"))

    try:
        db.execute("DELETE FROM sale_products WHERE sale_id = ?", (sale_id,))
        db.execute("DELETE FROM sales WHERE id = ?", (sale_id,))
        db.commit()
        flash(f"Sale '{sale['name']}' deleted successfully.", "success")
        log_update("Sale Deleted", f"Sale '{sale['name']}' was deleted", "sales")
    except Exception as e:
        db.rollback()
        flash(f"Error deleting sale: {str(e)}", "error")

    return redirect(url_for("sales_list"))


@app.route("/admin/sales/<int:sale_id>/summary")
def sales_summary(sale_id):
    if not admin_authenticated():
        flash("Please unlock Admin to view Sales Summary.", "error")
        return redirect(url_for("admin", next=url_for("sales_summary", sale_id=sale_id)))

    db = get_db()
    sale = db.execute("SELECT * FROM sales WHERE id = ?", (sale_id,)).fetchone()

    if not sale:
        flash("Sale not found.", "error")
        return redirect(url_for("sales_list"))

    # Get products in this sale
    sale_products = db.execute(
        """
        SELECT p.id, p.name, p.cost_price, p.selling_price, p.quantity
        FROM products p
        JOIN sale_products sp ON p.id = sp.product_id
        WHERE sp.sale_id = ?
        """,
        (sale_id,),
    ).fetchall()

    # Get bills where these products were sold (using sale discount)
    sale_product_ids = [p["id"] for p in sale_products]
    
    if not sale_product_ids:
        return render_template(
            "sales_summary.html",
            sale=sale,
            total_sales=0,
            total_quantity=0,
            total_profit=0,
            total_cost=0,
            unique_customers=0,
            bill_items=[],
        )

    placeholders = ",".join("?" * len(sale_product_ids))
    bill_items = db.execute(
        f"""
        SELECT 
            bi.id, bi.bill_id, bi.product_id, bi.product_name, bi.quantity,
            bi.unit_price, bi.total_price,
            b.customer_name, b.customer_phone, b.created_at,
            b.discount_amount, b.discount_percent,
            p.cost_price
        FROM bill_items bi
        JOIN bills b ON bi.bill_id = b.id
        JOIN products p ON bi.product_id = p.id
        WHERE bi.product_id IN ({placeholders})
        AND b.created_at >= ?
        AND b.created_at <= ?
        AND b.bill_type = 'sale'
        ORDER BY b.created_at DESC
        """,
        (*sale_product_ids, sale["start_date"], sale["end_date"]),
    ).fetchall()

    # Calculate metrics
    total_sales = sum(float(item["total_price"] or 0) for item in bill_items)
    total_quantity = sum(int(item["quantity"] or 0) for item in bill_items)
    total_cost = sum(
        float(item["cost_price"] or 0) * int(item["quantity"] or 0)
        for item in bill_items
    )
    total_profit = total_sales - total_cost
    
    # Count unique customers
    unique_customers = len(
        set((item["customer_phone"] or "", item["customer_name"] or "") 
            for item in bill_items)
    )

    return render_template(
        "sale_detail_summary.html",
        sale=sale,
        total_sales=total_sales,
        total_quantity=total_quantity,
        total_profit=total_profit,
        total_cost=total_cost,
        unique_customers=unique_customers,
        bill_items=bill_items,
    )
