from core import *  # noqa: F401,F403


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


