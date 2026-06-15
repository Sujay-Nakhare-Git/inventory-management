from core import *  # noqa: F401,F403
from core import _insert_exchange_bill


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


