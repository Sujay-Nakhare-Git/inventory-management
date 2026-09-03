"""Login, logout, and user/permission management routes."""

from core import *  # noqa: F401,F403


@app.route("/login", methods=["GET", "POST"])
def login():
    existing_user = get_current_user()
    if existing_user:
        return redirect(default_landing_url(existing_user))

    db = get_db()
    first_time_setup = db.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0
    next_url = request.values.get("next", "").strip()

    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")

        user_row = db.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        authenticated_user_id = None

        if user_row:
            if not user_row["is_active"]:
                flash("This account has been disabled. Contact your administrator.", "error")
                return render_template("login.html", next_url=next_url, first_time_setup=False)
            if verify_password(user_row["password_hash"], password):
                authenticated_user_id = user_row["id"]
        elif username == "admin" and first_time_setup:
            # One-time migration: accept the legacy shared admin password and
            # turn it into a real superadmin account.
            entered_hash = hashlib.sha256(password.encode()).hexdigest()
            if hmac.compare_digest(entered_hash, LEGACY_ADMIN_PASSWORD_HASH):
                cursor = db.execute(
                    "INSERT INTO users (username, password_hash, is_superadmin, is_active, created_at) "
                    "VALUES ('admin', ?, 1, 1, datetime('now','+5 hours','+30 minutes'))",
                    (hash_password(password),),
                )
                db.commit()
                authenticated_user_id = cursor.lastrowid

        if authenticated_user_id is not None:
            session.clear()
            session["user_id"] = authenticated_user_id
            session["last_activity_ts"] = current_ts()
            db.execute(
                "UPDATE users SET last_login_at = datetime('now','+5 hours','+30 minutes') WHERE id = ?",
                (authenticated_user_id,),
            )
            db.commit()
            flash("Login successful.", "success")
            if (
                next_url.startswith("/")
                and not next_url.startswith("//")
                and not next_url.startswith("/login")
            ):
                return redirect(next_url)
            return redirect(default_landing_url(get_current_user()))

        flash("Invalid username or password.", "error")

    return render_template("login.html", next_url=next_url, first_time_setup=first_time_setup)


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "success")
    return redirect(url_for("login"))


@app.route("/no-access")
def no_access():
    return render_template("no_access.html")


@app.route("/account/change-password", methods=["GET", "POST"])
def change_password():
    user = get_current_user()

    if request.method == "POST":
        current_password = request.form.get("current_password", "")
        new_password = request.form.get("new_password", "")
        confirm_password = request.form.get("confirm_password", "")

        db = get_db()
        row = db.execute("SELECT password_hash FROM users WHERE id = ?", (user["id"],)).fetchone()

        if not row or not verify_password(row["password_hash"], current_password):
            flash("Current password is incorrect.", "error")
        elif len(new_password) < 8:
            flash("New password must be at least 8 characters.", "error")
        elif new_password != confirm_password:
            flash("New password and confirmation do not match.", "error")
        else:
            db.execute(
                "UPDATE users SET password_hash = ? WHERE id = ?",
                (hash_password(new_password), user["id"]),
            )
            db.commit()
            flash("Password updated successfully.", "success")
            return redirect(default_landing_url(user))

    return render_template("change_password.html")


# ── User Management (superadmin only) ────────────────────────────────────
@app.route("/admin/users")
def manage_users():
    db = get_db()
    users = db.execute(
        "SELECT id, username, is_superadmin, is_active, created_at, last_login_at "
        "FROM users ORDER BY is_superadmin DESC, username"
    ).fetchall()
    permissions_by_user = {row["id"]: get_user_permissions(db, row["id"]) for row in users}
    return render_template(
        "manage_users.html",
        users=users,
        permissions_by_user=permissions_by_user,
    )


@app.route("/admin/users/add", methods=["POST"])
def add_user():
    db = get_db()
    username = request.form.get("username", "").strip().lower()
    password = request.form.get("password", "")
    selected_permissions = set(request.form.getlist("permissions")) & ALL_PERMISSION_KEYS

    if not username or not password:
        flash("Username and password are required.", "error")
        return redirect(url_for("manage_users"))
    if len(username) < 3:
        flash("Username must be at least 3 characters.", "error")
        return redirect(url_for("manage_users"))
    if len(password) < 8:
        flash("Password must be at least 8 characters.", "error")
        return redirect(url_for("manage_users"))

    existing = db.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
    if existing:
        flash(f"Username '{username}' is already taken.", "error")
        return redirect(url_for("manage_users"))

    cursor = db.execute(
        "INSERT INTO users (username, password_hash, is_superadmin, is_active, created_at) "
        "VALUES (?, ?, 0, 1, datetime('now','+5 hours','+30 minutes'))",
        (username, hash_password(password)),
    )
    user_id = cursor.lastrowid
    for key in selected_permissions:
        db.execute(
            "INSERT OR IGNORE INTO user_permissions (user_id, permission_key) VALUES (?, ?)",
            (user_id, key),
        )
    db.commit()
    log_update("User Created", f"Created user '{username}' with {len(selected_permissions)} permission(s)", "user")
    flash(f"User '{username}' created.", "success")
    return redirect(url_for("manage_users"))


@app.route("/admin/users/<int:user_id>/edit", methods=["POST"])
def edit_user(user_id):
    db = get_db()
    target = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    if not target:
        flash("User not found.", "error")
        return redirect(url_for("manage_users"))
    if target["is_superadmin"]:
        flash("Superadmin permissions cannot be edited.", "error")
        return redirect(url_for("manage_users"))

    new_password = request.form.get("password", "").strip()
    is_active = 1 if request.form.get("is_active") else 0
    selected_permissions = set(request.form.getlist("permissions")) & ALL_PERMISSION_KEYS

    if new_password:
        if len(new_password) < 8:
            flash("Password must be at least 8 characters.", "error")
            return redirect(url_for("manage_users"))
        db.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (hash_password(new_password), user_id),
        )

    db.execute("UPDATE users SET is_active = ? WHERE id = ?", (is_active, user_id))
    db.execute("DELETE FROM user_permissions WHERE user_id = ?", (user_id,))
    for key in selected_permissions:
        db.execute(
            "INSERT OR IGNORE INTO user_permissions (user_id, permission_key) VALUES (?, ?)",
            (user_id, key),
        )
    db.commit()
    log_update("User Updated", f"Updated permissions for '{target['username']}'", "user")
    flash(f"User '{target['username']}' updated.", "success")
    return redirect(url_for("manage_users"))


@app.route("/admin/users/<int:user_id>/delete", methods=["POST"])
def delete_user(user_id):
    db = get_db()
    target = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    if not target:
        flash("User not found.", "error")
        return redirect(url_for("manage_users"))
    if target["is_superadmin"]:
        flash("Cannot delete a superadmin account.", "error")
        return redirect(url_for("manage_users"))

    db.execute("DELETE FROM user_permissions WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    db.commit()
    log_update("User Deleted", f"Deleted user '{target['username']}'", "user")
    flash(f"User '{target['username']}' deleted.", "success")
    return redirect(url_for("manage_users"))
