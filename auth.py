import functools
from datetime import datetime
from flask import session, g, request, redirect, url_for, flash, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from config import (
    SESSION_IDLE_TIMEOUT_SECONDS, SUPERADMIN_ONLY, ENDPOINT_PERMISSIONS,
    LOGIN_ONLY_ENDPOINTS, PUBLIC_ENDPOINTS, MAIN_PERMISSIONS, ADMIN_PERMISSIONS,
    ADMIN_PERMISSION_KEYS, IST
)
from models import get_db
from services import now_ist


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


def require_admin(next_endpoint="admin"):
    """Decorator to guard admin-only routes.

    Usage:
        @app.route("/admin/dangerous", methods=["POST"])
        @require_admin(next_endpoint="admin_tools")
        def dangerous_operation():
            ...

    Replaces repeated:
        if not admin_authenticated():
            flash("Please unlock Admin...")
            return redirect(url_for("admin", next=url_for("endpoint_name")))

    Args:
        next_endpoint: endpoint name to redirect back to (default: "admin")
    """
    from functools import wraps
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not admin_authenticated():
                flash("Please unlock Admin to access this section.", "error")
                return redirect(url_for(next_endpoint, next=request.path))
            return f(*args, **kwargs)
        return decorated_function
    return decorator



