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
