import os
import json
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")


def _load_or_create_secret_key():
    """Load or create a stable secret key persisted to instance/secret.key."""
    instance_dir = os.path.join(os.path.dirname(__file__), "instance")
    os.makedirs(instance_dir, exist_ok=True)
    secret_key_path = os.path.join(instance_dir, "secret.key")

    if os.path.exists(secret_key_path):
        with open(secret_key_path, "rb") as f:
            return f.read()

    import secrets
    key = secrets.token_bytes(32)
    with open(secret_key_path, "wb") as f:
        f.write(key)
    return key


SECRET_KEY = os.environ.get("SECRET_KEY") or _load_or_create_secret_key()

# Database
DATABASE = os.path.join(os.path.dirname(__file__), "boutique.db")

# Image upload directories
EXPENSE_BILL_UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "static", "expense_bills")
PRODUCT_IMAGE_UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "static", "product_images")

# Image allowed extensions and sizes
ALLOWED_BILL_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp"}
ALLOWED_PRODUCT_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "gif"}
MAX_IMAGE_SIZE = (1600, 1600)

# WhatsApp configuration
WHATSAPP_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "instance", "whatsapp_config.json")

# Payment methods
ALLOWED_PAYMENT_METHODS = {"Cash", "UPI", "Card", "Bank Transfer"}

# Auth configuration
LEGACY_ADMIN_PASSWORD_HASH = "d1215baec4cf39b5c9cc710527fbbfcb3d4290caaf9b0f095d32198c9d5e28aa"
SESSION_IDLE_TIMEOUT_SECONDS = 20 * 60

# Permission registry: (key, label) pairs for the UI
MAIN_PERMISSIONS = [
    ("dashboard", "Dashboard"),
    ("sku_size_checker", "Size Availability"),
    ("billing", "New Bill / Billing"),
]

ADMIN_PERMISSIONS = [
    ("daily_summary", "Daily Summary"),
    ("bills", "Bill History"),
    ("refunds", "Refund Details"),
    ("expenses", "Expenses"),
    ("profit_loss", "Profit & Loss"),
    ("store_credits", "Store Credits"),
    ("categories", "Categories"),
    ("vendors", "Vendors"),
    ("customers", "Customers"),
    ("vendor_summary", "Vendor Summary"),
    ("inventory", "Inventory"),
    ("dead_stock", "Backlog / Dead Stock"),
    ("inventory_overview", "Inventory Overview"),
    ("sales_summary", "Sales Summary"),
    ("sales_exhibition", "Sales / Exhibition"),
    ("investments", "Initial Investments"),
    ("labels", "Print Labels"),
    ("low_stock_alerts", "Low Stock Alerts"),
    ("admin_tools", "Tools & Danger Zone"),
]

ALL_PERMISSION_KEYS = {key for key, _ in MAIN_PERMISSIONS + ADMIN_PERMISSIONS}
ADMIN_PERMISSION_KEYS = [key for key, _ in ADMIN_PERMISSIONS]

# Sentinel for superadmin-only routes
SUPERADMIN_ONLY = object()

# Endpoint -> required permission mapping
ENDPOINT_PERMISSIONS = {
    "dashboard": "dashboard",
    "get_dashboard_notes": "dashboard",
    "save_dashboard_notes": "dashboard",
    "sku_size_checker": "sku_size_checker",
    "billing": "billing",
    "api_products": "billing",
    "api_customers_search": "billing",
    "create_bill": "billing",
    "lookup_store_credit": "billing",
    "bill_detail": ("billing", "bills"),
    "bill_thermal_print": ("billing", "bills"),
    "rental_deposit_return_receipt": ("billing", "bills"),
    "bills_list": "bills",
    "edit_bill": "bills",
    "delete_bill": "bills",
    "return_rental_deposit": "bills",
    "export_sales": "bills",
    "daily_summary": "daily_summary",
    "refunds_list": "refunds",
    "new_refund": "refunds",
    "process_refund": "refunds",
    "expenses": "expenses",
    "add_expense": "expenses",
    "edit_expense": "expenses",
    "delete_expense": "expenses",
    "export_expenses": "expenses",
    "profit_loss": "profit_loss",
    "store_credits": "store_credits",
    "add_store_credit": "store_credits",
    "add_credit_balance": "store_credits",
    "credit_transactions": "store_credits",
    "delete_store_credit": "store_credits",
    "delete_credit_transaction": "store_credits",
    "edit_credit_transaction": "store_credits",
    "categories": "categories",
    "edit_category": "categories",
    "delete_category": "categories",
    "vendors": "vendors",
    "add_vendor": "vendors",
    "edit_vendor": "vendors",
    "delete_vendor": "vendors",
    "customers": "customers",
    "edit_customer": "customers",
    "customer_detail": "customers",
    "vendor_summary": "vendor_summary",
    "inventory": "inventory",
    "admin_dead_stock": "dead_stock",
    "add_product": "inventory",
    "edit_product": "inventory",
    "delete_product": "inventory",
    "bulk_assign_vendor": "inventory",
    "next_sku": "inventory",
    "product_variants": "inventory",
    "product_bills": "inventory",
    "link_variant": "inventory",
    "unlink_variant": "inventory",
    "admin_inventory_overview": "inventory_overview",
    "admin_sales_summary": "sales_summary",
    "sales_list": "sales_exhibition",
    "sales_create_form": "sales_exhibition",
    "sales_edit_form": "sales_exhibition",
    "sales_add": "sales_exhibition",
    "sales_delete": "sales_exhibition",
    "sales_summary": "sales_exhibition",
    "admin_investments": "investments",
    "add_investment": "investments",
    "delete_investment": "investments",
    "inventory_labels": "labels",
    "inventory_labels_print": "labels",
    "low_stock_alerts": "low_stock_alerts",
    "add_low_stock_alert": "low_stock_alerts",
    "edit_low_stock_alert": "low_stock_alerts",
    "delete_low_stock_alert": "low_stock_alerts",
    "admin_tools": "admin_tools",
    "admin_whatsapp_send": "admin_tools",
    "clean_all_data": "admin_tools",
    "manage_users": SUPERADMIN_ONLY,
    "add_user": SUPERADMIN_ONLY,
    "edit_user": SUPERADMIN_ONLY,
    "delete_user": SUPERADMIN_ONLY,
}

LOGIN_ONLY_ENDPOINTS = {
    "admin",
    "logout",
    "no_access",
    "change_password",
    "updates",
    "add_update",
    "get_dashboard_notes",
    "save_dashboard_notes",
}

PUBLIC_ENDPOINTS = {"login", "static", "whatsapp_webhook"}


def load_whatsapp_cloud_config():
    """Load WhatsApp Cloud API config from env or JSON file."""
    token = os.getenv("WHATSAPP_CLOUD_API_TOKEN", "").strip()
    phone_number_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID", "").strip()
    graph_version = os.getenv("WHATSAPP_GRAPH_VERSION", "v22.0").strip() or "v22.0"
    if token and phone_number_id:
        return token, phone_number_id, graph_version

    try:
        with open(WHATSAPP_CONFIG_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh) or {}
    except (OSError, json.JSONDecodeError):
        data = {}

    file_token = str(data.get("token", "")).strip()
    file_phone_number_id = str(data.get("phone_number_id", "")).strip()
    file_graph_version = str(data.get("graph_version", "v22.0")).strip() or "v22.0"
    return file_token, file_phone_number_id, file_graph_version


def load_whatsapp_webhook_config():
    """Load WhatsApp webhook config from env or JSON file."""
    verify_token = os.getenv("WHATSAPP_WEBHOOK_VERIFY_TOKEN", "").strip()
    app_secret = os.getenv("WHATSAPP_APP_SECRET", "").strip()
    if verify_token and app_secret:
        return verify_token, app_secret

    try:
        with open(WHATSAPP_CONFIG_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh) or {}
    except (OSError, json.JSONDecodeError):
        data = {}

    return (
        verify_token or str(data.get("webhook_verify_token", "")).strip(),
        app_secret or str(data.get("app_secret", "")).strip(),
    )


os.makedirs(EXPENSE_BILL_UPLOAD_DIR, exist_ok=True)
os.makedirs(PRODUCT_IMAGE_UPLOAD_DIR, exist_ok=True)
os.makedirs(os.path.dirname(WHATSAPP_CONFIG_PATH), exist_ok=True)
