"""Application entry point.

The application is split into focused modules:

- ``core``            – Flask app, configuration, database, shared helpers,
                        template filters, WhatsApp + image utilities, auth.
- ``routes_inventory`` – dashboard, inventory, categories, billing.
- ``routes_sales``     – bills, store credits, refunds, updates.
- ``routes_admin``     – admin panel, vendors, investments, tools, labels.
- ``routes_reports``   – daily summary, expenses, profit & loss, exports.

Importing the route modules registers their routes on the shared ``app``.
"""

from core import app

# Importing these modules registers their @app.route handlers.
import routes_inventory  # noqa: E402,F401
import routes_sales  # noqa: E402,F401
import routes_admin  # noqa: E402,F401
import routes_reports  # noqa: E402,F401


if __name__ == "__main__":
    app.run(debug=True, port=5000)
