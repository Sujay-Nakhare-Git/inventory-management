# Inventory Management System

A boutique management web application for inventory, billing, refunds/exchanges, expenses, and admin analytics.

## Release Notes

### 2026-06-12

- Added Vendor management:
	- New Vendors page under Admin to add, edit, and delete vendors.
	- Vendor dropdown when adding/editing products in Inventory.
	- Vendor Sale & Stock Summary page (per-vendor stock value, units sold, sales, and gross profit).
- Reorganized the Admin page into a clean hub of tab/link cards; each component now lives on its own page:
	- Inventory Overview, Sales Summary, Initial Investments, and Tools & Danger Zone (WhatsApp test + Clean All Data).
- Added Inventory filters for Size and Vendor, plus an "Available stock only" checkbox (quantity > 0).
- Normalized all dates and times to IST (Asia/Kolkata):
	- New `istdatetime` and `istdate` template filters.
	- All record timestamps now written in IST regardless of server timezone (fixes UTC times on PythonAnywhere).
	- Added a one-shot migration script (`tools/migrate_utc_to_ist.py`) to shift existing UTC rows to IST.

### 2026-05-25

- Added G-series bill numbering (`G001`) with legacy fallback support.
- Enabled direct thermal print popup immediately after bill generation.
- Added backend WhatsApp Cloud API integration for auto-notifications on bill creation.
- Added Admin Sales Summary:
	- High selling category
	- High selling size
	- Detailed category x size sold report (descending)
- Updated documentation for new environment variables and billing behavior.

## Quick Deploy Checklist

```
1. git clone git@github.com:Sujay-Nakhare-Git/inventory-management.git
2. cd inventory-management
3. python -m venv venv && source venv/bin/activate
4. pip install -r requirements.txt
5. python app.py
→ Open http://127.0.0.1:5000
```


## Overview

Inventory Management System helps manage day-to-day boutique operations in one place:

- Inventory and category management
- Vendor management with per-vendor sale & stock summary
- Billing and bill history
- Auto bill numbering in `G001` format
- Refund and exchange workflow
- Expense tracking
- Daily Summary (admin-only)
- Profit & Loss reporting (admin-only)
- Sales Summary in Admin (top category/size + detailed descending breakdown)
- All dates and times displayed in IST (Asia/Kolkata)

## Tech Stack

- Backend: Python + Flask
- Frontend: Jinja2 templates, HTML, CSS, vanilla JavaScript
- Database: SQLite (`boutique.db`)

## Project Structure

- `app.py` : Main Flask application (routes, business logic, DB initialization)
- `templates/` : Jinja templates for all pages
- `static/` : CSS, JavaScript, logo assets
- `requirements.txt` : Python dependency list
- `boutique.db` : Local SQLite database file (auto-created)

## Features

### Core Operations

- Add, edit, delete products
- Category-wise organization
- Assign products to vendors
- Filter inventory by category, size, vendor, and available stock
- Generate bills with discount/tax/payment method
- Auto-open thermal print window right after bill generation
- Optional backend WhatsApp notification to billed customer (if phone is present and API is configured)
- View bill history and bill details

### Returns Workflow

- Delete bill/sale with stock restoration
- Process refund and exchange
- Track refunds with history

### Admin Area (Password Protected)

- Admin landing page is a hub of tab/link cards to each tool
- Admin login/unlock
- Vendors page (add/edit/delete vendors)
- Vendor Sale & Stock Summary (per-vendor stock value, units sold, sales, gross profit)
- Inventory Overview (totals + counts by category and size)
- Expenses page (with date filtering)
- Profit & Loss dashboard
- Daily Summary page (with date selection and Today quick action)
- Initial Investments page
- Tools & Danger Zone (WhatsApp test, Clean All Data)
- Sales Summary page with:
	- High selling category
	- High selling size
	- Detailed category x size sold report in descending order

## Local Setup

### Prerequisites

- Python 3.10+ recommended
- `pip`

### Installation

1. Open terminal in project folder
2. Install dependencies:

```bash
pip install -r requirements.txt
```

### Run the App

```bash
python app.py
```

Open in browser:

- http://127.0.0.1:5000

## Data & Persistence

- App uses SQLite for persistent storage.
- Database file: `boutique.db`
- Tables are auto-created on first run.

## Deploy for Free (PythonAnywhere)

PythonAnywhere is the easiest free option for this Flask + SQLite app.

1. Create a free account on PythonAnywhere
2. Upload this project folder (zip upload or git clone)
3. Create a virtualenv and install requirements
4. Create a new web app (Flask/manual)
5. Configure WSGI to point to `app.py` application object
6. Reload web app

### Recommended Production Environment Variables

Use a fixed secret key in production (instead of random restart-based key):

- `FLASK_SECRET_KEY`

For backend WhatsApp Cloud API sending:

- `WHATSAPP_CLOUD_API_TOKEN`
- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_GRAPH_VERSION` (optional, default: `v22.0`)

Example value:

- `a-long-random-secret-string`

Notes for WhatsApp Cloud API:

- Messages are sent from backend when a bill is generated and a valid customer phone is available.
- Destination phone numbers are normalized to Indian format (`91XXXXXXXXXX`).
- Your WhatsApp Business sender must be properly configured in Meta for delivery.

### Billing Counter Notes

- Bill references are stored as `bill_number` and shown in `G001` style for new bills.
- Legacy rows without `bill_number` continue to display using `#<id>` fallback.

## Notes

- Keep `debug` disabled in production environments.
- Backup `boutique.db` regularly if used as primary production storage.

## Utilities

### Bulk Attach Product Images By SKU

Use the helper script to scan a folder of images, detect SKU from file name (and optional OCR), and attach each image to the matching inventory product.
Saved image filenames are SKU-based by default in dashed format when possible (example: `CS-001.jpg`).

Run (filename matching only):

```bash
python tools/bulk_attach_images_local.py --folder "/path/to/images"
```

Run with OCR fallback:

```bash
python tools/bulk_attach_images_local.py --folder "/path/to/images" --ocr tesseract
```

Optional flags:

- `--dry-run` to preview changes without writing DB/files.
- `--overwrite` to replace existing product image mappings.
- `--db` to use a custom SQLite DB path.
- `--images-dir` to use a custom destination image folder.

## License

Private project for Gulmohar by Ankita.
