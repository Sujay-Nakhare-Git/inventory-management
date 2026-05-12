# Inventory Management System

A boutique management web application for inventory, billing, refunds/exchanges, expenses, and admin analytics.

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
- Billing and bill history
- Refund and exchange workflow
- Expense tracking
- Daily Summary (admin-only)
- Profit & Loss reporting (admin-only)

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
- Generate bills with discount/tax/payment method
- View bill history and bill details

### Returns Workflow

- Delete bill/sale with stock restoration
- Process refund and exchange
- Track refunds with history

### Admin Area (Password Protected)

- Admin login/unlock
- Expenses page (with date filtering)
- Profit & Loss dashboard
- Daily Summary page (with date selection and Today quick action)

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

Example value:

- `a-long-random-secret-string`

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
