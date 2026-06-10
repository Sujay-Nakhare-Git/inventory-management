"""
One-shot migration: shift existing UTC timestamps in the SQLite DB to IST (+05:30).

Use this ONLY on environments (e.g., PythonAnywhere) where past rows were stored
in UTC because the original column defaults resolved against a UTC host clock.

Safe to run once per database — a marker file (instance/timezone_migration.json)
records that the migration has been applied and prevents double-shifting.

Usage (from project root, with virtualenv activated):
    python tools/migrate_utc_to_ist.py
    python tools/migrate_utc_to_ist.py --dry-run
    python tools/migrate_utc_to_ist.py --force   # bypass marker (DANGEROUS)
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "boutique.db")
MARKER_PATH = os.path.join(PROJECT_ROOT, "instance", "timezone_migration.json")

# (table, column) pairs to shift. Skip tables without time columns.
TARGETS = [
    ("bills", "created_at"),
    ("updates", "created_at"),
    ("expenses", "created_at"),
    ("refunds", "created_at"),
    ("store_credits", "created_at"),
    ("store_credits", "updated_at"),
    ("credit_transactions", "created_at"),
    ("investments", "created_at"),
    ("products", "created_at"),
    ("products", "updated_at"),
]


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.OperationalError:
        return False
    return any(r[1] == column for r in rows)


def load_marker() -> dict:
    if not os.path.exists(MARKER_PATH):
        return {}
    try:
        with open(MARKER_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh) or {}
    except (OSError, json.JSONDecodeError):
        return {}


def save_marker(payload: dict) -> None:
    os.makedirs(os.path.dirname(MARKER_PATH), exist_ok=True)
    with open(MARKER_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Shift UTC timestamps to IST (+05:30).")
    parser.add_argument("--dry-run", action="store_true", help="Show counts without modifying data.")
    parser.add_argument("--force", action="store_true", help="Run even if marker indicates migration already applied.")
    args = parser.parse_args()

    if not os.path.exists(DB_PATH):
        print(f"ERROR: database not found at {DB_PATH}", file=sys.stderr)
        return 2

    marker = load_marker()
    if marker.get("utc_to_ist_applied") and not args.force:
        print("Migration already applied on this DB (per marker file). Use --force to re-run.")
        print(f"Marker: {MARKER_PATH}")
        return 0

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    summary = []
    try:
        for table, column in TARGETS:
            if not column_exists(conn, table, column):
                summary.append((table, column, "skipped (no column)", 0))
                continue

            count_row = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL AND {column} != ''"
            ).fetchone()
            count = count_row[0] if count_row else 0

            if args.dry_run:
                summary.append((table, column, "dry-run", count))
                continue

            conn.execute(
                f"UPDATE {table} "
                f"SET {column} = datetime({column}, '+5 hours', '+30 minutes') "
                f"WHERE {column} IS NOT NULL AND {column} != ''"
            )
            summary.append((table, column, "shifted", count))

        if not args.dry_run:
            conn.commit()
            save_marker(
                {
                    "utc_to_ist_applied": True,
                    "applied_at": datetime.utcnow().isoformat() + "Z",
                }
            )
    finally:
        conn.close()

    print("Timestamp migration summary:")
    print(f"  DB: {DB_PATH}")
    for table, column, status, count in summary:
        print(f"  - {table}.{column}: {status} ({count} rows)")
    if args.dry_run:
        print("Dry-run only. No changes written. Marker not updated.")
    else:
        print("Done. Marker written to:", MARKER_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
