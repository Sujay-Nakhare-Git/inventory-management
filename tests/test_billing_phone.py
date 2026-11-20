import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import core
from app import app


class BillingPhoneTests(unittest.TestCase):
    def setUp(self):
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        database_patch = patch.object(core, "DATABASE", str(Path(temp_dir.name) / "test.db"))
        database_patch.start()
        self.addCleanup(database_patch.stop)
        user_patch = patch("core.get_current_user", return_value={
            "id": 1, "username": "tester", "is_superadmin": False,
            "permissions": {"billing"},
        })
        user_patch.start()
        self.addCleanup(user_patch.stop)
        whatsapp_patch = patch("routes_inventory.send_whatsapp_bill_message", return_value={
            "sent": False, "reason": "test",
        })
        whatsapp_patch.start()
        self.addCleanup(whatsapp_patch.stop)
        with app.app_context():
            core.init_db()
            db = core.get_db()
            db.execute(
                "INSERT INTO products (id, name, selling_price, quantity) VALUES (1, 'Test', 100, 5)"
            )
            db.commit()
        self.client = app.test_client()

    def payload(self, bill_type, phone):
        return {
            "bill_type": bill_type, "customer_name": "Test Customer",
            "customer_phone": phone,
            "items": [{"product_id": 1, "quantity": 1}],
            "rental_days": 1, "rent_amount": 100, "deposit_amount": 0,
            "payment_method": "Cash",
        }

    def test_invalid_phones_do_not_create_bills_or_reduce_stock(self):
        for bill_type in ("sale", "rental"):
            for phone in ("123456789", "12345678901", "12345abcde", "+919876543210",
                          "98765 4321", "１２３４５６７８９０", None, 9876543210):
                with self.subTest(bill_type=bill_type, phone=phone):
                    response = self.client.post("/api/billing", json=self.payload(bill_type, phone))
                    self.assertEqual(response.status_code, 400)
                    self.assertEqual(
                        response.get_json()["error"],
                        "Phone number must contain exactly 10 digits.",
                    )
        with app.app_context():
            self.assertEqual(core.get_db().execute("SELECT COUNT(*) FROM bills").fetchone()[0], 0)
            self.assertEqual(core.get_db().execute("SELECT quantity FROM products").fetchone()[0], 5)

    def test_ten_digit_phone_is_saved_for_sales_and_rentals(self):
        for bill_type in ("sale", "rental"):
            with self.subTest(bill_type=bill_type):
                response = self.client.post(
                    "/api/billing", json=self.payload(bill_type, " 0123456789 ")
                )
                self.assertEqual(response.status_code, 200, response.get_json())
                with app.app_context():
                    row = core.get_db().execute(
                        "SELECT customer_phone FROM bills WHERE id = ?",
                        (response.get_json()["bill_id"],),
                    ).fetchone()
                    self.assertEqual(row["customer_phone"], "0123456789")


if __name__ == "__main__":
    unittest.main()
