import sqlite3
import unittest
from decimal import Decimal
from unittest.mock import patch

from flask import template_rendered

from app import app


class PriceLookupTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute(
            "CREATE TABLE products (name TEXT, sku TEXT, size TEXT, color TEXT, "
            "cost_price REAL, selling_price REAL)"
        )
        self.db.execute(
            "INSERT INTO products VALUES (?, ?, ?, ?, ?, ?)",
            ("Test item", "KUR-001", "M", "Blue", 100, 175),
        )
        self.user = {
            "id": 1, "username": "tester", "is_superadmin": False,
            "permissions": {"price_lookup"},
        }
        db_patch = patch("routes_admin.get_db", return_value=self.db)
        user_patch = patch("core.get_current_user", side_effect=lambda: self.user)
        db_patch.start()
        user_patch.start()
        self.addCleanup(db_patch.stop)
        self.addCleanup(user_patch.stop)
        self.addCleanup(self.db.close)
        self.client = app.test_client()

    def lookup(self, sku="KUR-001"):
        rendered = {}

        def capture(sender, template, context, **extra):
            rendered.update(context)

        with template_rendered.connected_to(capture, app):
            response = self.client.get("/admin/price-lookup", query_string={"sku": sku})
        self.assertEqual(response.status_code, 200)
        return response.get_data(as_text=True), rendered

    def test_lookup_and_steps_stop_before_selling_price(self):
        html, context = self.lookup(" kur-001 ")
        self.assertIn("Test item", html)
        self.assertEqual(context["product"]["sku"], "KUR-001")
        self.assertEqual(context["difference"], Decimal("75"))
        self.assertEqual(
            [(step["percent"], step["price"]) for step in context["price_steps"]],
            [(40, Decimal("140")), (50, Decimal("150")),
             (60, Decimal("160")), (70, Decimal("170"))],
        )
        self.assertEqual(context["selling_profit_percent"], 75)
        self.assertIn("75.00% (Selling Price)", html)
        self.assertIn("175.00", html)

    def test_equal_step_is_only_shown_as_final_selling_price(self):
        self.db.execute("UPDATE products SET selling_price = 160")
        html, context = self.lookup()
        self.assertEqual([step["percent"] for step in context["price_steps"]], [40, 50])
        self.assertIn("60.00% (Selling Price)", html)

    def test_low_selling_price_and_zero_cost(self):
        for cost, selling, percent in [(100, 130, 30), (100, 80, -20), (0, 100, None)]:
            with self.subTest(cost=cost, selling=selling):
                self.db.execute(
                    "UPDATE products SET cost_price = ?, selling_price = ?",
                    (cost, selling),
                )
                html, context = self.lookup()
                self.assertEqual(context["price_steps"], [])
                self.assertEqual(context["selling_profit_percent"], percent)
                self.assertEqual(context["difference"], selling - cost)
                if cost == 0:
                    self.assertIn("Profit percentages cannot be calculated", html)

    def test_prices_are_rounded_to_paise(self):
        self.db.execute("UPDATE products SET cost_price = 19.99, selling_price = 30")
        _, context = self.lookup()
        self.assertEqual(
            [step["price"] for step in context["price_steps"]],
            [Decimal("27.99"), Decimal("29.99")],
        )

    def test_steps_continue_past_one_hundred_percent(self):
        self.db.execute("UPDATE products SET selling_price = 255")
        _, context = self.lookup()
        self.assertEqual(
            [step["percent"] for step in context["price_steps"]],
            list(range(40, 160, 10)),
        )
        self.assertEqual(context["price_steps"][-1]["price"], Decimal("250"))
        self.assertEqual(context["selling_profit_percent"], 155)

    def test_empty_and_unknown_codes(self):
        for sku in ("", "UNKNOWN", "' OR 1=1 --"):
            with self.subTest(sku=sku):
                html, context = self.lookup(sku)
                self.assertIsNone(context["product"])
                if sku:
                    self.assertIn("No item found for code", html)

    def test_permission_controls_route_and_admin_link(self):
        response = self.client.get("/admin")
        self.assertIn(b"/admin/price-lookup", response.data)
        self.user["permissions"] = {"sku_size_checker"}
        response = self.client.get("/admin/price-lookup?sku=KUR-001")
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.location.endswith("/no-access"))
        self.assertNotIn(b"/admin/price-lookup", self.client.get("/admin").data)
        self.user = None
        response = self.client.get("/admin/price-lookup?sku=KUR-001")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.location)


if __name__ == "__main__":
    unittest.main()
