import unittest
import re
from Final_Project.dags.src.learn import db_redact

class TestDBRedact(unittest.TestCase):

    # --- Normal Cases ---
    def test_redact_username(self):
        value = "fmiller"
        redacted = db_redact(value, r"(.{2})(.*)", r"\1****")

        print("username --> ", redacted)

        self.assertTrue(redacted.startswith("fm"))
        self.assertIn("****", redacted)

    def test_redact_email(self):
        value = "arroyocolton@gmail.com"
        redacted = db_redact(value, r"(.{2})(.*)(@.*)", r"\1****\3")

        print("email --> ", redacted)

        self.assertTrue(redacted.startswith("ar"))
        self.assertTrue(redacted.endswith("@gmail.com"))
        self.assertIn("****", redacted)

    def test_redact_name(self):
        value = "Elizabeth Ray"
        redacted = db_redact(value, r"([A-Za-z])", "*")

        print("name --> ", redacted)

        self.assertTrue(all(c == "*" or c.isspace() for c in redacted))

    def test_redact_address(self):
        value = "9286 Bethany Glens\nVasqueztown, CO 22939"
        redacted = db_redact(value, r"\d", "X")

        print("address --> ", redacted)

        self.assertNotIn("9286", redacted)
        self.assertIn("X", redacted)

    def test_redact_products_list(self):
        products = ["Derivatives", "InvestmentStock"]
        redacted_list = [db_redact(p, r".+$", "XXX") for p in products]
        self.assertEqual(redacted_list, ["XXX", "XXX"])

    # --- Edge Cases ---
    def test_redact_empty_string(self):
        value = ""
        redacted = db_redact(value, r".*", "MASKED")

        print("Empty Input --> ", redacted)

        self.assertEqual(redacted, "MASKED")

    def test_redact_none_value(self):
        value = None
        redacted = db_redact(value, r".*", "MASKED")

        print("None Input --> ", redacted)

        self.assertEqual(redacted, "MASKED")

    def test_redact_invalid_regex(self):
        value = "test"
        with self.assertRaises(ValueError):
            db_redact(value, r"[", "MASKED")  # invalid regex

    def test_redact_special_characters(self):
        value = "email+123@gmail.com"
        redacted = db_redact(value, r"([+])", "_")

        print("special char --> ", redacted)

        self.assertNotIn("+", redacted)
        self.assertIn("_", redacted)

# --- Run the tests ---
if __name__ == "__main__":
    unittest.main()
