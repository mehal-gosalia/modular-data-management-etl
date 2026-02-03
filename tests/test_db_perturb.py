import unittest
from Final_Project.dags.src.learn import db_perturb

class TestDbPerturb(unittest.TestCase):

    # --- Normal Cases ---
    def test_perturb_normal(self):
        value = 1000
        result = db_perturb(value, noise_percent=0.05)
        self.assertTrue(950 <= result <= 1050)

    def test_perturb_zero(self):
        value = 0
        result = db_perturb(value, noise_percent=0.1)
        self.assertEqual(result, 0)

    def test_perturb_negative(self):
        value = -500
        result = db_perturb(value, noise_percent=0.1)
        self.assertTrue(-550 <= result <= -450)

    def test_perturb_large_number(self):
        value = 1e12
        result = db_perturb(value, noise_percent=0.01)
        self.assertTrue(0.99e12 <= result <= 1.01e12)

    def test_perturb_fractional(self):
        value = 123.45
        result = db_perturb(value, noise_percent=0.2)
        self.assertTrue(98.76 <= result <= 148.14)

    def test_perturb_zero_pct(self):
        value = 123
        result = db_perturb(value, noise_percent=0)
        self.assertEqual(result, 123)

    # --- Edge Cases ---
    def test_perturb_none(self):
        with self.assertRaises(TypeError):
            db_perturb(None)

    def test_perturb_non_numeric(self):
        with self.assertRaises(TypeError):
            db_perturb("abc")


if __name__ == "__main__":
    unittest.main()
