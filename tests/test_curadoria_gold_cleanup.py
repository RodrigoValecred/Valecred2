import unittest
import os

class TestCuradoriaGoldCleanup(unittest.TestCase):
    def setUp(self):
        self.notebook_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Shared.Notebook/notebook-content.py"
        with open(self.notebook_path, "r", encoding="utf-8") as f:
            self.content = f.read()

    def test_find_column_removed(self):
        """Test that find_column function definition is removed."""
        self.assertNotIn("def find_column", self.content)

    def test_candidates_lists_removed(self):
        """Test that candidate lists are removed."""
        self.assertNotIn('risk_candidates = ["valoremabertort"', self.content)
        self.assertNotIn('limit_candidates = ["limitefomento"', self.content)

    def test_standardized_columns_usage(self):
        """Test that standardized columns are used."""
        self.assertIn('col("estudo.valor_risco_estudo")', self.content)
        self.assertIn('col("estudo.valor_limite_estudo")', self.content)

if __name__ == "__main__":
    unittest.main()
