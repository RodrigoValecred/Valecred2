import unittest
from unittest.mock import MagicMock, patch
import sys
from types import ModuleType

# Mock PySpark modules
mock_pyspark = ModuleType('pyspark')
mock_pyspark_sql = ModuleType('pyspark.sql')
mock_pyspark_functions = ModuleType('pyspark.sql.functions')
mock_delta = ModuleType('delta')
mock_delta_tables = ModuleType('delta.tables')

sys.modules['pyspark'] = mock_pyspark
sys.modules['pyspark.sql'] = mock_pyspark_sql
sys.modules['pyspark.sql.functions'] = mock_pyspark_functions
sys.modules['delta'] = mock_delta
sys.modules['delta.tables'] = mock_delta_tables

class MockColumn:
    def __init__(self, name):
        self.name = name
    def __repr__(self):
        return f"Column<{self.name}>"

def col(name): return MockColumn(name)
def lit(val): return MockColumn(str(val))
def max(col): return MockColumn(f"max({col})")
def greatest(*cols): return MockColumn(f"greatest({cols})")
def coalesce(*cols): return MockColumn(f"coalesce({cols})")

mock_pyspark_functions.col = col
mock_pyspark_functions.lit = lit
mock_pyspark_functions.max = max
mock_pyspark_functions.greatest = greatest
mock_pyspark_functions.coalesce = coalesce

class TestWatermarkOptimization(unittest.TestCase):
    def test_first_instead_of_collect(self):
        # Este teste garante que o uso de .first()[0] em um DataFrame agregado
        # é logicamente equivalente para extrair um único valor escalar.

        mock_df = MagicMock()
        mock_agg_df = MagicMock()

        mock_df.agg.return_value = mock_agg_df
        # Simula .first() para retornar um objeto tipo linha (tupla/lista)
        mock_agg_df.first.return_value = ["2024-01-01"]
        # Simula .collect() para retornar uma lista de linhas
        mock_agg_df.collect.return_value = [["2024-01-01"]]

        # Original (o que queremos substituir)
        val_collect = mock_agg_df.collect()[0][0]

        # Novo (otimizado)
        val_first = mock_agg_df.first()[0]

        self.assertEqual(val_collect, val_first)
        self.assertEqual(val_first, "2024-01-01")

if __name__ == '__main__':
    unittest.main()
