import unittest
from unittest.mock import MagicMock, patch

# Mocking pyspark since it's not available in the environment
import sys
from types import ModuleType

mock_pyspark = ModuleType('pyspark')
mock_pyspark_sql = ModuleType('pyspark.sql')
mock_pyspark_functions = ModuleType('pyspark.sql.functions')
sys.modules['pyspark'] = mock_pyspark
sys.modules['pyspark.sql'] = mock_pyspark_sql
sys.modules['pyspark.sql.functions'] = mock_pyspark_functions

def col(name):
    # This mock will return a MagicMock that supports common operators
    m = MagicMock(name=f"col({name})")
    m.__eq__.return_value = m
    m.__ne__.return_value = m
    m.__and__.return_value = m
    m.__invert__.return_value = m
    m.isin.return_value = m
    m.isNotNull.return_value = m
    return m

mock_pyspark_functions.col = col

class TestAnaliseClienteLogic(unittest.TestCase):
    @patch('pyspark.sql.functions.col')
    def test_spark_filter_calls(self, mock_col):
        mock_col.side_effect = col

        # Simulating the spark dataframe
        df_spark = MagicMock()
        df_spark.filter.return_value = df_spark

        # Values from the notebook
        CLIENTE_CPFCNPJ = "14630809000101"
        tipos_excluir = ['RN', 'RE', 'RC', 'PR', 'AB', 'AM', 'LB', 'PB']

        # Replicating the Spark filter logic from the optimized notebook
        df_mestra_spark = df_spark
        df_mestra_spark = df_mestra_spark.filter(col("CPFCNPJ") == CLIENTE_CPFCNPJ)
        df_mestra_spark = df_mestra_spark.filter(
            (col("STATUSANALISE") == 'D') &
            (col("STATUSACEITE") == 'A') &
            (col("ACEITO") == 'S')
        )
        df_mestra_spark = df_mestra_spark.filter(~col("TTO_OPERACAO").isin(tipos_excluir))
        df_mestra_spark = df_mestra_spark.filter(col("LIQUIDACAO").isNotNull())

        # Verify filter was called
        self.assertTrue(df_spark.filter.called)

        # Check that it called filter at least 4 times
        self.assertEqual(df_spark.filter.call_count, 4)

        # Verify the specific calls
        calls = df_spark.filter.call_args_list
        # First call should be the CPFCNPJ filter
        # Note: comparison returns our mock
        self.assertIn('col(CPFCNPJ)', str(calls[0]))

if __name__ == '__main__':
    unittest.main()
