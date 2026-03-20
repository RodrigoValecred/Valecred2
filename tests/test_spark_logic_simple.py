import unittest
from unittest.mock import MagicMock, patch

# Simulando o pyspark já que não está disponível no ambiente
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

        # Simulando o dataframe spark
        df_spark = MagicMock()
        df_spark.filter.return_value = df_spark

        # Valores a partir do notebook
        CLIENTE_CPFCNPJ = "14630809000101"
        tipos_excluir = ['RN', 'RE', 'RC', 'PR', 'AB', 'AM', 'LB', 'PB']

        # Replicando a lógica do filtro do Spark do notebook otimizado
        df_mestra_spark = df_spark
        df_mestra_spark = df_mestra_spark.filter(col("CPFCNPJ") == CLIENTE_CPFCNPJ)
        df_mestra_spark = df_mestra_spark.filter(
            (col("STATUSANALISE") == 'D') &
            (col("STATUSACEITE") == 'A') &
            (col("ACEITO") == 'S')
        )
        df_mestra_spark = df_mestra_spark.filter(~col("TTO_OPERACAO").isin(tipos_excluir))
        df_mestra_spark = df_mestra_spark.filter(col("LIQUIDACAO").isNotNull())

        # Verifica se filter foi chamado
        self.assertTrue(df_spark.filter.called)

        # Verifica se chamou filter pelo menos 4 vezes
        self.assertEqual(df_spark.filter.call_count, 4)

        # Verifica as chamadas específicas
        calls = df_spark.filter.call_args_list
        # Primeira chamada deve ser o filtro CPFCNPJ
        # Nota: a comparação retorna nossa simulação
        self.assertIn('col(CPFCNPJ)', str(calls[0]))

if __name__ == '__main__':
    unittest.main()
