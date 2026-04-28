import unittest
from unittest.mock import MagicMock, patch
import sys
import os
from types import ModuleType

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Mock (simulação) do pyspark caso não exista
if 'pyspark' not in sys.modules:
    mock_pyspark = ModuleType('pyspark')
    mock_pyspark_sql = ModuleType('pyspark.sql')
    mock_pyspark_functions = ModuleType('pyspark.sql.functions')
    sys.modules['pyspark'] = mock_pyspark
    sys.modules['pyspark.sql'] = mock_pyspark_sql
    sys.modules['pyspark.sql.functions'] = mock_pyspark_functions

    # Classe simples de Mock (simulação) de Column para suportar operadores
    class MockColumn:
        def __init__(self, name):
            self.name = name
        def __eq__(self, other):
            return MockColumn(f"({self.name} == {other})")
        def __ge__(self, other):
            return MockColumn(f"({self.name} >= {other})")
        def __and__(self, other):
            return MockColumn(f"({self.name} & {other})")
        def __repr__(self):
            return self.name
        def __str__(self):
            return self.name

    mock_pyspark_functions.col = lambda x: MockColumn(f"col({x})")
    mock_pyspark_functions.to_date = lambda x: MockColumn(f"to_date({x})")
    mock_pyspark_functions.lit = lambda x: MockColumn(f"lit({x})")
    mock_pyspark_functions.when = MagicMock()

    import pyspark.sql.functions as F
else:
    from pyspark.sql import functions as F

NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py"

# Extrair globalmente para evitar problemas de escopo repetido
_local_scope = {'F': F}
_source = extract_function_from_file(NOTEBOOK_PATH, "check_sequential_invoices")
if _source:
    exec(_source, _local_scope)
    _check_sequential_invoices = _local_scope['check_sequential_invoices']
else:
    _check_sequential_invoices = None

class TestCheckSequentialInvoices(unittest.TestCase):
    def test_check_sequential_invoices_logic(self):
        if not _check_sequential_invoices:
            self.fail("Function not found")

        df = MagicMock()
        df.withColumn.return_value = df

        # Configuração do mock F.when para retornar um objeto mock que possui .otherwise
        when_result = MagicMock()
        F.when.return_value = when_result

        _check_sequential_invoices(df)

        # Verifica se withColumn foi chamado
        df.withColumn.assert_called_once()
        args, _ = df.withColumn.call_args
        self.assertEqual(args[0], "alerta_notas_sequenciais")

        # Verifica se F.when foi chamado com a lógica correta
        F.when.assert_called_once()
        condition = F.when.call_args[0][0]

        # Verificando a representação em string por causa do nosso MockColumn
        cond_str = str(condition)
        self.assertIn("to_date(col(data_emissao))", cond_str)
        self.assertIn("to_date(col(data_entrada))", cond_str)
        self.assertIn("col(vlr_total_sacado)", cond_str)
        self.assertIn("100000.0", cond_str)

        # Verifica se lit(True) foi o segundo argumento para when
        self.assertEqual(str(F.when.call_args[0][1]), "lit(True)")

        # Verifica se otherwise foi chamado com lit(False)
        self.assertTrue(when_result.otherwise.called)
        self.assertEqual(str(when_result.otherwise.call_args[0][0]), "lit(False)")

    def test_custom_threshold_and_columns(self):
        if not _check_sequential_invoices:
            self.fail("Function not found")

        df = MagicMock()
        df.withColumn.return_value = df

        # Configuração do mock F.when para retornar um objeto mock que possui .otherwise
        when_result = MagicMock()
        F.when.return_value = when_result

        _check_sequential_invoices(
            df,
            col_emission_date="dt_emi",
            col_entry_date="dt_ent",
            col_volume="valor",
            threshold_volume=50000.0
        )

        # Verifica se F.when foi chamado com a lógica personalizada
        condition = F.when.call_args[0][0]
        cond_str = str(condition)

        self.assertIn("to_date(col(dt_emi))", cond_str)
        self.assertIn("to_date(col(dt_ent))", cond_str)
        self.assertIn("col(valor)", cond_str)
        self.assertIn("50000.0", cond_str)

if __name__ == '__main__':
    unittest.main()
