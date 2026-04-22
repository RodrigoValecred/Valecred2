import unittest
from unittest.mock import MagicMock, patch
import sys
import os
from types import ModuleType

# Mocking pyspark
mock_pyspark = ModuleType('pyspark')
mock_pyspark_sql = ModuleType('pyspark.sql')
mock_pyspark_functions = ModuleType('pyspark.sql.functions')
sys.modules['pyspark'] = mock_pyspark
sys.modules['pyspark.sql'] = mock_pyspark_sql
sys.modules['pyspark.sql.functions'] = mock_pyspark_functions

# Simple Mock Column class to support operators
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
from create_seq_tool import create_sequential_invoices_tool

# Extract the function from the tool string
# create_sequential_invoices_tool() returns the source code string.
tool_code = create_sequential_invoices_tool()
local_scope = {'F': F}
exec(tool_code, local_scope)
check_sequential_invoices = local_scope['check_sequential_invoices']

class TestCheckSequentialInvoices(unittest.TestCase):
    def test_check_sequential_invoices_logic(self):
        df = MagicMock()
        df.withColumn.return_value = df

        # Setup F.when mock to return a mock object that has .otherwise
        when_result = MagicMock()
        F.when.return_value = when_result

        check_sequential_invoices(df)

        # Verify withColumn was called
        df.withColumn.assert_called_once()
        args, _ = df.withColumn.call_args
        self.assertEqual(args[0], "alerta_notas_sequenciais")

        # Verify F.when was called with the correct logic
        F.when.assert_called_once()
        condition = F.when.call_args[0][0]

        # Checking string representation because of our MockColumn
        cond_str = str(condition)
        self.assertIn("to_date(col(data_emissao))", cond_str)
        self.assertIn("to_date(col(data_entrada))", cond_str)
        self.assertIn("col(vlr_total_sacado)", cond_str)
        self.assertIn("100000.0", cond_str)

        # Verify lit(True) was second arg to when
        self.assertEqual(str(F.when.call_args[0][1]), "lit(True)")

        # Verify otherwise was called with lit(False)
        self.assertTrue(when_result.otherwise.called)
        self.assertEqual(str(when_result.otherwise.call_args[0][0]), "lit(False)")

if __name__ == '__main__':
    unittest.main()
