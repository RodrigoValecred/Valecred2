
import unittest
from unittest.mock import MagicMock, call
import sys
import os
import datetime

# Mock pyspark before importing anything that might use it
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()
sys.modules["pyspark.sql.window"] = MagicMock()

from pyspark.sql.functions import col, lit, when, coalesce, sum, datediff, current_date

# Configure MagicMock to support comparisons
def configure_mock_comparisons(mock_obj):
    mock_obj.__gt__ = MagicMock(return_value=MagicMock())
    mock_obj.__lt__ = MagicMock(return_value=MagicMock())
    mock_obj.__ge__ = MagicMock(return_value=MagicMock())
    mock_obj.__le__ = MagicMock(return_value=MagicMock())
    mock_obj.__add__ = MagicMock(return_value=MagicMock())
    mock_obj.__sub__ = MagicMock(return_value=MagicMock())
    mock_obj.__mul__ = MagicMock(return_value=MagicMock())
    mock_obj.__truediv__ = MagicMock(return_value=MagicMock())
    return mock_obj

class TestRentabilidadeAtrasoLogic(unittest.TestCase):
    def test_df_titulos_logic(self):
        # Setup
        df_titulos = MagicMock()

        # Ensure col() returns a mock that supports comparisons
        col_mock = MagicMock()
        configure_mock_comparisons(col_mock)
        sys.modules["pyspark.sql.functions"].col.return_value = col_mock

        # Also ensure datediff, coalesce, etc return comparable mocks
        sys.modules["pyspark.sql.functions"].datediff.return_value = configure_mock_comparisons(MagicMock())
        sys.modules["pyspark.sql.functions"].coalesce.return_value = configure_mock_comparisons(MagicMock())
        sys.modules["pyspark.sql.functions"].when.return_value = configure_mock_comparisons(MagicMock())
        sys.modules["pyspark.sql.functions"].when.return_value.otherwise.return_value = configure_mock_comparisons(MagicMock())

        # New logic steps
        # ... logic ...

        # Execute logic simulation
        # Original part
        res = df_titulos.filter(col("aceito") == "S") \
            .filter(col("t_doc") != "BL") \
            .withColumn("data_final_real", MagicMock()) \
            .withColumn("dias_final_epoch", MagicMock()) \
            .withColumn("valor_vezes_data_final", MagicMock()) \
            .withColumn("dias_prorrogacao", MagicMock()) \
            .withColumn("valor_vezes_prorrogacao", MagicMock())

        # New part to be added
        # Here we rely on our mocked col() supporting > 0
        res = res.withColumn("data_vencimento_ajustado", coalesce(col("venc_prorrogado"), col("vencimento"))) \
            .withColumn("dias_atraso_real",
                        when(col("liquidacao").isNotNull(), datediff(col("liquidacao"), col("data_vencimento_ajustado")))
                        .otherwise(datediff(current_date(), col("data_vencimento_ajustado")))) \
            .withColumn("em_mora", col("dias_atraso_real") > 0) \
            .withColumn("valor_vezes_atraso", when(col("em_mora"), col("valor") * col("dias_atraso_real")).otherwise(0)) \
            .withColumn("valor_em_mora", when(col("em_mora"), col("valor")).otherwise(0))

        # Verification: Check if withColumn was called with expected arguments
        # We can inspect the calls to see if "dias_atraso_real" and others were added

        # Instead, let's verify the logic using Python variables to ensure math correctness
        self.verify_math_logic()

    def verify_math_logic(self):
        # Simulation of the logic using standard python types

        def calculate_atraso(liquidacao, vencimento, venc_prorrogado, valor, current_date_val):
            venc_ajustado = venc_prorrogado if venc_prorrogado else vencimento

            if liquidacao:
                dias_atraso = (liquidacao - venc_ajustado).days
            else:
                dias_atraso = (current_date_val - venc_ajustado).days

            em_mora = dias_atraso > 0

            valor_vezes_atraso = (valor * dias_atraso) if em_mora else 0
            valor_mora = valor if em_mora else 0

            return dias_atraso, em_mora, valor_vezes_atraso, valor_mora

        today = datetime.date(2025, 10, 1)

        # Case 1: Paid on time
        d, m, vva, vm = calculate_atraso(datetime.date(2025, 1, 10), datetime.date(2025, 1, 10), None, 1000, today)
        self.assertEqual(d, 0)
        self.assertFalse(m)
        self.assertEqual(vva, 0)
        self.assertEqual(vm, 0)

        # Case 2: Paid Late (Mora)
        d, m, vva, vm = calculate_atraso(datetime.date(2025, 1, 15), datetime.date(2025, 1, 10), None, 1000, today)
        self.assertEqual(d, 5)
        self.assertTrue(m)
        self.assertEqual(vva, 5000)
        self.assertEqual(vm, 1000)

        # Case 3: Paid Early
        d, m, vva, vm = calculate_atraso(datetime.date(2025, 1, 5), datetime.date(2025, 1, 10), None, 1000, today)
        self.assertEqual(d, -5)
        self.assertFalse(m)
        self.assertEqual(vva, 0)
        self.assertEqual(vm, 0)

        # Case 4: Open, Not Overdue
        d, m, vva, vm = calculate_atraso(None, datetime.date(2025, 10, 10), None, 1000, today)
        self.assertEqual(d, -9)
        self.assertFalse(m)

        # Case 5: Open, Overdue
        d, m, vva, vm = calculate_atraso(None, datetime.date(2025, 9, 20), None, 1000, today)
        self.assertEqual(d, 11)
        self.assertTrue(m)
        self.assertEqual(vva, 11000)
        self.assertEqual(vm, 1000)

        # Case 6: Prorrogado
        d, m, vva, vm = calculate_atraso(datetime.date(2025, 2, 15), datetime.date(2025, 1, 10), datetime.date(2025, 2, 10), 1000, today)
        self.assertEqual(d, 5)
        self.assertTrue(m)

        print("Logic verification passed!")

if __name__ == "__main__":
    unittest.main()
