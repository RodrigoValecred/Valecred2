
import unittest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from datetime import date
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, lead, when, lit, max, to_date

def process_fechamento_prorrogacao(df_prorrog):
    # Logic extracted from NB_Fechamento_Prorrogacao_Mensal.Notebook

    # 1. Normalize Status
    df_prorrog_prep = df_prorrog \
        .withColumn("data_referencia", to_date(col("data_inclusao"))) \
        .withColumn("status_analise_norm",
            when(col("status_analise") == "D", "DEFERIDO")
            .otherwise("INDEFERIDO")
        )

    # 2. Identify if it was eventually deferred
    w_titulo = Window.partitionBy("cod_titulo")

    # Flag: 1 if status_analise_norm == 'DEFERIDO'
    df_flagged = df_prorrog_prep.withColumn("is_deferido",
        when(col("status_analise_norm") == "DEFERIDO", 1).otherwise(0)
    )

    # Calculate if there was any deferral in history
    df_calculated = df_flagged.withColumn("foi_deferido_eventualmente",
        max("is_deferido").over(w_titulo)
    )

    # 3. Categorization
    df_categorized = df_calculated.withColumn("status_final_prorrogacao",
        when(col("status_analise_norm") == "DEFERIDO", "DEFERIDO")
        .when((col("status_analise_norm") != "DEFERIDO") & (col("foi_deferido_eventualmente") == 1), "RECUPERADA")
        .otherwise("INDEFERIDO")
    )

    return df_categorized

class TestProrrogacaoLogic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestProrrogacao").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_prorrogacao_recovery_logic(self):
        # Sample Data
        data = [
            # Case 1: Recovered (Rejected -> Accepted)
            Row(cod_operacao=101, cod_titulo=1, cod_cliente=10, status_analise='I', data_inclusao=date(2025, 1, 1), valor=100.0),
            Row(cod_operacao=102, cod_titulo=1, cod_cliente=10, status_analise='D', data_inclusao=date(2025, 1, 5), valor=100.0),

            # Case 2: Unrecovered (Rejected only)
            Row(cod_operacao=201, cod_titulo=2, cod_cliente=20, status_analise='I', data_inclusao=date(2025, 1, 2), valor=200.0),

            # Case 3: Normal Accepted
            Row(cod_operacao=301, cod_titulo=3, cod_cliente=30, status_analise='D', data_inclusao=date(2025, 1, 3), valor=300.0),

            # Case 4: Multiple Rejections then Accepted
            Row(cod_operacao=401, cod_titulo=4, cod_cliente=40, status_analise='I', data_inclusao=date(2025, 1, 1), valor=400.0),
            Row(cod_operacao=402, cod_titulo=4, cod_cliente=40, status_analise='I', data_inclusao=date(2025, 1, 2), valor=400.0),
            Row(cod_operacao=403, cod_titulo=4, cod_cliente=40, status_analise='D', data_inclusao=date(2025, 1, 4), valor=400.0),
        ]

        schema = StructType([
            StructField("cod_operacao", IntegerType(), True),
            StructField("cod_titulo", IntegerType(), True),
            StructField("cod_cliente", IntegerType(), True),
            StructField("status_analise", StringType(), True),
            StructField("data_inclusao", DateType(), True),
            StructField("valor", DoubleType(), True)
        ])

        df = self.spark.createDataFrame(data, schema)

        # Apply extracted logic
        df_final = process_fechamento_prorrogacao(df)

        # Verification
        results = df_final.orderBy("cod_operacao").collect()

        # Check Case 1 (Recovered)
        row_101 = next(r for r in results if r.cod_operacao == 101) # Rejected
        row_102 = next(r for r in results if r.cod_operacao == 102) # Accepted
        self.assertEqual(row_101.status_final_prorrogacao, "RECUPERADA")
        self.assertEqual(row_102.status_final_prorrogacao, "DEFERIDO")

        # Check Case 2 (Unrecovered)
        row_201 = next(r for r in results if r.cod_operacao == 201)
        self.assertEqual(row_201.status_final_prorrogacao, "INDEFERIDO")

        # Check Case 3 (Normal)
        row_301 = next(r for r in results if r.cod_operacao == 301)
        self.assertEqual(row_301.status_final_prorrogacao, "DEFERIDO")

        # Check Case 4 (Multiple Attempts)
        row_401 = next(r for r in results if r.cod_operacao == 401)
        row_402 = next(r for r in results if r.cod_operacao == 402)
        row_403 = next(r for r in results if r.cod_operacao == 403)
        self.assertEqual(row_401.status_final_prorrogacao, "RECUPERADA")
        self.assertEqual(row_402.status_final_prorrogacao, "RECUPERADA")
        self.assertEqual(row_403.status_final_prorrogacao, "DEFERIDO")

        print("Test Passed!")

if __name__ == '__main__':
    unittest.main()
