import unittest
import sys
import os
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define o caminho para o arquivo do notebook em relação à raiz do repositório
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"
)

class TestVOPMetrics(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Iniciamos uma sessão spark para testes
        cls.spark = SparkSession.builder.master("local[1]").appName("TestVOPMetrics").getOrCreate()

        func_source = extract_function_from_file(NOTEBOOK_PATH, "calculate_vop_metrics")

        from pyspark.sql.functions import col, sum, row_number
        from pyspark.sql.window import Window

        exec_globals = {
            'col': col,
            'sum': sum,
            'row_number': row_number,
            'Window': Window,
        }

        local_scope = {}
        exec(func_source, exec_globals, local_scope)
        cls.calculate_vop_metrics = staticmethod(local_scope['calculate_vop_metrics'])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_calculate_vop_metrics_edge_cases(self):
        # Casos extremos (Edge cases):
        # 1. Empates em VOP (deve escolher um com base na ordenação interna, mas determinístico é melhor, embora row_number().over(orderBy(desc)) gerencie isso)
        # 2. Multiple clients
        # 3. Várias entradas para o mesmo dia
        data = [
            # Cliente 1: vencedor claro
            Row(cod_cliente=1, dia_da_semana_da_operacao=2, dia_da_operacao=10, valor_de_face=100.0),
            Row(cod_cliente=1, dia_da_semana_da_operacao=3, dia_da_operacao=15, valor_de_face=200.0), # max week 3, max month 15

            # Client 2: Várias entradas para o mesmo dia should aggregate
            Row(cod_cliente=2, dia_da_semana_da_operacao=4, dia_da_operacao=20, valor_de_face=50.0),
            Row(cod_cliente=2, dia_da_semana_da_operacao=4, dia_da_operacao=20, valor_de_face=50.0), # sum = 100
            Row(cod_cliente=2, dia_da_semana_da_operacao=5, dia_da_operacao=21, valor_de_face=60.0), # max week 4 (100 > 60)

            # Client 3: Empate nas somas (semana 1=100, semana 2=100), row_number escolhe um (não determinístico qual sem desempate, mas garantimos que um seja escolhido)
            Row(cod_cliente=3, dia_da_semana_da_operacao=1, dia_da_operacao=1, valor_de_face=100.0),
            Row(cod_cliente=3, dia_da_semana_da_operacao=2, dia_da_operacao=2, valor_de_face=100.0),
        ]

        df = self.spark.createDataFrame(data)

        df_semana, df_mes = self.calculate_vop_metrics(df)

        res_semana = df_semana.collect()
        res_mes = df_mes.collect()

        res_semana.sort(key=lambda x: x.cod_cliente)
        res_mes.sort(key=lambda x: x.cod_cliente)

        self.assertEqual(len(res_semana), 3)
        self.assertEqual(len(res_mes), 3)

        # Client 1
        self.assertEqual(res_semana[0].cod_cliente, 1)
        self.assertEqual(res_semana[0].dia_semana_mais_vop, 3)

        self.assertEqual(res_mes[0].cod_cliente, 1)
        self.assertEqual(res_mes[0].dia_mes_mais_vop, 15)

        # Cliente 2 (Verificação de Agregação)
        self.assertEqual(res_semana[1].cod_cliente, 2)
        self.assertEqual(res_semana[1].dia_semana_mais_vop, 4) # 100 > 60

        self.assertEqual(res_mes[1].cod_cliente, 2)
        self.assertEqual(res_mes[1].dia_mes_mais_vop, 20)

        # Cliente 3 (Verificação de Empate)
        self.assertEqual(res_semana[2].cod_cliente, 3)
        self.assertIn(res_semana[2].dia_semana_mais_vop, [1, 2])

    def test_calculate_vop_metrics_empty(self):
        # Caso extremo (Edge case): DataFrame vazio
        from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
        schema = StructType([
            StructField("cod_cliente", IntegerType(), True),
            StructField("dia_da_semana_da_operacao", IntegerType(), True),
            StructField("dia_da_operacao", IntegerType(), True),
            StructField("valor_de_face", DoubleType(), True)
        ])
        df = self.spark.createDataFrame([], schema)
        df_semana, df_mes = self.calculate_vop_metrics(df)

        self.assertEqual(df_semana.count(), 0)
        self.assertEqual(df_mes.count(), 0)

if __name__ == '__main__':
    unittest.main()
