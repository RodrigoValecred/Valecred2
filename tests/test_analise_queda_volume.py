import unittest
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import datetime, timedelta

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define o caminho para o arquivo do notebook
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Analise_Queda_Volume_Clientes.Notebook/notebook-content.py"
)

class TestAnaliseQuedaVolume(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestAnaliseQuedaVolume").getOrCreate()

        func_source = extract_function_from_file(NOTEBOOK_PATH, "calcular_queda_volume")

        import pyspark.sql.functions as F
        from pyspark.sql.window import Window

        exec_globals = {
            'F': F,
            'Window': Window,
        }

        local_scope = {}
        exec(func_source, exec_globals, local_scope)
        cls.calcular_queda_volume = staticmethod(local_scope['calcular_queda_volume'])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_queda_volume_identifica_corretamente(self):
        # 1. Datas de referência (mock)
        # Assumindo data max de análise hoje para testar as janelas
        hoje = datetime.now()
        recentes_dt = hoje - timedelta(days=15) # dentro de "recentes_30d"
        anteriores_dt = hoje - timedelta(days=45) # dentro de "anteriores_30d"

        # 2. Dados de Teste
        data = [
            # Cliente 1: Queda drástica de VOP (1000 antes -> 100 hoje)
            Row(cod_operacao=1, cod_cliente=1, data_analise=anteriores_dt, valor_de_face=1000.0),
            Row(cod_operacao=2, cod_cliente=1, data_analise=recentes_dt, valor_de_face=100.0),

            # Cliente 2: Aumento de VOP (sem queda, não deve retornar no df_queda)
            Row(cod_operacao=3, cod_cliente=2, data_analise=anteriores_dt, valor_de_face=500.0),
            Row(cod_operacao=4, cod_cliente=2, data_analise=recentes_dt, valor_de_face=1500.0),

            # Cliente 3: Cessou atividades (1000 antes -> 0 hoje)
            Row(cod_operacao=5, cod_cliente=3, data_analise=anteriores_dt, valor_de_face=2000.0)
        ]

        df_mock = self.spark.createDataFrame(data)

        # 3. Execução
        df_resultado = self.calcular_queda_volume(df_mock)
        resultados = df_resultado.collect()

        # Ordem desc de queda_absoluta
        resultados.sort(key=lambda x: x.queda_absoluta, reverse=True)

        # 4. Verificação
        self.assertEqual(len(resultados), 2) # Apenas Clientes 1 e 3 devem retornar

        # Cliente 3 deve ser o primeiro pois perdeu mais absoluto (2000 vs 900 do cli 1)
        self.assertEqual(resultados[0].cod_cliente, 3)
        self.assertEqual(resultados[0].vop_anteriores_30d, 2000.0)
        self.assertEqual(resultados[0].vop_recentes_30d, 0.0)
        self.assertEqual(resultados[0].queda_absoluta, 2000.0)
        self.assertEqual(resultados[0].queda_percentual, 100.0)

        # Cliente 1 perdeu 900
        self.assertEqual(resultados[1].cod_cliente, 1)
        self.assertEqual(resultados[1].vop_anteriores_30d, 1000.0)
        self.assertEqual(resultados[1].vop_recentes_30d, 100.0)
        self.assertEqual(resultados[1].queda_absoluta, 900.0)
        self.assertEqual(resultados[1].queda_percentual, 90.0)

if __name__ == '__main__':
    unittest.main()
