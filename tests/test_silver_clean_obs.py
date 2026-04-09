import unittest
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define o caminho para o arquivo do notebook em relação à raiz do repositório
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Preparacao_Silver.Notebook/notebook-content.py"
)

class TestSilverCleanObs(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Inicializa o SparkSession local para testes
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TestSilverCleanObs") \
            .getOrCreate()

        print(f"Extracting clean_obs from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "clean_obs")
        if not cls.func_source:
             raise ValueError("Function clean_obs not found in notebook.")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'spark'):
            cls.spark.stop()

    def test_clean_obs(self):
        # Globais de execução (incluindo as funções do PySpark necessárias)
        exec_globals = {
            'col': col,
            'regexp_replace': regexp_replace
        }

        # Carrega a função
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        clean_obs = local_scope['clean_obs']

        # Dados de Teste
        test_data = [
            (1, "Texto normal"),
            (2, "Aten&ccedil;&atilde;o"),
            (3, "Sem altera&ccedil;&atilde;o"),
            (4, "&ccedil;&atilde;o"),
            (5, None)
        ]
        df = self.spark.createDataFrame(test_data, ["id", "observacao"])

        # Aplica a função clean_obs
        result_df = df.withColumn("observacao_limpa", clean_obs("observacao"))

        # Coleta os resultados
        results = {row["id"]: row["observacao_limpa"] for row in result_df.collect()}

        # Verifications
        self.assertEqual(results[1], "Texto normal")
        self.assertEqual(results[2], "Atenção")
        self.assertEqual(results[3], "Sem alteração")
        self.assertEqual(results[4], "ção")
        self.assertIsNone(results[5])

if __name__ == '__main__':
    unittest.main()
