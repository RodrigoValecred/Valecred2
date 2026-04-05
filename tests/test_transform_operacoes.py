import unittest
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import datetime

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define o caminho para o arquivo do notebook
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Operacoes.Notebook/notebook-content.py"
)

class TestTransformOperacoes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 1. Initialize local SparkSession
        cls.spark = SparkSession.builder \
            .appName("TestTransformOperacoes") \
            .master("local[1]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

        # 2. Extract `transform_operacoes`
        func_source = extract_function_from_file(NOTEBOOK_PATH, "transform_operacoes")
        if not func_source:
            raise ValueError(f"Function transform_operacoes not found in {NOTEBOOK_PATH}")

        # Also need get_operacoes_schema
        schema_source = extract_function_from_file(NOTEBOOK_PATH, "get_operacoes_schema")
        if not schema_source:
             raise ValueError("Function get_operacoes_schema not found")

        local_scope = {}
        # Precisamos das funções do PySpark no escopo para o `exec` funcionar
        exec_scope = {}
        exec("from pyspark.sql.functions import col, when, lit, row_number, concat, coalesce, desc", exec_scope)
        exec("from pyspark.sql.window import Window", exec_scope)

        # Load helper function
        exec(schema_source, exec_scope, local_scope)
        exec_scope["get_operacoes_schema"] = local_scope["get_operacoes_schema"]

        # Load main function
        exec(func_source, exec_scope, local_scope)
        cls.transform_operacoes = staticmethod(local_scope["transform_operacoes"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_transform_operacoes_tto_corrigido(self):
        # Create test DataFrame
        schema = StructType([
            StructField("CODOPERACAO", IntegerType(), True),
            StructField("TTO", StringType(), True),
            StructField("STTO", StringType(), True),
            StructField("DATAALTERACAO", StringType(), True),
            # Adiciona todas as colunas necessárias pelo get_operacoes_schema para evitar erros
            StructField("CODCLIENTE", IntegerType(), True),
            StructField("CODEMPRESA", IntegerType(), True),
            StructField("DATAINCLUSAO", StringType(), True),
            StructField("DATAANALISE", StringType(), True),
            StructField("STATUSACEITE", StringType(), True),
            StructField("STATUSANALISE", StringType(), True),
            StructField("CODBROKER", IntegerType(), True),
            StructField("NBORDERO", StringType(), True),
            StructField("NOTASERVICO", StringType(), True),
            StructField("TOTRETENCAO", FloatType(), True),
            StructField("TOTDES", FloatType(), True),
            StructField("TOTFAC", FloatType(), True),
            StructField("TOTDCP", FloatType(), True),
            StructField("TOTTAR", FloatType(), True),
            StructField("TOTPENDENCIAS", FloatType(), True),
            StructField("TOTRECOMPRA", FloatType(), True),
            StructField("FATOR", FloatType(), True),
            StructField("CODINDEFERIMENTO", IntegerType(), True),
            StructField("USUAINCLUSAO", IntegerType(), True),
            StructField("USUASTANALISE", IntegerType(), True),
            StructField("USUATRAVA", IntegerType(), True),
            StructField("TAC", FloatType(), True),
            StructField("TOTTAXAADM", FloatType(), True),
            StructField("TOTADVAL", FloatType(), True),
            StructField("NDOCSRECOMPRA", IntegerType(), True),
            StructField("TARIFA", FloatType(), True),
            StructField("NDOCS", IntegerType(), True),
            StructField("TARIFARECOMPRA", FloatType(), True),
            StructField("FLOATING", FloatType(), True),
            StructField("PMP", IntegerType(), True)
        ])

        data = [
            (3042074, "XX", "A", "2023-01-01", 1, 1, "2023-01-01", "2023-01-01", "A", "A", 1, "A", "A", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 1, 1, 1, 0.0, 0.0, 0.0, 1, 0.0, 1, 0.0, 0.0, 1), # Deve ser 'CS'
            (6048450, "YY", "B", "2023-01-01", 1, 1, "2023-01-01", "2023-01-01", "A", "A", 1, "A", "A", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 1, 1, 1, 0.0, 0.0, 0.0, 1, 0.0, 1, 0.0, 0.0, 1), # Deve ser 'CS'
            (6048449, "ZZ", "C", "2023-01-01", 1, 1, "2023-01-01", "2023-01-01", "A", "A", 1, "A", "A", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 1, 1, 1, 0.0, 0.0, 0.0, 1, 0.0, 1, 0.0, 0.0, 1), # Deve ser 'CS'
            (1111111, "ORIGINAL", "D", "2023-01-01", 1, 1, "2023-01-01", "2023-01-01", "A", "A", 1, "A", "A", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 1, 1, 1, 0.0, 0.0, 0.0, 1, 0.0, 1, 0.0, 0.0, 1) # Deve permanecer 'ORIGINAL'
        ]

        df = self.spark.createDataFrame(data, schema)

        result_df = self.transform_operacoes(df, ["CODOPERACAO"])

        # Verify TTO values
        results = result_df.select("cod_operacao", "tto").collect()
        tto_map = {row["cod_operacao"]: row["tto"] for row in results}

        self.assertEqual(tto_map[3042074], "CS")
        self.assertEqual(tto_map[6048450], "CS")
        self.assertEqual(tto_map[6048449], "CS")
        self.assertEqual(tto_map[1111111], "ORIGINAL")

    def test_transform_operacoes_deduplication(self):
        # Cria DataFrame de teste com duplicatas
        schema = StructType([
            StructField("CODOPERACAO", IntegerType(), True),
            StructField("TTO", StringType(), True),
            StructField("STTO", StringType(), True),
            StructField("DATAALTERACAO", StringType(), True),
            # Required columns
            StructField("CODCLIENTE", IntegerType(), True),
            StructField("CODEMPRESA", IntegerType(), True),
            StructField("DATAINCLUSAO", StringType(), True),
            StructField("DATAANALISE", StringType(), True),
            StructField("STATUSACEITE", StringType(), True),
            StructField("STATUSANALISE", StringType(), True),
            StructField("CODBROKER", IntegerType(), True),
            StructField("NBORDERO", StringType(), True),
            StructField("NOTASERVICO", StringType(), True),
            StructField("TOTRETENCAO", FloatType(), True),
            StructField("TOTDES", FloatType(), True),
            StructField("TOTFAC", FloatType(), True),
            StructField("TOTDCP", FloatType(), True),
            StructField("TOTTAR", FloatType(), True),
            StructField("TOTPENDENCIAS", FloatType(), True),
            StructField("TOTRECOMPRA", FloatType(), True),
            StructField("FATOR", FloatType(), True),
            StructField("CODINDEFERIMENTO", IntegerType(), True),
            StructField("USUAINCLUSAO", IntegerType(), True),
            StructField("USUASTANALISE", IntegerType(), True),
            StructField("USUATRAVA", IntegerType(), True),
            StructField("TAC", FloatType(), True),
            StructField("TOTTAXAADM", FloatType(), True),
            StructField("TOTADVAL", FloatType(), True),
            StructField("NDOCSRECOMPRA", IntegerType(), True),
            StructField("TARIFA", FloatType(), True),
            StructField("NDOCS", IntegerType(), True),
            StructField("TARIFARECOMPRA", FloatType(), True),
            StructField("FLOATING", FloatType(), True),
            StructField("PMP", IntegerType(), True)
        ])

        data = [
            # ID 100 possui dois registros, queremos a DATAALTERACAO mais recente
            (100, "AA", "1", "2023-01-01", 1, 1, "2023-01-01", "2023-01-01", "A", "A", 1, "A", "A", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 1, 1, 1, 0.0, 0.0, 0.0, 1, 0.0, 1, 0.0, 0.0, 1),
            (100, "AA", "1", "2023-01-05", 1, 1, "2023-01-01", "2023-01-01", "A", "A", 1, "A", "A", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 1, 1, 1, 0.0, 0.0, 0.0, 1, 0.0, 1, 0.0, 0.0, 1), # Este deve ser mantido
            # ID 200 tem três registros
            (200, "BB", "2", "2023-01-02", 1, 1, "2023-01-01", "2023-01-01", "A", "A", 1, "A", "A", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 1, 1, 1, 0.0, 0.0, 0.0, 1, 0.0, 1, 0.0, 0.0, 1),
            (200, "BB", "2", "2023-01-04", 1, 1, "2023-01-01", "2023-01-01", "A", "A", 1, "A", "A", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 1, 1, 1, 0.0, 0.0, 0.0, 1, 0.0, 1, 0.0, 0.0, 1), # Este deve ser mantido
            (200, "BB", "2", "2023-01-01", 1, 1, "2023-01-01", "2023-01-01", "A", "A", 1, "A", "A", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 1, 1, 1, 0.0, 0.0, 0.0, 1, 0.0, 1, 0.0, 0.0, 1)
        ]

        df = self.spark.createDataFrame(data, schema)

        result_df = self.transform_operacoes(df, ["CODOPERACAO"])

        # Verifica deduplicação
        self.assertEqual(result_df.count(), 2)

        # Verifica se as linhas corretas foram mantidas
        results = result_df.select("cod_operacao", "data_alteracao").collect()
        date_map = {row["cod_operacao"]: row["data_alteracao"] for row in results}

        self.assertEqual(date_map[100], "2023-01-05")
        self.assertEqual(date_map[200], "2023-01-04")

    def test_transform_operacoes_chave_produto(self):
        # Create test DataFrame
        schema = StructType([
            StructField("CODOPERACAO", IntegerType(), True),
            StructField("TTO", StringType(), True),
            StructField("STTO", StringType(), True),
            StructField("DATAALTERACAO", StringType(), True),
            # Required columns
            StructField("CODCLIENTE", IntegerType(), True),
            StructField("CODEMPRESA", IntegerType(), True),
            StructField("DATAINCLUSAO", StringType(), True),
            StructField("DATAANALISE", StringType(), True),
            StructField("STATUSACEITE", StringType(), True),
            StructField("STATUSANALISE", StringType(), True),
            StructField("CODBROKER", IntegerType(), True),
            StructField("NBORDERO", StringType(), True),
            StructField("NOTASERVICO", StringType(), True),
            StructField("TOTRETENCAO", FloatType(), True),
            StructField("TOTDES", FloatType(), True),
            StructField("TOTFAC", FloatType(), True),
            StructField("TOTDCP", FloatType(), True),
            StructField("TOTTAR", FloatType(), True),
            StructField("TOTPENDENCIAS", FloatType(), True),
            StructField("TOTRECOMPRA", FloatType(), True),
            StructField("FATOR", FloatType(), True),
            StructField("CODINDEFERIMENTO", IntegerType(), True),
            StructField("USUAINCLUSAO", IntegerType(), True),
            StructField("USUASTANALISE", IntegerType(), True),
            StructField("USUATRAVA", IntegerType(), True),
            StructField("TAC", FloatType(), True),
            StructField("TOTTAXAADM", FloatType(), True),
            StructField("TOTADVAL", FloatType(), True),
            StructField("NDOCSRECOMPRA", IntegerType(), True),
            StructField("TARIFA", FloatType(), True),
            StructField("NDOCS", IntegerType(), True),
            StructField("TARIFARECOMPRA", FloatType(), True),
            StructField("FLOATING", FloatType(), True),
            StructField("PMP", IntegerType(), True)
        ])

        data = [
            (1, "CM", "EB", "2023-01-01", 1, 1, "2023-01-01", "2023-01-01", "A", "A", 1, "A", "A", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 1, 1, 1, 0.0, 0.0, 0.0, 1, 0.0, 1, 0.0, 0.0, 1),
            (2, "PR", None, "2023-01-01", 1, 1, "2023-01-01", "2023-01-01", "A", "A", 1, "A", "A", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 1, 1, 1, 0.0, 0.0, 0.0, 1, 0.0, 1, 0.0, 0.0, 1), # Null STTO
        ]

        df = self.spark.createDataFrame(data, schema)

        result_df = self.transform_operacoes(df, ["CODOPERACAO"])

        # Verify chave_produto
        results = result_df.select("cod_operacao", "chave_produto").collect()
        chave_map = {row["cod_operacao"]: row["chave_produto"] for row in results}

        self.assertEqual(chave_map[1], "CMEB")
        self.assertEqual(chave_map[2], "PR")

if __name__ == '__main__':
    unittest.main()
