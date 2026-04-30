import unittest
import sys
import os
import datetime
from unittest.mock import MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, when, lit, concat, length, regexp_replace,
    collect_list, concat_ws, upper, greatest, substring, year,
    lead, date_add, lag, max, coalesce, date_sub, transform,
    filter as array_filter, split, array_join, array_contains,
    months_between, current_date, round
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, IntegerType
from functools import reduce

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define o caminho para o arquivo do notebook
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Cadastros.Notebook/notebook-content.py"
)

class TestProcessEnderecos(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Cria uma SparkSession local para os testes
        cls.spark = SparkSession.builder \
            .appName("TestProcessEnderecos") \
            .master("local[1]") \
            .getOrCreate()

        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "process_enderecos")
        if not cls.func_source:
             raise ValueError("Function process_enderecos not found in notebook.")

    @classmethod
    def tearDownClass(cls):
        # Abra o SparkSession
        cls.spark.stop()

    def test_process_enderecos_cidade_regex_with_pyspark(self):
        """
        Testa a lógica de limpeza (regex) da coluna CIDADE,
        extraída diretamente do notebook, usando DataFrames Spark.
        """
        # Preparação do Mock Spark
        mock_spark = MagicMock()

        # O process_enderecos usa:
        # spark.createDataFrame(data=data_regioes, schema=schema_regioes).cache()
        def mock_createDataFrame(data, schema):
            return self.spark.createDataFrame(data, schema)
        mock_spark.createDataFrame = mock_createDataFrame

        # Dados de entrada simulando a Bronze
        bronze_data = [
            ("12345678901234", "RUA A", "SÃO PAULO", "SP", "01000", "0", "0", "1", "0", "0", "0", "0", "0", "0", "0", "123", "B"),
            ("98765432109876", "RUA B", "RIBEIRÃO PRETO", "SP", "02000", "0", "0", "1", "0", "0", "0", "0", "0", "0", "0", "124", "C"),
            ("11111111111111", "RUA C", "MACEIÓ", "AL", "03000", "0", "0", "1", "0", "0", "0", "0", "0", "0", "0", "125", "D"),
            ("22222222222222", "RUA D", "BRASÍLIA", "DF", "04000", "0", "0", "1", "0", "0", "0", "0", "0", "0", "0", "126", "E"),
            ("33333333333333", "RUA E", "JAÚ", "SP", "05000", "0", "0", "1", "0", "0", "0", "0", "0", "0", "0", "127", "F"),
            ("44444444444444", "RUA F", "JUIZ DE FORA-MG", "MG", "06000", "0", "0", "1", "0", "0", "0", "0", "0", "0", "0", "128", "G"),
        ]

        schema_bronze = StructType([
            StructField("CPFCNPJ", StringType(), True), StructField("ENDERECO", StringType(), True),
            StructField("CIDADE", StringType(), True), StructField("UF", StringType(), True),
            StructField("CEP", StringType(), True), StructField("PAIS", StringType(), True),
            StructField("FONE", StringType(), True), StructField("FAX", StringType(), True),
            StructField("TIPO", StringType(), True), StructField("DATAINCLUSAO", StringType(), True),
            StructField("USUAINCLUSAO", StringType(), True), StructField("DATAALTERACAO", StringType(), True),
            StructField("USUAALTERACAO", StringType(), True), StructField("CODMUNICIPIO", StringType(), True),
            StructField("CODENDERECO", StringType(), True), StructField("NUMERO", StringType(), True),
            StructField("COMPLEMENTO", StringType(), True), StructField("BAIRRO", StringType(), True)
        ])

        bronze_data_full = [
            (c[0], c[1], c[2], c[3], c[4], "BR", "11", "0", "A", "2023", "User", "2023", "User", "M", "E", "10", "C", "Bairro")
            for c in bronze_data
        ]
        df_bronze = self.spark.createDataFrame(bronze_data_full, schema=schema_bronze)

        # O process_enderecos EUA: Spark.read.table(f"{source_lakehouse}.cad_enderecos")
        mock_read = MagicMock()
        mock_read.table.return_value = df_bronze
        mock_spark.read = mock_read

        # Mock do DataFrameWriter
        orig_write = DataFrame.write
        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.saveAsTable = MagicMock()
        DataFrame.write = property(lambda self: mock_writer)

        try:
            # Prepara escopo de execução reproduzindo EXATAMENTE as dependências do notebook
            exec_globals = {
                'spark': mock_spark,
                'source_lakehouse': 'mock_source',
                'target_lakehouse': 'mock_target',
                'Window': Window,
                'row_number': row_number, 'col': col, 'when': when, 'lit': lit,
                'concat': concat, 'length': length, 'regexp_replace': regexp_replace,
                'collect_list': collect_list, 'concat_ws': concat_ws, 'upper': upper,
                'greatest': greatest, 'substring': substring, 'year': year,
                'lead': lead, 'date_add': date_add, 'lag': lag, 'max': max,
                'coalesce': coalesce, 'date_sub': date_sub, 'transform': transform,
                'array_filter': array_filter, 'split': split, 'array_join': array_join,
                'array_contains': array_contains, 'months_between': months_between,
                'current_date': current_date, 'round': round,
                'StructType': StructType, 'StructField': StructField,
                'StringType': StringType, 'LongType': LongType,
                'TimestampType': TimestampType, 'IntegerType': IntegerType,
                'reduce': reduce,
                'datetime': datetime,
                'upsert_silver_table': MagicMock(),
            }

            local_scope = {}
            exec(self.func_source, exec_globals, local_scope)
            process_enderecos = local_scope['process_enderecos']

            # Executa a função do notebook
            df_result = process_enderecos()

            # Valida que o dataframe é retornado
            self.assertIsNotNone(df_result, "A função deveria retornar df_enderecos_final")

            # Coleta os resultados do DataFrame final
            results = df_result.collect()

            # Mapeia os resultados por CPFCNPJ
            res_dict = {row["cpf_cnpj"]: row["cidade"] for row in results}

            # Validar as transformações
            self.assertEqual(res_dict["12345678901234"], "SAO PAULO")
            self.assertEqual(res_dict["98765432109876"], "RIBEIRAO PRETO")
            self.assertEqual(res_dict["11111111111111"], "MACEIO")
            self.assertEqual(res_dict["22222222222222"], "BRASILIA")
            self.assertEqual(res_dict["33333333333333"], "JAU")
            self.assertEqual(res_dict["44444444444444"], "JUIZ DE FORA MG")

        finally:
            DataFrame.write = orig_write


if __name__ == "__main__":
    unittest.main()
