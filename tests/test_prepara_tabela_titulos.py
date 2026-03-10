import unittest
import sys
import os
from unittest.mock import MagicMock, call

# Ensure the tests directory is in the path to import notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define the path to the notebook file relative to the repository root
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Titulos.Notebook/notebook-content.py"
)

class MockColumn:
    def __init__(self, expr):
        self.expr = str(expr)

    def __str__(self):
        return self.expr

    def __repr__(self):
        return self.expr

    def alias(self, alias_name):
        return MockColumn(f"{self.expr} AS {alias_name}")

    def isNotNull(self):
        return MockColumn(f"{self.expr}.isNotNull()")

    def when(self, cond, val):
        cond_str = str(cond) if isinstance(cond, MockColumn) else repr(cond)
        val_str = str(val) if isinstance(val, MockColumn) else repr(val)
        return MockColumn(f"{self.expr}.when({cond_str}, {val_str})")

    def otherwise(self, val):
        val_str = str(val) if isinstance(val, MockColumn) else repr(val)
        return MockColumn(f"{self.expr}.otherwise({val_str})")

    def __gt__(self, other):
        other_str = str(other) if isinstance(other, MockColumn) else repr(other)
        return MockColumn(f"({self.expr} > {other_str})")

def mock_when(cond, val):
    cond_str = str(cond) if isinstance(cond, MockColumn) else repr(cond)
    val_str = str(val) if isinstance(val, MockColumn) else repr(val)
    return MockColumn(f"when({cond_str}, {val_str})")

def mock_col(name):
    return MockColumn(f"col('{name}')")

def mock_datediff(end, start):
    end_str = str(end) if isinstance(end, MockColumn) else repr(end)
    start_str = str(start) if isinstance(start, MockColumn) else repr(start)
    return MockColumn(f"datediff({end_str}, {start_str})")

def mock_current_date():
    return MockColumn("current_date()")

def mock_coalesce(*cols):
    cols_str = ", ".join(str(c) if isinstance(c, MockColumn) else repr(c) for c in cols)
    return MockColumn(f"coalesce({cols_str})")

def mock_lit(val):
    val_str = str(val) if isinstance(val, MockColumn) else repr(val)
    return MockColumn(f"lit({val_str})")

class TestSelectTitulos(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting select_titulos from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "select_titulos")
        if not cls.func_source:
             raise ValueError("Function select_titulos not found in notebook.")

    def test_select_titulos_structure(self):
        # Mock DataFrame
        mock_df = MagicMock(name="df")

        # We need to track the arguments passed to df.select
        captured_args = []
        def select_mock(*args):
            captured_args.extend(args)
            return "df_result"

        mock_df.select.side_effect = select_mock

        # Execution context using our MockColumn implementation
        exec_globals = {
            'col': mock_col,
            'when': mock_when,
            'datediff': mock_datediff,
            'current_date': mock_current_date,
            'coalesce': mock_coalesce,
            'lit': mock_lit,
        }

        # Execute the function definition
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        select_titulos = local_scope['select_titulos']

        # Call the function
        result = select_titulos(mock_df)

        # Verify it returns the result of df.select
        self.assertEqual(result, "df_result")

        # 1. Verify df.select was called
        self.assertTrue(mock_df.select.called)

        # We expect 36 arguments (columns)
        self.assertEqual(len(captured_args), 36, f"Expected 36 columns, got {len(captured_args)}")

        # Convert all captured arguments to their string representations
        args_str = [str(arg) for arg in captured_args]

        # 2. Verify all simple column aliases are present
        expected_simple_aliases = {
            "col('CODTITULO') AS cod_titulo",
            "col('CODOPERACAO') AS cod_operacao",
            "col('NDOC') AS n_doc",
            "col('TDOC') AS t_doc",
            "col('VENCIMENTO') AS vencimento",
            "col('VENCPRORROGADO') AS venc_prorrogado",
            "col('PRAZO') AS prazo",
            "col('CPFCNPJSACADO') AS cpf_cnpj_sacado",
            "col('CPFCNPJCEDENTE') AS cpf_cnpj_cedente",
            "col('VALOR') AS valor",
            "col('DESAGIO') AS desagio",
            "col('LIQUIDO') AS liquido",
            "col('AMORTIZACOES') AS amortizacoes",
            "col('VALORDEVIDO') AS valor_devido",
            "col('LIQUIDACAO') AS liquidacao",
            "col('ACEITO') AS aceito",
            "col('CODBANCOCOBR') AS cod_banco_cobr",
            "col('DATACONF') AS data_conf",
            "col('USUACONF') AS usua_conf",
            "col('DATAALTERACAO') AS data_alteracao",
            "col('DATAINCLUSAO') AS data_inclusao",
            "col('DOCCONFIRMADO') AS doc_confirmado",
            "col('MOTIVO') AS motivo",
            "col('PRACA') AS praca",
            "col('CHAVEDANFE') AS chave_danfe",
            "col('NOSSONUMERO') AS nosso_numero",
            "col('CODFUNDO') AS cod_fundo",
            "col('TTO') AS tipo_cobranca",
            "col('FILIAL') AS raiz_cnpj",
            "col('CODEMISSAO') AS cod_emissao",
            "col('STATUSCONFIRMACAO') AS status_confirmacao",
            "col('SEUNUMERO') AS seu_numero_bancario",
            "col('CODREMESSA') AS cod_remessa"
        }

        for expected_alias in expected_simple_aliases:
            self.assertIn(expected_alias, args_str, f"Missing expected simple alias mapping: {expected_alias}")

        # 3. Verify complex calculated columns using their string representations

        # vencimento_efetivo uses coalesce
        expected_vencimento_efetivo = "coalesce(col('VENCPRORROGADO'), col('VENCIMENTO')) AS vencimento_efetivo"
        self.assertIn(expected_vencimento_efetivo, args_str, "Missing or incorrect vencimento_efetivo expression")

        # dias_atraso uses datediff and when
        expected_dias_atraso = "when(col('LIQUIDACAO').isNotNull(), datediff(col('LIQUIDACAO'), coalesce(col('VENCPRORROGADO'), col('VENCIMENTO')))).otherwise(datediff(current_date(), coalesce(col('VENCPRORROGADO'), col('VENCIMENTO')))) AS dias_atraso"
        self.assertIn(expected_dias_atraso, args_str, "Missing or incorrect dias_atraso expression")

        # status_titulo uses when chain
        expected_status_titulo = "when(col('LIQUIDACAO').isNotNull(), 'LIQUIDADO').when((datediff(current_date(), coalesce(col('VENCPRORROGADO'), col('VENCIMENTO'))) > 0), 'EM ATRASO').otherwise('EM DIA') AS status_titulo"
        self.assertIn(expected_status_titulo, args_str, "Missing or incorrect status_titulo expression")

class TestDeduplicateTitulos(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.appName("TestDeduplicateTitulos").master("local[1]").getOrCreate()
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "deduplicate_titulos")
        if not cls.func_source:
             raise ValueError("Function deduplicate_titulos not found in notebook.")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_deduplicate_titulos_logic(self):
        from pyspark.sql.functions import col, greatest, row_number
        from pyspark.sql.window import Window

        # Create execution context
        exec_globals = {
            'col': col,
            'greatest': greatest,
            'Window': Window,
            'row_number': row_number,
        }
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        deduplicate_titulos = local_scope['deduplicate_titulos']

        # Sample data with duplicates for CODTITULO
        data = [
            # Título 1 - Row 1 is the most recent due to DATAALTERACAO
            (1, "2023-01-01", "2023-01-10", None, "A"),
            (1, "2023-01-01", "2023-01-05", None, "B"),

            # Título 2 - Row 1 is most recent due to LIQUIDACAO
            (2, "2023-02-01", "2023-02-02", "2023-02-20", "C"),
            (2, "2023-02-01", "2023-02-15", None, "D"),

            # Título 3 - Only one row
            (3, "2023-03-01", "2023-03-01", None, "E")
        ]

        from pyspark.sql.types import StructType, StructField, IntegerType, StringType

        schema = StructType([
            StructField("CODTITULO", IntegerType(), True),
            StructField("DATAINCLUSAO", StringType(), True),
            StructField("DATAALTERACAO", StringType(), True),
            StructField("LIQUIDACAO", StringType(), True),
            StructField("OTHER_DATA", StringType(), True)
        ])

        df = self.spark.createDataFrame(data, schema=schema)

        key_columns = ["CODTITULO"]

        # Execute deduplication
        df_result = deduplicate_titulos(df, key_columns)

        # Verify results
        results = df_result.orderBy("CODTITULO").collect()

        self.assertEqual(len(results), 3, "Should have 3 unique titles")

        # Check Título 1 -> expects OTHER_DATA = "A" because DATAALTERACAO is highest ("2023-01-10")
        row1 = [r for r in results if r.CODTITULO == 1][0]
        self.assertEqual(row1.OTHER_DATA, "A")

        # Check Título 2 -> expects OTHER_DATA = "C" because LIQUIDACAO is highest ("2023-02-20")
        row2 = [r for r in results if r.CODTITULO == 2][0]
        self.assertEqual(row2.OTHER_DATA, "C")

        # Check Título 3 -> expects OTHER_DATA = "E"
        row3 = [r for r in results if r.CODTITULO == 3][0]
        self.assertEqual(row3.OTHER_DATA, "E")

        # Check that DATA_MAIS_RECENTE and row_num were correctly dropped
        self.assertNotIn("DATA_MAIS_RECENTE", df_result.columns)
        self.assertNotIn("row_num", df_result.columns)

if __name__ == '__main__':
    unittest.main()
