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

class TestSelectTitulos(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting select_titulos from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "select_titulos")
        if not cls.func_source:
             raise ValueError("Function select_titulos not found in notebook.")

    def test_select_titulos_structure(self):
        # Mocks for PySpark functions
        mock_col = MagicMock(name="col")
        mock_when = MagicMock(name="when")
        mock_datediff = MagicMock(name="datediff")
        mock_current_date = MagicMock(name="current_date")
        mock_coalesce = MagicMock(name="coalesce")
        mock_lit = MagicMock(name="lit")

        # Mock Column behavior
        # col("name") returns a mock object that supports .alias(), .isNotNull(), etc.
        def col_side_effect(name):
            m = MagicMock(name=f"col('{name}')")
            m.alias.return_value = m # Chain alias
            m.isNotNull.return_value = MagicMock(name=f"col('{name}').isNotNull()")
            return m

        mock_col.side_effect = col_side_effect

        # Mock datediff return to support > 0 comparison
        mock_datediff_ret = MagicMock(name="datediff_ret")
        # Mock the __gt__ operator so (datediff(...) > 0) doesn't raise TypeError
        mock_datediff_ret.__gt__ = MagicMock(name="gt_mock")
        mock_datediff.return_value = mock_datediff_ret

        # Mock coalesce return to support alias
        mock_coalesce_ret = MagicMock(name="coalesce_ret")
        mock_coalesce_ret.alias.return_value = mock_coalesce_ret
        mock_coalesce.return_value = mock_coalesce_ret

        # Mock when chain
        # when(cond, val) returns a Column which has .when() and .otherwise() methods
        mock_when_ret = MagicMock(name="when_ret")
        mock_when.return_value = mock_when_ret
        mock_when_ret.when.return_value = mock_when_ret

        # otherwise returns a mock that supports alias
        mock_otherwise_ret = MagicMock(name="otherwise_ret")
        mock_otherwise_ret.alias.return_value = mock_otherwise_ret
        mock_when_ret.otherwise.return_value = mock_otherwise_ret


        # Mock DataFrame
        mock_df = MagicMock(name="df")

        # Execution context
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
        result_df = select_titulos(mock_df)

        # Assertions
        # 1. Verify df.select was called
        mock_df.select.assert_called_once()

        # 2. Verify arguments passed to select
        args, kwargs = mock_df.select.call_args

        # We expect 36 arguments (columns)
        self.assertEqual(len(args), 36, f"Expected 36 columns, got {len(args)}")

        # Verify specific columns were accessed via col()
        expected_cols = [
            "CODTITULO", "CODOPERACAO", "NDOC", "TDOC", "VENCIMENTO",
            "VENCPRORROGADO", "PRAZO", "CPFCNPJSACADO", "CPFCNPJCEDENTE",
            "VALOR", "DESAGIO", "LIQUIDO", "AMORTIZACOES", "VALORDEVIDO",
            "LIQUIDACAO", "ACEITO", "CODBANCOCOBR", "DATACONF", "USUACONF",
            "DATAALTERACAO", "DATAINCLUSAO", "DOCCONFIRMADO", "MOTIVO",
            "PRACA", "CHAVEDANFE", "NOSSONUMERO", "CODFUNDO", "TTO",
            "FILIAL", "CODEMISSAO", "STATUSCONFIRMACAO", "SEUNUMERO", "CODREMESSA"
        ]

        # Get all calls to col()
        calls = [c[0][0] for c in mock_col.call_args_list]
        for col_name in expected_cols:
            self.assertIn(col_name, calls, f"Column {col_name} was not accessed via col()")

        # Verify calculated columns logic calls
        # vencimento_efetivo uses coalesce
        mock_coalesce.assert_called()

        # dias_atraso uses datediff and when
        mock_datediff.assert_called()
        mock_when.assert_called()

        # status_titulo uses when chain
        # confirmed by mock_when.assert_called()

        # Verify current_date was called (for dias_atraso and status_titulo logic)
        mock_current_date.assert_called()

        # Verify output aliases
        expected_aliases = {
            "cod_titulo", "cod_operacao", "n_doc", "t_doc", "vencimento",
            "venc_prorrogado", "prazo", "cpf_cnpj_sacado", "cpf_cnpj_cedente",
            "valor", "desagio", "liquido", "amortizacoes", "valor_devido",
            "liquidacao", "aceito", "cod_banco_cobr", "data_conf", "usua_conf",
            "data_alteracao", "data_inclusao", "doc_confirmado", "motivo",
            "praca", "chave_danfe", "nosso_numero", "cod_fundo", "tipo_cobranca",
            "raiz_cnpj", "cod_emissao", "status_confirmacao", "seu_numero_bancario", "cod_remessa",
            "vencimento_efetivo", "dias_atraso", "status_titulo"
        }

        found_aliases = set()
        for arg in args:
            # We mocked .alias() to return the same mock object, so we can check if it was called
            if arg.alias.called:
                # Check all calls to alias on this mock object
                for c in arg.alias.call_args_list:
                    alias_name = c[0][0]
                    found_aliases.add(alias_name)

        missing_aliases = expected_aliases - found_aliases
        self.assertFalse(missing_aliases, f"Missing output aliases: {missing_aliases}")

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

class TestSelectBaixas(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting select_baixas from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "select_baixas")
        if not cls.func_source:
             raise ValueError("Function select_baixas not found in notebook.")

    def test_select_baixas_structure(self):
        # Mocks for PySpark functions
        mock_col = MagicMock(name="col")

        # Mock Column behavior
        def col_side_effect(name):
            m = MagicMock(name=f"col('{name}')")
            m.alias.return_value = m # Chain alias
            return m

        mock_col.side_effect = col_side_effect

        # Mock DataFrame
        mock_df = MagicMock(name="df")

        # Execution context
        exec_globals = {
            'col': mock_col,
        }

        # Execute the function definition
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        select_baixas = local_scope['select_baixas']

        # Call the function
        result_df = select_baixas(mock_df)

        # Assertions
        # 1. Verify df.select was called
        mock_df.select.assert_called_once()

        # 2. Verify arguments passed to select
        args, kwargs = mock_df.select.call_args

        # Verify specific columns were accessed via col()
        expected_cols = [
            "CODTITULOBAIXAS", "CODTITULO", "DATAINCLUSAO", "DATAALTERACAO",
            "VLPAGO", "DATABAIXA", "DATABAIXASIST", "DESCONTO", "JUROS",
            "TARIFARECOMPRA", "DATAVENCIMENTO", "PAGOPELO", "FORMA",
            "TIPOBAIXA", "MOTIVO", "CODOPERACAO"
        ]

        self.assertEqual(len(args), len(expected_cols), f"Expected {len(expected_cols)} columns, got {len(args)}")

        # Get all calls to col()
        calls = [c[0][0] for c in mock_col.call_args_list]
        for col_name in expected_cols:
            self.assertIn(col_name, calls, f"Column {col_name} was not accessed via col()")

        # Verify output aliases
        expected_aliases = {
            "cod_titulo_baixas", "cod_titulo", "data_inclusao", "data_alteracao",
            "valor_pago", "data_baixa", "data_baixa_sist", "desconto", "juros",
            "tarifa_recompra", "data_vencimento", "pago_pelo", "forma",
            "tipo_baixa", "motivo", "cod_operacao"
        }

        found_aliases = set()
        for arg in args:
            if arg.alias.called:
                for c in arg.alias.call_args_list:
                    alias_name = c[0][0]
                    found_aliases.add(alias_name)

        missing_aliases = expected_aliases - found_aliases
        self.assertFalse(missing_aliases, f"Missing output aliases: {missing_aliases}")

if __name__ == '__main__':
    unittest.main()
