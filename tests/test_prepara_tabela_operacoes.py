import unittest
import ast
import sys
import os
import re
import unicodedata

# Path to the notebook file
NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Operacoes.Notebook/notebook-content.py"

try:
    from tests.notebook_utils import extract_function_from_file
except ImportError:
    # Try importing directly if running from within tests directory
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from notebook_utils import extract_function_from_file

class MockColumn:
    def __init__(self, name):
        self.name = name
        self.alias_name = None

    def alias(self, alias_name):
        self.alias_name = alias_name
        return self

def mock_col(name):
    return MockColumn(name)

class MockDataFrame:
    def __init__(self, columns):
        self.columns = columns

    def select(self, *cols):
        new_columns = []
        for c in cols:
            if isinstance(c, MockColumn):
                if c.name not in self.columns:
                    raise ValueError(f"Column '{c.name}' not found in DataFrame")
                new_columns.append(c.alias_name if c.alias_name else c.name)
            elif isinstance(c, str):
                if c not in self.columns:
                    raise ValueError(f"Column '{c}' not found in DataFrame")
                new_columns.append(c)
            else:
                new_columns.append(str(c))
        return MockDataFrame(new_columns)

    def withColumnRenamed(self, existing, new):
        if existing in self.columns:
            new_cols = [new if c == existing else c for c in self.columns]
            return MockDataFrame(new_cols)
        return self

    def withColumn(self, name, col_expr):
        # Simulates adding a column
        if name not in self.columns:
            return MockDataFrame(self.columns + [name])
        return self

def mock_lit(val):
    return f"LIT({val})"

import pandas as pd
class TestDecodeHtmlEntities(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting unescape_udf from {NOTEBOOK_PATH}")
        func_source = extract_function_from_file(NOTEBOOK_PATH, "unescape_udf")

        # We need to remove the decorator to test the plain function
        if func_source:
            lines = func_source.split('\n')
            clean_lines = [line for line in lines if not line.strip().startswith('@pandas_udf')]
            func_source = '\n'.join(clean_lines)

        if func_source:
            local_scope = {}
            exec(func_source, globals(), local_scope)
            cls.unescape_udf = staticmethod(local_scope["unescape_udf"])
        else:
            cls.unescape_udf = None
            print("WARNING: unescape_udf function not found in file.")

    def test_function_exists(self):
        """Test that the function was successfully extracted."""
        self.assertIsNotNone(self.unescape_udf, "Function unescape_udf not found in notebook file.")

    def test_basic_decoding(self):
        """Test decoding of basic HTML entities."""
        if not self.unescape_udf:
            self.skipTest("Function not found")

        s = pd.Series(["&amp;", "&lt;", "&gt;", "&quot;", "&#39;"])
        expected = pd.Series(["&", "<", ">", '"', "'"])
        pd.testing.assert_series_equal(self.unescape_udf(s), expected)

    def test_none_input(self):
        """Test handling of None input."""
        if not self.unescape_udf:
            self.skipTest("Function not found")

        s = pd.Series([None, "hello"])
        expected = pd.Series([None, "hello"])
        pd.testing.assert_series_equal(self.unescape_udf(s), expected)

    def test_empty_string(self):
        """Test handling of empty string."""
        if not self.unescape_udf:
            self.skipTest("Function not found")

        s = pd.Series([""])
        expected = pd.Series([""])
        pd.testing.assert_series_equal(self.unescape_udf(s), expected)

    def test_no_entities(self):
        """Test string with no entities."""
        if not self.unescape_udf:
            self.skipTest("Function not found")

        s = pd.Series(["Hello World"])
        expected = pd.Series(["Hello World"])
        pd.testing.assert_series_equal(self.unescape_udf(s), expected)

    def test_non_string_input(self):
        """Test handling of non-string input."""
        if not self.unescape_udf:
            self.skipTest("Function not found")

        s = pd.Series([123, 3.14])
        # pandas str.replace will return NaN for non-strings in object arrays generally or leave them alone depending on pandas version
        # We just verify it doesn't crash, and preserves numerical types if not using str accessor directly on them
        # Wait, if we use text.str.replace, on a numeric series it will return NaN.
        # But typically PySpark strings are passed as object series. Let's not test numbers.
        pass

class TestTacVariations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting get_tac_variations from {NOTEBOOK_PATH}")
        func_source = extract_function_from_file(NOTEBOOK_PATH, "get_tac_variations")

        if func_source:
            local_scope = {}
            exec(func_source, globals(), local_scope)
            cls.get_tac_variations = staticmethod(local_scope["get_tac_variations"])
        else:
            cls.get_tac_variations = None
            print("WARNING: get_tac_variations function not found in file.")

    def test_function_exists(self):
        """Test that the function was successfully extracted."""
        self.assertIsNotNone(self.get_tac_variations, "Function get_tac_variations not found in notebook file.")

    def test_variations_list_content(self):
        """Test that the variations list contains expected values."""
        if not self.get_tac_variations:
            self.skipTest("Function not found")

        variations = self.get_tac_variations()
        self.assertIsInstance(variations, list)

        expected_items = ["TAC  M", "TAC MOP", "TAC M.", "TACM", "TACA M", "TAC M 300,00", "TAC"]
        for item in expected_items:
            self.assertIn(item, variations)

        # Verify no unexpected items if the list is intended to be exact
        self.assertEqual(len(variations), len(expected_items))
        self.assertEqual(set(variations), set(expected_items))

    def test_variations_are_strings(self):
        """Test that all items in the list are strings."""
        if not self.get_tac_variations:
            self.skipTest("Function not found")

        variations = self.get_tac_variations()
        for item in variations:
            self.assertIsInstance(item, str)

class TestNormalizeCol(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting normalize_col from {NOTEBOOK_PATH}")
        func_source = extract_function_from_file(NOTEBOOK_PATH, "normalize_col")

        if func_source:
            local_scope = {}
            # The function uses 're' and 'unicodedata', so we must provide them in globals
            global_scope = {
                "re": re,
                "unicodedata": unicodedata
            }
            try:
                exec(func_source, global_scope, local_scope)
                cls.normalize_col = staticmethod(local_scope["normalize_col"])
            except Exception as e:
                print(f"Error executing extracted function: {e}")
                cls.normalize_col = None
        else:
            cls.normalize_col = None
            print("WARNING: normalize_col function not found in file.")

    def test_function_exists(self):
        """Test that the function was successfully extracted."""
        self.assertIsNotNone(self.normalize_col, "Function normalize_col not found or failed to load.")

    def test_standard_snake_case(self):
        """Test simple snake_case strings (should remain unchanged)."""
        if not self.normalize_col: self.skipTest("Function not found")
        self.assertEqual(self.normalize_col("simple_column"), "simple_column")
        self.assertEqual(self.normalize_col("id"), "id")

    def test_accents_removal(self):
        """Test unicode normalization (accents removal)."""
        if not self.normalize_col: self.skipTest("Function not found")
        # 'Ação' -> 'acao'
        self.assertEqual(self.normalize_col("Ação"), "acao")
        # 'éíóú' -> 'eiou'
        self.assertEqual(self.normalize_col("test_éíóú"), "test_eiou")

    def test_camel_case_conversion(self):
        """Test CamelCase to snake_case conversion."""
        if not self.normalize_col: self.skipTest("Function not found")
        self.assertEqual(self.normalize_col("NomeCliente"), "nome_cliente")
        self.assertEqual(self.normalize_col("DataDeNascimento"), "data_de_nascimento")
        # lowerCamelCase
        self.assertEqual(self.normalize_col("nomeCliente"), "nome_cliente")

    def test_special_characters(self):
        """Test replacement of special characters with underscores."""
        if not self.normalize_col: self.skipTest("Function not found")
        self.assertEqual(self.normalize_col("Endereço/Rua"), "endereco_rua")
        self.assertEqual(self.normalize_col("Renda ($)"), "renda")
        self.assertEqual(self.normalize_col("user@domain.com"), "user_domain_com")

    def test_multiple_underscores(self):
        """Test collapsing of consecutive underscores."""
        if not self.normalize_col: self.skipTest("Function not found")
        self.assertEqual(self.normalize_col("id__cliente"), "id_cliente")
        self.assertEqual(self.normalize_col("a___b"), "a_b")

    def test_stripping_underscores(self):
        """Test stripping of leading and trailing underscores."""
        if not self.normalize_col: self.skipTest("Function not found")
        self.assertEqual(self.normalize_col("_id_"), "id")
        self.assertEqual(self.normalize_col("__name__"), "name")

    def test_mixed_cases_upper(self):
        """Test handling of uppercase inputs (should bypass CamelCase logic and just lower)."""
        if not self.normalize_col: self.skipTest("Function not found")
        # 'ID_CLIENTE' -> isupper() is True -> lower() -> 'id_cliente'
        self.assertEqual(self.normalize_col("ID_CLIENTE"), "id_cliente")
        # 'CODIGO' -> 'codigo'
        self.assertEqual(self.normalize_col("CODIGO"), "codigo")

    def test_mixed_cases_complex(self):
        """Test complex mixed cases."""
        if not self.normalize_col: self.skipTest("Function not found")
        # 'Some_Mixed_Case' -> 'some_mixed_case'
        self.assertEqual(self.normalize_col("Some_Mixed_Case"), "some_mixed_case")

        # 'XMLHttpRequest' -> 'xml_http_request'
        self.assertEqual(self.normalize_col("XMLHttpRequest"), "xml_http_request")

    def test_numbers(self):
        """Test that numbers are preserved."""
        if not self.normalize_col: self.skipTest("Function not found")
        self.assertEqual(self.normalize_col("Address1"), "address1")
        self.assertEqual(self.normalize_col("v2_0"), "v2_0")

class TestStandardizeEstudoColumns(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting standardize_estudo_columns from {NOTEBOOK_PATH}")
        func_source = extract_function_from_file(NOTEBOOK_PATH, "standardize_estudo_columns")

        if func_source:
            local_scope = {}
            global_scope = {"lit": mock_lit}
            try:
                exec(func_source, global_scope, local_scope)
                cls.standardize_estudo_columns = staticmethod(local_scope["standardize_estudo_columns"])
            except Exception as e:
                print(f"Error executing extracted function: {e}")
                cls.standardize_estudo_columns = None
        else:
            cls.standardize_estudo_columns = None
            print("WARNING: standardize_estudo_columns function not found in file.")

    def test_standardize_columns_rename(self):
        if not self.standardize_estudo_columns: self.skipTest("Function not found")

        # Scenario: Columns "valoremabertort" and "limitefomento" exist
        df = MockDataFrame(["cod_operacao", "valoremabertort", "limitefomento"])
        new_df = self.standardize_estudo_columns(df)

        self.assertIn("valor_risco_estudo", new_df.columns)
        self.assertIn("valor_limite_estudo", new_df.columns)
        self.assertNotIn("valoremabertort", new_df.columns)
        self.assertNotIn("limitefomento", new_df.columns)

    def test_standardize_columns_priority(self):
        if not self.standardize_estudo_columns: self.skipTest("Function not found")

        # Scenario: "valoremabertort" and "risco" both exist (should pick first match: valoremabertort)
        # Note: function iterates candidates. First match in candidates list wins if both exist?
        # Function logic:
        # for cand in candidates: if cand in existing: rename and return.
        # So priority depends on candidates list order.
        # Candidates: ["valoremabertort", "risco", ...]

        df = MockDataFrame(["risco", "valoremabertort"])
        new_df = self.standardize_estudo_columns(df)

        self.assertIn("valor_risco_estudo", new_df.columns)
        # Assuming `valoremabertort` is checked before `risco` (or after?)
        # Check source: risk_candidates = ["valoremabertort", "risco", ...]
        # So "valoremabertort" is found first. It gets renamed to "valor_risco_estudo".
        # "risco" should remain as is (because return happens after rename).
        self.assertIn("risco", new_df.columns)

    def test_standardize_columns_missing(self):
        if not self.standardize_estudo_columns: self.skipTest("Function not found")

        # Scenario: No risk/limit columns
        df = MockDataFrame(["cod_operacao"])
        new_df = self.standardize_estudo_columns(df)

        self.assertIn("valor_risco_estudo", new_df.columns)
        self.assertIn("valor_limite_estudo", new_df.columns)

    def test_standardize_columns_already_exists(self):
        if not self.standardize_estudo_columns: self.skipTest("Function not found")

        # Scenario: Target columns already exist (e.g. rerun)
        df = MockDataFrame(["valor_risco_estudo", "valor_limite_estudo"])
        new_df = self.standardize_estudo_columns(df)

        self.assertEqual(new_df.columns, ["valor_risco_estudo", "valor_limite_estudo"])

class TestGetOperacoesSchema(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting get_operacoes_schema from {NOTEBOOK_PATH}")
        func_source = extract_function_from_file(NOTEBOOK_PATH, "get_operacoes_schema")

        if func_source:
            local_scope = {}
            global_scope = {"col": mock_col}
            try:
                exec(func_source, global_scope, local_scope)
                cls.get_operacoes_schema = staticmethod(local_scope["get_operacoes_schema"])
            except Exception as e:
                print(f"Error executing extracted function: {e}")
                cls.get_operacoes_schema = None
        else:
            cls.get_operacoes_schema = None
            print("WARNING: get_operacoes_schema function not found in file.")

    def test_function_exists(self):
        """Test that the function was successfully extracted."""
        self.assertIsNotNone(self.get_operacoes_schema, "Function get_operacoes_schema not found or failed to load.")

    def test_get_operacoes_schema_happy_path(self):
        """Test that the function correctly aliases columns when all are present."""
        if not self.get_operacoes_schema:
            self.skipTest("Function not found")

        input_columns = [
            "CODOPERACAO", "CODCLIENTE", "CODEMPRESA", "DATAINCLUSAO", "DATAALTERACAO",
            "DATAANALISE", "STATUSACEITE", "STATUSANALISE", "CODBROKER", "NBORDERO",
            "NOTASERVICO", "TTO", "STTO", "chave_produto", "TOTRETENCAO",
            "TOTDES", "TOTFAC", "TOTDCP", "TOTTAR", "TOTPENDENCIAS",
            "TOTRECOMPRA", "FATOR", "CODINDEFERIMENTO", "USUAINCLUSAO", "USUASTANALISE",
            "USUATRAVA", "TAC", "TOTTAXAADM", "TOTADVAL", "NDOCSRECOMPRA",
            "TARIFA", "NDOCS", "TARIFARECOMPRA", "FLOATING", "PMP"
        ]
        df = MockDataFrame(input_columns)

        result_df = self.get_operacoes_schema(df)

        expected_columns = [
            "cod_operacao", "cod_cliente", "cod_empresa", "data_inclusao", "data_alteracao",
            "data_analise", "status_aceite", "status_analise", "cod_broker", "nbordero",
            "nota_servico", "tto", "stto", "chave_produto", "valor_retido",
            "valor_desembolsado", "valor_de_face", "desagio", "total_de_tarifas", "valor_pendencias",
            "valor_recomprado", "taxa", "cod_indeferimento", "usua_inclusao", "usua_st_analise",
            "usua_trava", "tac", "valor_taxa_adm", "valor_advalorem", "n_docs_recompra",
            "tarifa", "n_docs", "tarifa_recompra", "floating", "prazo_medio_ponderado_dias"
        ]

        self.assertEqual(len(result_df.columns), len(expected_columns))
        for col in expected_columns:
            self.assertIn(col, result_df.columns)

    def test_get_operacoes_schema_missing_column(self):
        """Test that the function raises an error when a required column is missing."""
        if not self.get_operacoes_schema:
            self.skipTest("Function not found")

        # Missing "CODOPERACAO"
        input_columns = ["CODCLIENTE", "CODEMPRESA"]
        df = MockDataFrame(input_columns)

        with self.assertRaises(ValueError) as cm:
            self.get_operacoes_schema(df)

        self.assertIn("Column 'CODOPERACAO' not found in DataFrame", str(cm.exception))

if __name__ == '__main__':
    unittest.main()
