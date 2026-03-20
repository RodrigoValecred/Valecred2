import unittest
import sys
import os
import re
import unicodedata

try:
    from tests.notebook_utils import extract_function_from_file
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from notebook_utils import extract_function_from_file

# Caminho para o notebook
NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Load_Silver_From_Manual_Uploads.Notebook/notebook-content.py"

class TestSanitizeColumnName(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting sanitize_column_name from {NOTEBOOK_PATH}")
        # sanitize_column_name is a nested function, but extract_function_from_file uses ast.walk so it should find it.
        # Depende de notebook_utils.py usando textwrap.dedent para gerenciar a indentação.
        func_source = extract_function_from_file(NOTEBOOK_PATH, "sanitize_column_name")

        if func_source:
            local_scope = {}
            # A função usa 're' e 'unicodedata', portanto, devemos fornecê-los em globais
            global_scope = {
                "re": re,
                "unicodedata": unicodedata
            }
            try:
                exec(func_source, global_scope, local_scope)
                cls.sanitize_column_name = staticmethod(local_scope["sanitize_column_name"])
            except Exception as e:
                print(f"Error executing extracted function: {e}")
                cls.sanitize_column_name = None
        else:
            cls.sanitize_column_name = None
            print("WARNING: sanitize_column_name function not found in file.")

    def test_function_exists(self):
        """Test that the function was successfully extracted."""
        self.assertIsNotNone(self.sanitize_column_name, "Function sanitize_column_name not found or failed to load.")

    def test_standard_snake_case(self):
        """Test simple snake_case strings (should remain unchanged)."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        self.assertEqual(self.sanitize_column_name("simple_column"), "simple_column")
        self.assertEqual(self.sanitize_column_name("id"), "id")

    def test_accents_removal(self):
        """Test unicode normalization (accents removal)."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        # 'Ação' -> 'acao'
        self.assertEqual(self.sanitize_column_name("Ação"), "acao")
        # 'éíóú' -> 'eiou'
        self.assertEqual(self.sanitize_column_name("test_éíóú"), "test_eiou")

    def test_camel_case_conversion(self):
        """Test CamelCase to snake_case conversion."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        self.assertEqual(self.sanitize_column_name("NomeCliente"), "nome_cliente")
        self.assertEqual(self.sanitize_column_name("DataDeNascimento"), "data_de_nascimento")
        # lowerCamelCase
        self.assertEqual(self.sanitize_column_name("nomeCliente"), "nome_cliente")

    def test_special_characters(self):
        """Test replacement of special characters with underscores."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        self.assertEqual(self.sanitize_column_name("Endereço/Rua"), "endereco_rua")
        self.assertEqual(self.sanitize_column_name("Renda ($)"), "renda")
        self.assertEqual(self.sanitize_column_name("user@domain.com"), "user_domain_com")

    def test_multiple_underscores(self):
        """Test collapsing of consecutive underscores."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        self.assertEqual(self.sanitize_column_name("id__cliente"), "id_cliente")
        self.assertEqual(self.sanitize_column_name("a___b"), "a_b")

    def test_stripping_underscores(self):
        """Test stripping of leading and trailing underscores."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        self.assertEqual(self.sanitize_column_name("_id_"), "id")
        self.assertEqual(self.sanitize_column_name("__name__"), "name")

    def test_mixed_cases_upper(self):
        """Test handling of uppercase inputs (should bypass CamelCase logic and just lower)."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        # 'ID_CLIENTE' -> isupper() é True -> lower() -> 'id_cliente'
        self.assertEqual(self.sanitize_column_name("ID_CLIENTE"), "id_cliente")
        # 'CODIGO' -> 'codigo'
        self.assertEqual(self.sanitize_column_name("CODIGO"), "codigo")

    def test_mixed_cases_complex(self):
        """Test complex mixed cases."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        # 'Some_Mixed_Case' -> 'some_mixed_case' (lógica CamelCase aplica-se às partes)
        # 'Some' -> 'some', '_', 'Mixed' -> '_mixed' ...
        self.assertEqual(self.sanitize_column_name("Some_Mixed_Case"), "some_mixed_case")

        # 'XMLHttpRequest' -> 'xml_http_request'
        self.assertEqual(self.sanitize_column_name("XMLHttpRequest"), "xml_http_request")

    def test_numbers(self):
        """Test that numbers are preserved."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        # Análise lógica: 'Address1' -> 'Address1' -> 'address1' (sem split antes de número exceto se CamelCase)
        self.assertEqual(self.sanitize_column_name("Address1"), "address1")
        self.assertEqual(self.sanitize_column_name("v2_0"), "v2_0")

if __name__ == '__main__':
    unittest.main()
