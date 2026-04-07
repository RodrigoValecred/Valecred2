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
        # sanitize_column_name é uma função aninhada, mas extract_function_from_file usa ast.walk, então deve encontrá-la.
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
        """Testa se a função foi extraída com sucesso."""
        self.assertIsNotNone(self.sanitize_column_name, "Function sanitize_column_name not found or failed to load.")

    def test_standard_snake_case(self):
        """Teste strings simples de snake_case (devem permanecer inalteradas)."""
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
        """Testa a conversão de CamelCase para snake_case."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        self.assertEqual(self.sanitize_column_name("NomeCliente"), "nome_cliente")
        self.assertEqual(self.sanitize_column_name("DataDeNascimento"), "data_de_nascimento")
        # inferiorCamelCase
        self.assertEqual(self.sanitize_column_name("nomeCliente"), "nome_cliente")

    def test_special_characters(self):
        """Testa a substituição de caracteres especiais por sublinhados."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        self.assertEqual(self.sanitize_column_name("Endereço/Rua"), "endereco_rua")
        self.assertEqual(self.sanitize_column_name("Renda ($)"), "renda")
        self.assertEqual(self.sanitize_column_name("user@domain.com"), "user_domain_com")

    def test_multiple_underscores(self):
        """Testa o colapso de sublinhados consecutivos."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        self.assertEqual(self.sanitize_column_name("id__cliente"), "id_cliente")
        self.assertEqual(self.sanitize_column_name("a___b"), "a_b")

    def test_stripping_underscores(self):
        """Testa a remoção de sublinhados no início e no final."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        self.assertEqual(self.sanitize_column_name("_id_"), "id")
        self.assertEqual(self.sanitize_column_name("__name__"), "name")

    def test_mixed_cases_upper(self):
        """Testa o tratamento de entradas em maiúsculas (deve ignorar a lógica CamelCase e apenas colocar em minúsculas)."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        # 'ID_CLIENTE' -> isupper() é True -> lower() -> 'id_cliente'
        self.assertEqual(self.sanitize_column_name("ID_CLIENTE"), "id_cliente")
        # 'CODIGO' -> 'codigo'
        self.assertEqual(self.sanitize_column_name("CODIGO"), "codigo")

    def test_mixed_cases_complex(self):
        """Teste casos mistos complexos."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        # 'Some_Mixed_Case' -> 'some_mixed_case' (lógica CamelCase aplica-se às partes)
        # 'Some' -> 'some', '_', 'Mixed' -> '_mixed' ...
        self.assertEqual(self.sanitize_column_name("Some_Mixed_Case"), "some_mixed_case")

        # 'XMLHttpRequest' -> 'xml_http_request'
        self.assertEqual(self.sanitize_column_name("XMLHttpRequest"), "xml_http_request")

    def test_numbers(self):
        """Testa se os números são preservados."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        # Análise lógica: 'Address1' -> 'Address1' -> 'address1' (sem split antes de número exceto se CamelCase)
        self.assertEqual(self.sanitize_column_name("Address1"), "address1")
        self.assertEqual(self.sanitize_column_name("v2_0"), "v2_0")

        # Testa camel case com números
        self.assertEqual(self.sanitize_column_name("Version2_1"), "version2_1")
        self.assertEqual(self.sanitize_column_name("v2_0_NewVersion"), "v2_0_new_version")

    def test_empty_string(self):
        """Testa o comportamento com string vazia."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        self.assertEqual(self.sanitize_column_name(""), "")
        self.assertEqual(self.sanitize_column_name("   "), "")

    def test_non_string_inputs(self):
        """Testa o comportamento com entradas não-string, que devem ser convertidas."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        self.assertEqual(self.sanitize_column_name(None), "none")
        self.assertEqual(self.sanitize_column_name(123), "123")
        self.assertEqual(self.sanitize_column_name(45.67), "45_67")
        self.assertEqual(self.sanitize_column_name(True), "true")

    def test_messy_strings(self):
        """Testa strings com muitos espaços, quebras de linha e caracteres especiais."""
        if not self.sanitize_column_name: self.skipTest("Function not found")
        messy_str = "  \n  Coluna   Muito\t \n Estranha !@# %& *()  "
        self.assertEqual(self.sanitize_column_name(messy_str), "coluna_muito_estranha")
        self.assertEqual(self.sanitize_column_name("A.B.C"), "a_b_c")
        self.assertEqual(self.sanitize_column_name("---A---B---"), "a_b")

if __name__ == '__main__':
    unittest.main()
