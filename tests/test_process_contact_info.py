import unittest
from unittest.mock import MagicMock, call
import sys
import os

# Garante que o pacote tests está no path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from tests.notebook_utils import extract_function_from_file
except ImportError:
    from notebook_utils import extract_function_from_file

NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Process_Contact_Info.Notebook/notebook-content.py"

class TestUnfoldContactInfo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting unfold_contact_info from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "unfold_contact_info")
        if not cls.func_source:
             print("WARNING: unfold_contact_info function not found in file.")

    def setUp(self):
        if not self.func_source:
            self.skipTest("Function not found")

        # Prepara o escopo com simulações para as funções do Spark
        # Retornamos strings para verificar facilmente a composição das funções
        self.mock_col = MagicMock(side_effect=lambda x: f"col({x})")
        self.mock_explode = MagicMock(side_effect=lambda x: f"explode({x})")
        self.mock_split = MagicMock(side_effect=lambda x, y: f"split({x}, {y})")
        self.mock_trim = MagicMock(side_effect=lambda x: f"trim({x})")

        local_scope = {}
        global_scope = {
            "col": self.mock_col,
            "explode": self.mock_explode,
            "split": self.mock_split,
            "trim": self.mock_trim
        }

        exec(self.func_source, global_scope, local_scope)
        self.unfold_contact_info = local_scope["unfold_contact_info"]

    def test_function_exists(self):
        """Testa se a função foi extraída com sucesso."""
        self.assertIsNotNone(self.func_source, "Function unfold_contact_info not found in notebook file.")

    def test_unfold_logic(self):
        """Testa a lógica principal: explode(split) e então trim."""
        df = MagicMock()
        # Mocking do method chaining
        df_unfolded = MagicMock()
        df_cleaned = MagicMock()

        df.withColumn.return_value = df_unfolded
        df_unfolded.withColumn.return_value = df_cleaned

        result = self.unfold_contact_info(df, "INPUT_COL", "OUTPUT_COL", ";")

        # Verifica first withColumn call (explode + split)
        # Expected: explode(split(col(INPUT_COL), ;))
        df.withColumn.assert_called_once()
        args, _ = df.withColumn.call_args
        self.assertEqual(args[0], "OUTPUT_COL")
        self.assertEqual(args[1], "explode(split(col(INPUT_COL), ;))")

        # Verifica a segunda chamada withColumn (trim)
        # Expected: trim(col(OUTPUT_COL))
        df_unfolded.withColumn.assert_called_once()
        args, _ = df_unfolded.withColumn.call_args
        self.assertEqual(args[0], "OUTPUT_COL")
        self.assertEqual(args[1], "trim(col(OUTPUT_COL))")

        self.assertEqual(result, df_cleaned)

    def test_delimiter_parameter(self):
        """Testa se o parâmetro delimitador é passado corretamente para o split."""
        df = MagicMock()
        df.withColumn.return_value = MagicMock()

        # Usa um delimitador seguro como "," para evitar confusão de regex no teste
        self.unfold_contact_info(df, "INPUT_COL", "OUTPUT_COL", ",")

        # Verifica se split usa o delimitador correto
        self.mock_split.assert_called_with("col(INPUT_COL)", ",")

if __name__ == '__main__':
    unittest.main()
