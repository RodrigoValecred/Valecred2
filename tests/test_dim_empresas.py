import unittest
import sys
import os

try:
    from tests.notebook_utils import extract_function_from_file
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from notebook_utils import extract_function_from_file

# Path to the notebook
NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Gold_Dim_Empresas.Notebook/notebook-content.py"

class TestDeriveEmpresaName(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting derive_empresa_name from {NOTEBOOK_PATH}")
        func_source = extract_function_from_file(NOTEBOOK_PATH, "derive_empresa_name")

        if func_source:
            local_scope = {}
            exec(func_source, globals(), local_scope)
            cls.derive_empresa_name = staticmethod(local_scope["derive_empresa_name"])
        else:
            cls.derive_empresa_name = None
            print("WARNING: derive_empresa_name function not found in file.")

    def test_function_exists(self):
        """Test that the function was successfully extracted."""
        self.assertIsNotNone(self.derive_empresa_name, "Function derive_empresa_name not found in notebook file.")

    def test_none_input(self):
        """Test handling of None input."""
        if not self.derive_empresa_name: self.skipTest("Function not found")
        self.assertIsNone(self.derive_empresa_name(None))

    def test_empty_string(self):
        """Test handling of empty string."""
        if not self.derive_empresa_name: self.skipTest("Function not found")
        self.assertIsNone(self.derive_empresa_name(""))

    def test_non_string_input(self):
        """Test handling of non-string input (should trigger exception and return None)."""
        if not self.derive_empresa_name: self.skipTest("Function not found")
        self.assertIsNone(self.derive_empresa_name(123))

    def test_short_string_no_match(self):
        """Test a short string that doesn't trigger any extraction logic."""
        if not self.derive_empresa_name: self.skipTest("Function not found")
        # "TEST" -> All splits fail or produce parts too short
        # char7 (split by .) -> "TEST" -> len 4. slice [4:5] is empty.
        self.assertEqual(self.derive_empresa_name("TEST"), "")

    def test_simple_string_match_char7(self):
        """Test a simple string that triggers char7 logic (split by .)."""
        if not self.derive_empresa_name: self.skipTest("Function not found")
        # Logic: split by ".", take last part. If len > 4, take 5th char.
        # "SIMPLE" -> "SIMPLE" -> 5th char 'L'
        self.assertEqual(self.derive_empresa_name("SIMPLE"), "L")

    def test_securitizadora_match_char1(self):
        """Test string containing ' SECURITIZADORA ' to trigger char1 logic."""
        if not self.derive_empresa_name: self.skipTest("Function not found")
        # Logic: split by " SECURITIZADORA ", reverse, take 2nd item (original first), take 1st char.
        # "PREFIX SECURITIZADORA SUFFIX" -> ["PREFIX", "SUFFIX"] -> rev ["SUFFIX", "PREFIX"] -> part1 "PREFIX" -> char1 "P"
        # char7 might also trigger? "PREFIX SECURITIZADORA SUFFIX" split by . is single part. Length > 4. 5th char of "PREFIX..." is 'I'.
        # So result = "P" + "" + ... + "I" = "PI"
        result = self.derive_empresa_name("PREFIX SECURITIZADORA SUFFIX")
        # char1='P'
        # char7 (split by .): "PREFIX SECURITIZADORA SUFFIX". 5th char is 'I'.
        # Expect "PI"
        self.assertEqual(result, "PI")

    def test_valecred_logic(self):
        """Test string containing 'VALECRED ' to trigger char3 logic."""
        if not self.derive_empresa_name: self.skipTest("Function not found")
        # Logic: split by "VALECRED ", take part 1 (after), split by ".", reverse, take 1st char of each part, join.
        # "VALECRED ALPHA.BETA" -> part="ALPHA.BETA" -> split=["ALPHA", "BETA"] -> rev=["BETA", "ALPHA"] -> chars="B"+"A" -> "BA"
        # char7 also triggers? "VALECRED ALPHA.BETA". Last part of "." split is "BETA". Len 4. slice [4:5] empty.
        # char4 (space split). "VALECRED ALPHA.BETA". 1 part (no spaces after VALECRED?). Wait "VALECRED " has space.
        # ["", "ALPHA.BETA"]. len 2. char4 needs index 4 (5th part). No.
        # So result "BA".
        self.assertEqual(self.derive_empresa_name("VALECRED ALPHA.BETA"), "BA")

    def test_space_split_logic(self):
        """Test string with multiple spaces to trigger char4 logic."""
        if not self.derive_empresa_name: self.skipTest("Function not found")
        # Logic: split by " ". 5th part (index 4). slice [4:5].
        # "ONE TWO THREE FOUR FIVER" -> parts ["ONE", "TWO", "THREE", "FOUR", "FIVER"].
        # part4="FIVER". len 5. char4='R'.
        # char7 (split by .). Whole string "ONE TWO THREE FOUR FIVER". 5th char 'T' (index 4).
        # "O", "N", "E", " ", "T". Yes 'T'.
        # Result "RT" (order: char4 + ... + char7? No char1+char2+char3+char4+char5+char6+char7)
        # char4='R'. char7='T'. Result "RT".
        self.assertEqual(self.derive_empresa_name("ONE TWO THREE FOUR FIVER"), "RT")

if __name__ == '__main__':
    unittest.main()
