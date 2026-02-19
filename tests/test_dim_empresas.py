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

    def test_basic_inputs(self):
        """Test basic inputs: None, Empty, Non-string."""
        if not self.derive_empresa_name: self.skipTest("Function not found")
        self.assertIsNone(self.derive_empresa_name(None))
        self.assertIsNone(self.derive_empresa_name(""))
        self.assertIsNone(self.derive_empresa_name(123))

    def test_char1_securitizadora(self):
        """Test logic 1: Split by ' SECURITIZADORA '."""
        # Need space after SECURITIZADORA for exact match
        inp = "PRE SECURITIZADORA "
        # char1='P'.
        # char7 (index 4): "PRE S...". Index 4 is 'S'.
        # Result: "PS"
        res = self.derive_empresa_name(inp)
        self.assertEqual(res, "PS")

    def test_char2_space_reverse(self):
        """Test logic 2: Split by ' ', reverse, index 3."""
        # "1 2 3 4 TARGET 6 7 8"
        target = "123456789T" # 10 chars
        inp = f"1 2 3 4 {target} 6 7 8"
        # char2='T'.
        # char7='3' (index 4 of "1 2 3 4..."). Indices: 0(1) 1(sp) 2(2) 3(sp) 4(3).
        # char4 (index 4 of split): "TARGET". len 10. char4 (index 4)='5'.
        # char6 (index 5 of split): "6". len 1.
        res = self.derive_empresa_name(inp)
        self.assertEqual(res, "T53")

    def test_char3_valecred(self):
        """Test logic 3: Split by 'VALECRED '."""
        # "VALECRED TARGET"
        res = self.derive_empresa_name("VALECRED TARGET")
        # char3="T".
        # char7="R" (index 4 of "VALECRED TARGET"). "VALEC..." C=4, R=5.
        # "V A L E C". Index 4 is 'C'.
        self.assertEqual(res, "TC")

    def test_char4_space_forward(self):
        """Test logic 4: Split by ' ', index 4."""
        # "0 1 2 3 TARGET"
        target = "0123R"
        inp = f"0 1 2 3 {target}"
        res = self.derive_empresa_name(inp)
        # char4="R".
        # char7="2" (index 4 of whole string "0 1 2...").
        self.assertEqual(res, "R2")

    def test_char5_reverse_index(self):
        """Test logic 5: Reverse string, index 32."""
        # rev_nome[32] is 33rd char of reversed.
        # So 1st char of original (index 0).
        # "E" + 32 dots.
        inp = "E" + "." * 32
        res = self.derive_empresa_name(inp)
        # char5="E".
        # char7 (split .). "E................................".
        # split(".") -> ["E", "", "", ..., ""].
        # part5="". char7="".
        self.assertEqual(res, "E")

    def test_char6_space_forward_complex(self):
        """Test logic 6: Split by ' ', index 5, reverse logic."""
        # "0 1 2 3 4 TARGET"
        target = "D1234567"
        inp = f"0 1 2 3 4 {target}"
        res = self.derive_empresa_name(inp)
        # char6="D".
        # char4="4" (from part index 4 "4"? No, "4" is len 1. index 4 of "4" is invalid).
        # part4="4". len=1. char4="".
        # char7='2' (index 4 of "0 1 2...").
        self.assertEqual(res, "D2")

    def test_char7_dot_split(self):
        """Test logic 7: Split by '.', last part, index 4."""
        target = "0123T"
        inp = f"PREFIX.{target}"
        res = self.derive_empresa_name(inp)
        # char7="T".
        # char4 (space split). "PREFIX.0123T". 1 part. char4="".
        self.assertEqual(res, "T")

    def test_itcredo_replacement(self):
        """
        Test the specific replacement rule: "ITCREDO" -> "TATUHY".
        Constructed string: "I   O SECURITIZADORA D123E567 123456789T filler VALECRED C"
        """
        inp = "I   O SECURITIZADORA D123E567 123456789T filler VALECRED C"
        res = self.derive_empresa_name(inp)
        self.assertEqual(res, "TATUHY", f"Failed to reproduce ITCREDO replacement. Got '{res}'")

if __name__ == '__main__':
    unittest.main()
