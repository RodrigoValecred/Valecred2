import unittest
import sys
import os
import zipfile
import shutil
import tempfile
from io import BytesIO
from unittest.mock import patch, MagicMock

# Ensure the tests directory is in the path to import notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define paths
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERPRO_NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/7_Dados_Externos/NB_Load_Bronze_From_SERPRO.Notebook/notebook-content.py"
)
CVM_NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/7_Dados_Externos/CVM/NB_Load_From_CVM.Notebook/notebook-content.py"
)
RFB_NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/7_Dados_Externos/Receita Federal/NB_Extract_Bronze_Receita_Federal_Full.Notebook/notebook-content.py"
)

class TestSafeExtract(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.malicious_zip_path = os.path.join(self.test_dir, "malicious.zip")
        self.safe_zip_path = os.path.join(self.test_dir, "safe.zip")
        self.output_dir = os.path.join(self.test_dir, "output")
        os.makedirs(self.output_dir)

        # Create malicious zip
        with zipfile.ZipFile(self.malicious_zip_path, 'w') as zf:
            zf.writestr('../evil.txt', 'evil content')
            zf.writestr('good.txt', 'good content')

        # Create safe zip
        with zipfile.ZipFile(self.safe_zip_path, 'w') as zf:
            zf.writestr('safe.txt', 'safe content')
            zf.writestr('subdir/nested.txt', 'nested content')

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_serpro_safe_extract(self):
        if not os.path.exists(SERPRO_NOTEBOOK_PATH):
            self.skipTest(f"File not found: {SERPRO_NOTEBOOK_PATH}")
        self._test_safe_extract(SERPRO_NOTEBOOK_PATH)

    def test_cvm_safe_extract(self):
        self._test_safe_extract(CVM_NOTEBOOK_PATH)

    def test_rfb_safe_extract_error_handling(self):
        self._test_safe_extract(RFB_NOTEBOOK_PATH)

        func_source = extract_function_from_file(RFB_NOTEBOOK_PATH, "safe_extract")
        if not func_source:
             self.fail(f"Function safe_extract not found in {RFB_NOTEBOOK_PATH}")

        # Compile the function
        exec_globals = {
            'os': os,
            'zipfile': zipfile,
            'Exception': Exception
        }
        exec(func_source, exec_globals)
        safe_extract = exec_globals['safe_extract']

        # Mock zip_ref to raise Exception on extractall
        mock_zip_ref = MagicMock()
        mock_zip_ref.namelist.return_value = ['file.txt']
        mock_zip_ref.filename = "test_error.zip"
        mock_zip_ref.extractall.side_effect = Exception("Mock extraction error")

        with patch('builtins.print') as mock_print:
            # Call safe_extract which should catch the exception and print it
            safe_extract(mock_zip_ref, self.output_dir)
            mock_print.assert_called_with("Erro ao extrair test_error.zip: Mock extraction error")

    def _test_safe_extract(self, notebook_path):
        print(f"Testing safe_extract from {notebook_path}")
        func_source = extract_function_from_file(notebook_path, "safe_extract")
        if not func_source:
             self.fail(f"Function safe_extract not found in {notebook_path}")

        # Compile the function
        exec_globals = {
            'os': os,
            'zipfile': zipfile,
            'Exception': Exception
        }
        exec(func_source, exec_globals)
        safe_extract = exec_globals['safe_extract']

        # 1. Test Malicious Zip
        with zipfile.ZipFile(self.malicious_zip_path, 'r') as zf:
            with self.assertRaises(Exception) as cm:
                safe_extract(zf, self.output_dir)
            self.assertIn("Zip Slip vulnerability detected", str(cm.exception))

        # Verify nothing was extracted (fail-fast / atomic check behavior)
        self.assertFalse(os.path.exists(os.path.join(self.output_dir, "good.txt")), "good.txt should not be extracted if validation fails")
        self.assertFalse(os.path.exists(os.path.join(self.output_dir, "evil.txt")), "evil.txt should not be extracted")
        self.assertFalse(os.path.exists(os.path.join(self.test_dir, "evil.txt")), "evil.txt should not be extracted outside")

        # 2. Test Safe Zip
        with zipfile.ZipFile(self.safe_zip_path, 'r') as zf:
            safe_extract(zf, self.output_dir)

        self.assertTrue(os.path.exists(os.path.join(self.output_dir, "safe.txt")))
        self.assertTrue(os.path.exists(os.path.join(self.output_dir, "subdir", "nested.txt")))

if __name__ == '__main__':
    unittest.main()
