import unittest
import sys
import os
import zipfile
import shutil
import tempfile
from io import BytesIO
from unittest.mock import patch, MagicMock

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Definir caminhos
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERPRO_NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Dados_Externos/NB_Load_Bronze_From_SERPRO.Notebook/notebook-content.py"
)
CVM_NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Dados_Externos/CVM/NB_Load_From_CVM.Notebook/notebook-content.py"
)
RFB_NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Dados_Externos/Receita Federal/NB_Extract_Bronze_Receita_Federal_Full.Notebook/notebook-content.py"
)

class TestSafeExtract(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.malicious_zip_path = os.path.join(self.test_dir, "malicious.zip")
        self.safe_zip_path = os.path.join(self.test_dir, "safe.zip")
        self.output_dir = os.path.join(self.test_dir, "output")
        os.makedirs(self.output_dir)

        # Cria um arquivo zip malicioso
        with zipfile.ZipFile(self.malicious_zip_path, 'w') as zf:
            zf.writestr('../evil.txt', 'evil content')
            zf.writestr('/evil_absolute.txt', 'evil content')
            zf.writestr('subdir/../../evil_nested.txt', 'evil content')
            zf.writestr('good.txt', 'good content')

        # Cria um arquivo zip seguro
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

    def test_rfb_safe_extract(self):
        self._test_safe_extract(RFB_NOTEBOOK_PATH)

    def test_rfb_safe_extract_error_handling(self):
        func_source = extract_function_from_file(RFB_NOTEBOOK_PATH, "safe_extract")
        if not func_source:
             self.fail(f"Function safe_extract not found in {RFB_NOTEBOOK_PATH}")

        # Compila a função
        exec_globals = {
            'os': os,
            'zipfile': zipfile,
            'Exception': Exception
        }
        exec(func_source, exec_globals)
        safe_extract = exec_globals['safe_extract']

        # Simula zip_ref para lançar Exception em extractall
        mock_zip_ref = MagicMock()
        mock_zip_ref.namelist.return_value = ['file.txt']
        mock_zip_ref.filename = "test_error.zip"
        mock_zip_ref.extractall.side_effect = Exception("Simulação da extração error")

        # Chama safe_extract que deve subir a exceção
        with self.assertRaisesRegex(Exception, "Simulação da extração error"):
            safe_extract(mock_zip_ref, self.output_dir)

    def _test_safe_extract(self, notebook_path):
        print(f"Testing safe_extract from {notebook_path}")
        func_source = extract_function_from_file(notebook_path, "safe_extract")
        if not func_source:
             self.fail(f"Function safe_extract not found in {notebook_path}")

        # Compila a função
        exec_globals = {
            'os': os,
            'zipfile': zipfile,
            'Exception': Exception
        }
        exec(func_source, exec_globals)
        safe_extract = exec_globals['safe_extract']

        # 1. Testa Zip Malicioso
        with zipfile.ZipFile(self.malicious_zip_path, 'r') as zf:
            with self.assertRaises(Exception) as cm:
                safe_extract(zf, self.output_dir)
            self.assertIn("Zip Slip vulnerability detected", str(cm.exception))

        # Verifica se nada foi extraído (comportamento de verificação atômica / fail-fast)
        self.assertFalse(os.path.exists(os.path.join(self.output_dir, "good.txt")), "good.txt should not be extracted if validation fails")
        self.assertFalse(os.path.exists(os.path.join(self.output_dir, "evil.txt")), "evil.txt should not be extracted")
        self.assertFalse(os.path.exists(os.path.join(self.test_dir, "evil.txt")), "evil.txt should not be extracted outside")
        self.assertFalse(os.path.exists(os.path.join(self.test_dir, "evil_absolute.txt")), "evil_absolute.txt should not be extracted outside")
        self.assertFalse(os.path.exists(os.path.join(self.test_dir, "evil_nested.txt")), "evil_nested.txt should not be extracted outside")

        # 2. Testa Zip Seguro
        with zipfile.ZipFile(self.safe_zip_path, 'r') as zf:
            safe_extract(zf, self.output_dir)

        self.assertTrue(os.path.exists(os.path.join(self.output_dir, "safe.txt")))
        self.assertTrue(os.path.exists(os.path.join(self.output_dir, "subdir", "nested.txt")))

if __name__ == '__main__':
    unittest.main()
