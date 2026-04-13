import unittest
import sys
import os
from unittest.mock import patch, MagicMock, mock_open, ANY
import zipfile
import shutil
import tempfile

# Garante que o diretório de testes esteja no path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from tests.notebook_utils import extract_function_from_file

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Dados_Externos/Receita Federal/NB_Load_Bronze_Receita_Federal_Full.Notebook/notebook-content.py"
)

class TestDownloadAndExtract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting download_and_extract from {NOTEBOOK_PATH}")
        cls.safe_extract_source = extract_function_from_file(NOTEBOOK_PATH, "safe_extract")
        func_source = extract_function_from_file(NOTEBOOK_PATH, "download_and_extract")
        if func_source and cls.safe_extract_source:
            cls.func_source = func_source
        else:
            cls.func_source = None

    def setUp(self):
        if not self.func_source:
            self.skipTest("download_and_extract function not found")

        self.test_dir = tempfile.mkdtemp()
        self.download_dir = os.path.join(self.test_dir, "download")
        self.extract_dir = os.path.join(self.test_dir, "extract")
        os.makedirs(self.download_dir)
        os.makedirs(self.extract_dir)

        # Cria um arquivo zip malicioso (fixture)
        self.malicious_zip_name = "malicious.zip"
        self.malicious_zip_path = os.path.join(self.download_dir, self.malicious_zip_name)
        with zipfile.ZipFile(self.malicious_zip_path, 'w') as zf:
            zf.writestr('../evil.txt', 'evil content')
            zf.writestr('good.txt', 'good content')

        # Cria um arquivo zip seguro (fixture)
        self.safe_zip_name = "safe.zip"
        self.safe_zip_path = os.path.join(self.download_dir, self.safe_zip_name)
        with zipfile.ZipFile(self.safe_zip_path, 'w') as zf:
            zf.writestr('safe.txt', 'safe content')

        self.mock_requests = MagicMock()
        self.mock_mirrors = ['http://mirror1/', 'http://mirror2/']

        self.exec_globals = {
            'os': os,
            'requests': self.mock_requests,
            'zipfile': zipfile,
            'MIRRORS': self.mock_mirrors,
            'print': MagicMock()
        }

        # Carrega safe_extract
        exec(self.safe_extract_source, self.exec_globals)
        # Carrega download_and_extract
        exec(self.func_source, self.exec_globals)

        self.download_and_extract = self.exec_globals['download_and_extract']

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_successful_download_first_mirror(self):
        # Configura os mocks (simulações) para fingir que o zip seguro foi baixado
        def get_side_effect(*args, **kwargs):
            mock_resp = MagicMock(status_code=200)
            with open(self.safe_zip_path, 'rb') as f:
                mock_resp.iter_content.return_value = [f.read()]
            return mock_resp

        self.mock_requests.head.return_value.status_code = 200
        self.mock_requests.get.side_effect = get_side_effect

        result = self.download_and_extract(self.safe_zip_name, self.download_dir, self.extract_dir)

        self.assertTrue(result)
        self.mock_requests.head.assert_called_once_with(f'http://mirror1/{self.safe_zip_name}', headers=ANY, verify=True, timeout=30, allow_redirects=True)
        self.assertEqual(self.mock_requests.get.call_count, 1)

        # Verifica se a extração funcionou
        self.assertTrue(os.path.exists(os.path.join(self.extract_dir, "safe.txt")))

    def test_fallback_to_second_mirror(self):
        # O primeiro mirror falha na requisição head, o segundo funciona
        head_response1 = MagicMock(status_code=404)
        head_response2 = MagicMock(status_code=200)
        self.mock_requests.head.side_effect = [head_response1, head_response2]

        def get_side_effect(*args, **kwargs):
            mock_resp = MagicMock(status_code=200)
            with open(self.safe_zip_path, 'rb') as f:
                mock_resp.iter_content.return_value = [f.read()]
            return mock_resp

        self.mock_requests.get.side_effect = get_side_effect

        result = self.download_and_extract(self.safe_zip_name, self.download_dir, self.extract_dir)

        self.assertTrue(result)
        self.assertEqual(self.mock_requests.head.call_count, 2)
        self.mock_requests.get.assert_called_once_with(f'http://mirror2/{self.safe_zip_name}', headers=ANY, verify=True, stream=True, timeout=120)

    def test_all_mirrors_fail(self):
        self.mock_requests.head.return_value.status_code = 404

        result = self.download_and_extract("test.zip", self.download_dir, self.extract_dir)

        self.assertFalse(result)
        self.assertEqual(self.mock_requests.head.call_count, 2)
        self.mock_requests.get.assert_not_called()

    def test_zip_slip_vulnerability(self):
        # Retorna o conteúdo do arquivo zip malicioso
        def get_side_effect(*args, **kwargs):
            mock_resp = MagicMock(status_code=200)
            with open(self.malicious_zip_path, 'rb') as f:
                mock_resp.iter_content.return_value = [f.read()]
            return mock_resp

        self.mock_requests.head.return_value.status_code = 200
        self.mock_requests.get.side_effect = get_side_effect

        # Nota: `safe_extract` levanta Exception("Zip Slip vulnerability detected").
        # A função `download_and_extract` captura e levanta um RuntimeError
        # com "Security Check Failed: Extraction stopped due to path traversal violation."
        with self.assertRaisesRegex(RuntimeError, "Security Check Failed: Extraction stopped due to path traversal violation."):
            self.download_and_extract(self.malicious_zip_name, self.download_dir, self.extract_dir)

        # Verifica se o arquivo malicioso NÃO foi extraído fora do diretório
        self.assertFalse(os.path.exists(os.path.join(self.download_dir, "evil.txt")))
        self.assertFalse(os.path.exists(os.path.join(self.test_dir, "evil.txt")))

    def test_bad_zip_file(self):
        # Cria um arquivo zip genuinamente corrompido
        bad_zip_name = "bad.zip"
        bad_zip_path = os.path.join(self.download_dir, bad_zip_name)
        with open(bad_zip_path, 'w') as f:
            f.write("This is not a zip file.")

        def get_side_effect(*args, **kwargs):
            mock_resp = MagicMock(status_code=200)
            with open(bad_zip_path, 'rb') as f:
                mock_resp.iter_content.return_value = [f.read()]
            return mock_resp

        self.mock_requests.head.return_value.status_code = 200
        self.mock_requests.get.side_effect = get_side_effect

        result = self.download_and_extract(bad_zip_name, self.download_dir, self.extract_dir)
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()
