
import pytest
from unittest.mock import MagicMock, call, ANY, patch, mock_open
import sys
import os
import zipfile

# Mock pyspark modules
class MockSparkSession:
    def read(self): return self
    def table(self, name): return MagicMock()
    def format(self, fmt): return self
    def option(self, key, value): return self
    def schema(self, schema): return self
    def load(self, path): return MagicMock()
    def createDataFrame(self, data, schema=None): return MagicMock()

sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()
sys.modules['pyspark.sql.types'] = MagicMock()
sys.modules['notebookutils'] = MagicMock()
# Mock requests since it's missing in the test environment
sys.modules['requests'] = MagicMock()
import requests

# Redefine logic here because importing from notebook content is tricky
# Updated to match the refactored logic in the notebook (headers + HEAD check + new mirrors + zip check)
def download_logic_snippet(filename, base_dir_download, requests_mock, zipfile_mock=None):
    MIRRORS = [
        "https://dadosabertos.rfb.gov.br/CNPJ/",
        "http://200.152.38.155/CNPJ/",
        "https://github.com/jonathands/dados-abertos-receita-cnpj/releases/download/2024.09/"
    ]
    local_zip_path = os.path.join(base_dir_download, filename)
    success = False

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for base_url in MIRRORS:
        url = f"{base_url}{filename}"
        print(f"Tentando baixar de: {url}")

        try:
            # HEAD request check
            try:
                head_response = requests_mock.head(url, headers=headers, verify=False, timeout=30, allow_redirects=True)
                if head_response.status_code != 200:
                    print(f"Arquivo não encontrado ou erro no servidor (HEAD): {url} - Status: {head_response.status_code}")
                    continue
            except Exception as e:
                print(f"Erro no HEAD request para {url}: {e}. Tentando GET direto...")

            # GET request
            response = requests_mock.get(url, headers=headers, verify=False, stream=True, timeout=120)

            if response.status_code == 200:
                # In real code we write to file here
                print(f"Download concluído com sucesso: {local_zip_path}")

                # Mock extraction logic
                try:
                    print(f"Verificando integridade e extraindo {filename}...")
                    if zipfile_mock:
                         with zipfile_mock.ZipFile(local_zip_path, 'r') as zip_ref:
                            if zip_ref.testzip() is not None:
                                raise zipfile.BadZipFile("Teste de integridade falhou (CRC check)")
                            # Assume safe_extract works

                    success = True
                    break  # Exit loop
                except zipfile.BadZipFile as e:
                    print(f"ERRO: Arquivo corrompido baixado de {url}. Erro: {e}")
                    continue
                except Exception as e:
                    print(f"ERRO: Falha na extração de {filename}. Erro: {e}")
                    continue
            else:
                print(f"Falha ao baixar de {url}. Status Code: {response.status_code}")

        except Exception as e:
            print(f"Erro de conexão ao baixar de {url}: {e}")

    return success

def test_download_retry_mechanism():
    requests_mock = MagicMock()
    zipfile_mock = MagicMock()

    # URL constants
    primary_url = "https://dadosabertos.rfb.gov.br/CNPJ/test.zip"
    fallback_url = "http://200.152.38.155/CNPJ/test.zip"

    # Setup side effect for HEAD and GET:
    # 1. Primary URL -> Raises Exception (Simulate Timeout)
    # 2. Fallback URL -> Returns 200 OK

    def side_effect(url, **kwargs):
        if url == primary_url:
            raise Exception("ConnectTimeout")
        elif url == fallback_url:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.iter_content = MagicMock(return_value=[b'chunk'])
            return mock_resp
        return MagicMock(status_code=404)

    # Mock both HEAD and GET to use the side effect or behave appropriately
    requests_mock.head.side_effect = side_effect
    requests_mock.get.side_effect = side_effect

    # Mock zipfile to pass testzip
    mock_zip_instance = MagicMock()
    mock_zip_instance.testzip.return_value = None
    zipfile_mock.ZipFile.return_value.__enter__.return_value = mock_zip_instance


    # Execute
    success = download_logic_snippet("test.zip", "/tmp", requests_mock, zipfile_mock)

    # Verify
    assert success is True

    # Verify calls were made
    # Check that fallback was attempted
    requests_mock.get.assert_called_with(fallback_url, headers=ANY, verify=False, stream=True, timeout=120)

def test_download_all_fail():
    requests_mock = MagicMock()
    zipfile_mock = MagicMock()

    # Always raise exception
    requests_mock.head.side_effect = Exception("ConnectTimeout")
    requests_mock.get.side_effect = Exception("ConnectTimeout")

    success = download_logic_snippet("test.zip", "/tmp", requests_mock, zipfile_mock)

    assert success is False

def test_github_mirror_redirect():
    requests_mock = MagicMock()
    zipfile_mock = MagicMock()

    primary_url = "https://dadosabertos.rfb.gov.br/CNPJ/test.zip"
    fallback_url = "http://200.152.38.155/CNPJ/test.zip"
    github_url = "https://github.com/jonathands/dados-abertos-receita-cnpj/releases/download/2024.09/test.zip"

    # 1. Primary and Fallback fail/timeout
    # 2. GitHub succeeds with redirect allowed

    def head_side_effect(url, **kwargs):
        # Verify allow_redirects=True is present
        assert kwargs.get('allow_redirects') is True

        if url == github_url:
            return MagicMock(status_code=200)
        else:
            raise Exception("ConnectTimeout")

    def get_side_effect(url, **kwargs):
        if url == github_url:
             mock_resp = MagicMock()
             mock_resp.status_code = 200
             mock_resp.iter_content = MagicMock(return_value=[b'chunk'])
             return mock_resp
        raise Exception("ConnectTimeout")

    requests_mock.head.side_effect = head_side_effect
    requests_mock.get.side_effect = get_side_effect

    # Mock zipfile to pass testzip
    mock_zip_instance = MagicMock()
    mock_zip_instance.testzip.return_value = None
    zipfile_mock.ZipFile.return_value.__enter__.return_value = mock_zip_instance

    success = download_logic_snippet("test.zip", "/tmp", requests_mock, zipfile_mock)

    assert success is True

    # Verify GitHub mirror was tried with correct params
    requests_mock.head.assert_any_call(github_url, headers=ANY, verify=False, timeout=30, allow_redirects=True)
    requests_mock.get.assert_called_with(github_url, headers=ANY, verify=False, stream=True, timeout=120)

def test_corrupt_zip_fallback():
    requests_mock = MagicMock()
    zipfile_mock = MagicMock()

    primary_url = "https://dadosabertos.rfb.gov.br/CNPJ/test.zip"
    fallback_url = "http://200.152.38.155/CNPJ/test.zip"

    # 1. Primary URL returns a file, BUT it's corrupt
    # 2. Fallback URL returns a valid file

    def head_side_effect(url, **kwargs):
        return MagicMock(status_code=200)

    def get_side_effect(url, **kwargs):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.iter_content = MagicMock(return_value=[b'chunk'])
        return mock_resp

    requests_mock.head.side_effect = head_side_effect
    requests_mock.get.side_effect = get_side_effect

    # Define ZipFile behavior depending on which URL was called last or some state?
    # Since download_logic_snippet re-opens the file from disk, we can't easily distinguish
    # based on the file content unless we mock open.
    # Instead, we'll use side_effect on ZipFile to raise BadZipFile the first time, then succeed the second time.

    mock_corrupt_zip = MagicMock()
    # testzip returns file name if corrupt, None if valid.
    # But often BadZipFile is raised during __init__ or open.
    # Let's verify our logic: it calls ZipFile(path, 'r').
    # If that works, it calls testzip().

    # Scenario: 1st call (Primary) -> ZipFile opens but testzip fails
    # Scenario: 2nd call (Fallback) -> ZipFile opens and testzip passes

    mock_zip_instance_corrupt = MagicMock()
    mock_zip_instance_corrupt.testzip.return_value = "corrupt_file.txt" # returns name of first bad file

    mock_zip_instance_valid = MagicMock()
    mock_zip_instance_valid.testzip.return_value = None # None means no bad files

    # We need ZipFile context manager to return these instances sequentially
    # zipfile_mock.ZipFile.return_value.__enter__.side_effect = [mock_zip_instance_corrupt, mock_zip_instance_valid]

    # Wait, if testzip returns a string, our logic raises BadZipFile manually.
    # Logic: if zip_ref.testzip() is not None: raise zipfile.BadZipFile

    zipfile_mock.ZipFile.return_value.__enter__.side_effect = [
        mock_zip_instance_corrupt,
        mock_zip_instance_valid
    ]

    success = download_logic_snippet("test.zip", "/tmp", requests_mock, zipfile_mock)

    assert success is True

    # Verify we tried to download twice (Primary and Fallback)
    assert requests_mock.get.call_count == 2
    # Verify ZipFile was opened twice
    assert zipfile_mock.ZipFile.call_count == 2
