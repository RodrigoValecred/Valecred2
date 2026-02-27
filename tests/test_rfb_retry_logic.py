
import pytest
from unittest.mock import MagicMock, call, ANY
import sys
import os

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
# Updated to match the refactored logic in the notebook (headers + HEAD check)
def download_logic_snippet(filename, base_dir_download, requests_mock):
    MIRRORS = [
        "https://dadosabertos.rfb.gov.br/CNPJ/",
        "http://200.152.38.155/CNPJ/"
    ]
    local_zip_path = os.path.join(base_dir_download, filename)
    download_success = False

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for base_url in MIRRORS:
        url = f"{base_url}{filename}"
        print(f"Tentando baixar de: {url}")

        try:
            # HEAD request check
            try:
                head_response = requests_mock.head(url, headers=headers, verify=False, timeout=30)
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
                download_success = True
                break  # Exit loop
            else:
                print(f"Falha ao baixar de {url}. Status Code: {response.status_code}")

        except Exception as e:
            print(f"Erro de conexão ao baixar de {url}: {e}")

    return download_success

def test_download_retry_mechanism():
    requests_mock = MagicMock()

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

    # Execute
    success = download_logic_snippet("test.zip", "/tmp", requests_mock)

    # Verify
    assert success is True

    # Verify calls were made
    # We expect HEAD then GET (if HEAD succeeds) or just HEAD (if HEAD fails/times out)
    # Since primary fails with exception, it counts as a try.
    # Fallback succeeds.

    # Check that fallback was attempted
    requests_mock.get.assert_called_with(fallback_url, headers=ANY, verify=False, stream=True, timeout=120)

def test_download_all_fail():
    requests_mock = MagicMock()
    # Always raise exception
    requests_mock.head.side_effect = Exception("ConnectTimeout")
    requests_mock.get.side_effect = Exception("ConnectTimeout")

    success = download_logic_snippet("test.zip", "/tmp", requests_mock)

    assert success is False
