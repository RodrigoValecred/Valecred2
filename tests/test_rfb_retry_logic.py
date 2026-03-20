
import pytest
from unittest.mock import MagicMock, call, ANY, patch, mock_open
import sys
import os
import zipfile

# Simulação dos módulos pyspark
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

# Redefine a lógica aqui porque importar do conteúdo do notebook é complicado
# Atualizado para corresponder à lógica refatorada no notebook (cabeçalhos + verificação HEAD + novos espelhos + verificação zip)
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
            # Verificação de requisição HEAD
            try:
                head_response = requests_mock.head(url, headers=headers, verify=True, timeout=30, allow_redirects=True)
                if head_response.status_code != 200:
                    print(f"Arquivo não encontrado ou erro no servidor (HEAD): {url} - Status: {head_response.status_code}")
                    continue
            except Exception as e:
                print(f"Erro no HEAD request para {url}: {e}. Tentando GET direto...")

            # GET request
            response = requests_mock.get(url, headers=headers, verify=True, stream=True, timeout=120)

            if response.status_code == 200:
                # No código real nós escrevemos para o arquivo aqui
                print(f"Download concluído com sucesso: {local_zip_path}")

                # Simula a lógica de extração
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

    # Configura side_effect para HEAD e GET:
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

    # Simula HEAD e GET para usar o side_effect ou comportar-se adequadamente
    requests_mock.head.side_effect = side_effect
    requests_mock.get.side_effect = side_effect

    # Simula zipfile para passar testzip
    mock_zip_instance = MagicMock()
    mock_zip_instance.testzip.return_value = None
    zipfile_mock.ZipFile.return_value.__enter__.return_value = mock_zip_instance


    # Executa
    success = download_logic_snippet("test.zip", "/tmp", requests_mock, zipfile_mock)

    # Verifica
    assert success is True

    # Verifica se chamadas foram feitas
    # Verifica se a contingência (fallback) foi tentada
    requests_mock.get.assert_called_with(fallback_url, headers=ANY, verify=True, stream=True, timeout=120)

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

    # 1. Primário e Alternativa falham/tempo limite
    # 2. GitHub tem sucesso com redirecionamento permitido

    def head_side_effect(url, **kwargs):
        # Verifica se allow_redirects=True está presente
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

    # Simula zipfile para passar testzip
    mock_zip_instance = MagicMock()
    mock_zip_instance.testzip.return_value = None
    zipfile_mock.ZipFile.return_value.__enter__.return_value = mock_zip_instance

    success = download_logic_snippet("test.zip", "/tmp", requests_mock, zipfile_mock)

    assert success is True

    # Verifica se o espelho do GitHub foi tentado com os parâmetros corretos
    requests_mock.head.assert_any_call(github_url, headers=ANY, verify=True, timeout=30, allow_redirects=True)
    requests_mock.get.assert_called_with(github_url, headers=ANY, verify=True, stream=True, timeout=120)

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

    # Define o comportamento do ZipFile dependendo de qual URL foi chamado por último ou de algum estado?
    # Já que download_logic_snippet reabre o arquivo do disco, não podemos distinguir facilmente
    # com base no conteúdo do arquivo a menos que simulemos open.
    # Em vez disso, usaremos side_effect no ZipFile para levantar BadZipFile na primeira vez e então ter sucesso na segunda vez.

    mock_corrupt_zip = MagicMock()
    # testzip returns file name if corrupt, None if valid.
    # Mas frequentemente BadZipFile é levantado durante __init__ ou open.
    # Vamos verificar nossa lógica: ela chama ZipFile(path, 'r').
    # Se funcionar, chama testzip().

    # Cenário: 1ª chamada (Primário) -> ZipFile abre mas testzip falha
    # Cenário: 2ª chamada (Alternativa) -> ZipFile abre e testzip passa

    mock_zip_instance_corrupt = MagicMock()
    mock_zip_instance_corrupt.testzip.return_value = "corrupt_file.txt" # retorna o nome do primeiro arquivo ruim

    mock_zip_instance_valid = MagicMock()
    mock_zip_instance_valid.testzip.return_value = None # None means no bad files

    # Precisamos que o gerenciador de contexto ZipFile retorne essas instâncias sequencialmente
    # zipfile_mock.ZipFile.return_value.__enter__.side_effect = [mock_zip_instance_corrupt, mock_zip_instance_valid]

    # Wait, if testzip returns a string, our logic raises BadZipFile manually.
    # Lógica: se zip_ref.testzip() não for None: levanta zipfile.BadZipFile

    zipfile_mock.ZipFile.return_value.__enter__.side_effect = [
        mock_zip_instance_corrupt,
        mock_zip_instance_valid
    ]

    success = download_logic_snippet("test.zip", "/tmp", requests_mock, zipfile_mock)

    assert success is True

    # Verifica se tentamos baixar duas vezes (Primário e Alternativa)
    assert requests_mock.get.call_count == 2
    # Verifica se o ZipFile foi aberto duas vezes
    assert zipfile_mock.ZipFile.call_count == 2
