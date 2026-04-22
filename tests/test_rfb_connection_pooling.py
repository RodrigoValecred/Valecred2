
import pytest
from unittest.mock import MagicMock, ANY, patch
import sys
import os

# Simulação dos módulos pyspark e notebookutils
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()
sys.modules['pyspark.sql.types'] = MagicMock()
sys.modules['notebookutils'] = MagicMock()

from tests.notebook_utils import extract_function_from_file

def test_download_and_extract_uses_session():
    # Extrair a função do notebook
    notebook_path = "VALECRED_DEV/5_Notebooks/Dados_Externos/Receita Federal/NB_Load_Bronze_Receita_Federal_Full.Notebook/notebook-content.py"
    func_code = extract_function_from_file(notebook_path, "download_and_extract")

    # Criar um escopo global para o exec
    global_scope = {
        'os': os,
        'requests': MagicMock(),
        'zipfile': MagicMock(),
        'MIRRORS': ["https://mirror1.com/"],
        'print': print,
        'safe_extract': MagicMock() # Mocka safe_extract que é chamado dentro da função
    }

    # Executar o código da função para definí-la no escopo
    exec(func_code, global_scope)
    download_and_extract = global_scope['download_and_extract']

    # Mock da sessão
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.iter_content.return_value = [b"chunk"]
    mock_session.head.return_value = mock_response
    mock_session.get.return_value = mock_response

    # Mock do zipfile para evitar extração real
    with patch('zipfile.ZipFile'):
        # Precisamos mockar o 'open' também
        with patch('builtins.open', MagicMock()):
            result = download_and_extract("test.zip", "/tmp", "/tmp/ext", session=mock_session)

    # Verificar se a sessão foi usada em vez de requests
    mock_session.head.assert_called()
    mock_session.get.assert_called()
    global_scope['requests'].head.assert_not_called()
    global_scope['requests'].get.assert_not_called()

    assert result is True

def test_download_and_extract_fallback_no_session():
    # Extrair a função do notebook
    notebook_path = "VALECRED_DEV/5_Notebooks/Dados_Externos/Receita Federal/NB_Load_Bronze_Receita_Federal_Full.Notebook/notebook-content.py"
    func_code = extract_function_from_file(notebook_path, "download_and_extract")

    # Mock requests
    mock_requests = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.iter_content.return_value = [b"chunk"]
    mock_requests.head.return_value = mock_response
    mock_requests.get.return_value = mock_response

    global_scope = {
        'os': os,
        'requests': mock_requests,
        'zipfile': MagicMock(),
        'MIRRORS': ["https://mirror1.com/"],
        'print': print,
        'safe_extract': MagicMock()
    }

    exec(func_code, global_scope)
    download_and_extract = global_scope['download_and_extract']

    with patch('zipfile.ZipFile'):
        with patch('builtins.open', MagicMock()):
            result = download_and_extract("test.zip", "/tmp", "/tmp/ext", session=None)

    # Verificar se requests foi usado diretamente
    mock_requests.head.assert_called()
    mock_requests.get.assert_called()

    assert result is True
