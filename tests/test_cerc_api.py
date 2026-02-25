import sys
import os
import unittest
from unittest.mock import MagicMock, patch
import fastapi
from fastapi.testclient import TestClient
from fastapi import HTTPException, Depends, Security, status
from fastapi.security import APIKeyHeader
import secrets

# Add the root directory to sys.path to import tests.notebook_utils if run from root
# or handle relative import if run from tests/
if os.path.basename(os.getcwd()) == 'tests':
    sys.path.append(os.path.join(os.getcwd(), '..'))
else:
    sys.path.append(os.getcwd())

from tests.notebook_utils import extract_function_from_file

# Path to the notebook file
NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Utilitarios/NB_CERC_Consulta_API.Notebook/notebook-content.py"

class TestCercAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Extract the functions source code
        get_api_key_source = extract_function_from_file(NOTEBOOK_PATH, "get_api_key")
        create_app_source = extract_function_from_file(NOTEBOOK_PATH, "create_app")

        # Create a namespace for execution
        cls.namespace = {
            "fastapi": fastapi,
            "HTTPException": HTTPException,
            "Depends": Depends,
            "Security": Security,
            "status": status,
            "APIKeyHeader": APIKeyHeader,
            "TestClient": TestClient,
            "os": os,
            "secrets": secrets,
            "API_KEY_NAME": "X-API-Key",
            "api_key_header": APIKeyHeader(name="X-API-Key", auto_error=False),
        }

        # Execute the extracted code
        exec(get_api_key_source, cls.namespace)
        exec(create_app_source, cls.namespace)

        # Make functions available as class attributes
        cls.get_api_key = staticmethod(cls.namespace["get_api_key"])
        cls.create_app = staticmethod(cls.namespace["create_app"])

    def setUp(self):
        # Set up environment variable for testing
        self.test_api_key = "test-api-key-123"
        os.environ["CERC_API_KEY"] = self.test_api_key

        # Create app and client
        self.app = self.create_app()
        self.client = TestClient(self.app)
        self.auth_headers = {"X-API-Key": self.test_api_key}

    def tearDown(self):
        if "CERC_API_KEY" in os.environ:
            del os.environ["CERC_API_KEY"]

    def test_consulta_cerc_success_no_duplicates(self):
        response = self.client.get("/consulta_cerc?cpf_cnpj=14630809000101", headers=self.auth_headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"cpf_cnpj": "14630809000101", "duplicatas": "nenhuma duplicata encontrada"})

    def test_consulta_cerc_success_duplicates_found(self):
        response = self.client.get("/consulta_cerc?cpf_cnpj=12345678901234", headers=self.auth_headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"cpf_cnpj": "12345678901234", "duplicatas": "duplicatas encontradas"})

    def test_consulta_cerc_invalid_input_not_digits(self):
        response = self.client.get("/consulta_cerc?cpf_cnpj=invalid", headers=self.auth_headers)
        self.assertEqual(response.status_code, 400)
        self.assertIn("apenas números", response.json()["detail"])

    def test_consulta_cerc_invalid_input_length(self):
        response = self.client.get("/consulta_cerc?cpf_cnpj=123", headers=self.auth_headers)
        self.assertEqual(response.status_code, 400)
        self.assertIn("11 ou 14 dígitos", response.json()["detail"])

    def test_consulta_cerc_unauthorized_no_header(self):
        response = self.client.get("/consulta_cerc?cpf_cnpj=14630809000101")
        self.assertEqual(response.status_code, 401)
        self.assertIn("Invalid or missing API Key", response.json()["detail"])

    def test_consulta_cerc_unauthorized_invalid_key(self):
        response = self.client.get("/consulta_cerc?cpf_cnpj=14630809000101", headers={"X-API-Key": "wrong-key"})
        self.assertEqual(response.status_code, 401)
        self.assertIn("Invalid or missing API Key", response.json()["detail"])

    def test_consulta_cerc_server_error_missing_env_var(self):
        if "CERC_API_KEY" in os.environ:
            del os.environ["CERC_API_KEY"]

        response = self.client.get("/consulta_cerc?cpf_cnpj=14630809000101", headers=self.auth_headers)
        self.assertEqual(response.status_code, 500)
        self.assertIn("Server security configuration error", response.json()["detail"])

if __name__ == "__main__":
    unittest.main()
