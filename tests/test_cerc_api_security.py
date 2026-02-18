import unittest
import sys
import os
import fastapi
from fastapi import HTTPException
from fastapi.testclient import TestClient
import requests
import json

try:
    from tests.notebook_utils import extract_function_from_file
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from notebook_utils import extract_function_from_file

# Path to the notebook
NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Utilitarios/NB_CERC_Consulta_API.Notebook/notebook-content.py"

class TestCERCAPISecurity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting create_app from {NOTEBOOK_PATH}")
        func_source = extract_function_from_file(NOTEBOOK_PATH, "create_app")

        if func_source:
            local_scope = {}
            global_scope = {
                "fastapi": fastapi,
                "HTTPException": HTTPException,
                "TestClient": TestClient,
                "requests": requests,
                "json": json,
                "os": os
            }
            try:
                # Setup dummy API key for test environment
                os.environ["CERC_API_KEY"] = "test-secret-key"

                # The create_app function references fastapi.Header, etc.
                exec(func_source, global_scope, local_scope)
                cls.create_app = local_scope["create_app"]
                cls.client = TestClient(cls.create_app())
            except Exception as e:
                print(f"Error executing extracted function: {e}")
                cls.create_app = None
                cls.client = None
        else:
            cls.create_app = None
            cls.client = None
            print("WARNING: create_app function not found in file.")

    def test_app_exists(self):
        self.assertIsNotNone(self.create_app, "create_app function not found.")
        self.assertIsNotNone(self.client, "TestClient could not be initialized.")

    def test_missing_api_key(self):
        """Test that request without API key returns 401."""
        if not self.client: self.skipTest("Client not initialized")
        response = self.client.get("/consulta_cerc?cpf_cnpj=14630809000101")
        self.assertEqual(response.status_code, 401)
        self.assertIn("API Key ausente", response.json()["detail"])

    def test_invalid_api_key(self):
        """Test that request with invalid API key returns 401."""
        if not self.client: self.skipTest("Client not initialized")
        headers = {"X-API-KEY": "wrong-key"}
        response = self.client.get("/consulta_cerc?cpf_cnpj=14630809000101", headers=headers)
        self.assertEqual(response.status_code, 401)
        self.assertIn("API Key inválida", response.json()["detail"])

    def test_valid_api_key(self):
        """Test that request with valid API key returns 200."""
        if not self.client: self.skipTest("Client not initialized")
        headers = {"X-API-KEY": "test-secret-key"} # This matches the dummy key set in setUpClass
        response = self.client.get("/consulta_cerc?cpf_cnpj=14630809000101", headers=headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["cpf_cnpj"], "14630809000101")

if __name__ == '__main__':
    unittest.main()
