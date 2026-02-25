# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Notebook de API de Consulta CERC
# 
# **Objetivo:** Este notebook implementa uma API para consultar duplicatas na CERC.
# 
# **Passos:**
# 1. Configurar a API com FastAPI.
# 2. Definir o endpoint de consulta.
# 3. Implementar a lógica de consulta CERC.
# 4. Adicionar testes para a API.

# CELL ********************

import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install("fastapi")
install("uvicorn")
install("requests")
install("httpx")
install("typing_extensions>=4.10.0")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import fastapi
from fastapi import HTTPException, Depends, Security, status
from fastapi.security import APIKeyHeader
from fastapi.testclient import TestClient
import requests
import json
import os
import secrets

# Configuração de Segurança
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key: str = Security(api_key_header)):
    # Obtém a chave da variável de ambiente a cada requisição
    expected_api_key = os.environ.get("CERC_API_KEY")

    # Se a chave não estiver configurada no ambiente, falha de forma segura (500 Internal Server Error)
    if not expected_api_key:
        print("CRITICAL: CERC_API_KEY environment variable not set.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server security configuration error"
        )

    if api_key and secrets.compare_digest(api_key, expected_api_key):
        return api_key

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API Key",
    )

def create_app():
    """Defines the FastAPI application."""
    app = fastapi.FastAPI()

    @app.get("/consulta_cerc")
    async def consulta_cerc(cpf_cnpj: str, api_key: str = Depends(get_api_key)):
        """
        Consulta a CERC para verificar a existência de duplicatas para um CPF/CNPJ.
        """
        # Validação básica de segurança (Input Validation)
        # 1. Deve conter apenas números
        if not cpf_cnpj.isdigit():
            raise HTTPException(status_code=400, detail="CPF/CNPJ deve conter apenas números.")

        # 2. Deve ter tamanho válido (11 para CPF, 14 para CNPJ)
        if len(cpf_cnpj) not in [11, 14]:
            raise HTTPException(status_code=400, detail="CPF/CNPJ deve ter 11 ou 14 dígitos.")

        # Simulação da consulta CERC
        if cpf_cnpj == "14630809000101":
            return {"cpf_cnpj": cpf_cnpj, "duplicatas": "nenhuma duplicata encontrada"}
        else:
            return {"cpf_cnpj": cpf_cnpj, "duplicatas": "duplicatas encontradas"}
            
    return app

if __name__ == '__main__':
    # Configuração de ambiente isolado para testes
    # SENTINEL-FIX: Removido fallback inseguro global. Agora usamos uma chave dinâmica apenas no escopo do teste.
    print("Inicializando testes com chave de segurança dinâmica...")
    
    # Gera uma chave aleatória para o teste
    test_secret_key = secrets.token_urlsafe(32)

    # Salva o estado original do ambiente
    original_api_key = os.environ.get("CERC_API_KEY")
    
    # Define a chave de teste no ambiente (necessário pois get_api_key lê os.environ)
    os.environ["CERC_API_KEY"] = test_secret_key

    try:
        print("Inicializando a aplicação e o cliente de teste...")
        app = create_app()
        client = TestClient(app)

        # Header de autenticação para os testes usando a chave dinâmica
        auth_headers = {API_KEY_NAME: test_secret_key}

        print("Executando testes com TestClient (sem servidor HTTP exposto)...")

        print("Executando Cenário 1...")
        response = client.get("/consulta_cerc?cpf_cnpj=14630809000101", headers=auth_headers)
        response.raise_for_status()
        data = response.json()
        print(f"Cenário 1: {data}")
        assert data["duplicatas"] == "nenhuma duplicata encontrada"

        print("\nExecutando Cenário 2...")
        response = client.get("/consulta_cerc?cpf_cnpj=12345678901234", headers=auth_headers)
        response.raise_for_status()
        data = response.json()
        print(f"Cenário 2: {data}")
        assert data["duplicatas"] == "duplicatas encontradas"

        print("\nExecutando Cenário 3 (Validação de Segurança - Input)...")
        # Envia input inválido (não numérico)
        response = client.get("/consulta_cerc?cpf_cnpj=invalid_input", headers=auth_headers)
        print(f"Cenário 3 (Input Inválido): Status Code {response.status_code}")
        assert response.status_code == 400

        # Envia input inválido (tamanho errado)
        response = client.get("/consulta_cerc?cpf_cnpj=123", headers=auth_headers)
        print(f"Cenário 3 (Tamanho Errado): Status Code {response.status_code}")
        assert response.status_code == 400

        print("\nExecutando Cenário 4 (Validação de Segurança - Autenticação)...")
        # Sem header
        response = client.get("/consulta_cerc?cpf_cnpj=14630809000101")
        print(f"Cenário 4 (Sem Header): Status Code {response.status_code}")
        assert response.status_code == 401

        # Header inválido
        response = client.get("/consulta_cerc?cpf_cnpj=14630809000101", headers={API_KEY_NAME: "wrong-key"})
        print(f"Cenário 4 (Header Inválido): Status Code {response.status_code}")
        assert response.status_code == 401

        print("\nTestes concluídos com sucesso!")

    except Exception as e:
        print(f"\nErro ao executar os testes: {e}")

    finally:
        # Restaura o estado original do ambiente
        if original_api_key is not None:
            os.environ["CERC_API_KEY"] = original_api_key
        else:
            os.environ.pop("CERC_API_KEY", None)
        print("Ambiente de teste limpo (variável de ambiente restaurada).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
