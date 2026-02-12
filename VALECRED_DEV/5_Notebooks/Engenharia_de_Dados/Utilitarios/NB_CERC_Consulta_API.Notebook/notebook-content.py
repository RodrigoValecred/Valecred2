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
from fastapi import HTTPException
from fastapi.testclient import TestClient
import requests
import json

def create_app():
    """Defines the FastAPI application."""
    app = fastapi.FastAPI()

    @app.get("/consulta_cerc")
    async def consulta_cerc(cpf_cnpj: str):
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
    print("Inicializando a aplicação e o cliente de teste...")
    app = create_app()
    client = TestClient(app)
    
    print("Executando testes com TestClient (sem servidor HTTP exposto)...")
    
    try:
        print("Executando Cenário 1...")
        response = client.get("/consulta_cerc?cpf_cnpj=14630809000101")
        response.raise_for_status()
        data = response.json()
        print(f"Cenário 1: {data}")
        assert data["duplicatas"] == "nenhuma duplicata encontrada"

        print("\nExecutando Cenário 2...")
        response = client.get("/consulta_cerc?cpf_cnpj=12345678901234")
        response.raise_for_status()
        data = response.json()
        print(f"Cenário 2: {data}")
        assert data["duplicatas"] == "duplicatas encontradas"

        print("\nExecutando Cenário 3 (Validação de Segurança)...")
        # Envia input inválido (não numérico)
        response = client.get("/consulta_cerc?cpf_cnpj=invalid_input")
        print(f"Cenário 3 (Input Inválido): Status Code {response.status_code}")
        assert response.status_code == 400

        # Envia input inválido (tamanho errado)
        response = client.get("/consulta_cerc?cpf_cnpj=123")
        print(f"Cenário 3 (Tamanho Errado): Status Code {response.status_code}")
        assert response.status_code == 400

        print("\nTestes concluídos com sucesso!")

    except Exception as e:
        print(f"\nErro ao executar os testes: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
