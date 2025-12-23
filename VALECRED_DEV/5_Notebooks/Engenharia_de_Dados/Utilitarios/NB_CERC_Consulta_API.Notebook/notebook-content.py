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
install("typing_extensions>=4.10.0")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import fastapi
import uvicorn
import requests
import multiprocessing
import time
import json

def run_server():
    """Defines and runs the FastAPI application."""
    app = fastapi.FastAPI()

    @app.get("/consulta_cerc")
    async def consulta_cerc(cpf_cnpj: str):
        """
        Consulta a CERC para verificar a existência de duplicatas para um CPF/CNPJ.
        """
        # Simulação da consulta CERC
        if cpf_cnpj == "14630809000101":
            return {"cpf_cnpj": cpf_cnpj, "duplicatas": "nenhuma duplicata encontrada"}
        else:
            return {"cpf_cnpj": cpf_cnpj, "duplicatas": "duplicatas encontradas"}
            
    uvicorn.run(app, host="127.0.0.1", port=8001, log_level="info")

if __name__ == '__main__':
    print("Iniciando o servidor da API em um processo separado...")
    server_process = multiprocessing.Process(target=run_server)
    server_process.start()
    
    time.sleep(5) 
    
    print("Servidor iniciado. Executando testes...")
    
    api_url = "http://127.0.0.1:8001"
    
    try:
        print("Executando Cenário 1...")
        response = requests.get(f"{api_url}/consulta_cerc?cpf_cnpj=14630809000101")
        response.raise_for_status()
        data = response.json()
        print(f"Cenário 1: {data}")
        assert data["duplicatas"] == "nenhuma duplicata encontrada"

        print("\nExecutando Cenário 2...")
        response = requests.get(f"{api_url}/consulta_cerc?cpf_cnpj=12345678901234")
        response.raise_for_status()
        data = response.json()
        print(f"Cenário 2: {data}")
        assert data["duplicatas"] == "duplicatas encontradas"

        print("\nTestes concluídos com sucesso!")

    except requests.exceptions.RequestException as e:
        print(f"\nErro ao executar os testes: {e}")
    
    finally:
        print("Finalizando o servidor da API...")
        server_process.terminate()
        server_process.join()
        print("Servidor finalizado.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
