# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "553c2931-573b-4db0-838d-a70a01306d32",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "553c2931-573b-4db0-838d-a70a01306d32"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "00efd04d-54d0-417d-bcb5-a05d42633f12",
# META       "known_warehouses": [
# META         {
# META           "id": "00efd04d-54d0-417d-bcb5-a05d42633f12",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
from notebookutils import mssparkutils
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Autenticação
token = mssparkutils.credentials.getToken("pbi")
headers = {'Authorization': f'Bearer {token}'}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Obter lista de Workspaces
res_groups = requests.get("https://api.powerbi.com/v1.0/myorg/groups", headers=headers)
workspaces = res_groups.json().get('value', [])

full_inventory = []

print(f"Iniciando varredura em {len(workspaces)} workspaces...")

for ws in workspaces:
    ws_id = ws.get('id')
    ws_name = ws.get('name')
    # 3. Chamada específica para os relatórios DESTE workspace (ws_id)
    url_rep = f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/reports"
    res_rep = requests.get(url_rep, headers=headers)
    
    if res_rep.status_code == 200:
        reports_in_ws = res_rep.json().get('value', [])
        
        # Só adicionamos se houver relatórios de fato
        for r in reports_in_ws:
            full_inventory.append({
                "WorkspaceName": ws_name,
                "WorkspaceId": ws_id,
                "ReportName": r.get('name'),
                "ReportId": r.get('id'),
                "DatasetId": r.get('datasetId'),
                "WebUrl": r.get('webUrl'),
                "EmbedUrl": r.get('embedUrl'),
                "IsReadOnly": r.get('isReadOnly'),
                # Atribuindo tag de ambiente para sua análise de FIDC
                "Ambiente": "PROD" if "PROD" in ws_name.upper() else "DEV/UAT"
            })
    else:
        print(f"Não foi possível ler o workspace: {ws_name} (Status: {res_rep.status_code})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# 1. Definimos exatamente o que esperar de cada coluna
schema = StructType([
    StructField("WorkspaceName", StringType(), True),
    StructField("WorkspaceId", StringType(), True),
    StructField("ReportName", StringType(), True),
    StructField("ReportId", StringType(), True),
    StructField("DatasetId", StringType(), True),
    StructField("WebUrl", StringType(), True),
    StructField("EmbedUrl", StringType(), True),
    StructField("IsReadOnly", BooleanType(), True),
    StructField("Ambiente", StringType(), True)
])

# 2. Criar o DataFrame usando o Schema fixo
if full_inventory:
    # Passamos o schema como segundo argumento
    spark_df = spark.createDataFrame(full_inventory, schema=schema)
    
    # 3. Salvar na Bronze
    spark_df.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("LH_Bronze.inventario_completo_detalhado")
    
    print(f"Sucesso! {len(full_inventory)} relatórios salvos com tipos validados.")
else:
    print("A lista full_inventory está vazia. Verifique os loops anteriores.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
from datetime import datetime, timedelta
from notebookutils import mssparkutils

# 1. Configurar datas (Ontem)
# A API de logs trabalha com janelas de 24h para melhor performance
ontem = (datetime.now() - timedelta(1)).strftime('%Y-%m-%dT00:00:00')
ontem_fim = (datetime.now() - timedelta(1)).strftime('%Y-%m-%dT23:59:59')

# 2. Autenticação
# Tentando pedir um token com escopo de administração explicitamente
try:
    # Em alguns ambientes Fabric, o escopo padrão não cobre a Admin API
    token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api/.default")
    headers = {'Authorization': f'Bearer {token}'}
    
    # Teste novamente a URL de Admin
    url_admin_test = "https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime='2026-03-23T00:00:00Z'&endDateTime='2026-03-23T23:59:59Z'"
    res = requests.get(url_admin_test, headers=headers)
    print(f"Status Code com novo escopo: {res.status_code}")
except Exception as e:
    print(f"Erro ao obter token de admin: {e}")
headers = {'Authorization': f'Bearer {token}'}

# 3. Endpoint de Atividade (Admin)
# Nota: Este endpoint é diferente do de 'usuário'
url_activity = f"https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime='{ontem}Z'&endDateTime='{ontem_fim}Z'"

print(f"Tentando consultar logs de: {ontem}...")

res = requests.get(url_activity, headers=headers)

if res.status_code == 200:
    data = res.json().get('activityEventEntities', [])
    if data:
        print(f"✅ SUCESSO! Encontrados {len(data)} eventos de atividade.")
        # Mostra o primeiro evento para validar o que vem no log
        print(f"Exemplo de evento: {data[0].get('Activity')}")
        
        # Converter para Spark e salvar uma amostra
        spark.createDataFrame(data).write.mode("overwrite").saveAsTable("LH_Bronze.teste_logs_atividade")
    else:
        print("Empty: A API respondeu, mas não houve atividade registrada ontem.")
elif res.status_code == 401 or res.status_code == 403:
    print(f"❌ ACESSO NEGADO ({res.status_code}): Sua permissão de Admin ainda não propagou ou não foi atribuída corretamente.")
else:
    print(f"Erro inesperado: {res.status_code} - {res.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
