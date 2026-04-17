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

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

import requests
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.functions import col, coalesce, lit, sum
from notebookutils import mssparkutils

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

# 🧠 Bolt: Setup HTTP connection pooling for performance
session = requests.Session()
adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
session.mount('http://', adapter)
session.mount('https://', adapter)
session.headers.update(headers)

res_groups = session.get("https://api.powerbi.com/v1.0/myorg/groups")
workspaces = res_groups.json().get('value', [])

full_inventory = []

def fetch_reports_from_ws(ws):
    """Função auxiliar para buscar relatórios de um workspace específico via API."""
    ws_id = ws.get('id')
    ws_name = ws.get('name')
    url_rep = f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/reports"
    
    inventory_items = []
    try:
        res_rep = session.get(url_rep)
        if res_rep.status_code == 200:
            reports_in_ws = res_rep.json().get('value', [])
            for r in reports_in_ws:
                inventory_items.append({
                    "WorkspaceName": ws_name,
                    "WorkspaceId": ws_id,
                    "ReportName": r.get('name'),
                    "ReportId": r.get('id'),
                    "DatasetId": r.get('datasetId'),
                    "WebUrl": r.get('webUrl'),
                    "EmbedUrl": r.get('embedUrl'),
                    "IsReadOnly": r.get('isReadOnly'),
                    "Ambiente": "PROD" if "PROD" in ws_name.upper() else "DEV/UAT"
                })
        else:
            print(f"Não foi possível ler o workspace: {ws_name} (Status: {res_rep.status_code})")
    except Exception as e:
        print(f"Erro ao acessar reports do workspace {ws_name}: {str(e)}")
        
    return inventory_items

print(f"Inicianda varredura paralela em {len(workspaces)} workspaces...")

# Usando ThreadPoolExecutor para paralelizar as chamadas de I/O bloqueante (requests HTTP)
with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(executor.map(fetch_reports_from_ws, workspaces))

# Achatar a lista de listas em full_inventory
for report_list in results:
    full_inventory.extend(report_list)

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

# 4. Ler logs de acesso
log_path = "Files/logs_power_bi/PowerBI_ActivityLog_2026-01-01_ate_2026-02-28.csv"

try:
    df_logs = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(log_path)
    print(f"Sucesso ao ler os logs de acesso de: {log_path}")
    df_logs.printSchema()
except Exception as e:
    print(f"Erro ao tentar ler o arquivo de logs: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Ler tabela de inventario
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. Cruzamento (Join) e Agregação
if 'df_logs' in locals() and 'df_inventario' in locals():
    log_cols = df_logs.columns
    join_col_log = "ArtifactId"
    if "ReportId" in log_cols:
        join_col_log = "ObjectId"

    df_join = df_logs.join(
        df_inventario,
        df_logs[join_col_log] == df_inventario["ReportId"],
        "inner"
    )
    # 7. Agragação: Relatórios acessados, por quem e frequência
    user_col = "UserId" if "UserId" in log_cols else "UserKey" if "UserKey" in log_cols else "UserId"

    df_agrupado = df_join.groupBy(
        "WorkspaceName",
        "ReportName",
        "Ambiente",
        F.col(user_col).alias("Usuario")
    ).agg(
        F.count("*").alias("Frequencia_Acessos"),
        F.max("CreationTime").alias("Ultimo_Acesso")
    ).orderBy(
        F.col("Frequencia_Acessos").desc()
    )

    print("Resumo de Acessos aos Relatórios (Frequência por Usuários):")
    display(df_agurpado)

    df_agrupado.write.mode("overwrite").saveAsTable("LH_Bronze.relatorio_frequencia_acessos")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
