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

# 4. Ler logs de acesso (Lakehouse Bronze - Files)
# Path base do PBI log
log_path = "Files/logs_power_bi/PowerBI_ActivityLog_2026-01-01_ate_2026-02-28.csv"

# Lendo o CSV de logs com cabeçalho
try:
    df_logs = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(log_path)
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

# 5. Ler tabela de inventário já criada
try:
    df_inventario = spark.table("LH_Bronze.inventario_completo_detalhado")
    print("Sucesso ao carregar a tabela LH_Bronze.inventario_completo_detalhado")
    df_inventario.printSchema()
except Exception as e:
    print(f"Erro ao tentar ler a tabela de inventário: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 6. Cruzamento (Join) e Agregação
# O log do Power BI costuma usar "ArtifactId", "ReportId" ou "ObjectId" para identificar o relatório.
# Usaremos "ArtifactId" por padrão, mas você pode alterar para o nome correto caso seja diferente.
# O usuário costuma vir na coluna "UserId"

if 'df_logs' in locals() and 'df_inventario' in locals():
    # Verifica quais colunas de Id o log tem (ArtifactId, ObjectId, ou ReportId)
    log_cols = df_logs.columns
    join_col_log = "ArtifactId" # Padrão
    if "ReportId" in log_cols:
        join_col_log = "ReportId"
    elif "ObjectId" in log_cols:
        join_col_log = "ObjectId"

    # Fazendo o Join: logs de acesso com os detalhes do relatório (inventário)
    # Vamos usar um left join a partir do inventário ou um inner join para ver apenas o que foi acessado
    df_join = df_logs.join(
        df_inventario,
        df_logs[join_col_log] == df_inventario["ReportId"],
        "inner"
    )

    # 7. Agregação: Relatórios acessados, por quem e frequência
    # Assumindo que o UserId está na coluna "UserId" ou "UserKey"
    user_col = "UserId" if "UserId" in log_cols else "UserKey" if "UserKey" in log_cols else "UserId"

    # Agrupando e contando
    df_agrupado = df_join.groupBy(
        "WorkspaceName",
        "ReportName",
        "Ambiente",
        F.col(user_col).alias("Usuario")
    ).agg(
        F.count("*").alias("Frequencia_Acessos"),
        F.max("CreationTime").alias("Ultimo_Acesso") # Assumindo "CreationTime" como data do evento no log do PBI
    ).orderBy(
        F.col("Frequencia_Acessos").desc()
    )

    print("Resumo de Acessos aos Relatórios (Frequência por Usuário):")
    display(df_agrupado)

    # (Opcional) Salvar a tabela final cruzada para uso posterior no Power BI
    # df_agrupado.write.mode("overwrite").saveAsTable("LH_Bronze.relatorio_frequencia_acessos")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
