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

import sempy.fabric as fabric
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType([
    StructField("WorkspaceName", StringType(), True),
    StructField("WorkspaceId", StringType(), True),
    StructField("ReportName", StringType(), True),
    StructField("ReportId", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Obter a lista de todos os workspaces (Visão Admin)
df_workspaces = fabric.list_workspaces()
all_reports = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Varrendo {len(df_workspaces)} workspaces...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2- Loop para mapear Relatórios e Datas de Modificação
for ws in df_workspaces.itertuples():
    try:
        reports = fabric.list_reports(workspace=ws.Id)
        if not reports.empty:
            for report in reports.itertuples():
                all_reports.append({
                    "Workspace": ws.Name,
                    "Workspace_ID": ws.Id,
                    "Report_Name": report.Name,
                    "Report_ID": report.Id,
                    "Modified_Time": report.ModifiedTime
                })
    except Exception as e:
        continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df = spark.createDataFrame(all_reports, schema=schema)

if len(all_reports) > 0:
    spark_df.write.mode("overwrite").option("overwriteSchema","true").saveAsTable("LH_Bronze.inventario_powerbi")
    print(f"Sucesso! Invetário de {len(all_reports)} relatórios salvo na LH_Bronze.")
else:
    print("Atenção: Nenhum relatório foi encontrado. Verifique as permissões de 'Admin API' no Tenante Settings")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
