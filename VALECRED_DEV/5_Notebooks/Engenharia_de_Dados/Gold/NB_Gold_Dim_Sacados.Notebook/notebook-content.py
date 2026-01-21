# Fabric notebook source


# CELL ********************

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
# META         },
# META         {
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         },
# META         {
# META           "id": "ee40705b-0100-49bc-8f35-81d71839f042"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook Gold: Dimensão Sacados
# **Objetivo:** Construir a tabela `LH_Gold.dim_sacados` a partir de tabelas Silver (`staging_sacados_enriquecida`).
# **Dependências:** `NB_Prepara_Tabela_Cadastros` (Silver).

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col
from delta.tables import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Iniciando construção da dim_sacados...")
# A tabela já vem tratada da camada Silver (NB_Prepara_Tabela_Cadastros)
# Selecionamos as colunas e salvamos na Gold.

print("Lendo LH_Silver.staging_sacados_enriquecida...")
df_sacados_enriquecida = spark.read.table("LH_Silver.staging_sacados_enriquecida")

df_dim_sacados = df_sacados_enriquecida.select(
    col("cpf_cnpj"),
    col("nome_sacado"),
    col("emails"),
    col("telefones"),
    col("endereco"),
    col("numero"),
    col("complemento"),
    col("bairro"),
    col("cidade"),
    col("uf"),
    col("cep"),
    col("regiao")
)

output_path_dim_sacados = "LH_Gold.dim_sacados"
df_dim_sacados.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_dim_sacados)
print(f"Tabela 'dim_sacados' salva em: {output_path_dim_sacados}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
