# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ee40705b-0100-49bc-8f35-81d71839f042",
# META       "default_lakehouse_name": "LH_Gold",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "ee40705b-0100-49bc-8f35-81d71839f042"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook de Geração da Carteira de Títulos (Gold)
# **Objetivo:** Filtrar os títulos que temos a receber com base nas regras de negócio de análise e deferimento.
# 
# **Lógica:**
# - status_aceite = A
# - status_analise = D
# - aceito = S
# - tipo_documento <> BL (usando t_doc em fato_titulos)
# - liquidacao is not null
# 
# **Tabelas Origem:** `LH_Gold.fato_operacoes`, `LH_Gold.fato_titulos`.
# 
# **Tabela Destino:** `LH_Gold.carteira_de_titulos`.

# MARKDOWN ********************

# ## Seção 0: Configuração e Leitura

# CELL ********************

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Carregando tabelas...")

# Operações
df_ops = spark.read.table("LH_Gold.fato_operacoes")

# Titulos
df_titulos = spark.read.table("LH_Gold.fato_titulos")

print("Tabelas carregadas com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Processamento da Lógica de Carteira

# CELL ********************

print("Aplicando lógica de negócio...")

# Filtro de Operações: status_aceite = A e status_analise = D
df_ops_filtered = df_ops.filter(
    (col("status_aceite") == 'A') &
    (col("status_analise") == "D")
).select("cod_operacao", "status_aceite", "status_analise")

# Filtro de Títulos: aceito = S, t_doc <> BL, liquidacao is not null
df_titulos_filtered = df_titulos.filter(
    (col("aceito") =="S") &
    (col("t_doc") != "BL") &
    (col("liquidacao").isNotNull())
)

# Join para consolidar as regras
df_carteira = df_titulos_filtered.join(df_ops_filtered, "cod_operacao", "inner")

print(f"Titulos na carteira elegíveis: {df_carteira.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
