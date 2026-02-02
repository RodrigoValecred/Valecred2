# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8f85c372-56ad-4f3f-acf9-3be2e9b99513",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
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

# # Notebook Gold: Relatório de Entrada de Novos Clientes
# **Objetivo:** Gerar uma tabela que registre a data de entrada de clientes novos (ou grupos).
# 
# **Regra de Negócio:**
# 
# *   Data de entrada = Data da primeira operação aceita.
# 
# *   Agrupamento: Se o cliente faz parte de um grupo, vale a data da primeira operação do grupo todo.
# 
# *   Contagem: O grupo vale apenas 1 cliente.
# 
# *   Gerente: O gerente atribuído é aquele responsável pela primeira operação do grupo/cliente.


# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, min, row_number, coalesce, when, lit, first
from pyspark.sql.window import Window
from delta.tables import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Iniciando geração do Relatório de Novos Clientes...")

# 1. Leitura das Tabelas
print("Lendo tabelas de origem...")
df_ops = spark.read.table("LH_Silver.staging_operacoes_limpa")
df_grupos = spark.read.table("LH_Silver.sup_grupos_economicos")

# Tenta ler dim_gerentes (Gold), se não existir (primeira execução geral), avisa mas tenta continuar se possível ou falha.
# Assumimos que dim_gerentes já existe pois é um pré-requisito lógico.
df_gerentes = spark.read.table("LH_Gold.dim_gerentes")

# 2. Tratamento de Colunas (Grupos)
# Normalização para snake_case caso venha diferente
if "cod_cliente" not in df_grupos.columns and "codcliente" in df_grupos.columns:
    df_grupos = df_grupos.withColumnRenamed("codcliente", "cod_cliente")
if "grupo_economico" not in df_grupos.columns and "nomegrupo" in df_grupos.columns:
    df_grupos = df_grupos.withColumnRenamed("nomegrupo", "grupo_economico")

df_grupos = df_grupos.select("cod_cliente", "grupo_economico")

# 3. Filtrar Operações Válidas (Aceitas)
# "Data de entrada" implica sucesso na operação.
df_ops_validas = df_ops.filter(col("status_aceite") == 'A') \
    .select("cod_operacao", "cod_cliente", "data_inclusao", "cod_broker")

# 4. Join com Grupos
df_ops_grp = df_ops_validas.join(df_grupos, "cod_cliente", "left")

# 5. Definição da Entidade (Grupo ou Cliente Individual)
# Se grupo_economico existe, usa ele. Se não, usa cod_cliente.
df_ops_entity = df_ops_grp.withColumn(
    "entidade_id",
    coalesce(col("grupo_economico"), col("cod_cliente").cast("string"))
).withColumn(
    "tipo_entidade",
    when(col("grupo_economico").isNotNull(), "GRUPO").otherwise("CLIENTE_INDIVIDUAL")
)

# 6. Identificar a Primeira Data e o Gerente correspondente
# Agrupamos por entidade e ordenamos por data. Pegamos a primeira linha.
window_entity = Window.partitionBy("entidade_id").orderBy(col("data_inclusao").asc(), col("cod_operacao").asc())

df_first_ops = df_ops_entity.withColumn("rn", row_number().over(window_entity)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# 7. Join com Detalhes do Gerente
# O df_first_ops tem 'cod_broker' da primeira operação.
df_final = df_first_ops.join(df_gerentes, "cod_broker", "left") \
    .select(
        col("entidade_id").alias("codigo_grupo_ou_cliente"),
        col("tipo_entidade"),
        col("data_inclusao").alias("data_entrada"),
        col("cod_broker").alias("codigo_gerente"),
        coalesce(col("nome_gerente"), lit("Gerente Não Identificado")).alias("nome_gerente"),
        col("nome_plataforma"),
        col("gestor_da_plataforma")
    )

# 8. Salvar Tabela Gold
output_path = "LH_Gold.relatorio_novos_clientes"
print(f"Salvando tabela em {output_path}...")
df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path)
print("Concluído com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
