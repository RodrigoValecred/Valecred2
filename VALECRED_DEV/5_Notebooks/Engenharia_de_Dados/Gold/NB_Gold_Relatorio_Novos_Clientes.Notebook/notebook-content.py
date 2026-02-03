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

# CELL ********************

# Fabric notebook source

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Notebook Gold: Relatório de Entrada de Novos Clientes
# **Objetivo:** Gerar uma tabela que registre a data de entrada de clientes novos (ou grupos).
# **Regra de Negócio:**
# *   Data de entrada = Data da primeira operação aceita.
# *   Agrupamento: Se o cliente faz parte de um grupo, vale a data da primeira operação do grupo todo.
# *   Contagem: O grupo vale apenas 1 cliente.
# *   Gerente: O gerente atribuído é aquele responsável pela primeira operação do grupo/cliente.
#     *   **Enriquecimento (Strict):** Caso a operação não tenha gerente (cod_broker=0), busca na bridge pela data da operação.
#     *   **Enriquecimento (Fallback):** Se a operação for anterior ao histórico da bridge, assume o primeiro gerente registrado para o cliente.

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
df_bridge = spark.read.table("LH_Silver.bridge_cliente_gerente")

# Tenta ler dim_gerentes (Gold)
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
# Incluímos 'data_analise' para o join com a bridge
df_ops_validas = df_ops.filter(col("status_aceite") == 'A') \
    .select("cod_operacao", "cod_cliente", "data_inclusao", "data_analise", "cod_broker")

# 4. Enriquecimento de Gerente (Bridge)
print("Aplicando enriquecimento de gerentes via Bridge (Strict + Fallback)...")

df_bridge_prep = df_bridge.withColumnRenamed("cod_cliente", "cod_cliente_bridge")

# 4.1 Strict Join (Date Match)
# Join com Bridge baseado na data da análise
df_ops_enriched = df_ops_validas.join(
    df_bridge_prep,
    (df_ops_validas["cod_cliente"] == df_bridge_prep["cod_cliente_bridge"]) &
    (df_ops_validas["data_analise"].cast("date") >= df_bridge_prep["data_inicio_vigencia"]) &
    (df_ops_validas["data_analise"].cast("date") <= df_bridge_prep["data_fim_vigencia"]),
    "left"
)

# 4.2 Fallback Logic (Earliest Manager)
# Prepara fallback para casos onde a operação é anterior ao histórico da bridge
w_fallback = Window.partitionBy("cod_cliente").orderBy(col("data_inicio_vigencia").asc())
df_bridge_fallback = df_bridge.withColumn("rn", row_number().over(w_fallback)) \
    .filter(col("rn") == 1) \
    .select(col("cod_cliente").alias("cod_cliente_fb"), col("cod_gerente").alias("cod_gerente_fb"))

df_ops_enriched_2 = df_ops_enriched.join(
    df_bridge_fallback,
    df_ops_enriched.cod_cliente == df_bridge_fallback.cod_cliente_fb,
    "left"
)

# 4.3 Final Broker Selection
# Regra de Ouro (Descoberta): O campo cod_gerente (cod_broker) da tab_operacoes só começou a ser preenchido em 06/2025.
# Portanto, a prioridade deve ser a Bridge (Histórico de Vigência).
# Prioridade Atualizada: 1. Bridge Strict > 2. Broker Original (se válido) > 3. Bridge Fallback
df_ops_final_broker = df_ops_enriched_2.withColumn(
    "cod_broker_final",
    when(col("cod_gerente").isNotNull(), col("cod_gerente"))
    .when((col("cod_broker").isNotNull()) & (col("cod_broker") != 0), col("cod_broker"))
    .otherwise(col("cod_gerente_fb"))
).drop("cod_cliente_bridge", "cod_gerente", "data_inicio_vigencia", "data_fim_vigencia", "cod_cliente_fb", "cod_gerente_fb")

# 5. Join com Grupos
df_ops_grp = df_ops_final_broker.join(df_grupos, "cod_cliente", "left")

# 6. Definição da Entidade (Grupo ou Cliente Individual)
# Se grupo_economico existe, usa ele. Se não, usa cod_cliente.
df_ops_entity = df_ops_grp.withColumn(
    "entidade_id",
    coalesce(col("grupo_economico"), col("cod_cliente").cast("string"))
).withColumn(
    "tipo_entidade",
    when(col("grupo_economico").isNotNull(), "GRUPO").otherwise("CLIENTE_INDIVIDUAL")
)

# 7. Identificar a Primeira Data e o Gerente correspondente
# Agrupamos por entidade e ordenamos por data. Pegamos a primeira linha.
window_entity = Window.partitionBy("entidade_id").orderBy(col("data_inclusao").asc(), col("cod_operacao").asc())

df_first_ops = df_ops_entity.withColumn("rn", row_number().over(window_entity)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# 8. Join com Detalhes do Gerente
# O df_first_ops tem 'cod_broker_final' da primeira operação.
df_final = df_first_ops.join(
    df_gerentes,
    df_first_ops.cod_broker_final == df_gerentes.cod_broker,
    "left"
) \
    .select(
        col("entidade_id").alias("codigo_grupo_ou_cliente"),
        col("tipo_entidade"),
        col("data_inclusao").alias("data_entrada"),
        col("cod_broker_final").alias("codigo_gerente"),
        coalesce(col("nome_gerente"), lit("Gerente Não Identificado")).alias("nome_gerente"),
        col("nome_plataforma"),
        col("gestor_da_plataforma")
    )

# 9. Salvar Tabela Gold
output_path = "LH_Gold.relatorio_novos_clientes"
print(f"Salvando tabela em {output_path}...")
df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path)
print("Concluído com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import add_months, countDistinct

print("Calculando performance de Novos Clientes (Primeiros 6 meses)...")

# 1. Carregar Tabelas
# Usamos o resultado recém-gerado e a dimensão de gerentes
df_novos = spark.read.table("LH_Gold.relatorio_novos_clientes")
df_gerentes_dim = spark.read.table("LH_Gold.dim_gerentes")

# 2. Join para trazer Data de Início do Gerente
# O relatório de novos clientes tem 'codigo_gerente' que equivale a 'cod_broker'
df_joined = df_novos.join(
    df_gerentes_dim.select(col("cod_broker").alias("codigo_gerente_join"), col("data_inicio"), col("nome_gerente").alias("nome_gerente_dim")),
    df_novos.codigo_gerente == col("codigo_gerente_join"),
    "inner"
)

# 3. Aplicar Filtro de Tempo (6 Meses)
# A data de entrada do cliente deve ser >= data de início do gerente
# E <= data de início + 6 meses
df_filtered = df_joined.filter(
    (col("data_entrada") >= col("data_inicio")) &
    (col("data_entrada") <= add_months(col("data_inicio"), 6))
)

# 4. Agregação
# Contamos quantos clientes/grupos distintos cada gerente trouxe nesse período
df_result = df_filtered.groupBy(
    col("codigo_gerente"),
    coalesce(col("nome_gerente"), col("nome_gerente_dim")).alias("nome_gerente"),
    col("data_inicio")
).agg(
    countDistinct("codigo_grupo_ou_cliente").alias("qtd_novos_clientes_6_meses")
).orderBy(col("qtd_novos_clientes_6_meses").desc())

# 5. Exibição e Persistência
print("Top 20 Gerentes (Novos Clientes nos Primeiros 6 Meses):")
df_result.show(20, truncate=False)

output_perf_path = "LH_Gold.analise_performance_novos_clientes_6m"
print(f"Salvando análise em {output_perf_path}...")
df_result.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_perf_path)
print("Análise de performance concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
