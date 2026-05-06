# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8f85c372-56ad-4f3f-acf9-3be2e9b99513",
# META       "default_lakehouse_name": "LH_Gold",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Relatório de Títulos Vencidos por Gerente de Negócio
# **Objetivo:** Criar uma tabela na camada Gold (`relatorio_titulos_vencidos_gerente`) consolidando títulos vencidos e agrupando por gerente de negócio (cod_broker).
#
# **Origem:** `LH_Gold` (`fato_titulos`, `dim_clientes`, `dim_gerentes`).
#
# **Destino:** `LH_Gold` (`relatorio_titulos_vencidos_gerente`).
#

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, sum as _sum, count, when, current_date, broadcast
# from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Lendo tabelas da camada Gold...")

# Leitura da tabela Fato de Títulos
df_titulos = spark.read.table("LH_Gold.fato_titulos")

# Leitura da dimensão de Clientes para obter o vínculo com o corretor (cod_broker)
df_clientes = spark.read.table("LH_Gold.dim_clientes")

# Leitura da dimensão de Gerentes para obter o nome
df_gerentes = spark.read.table("LH_Gold.dim_gerentes")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Filtrando e enriquecendo dados...")

# Filtrar títulos vencidos
# Critério: liquidacao nula (não pago), e status_deferimento = 'Sim', e data_vencimento_util < data_atual
# Consideramos valor_devido para o montante

df_vencidos = df_titulos.filter(
    (col("liquidacao").isNull()) &
    (col("status_deferimento") == "Sim") &
    (col("data_vencimento_util") < current_date())
)

# Join com clientes para pegar cod_broker
# Para não estragar planos lógicos complexos e para obedecer às regras de "Ambiguous Reference",
# excluímos as colunas não utilizadas
df_clientes_broker = df_clientes.select("cod_cliente", "cod_broker")

# 🧠 Tensor: Aplicado broadcast() a dim_clientes para evitar shuffle na união com a tabela de fatos
# 💡 O que: Utiliza `broadcast()` nas tabelas de dimensão pequenas.
# 🎯 Por que: Tabelas de dimensões têm tamanhos curtos que entram em memória. Fazer o broadcast desabilita operações custosas de I/O na rede (SortMergeJoin).
# 📊 Impacto: Melhora consideravelmente a latência de junção.
# 🔬 Medição: Elimina Shuffles na interface de UI do Spark.

df_joined_clientes = df_vencidos.join(
    broadcast(df_clientes_broker),
    on="cod_cliente",
    how="inner"
)

# Join com Gerentes para obter o nome do gerente
df_gerentes_nome = df_gerentes.select("cod_broker", "nome_gerente", "cod_agencia", "nome_plataforma")

df_joined_gerentes = df_joined_clientes.join(
    broadcast(df_gerentes_nome),
    on="cod_broker",
    how="left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Agregando resultados...")

# Agrupando por gerente
df_relatorio = df_joined_gerentes.groupBy(
    "cod_broker", "nome_gerente", "cod_agencia", "nome_plataforma"
).agg(
    count("cod_titulo").alias("qtd_titulos_vencidos"),
    _sum("valor_devido").alias("valor_total_vencido")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Escrita na Camada Gold
output_table = "LH_Gold.relatorio_titulos_vencidos_gerente"
print(f"Salvando tabela {output_table}...")

df_relatorio.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

print("Tabela criada com sucesso.")
mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
