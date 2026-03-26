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

# # Relatório de Risco em Aberto por Sacado
# **Objetivo:** Gerar um relatório que apresente o total do valor devido (risco em aberto) agrupado por sacado.
# 
# **Lógica (Risco em Aberto):**
# - Títulos: `liquidacao is null` (em aberto), `aceito = 'S'` e `t_doc != 'BL'`
# - Operações: `status_aceite = 'A'` e `status_analise = 'D'` (operações deferidas)
# 
# **Tabelas Origem:** `LH_Gold.fato_titulos`, `LH_Gold.fato_operacoes`, `LH_Gold.dim_sacados`
# 
# **Tabela Destino:** `LH_Gold.relatorio_risco_sacado`

# CELL ********************

# Configuração e Imports
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, sum, coalesce, lit
from notebookutils import mssparkutils

print("Configuração concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Leitura das Tabelas Gold
print("Carregando tabelas da camada Gold...")

# Fato Operações
df_ops = spark.read.table("LH_Gold.fato_operacoes")

# Fato Títulos
df_titulos = spark.read.table("LH_Gold.fato_titulos")

# Dimensão Sacados
df_sacados = spark.read.table("LH_Gold.dim_sacados")

print("Tabelas carregadas com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Aplicação das Regras de Negócio e Filtros
print("Aplicando filtros para risco em aberto (operações deferidas)...")

# Filtro Operações (Deferidas e Aceitas)
df_ops_filtered = df_ops.filter(
    (col("status_aceite") == "A") &
    (col("status_analise") == "D")
).select("cod_operacao")

# Filtro Títulos (Em Aberto, Aceito e Não Bloqueado)
df_titulos_filtered = df_titulos.filter(
    (col("liquidacao").isNull()) &
    (col("aceito") == "S") &
    (col("t_doc") != "BL")
)

# Join Títulos e Operações para garantir que o título pertença a uma operação deferida
# Usar inner join, para manter apenas títulos de operações válidas
# Também usamos dropDuplicates para prevenir duplicação caso a operação tenha múltiplas linhas (best practice baseada em rules do projeto)
df_risco_base = df_titulos_filtered.join(
    df_ops_filtered.dropDuplicates(["cod_operacao"]),
    "cod_operacao",
    "inner"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Agregação por Sacado
print("Calculando valor em aberto por sacado...")

# Agrupamento e Soma
df_risco_sacado = df_risco_base.groupBy("cpf_cnpj_sacado").agg(
    sum("valor").alias("valor_risco_em_aberto")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Enriquecimento com Nome do Sacado
print("Adicionando nome do sacado...")

# Join com a dimensão sacados para trazer o nome
# Usando left join para não perder o risco caso o sacado não exista na dimensão
df_relatorio_final = df_risco_sacado.join(
    df_sacados.select("cpf_cnpj", "nome_sacado"),
    col("cpf_cnpj_sacado") == col("cpf_cnpj"),
    "left"
).select(
    col("cpf_cnpj_sacado"),
    coalesce(col("nome_sacado"), lit("NOME NÃO ENCONTRADO")).alias("nome_sacado"),
    col("valor_risco_em_aberto")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Persistência na Camada Gold
target_table = "LH_Gold.relatorio_risco_sacado"
print(f"Salvando o relatório em {target_table}...")

# Salvando como Delta Table, sobrescrevendo dados anteriores
df_relatorio_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

print(f"Relatório '{target_table}' gerado e salvo com sucesso.")

mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
