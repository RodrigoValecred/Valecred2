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
# META           "id": "ee40705b-0100-49bc-8f35-81d71839f042"
# META         },
# META         {
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# 
# # Notebook de Relatório de Novos Registros CVM
# **Objetivo:** Este notebook analisa a tabela `cvm_fidc_informe_mensal` para identificar novos registros (fundos/classes) que aparecem a cada mês e gera um relatório com a data de sua primeira aparição.
# **Processos realizados:**
# 1.  **Carregar Dados da Camada Bronze:** Lê a tabela `cvm_fidc_informe_mensal`.
# 2.  **Identificar Primeira Aparição:** Usa uma função de janela para encontrar a primeira vez que cada combinação `CNPJ_FUNDO` e `CLASSE` aparece.
# 3.  **Salvar Relatório na Camada Silver:** Salva a tabela resultante no Lakehouse `LH_Silver`.

# MARKDOWN ********************

# ## Seção 1: Configuração e Carga dos Dados

# CELL ********************

from pyspark.sql.functions import col, concat, lit, to_date, lpad, min as spark_min, first, trim, regexp_replace

# Define os nomes do Lakehouse de origem e da tabela de destino
source_lakehouse = "LH_Bronze"
target_lakehouse = "LH_Silver"
source_table = "cvm_fidc_informe_mensal"
target_table = "report_novos_registros_cvm"

print("Carregando a tabela de informes mensais da CVM...")
df_informe_mensal = spark.read.table(f"{source_lakehouse}.{source_table}")
print("Tabela carregada com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1.1: Limpeza de Dados
# **Objetivo:** Limpar a coluna de CNPJ para remover espaços em branco e caracteres não imprimíveis que causam problemas em joins e filtros.

# CELL ********************

print("Limpando a coluna CNPJ_FUNDO_CLASSE...")
df_limpo = df_informe_mensal.withColumn(
    "CNPJ_FUNDO_LIMPO",
    trim(regexp_replace(col("CNPJ_FUNDO_CLASSE"), "[^\\x20-\\x7E]", ""))
)
print("Limpeza concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Análise de Novos Registros

# CELL ********************

print("Iniciando a análise para identificar a primeira aparição de cada registro...")

# 1. Criar uma coluna de data para ordenação cronológica
df_com_data = df_limpo.withColumn(
    "DT_REFERENCIA",
    to_date(concat(col("ANO_REF"), lit("-"), lpad(col("MES_REF"), 2, '0'), lit("-01")), "yyyy-MM-dd")
)

# 2. Realizar a agregação para encontrar a data mais antiga de cada fundo
print("Agregando para encontrar a primeira data de referência de cada fundo...")
df_primeira_aparicao = df_com_data.groupBy("CNPJ_FUNDO_LIMPO").agg(
    spark_min("DT_REFERENCIA").alias("DT_PRIMEIRA_APARICAO"),
    first("DENOM_SOCIAL", ignorenulls=True).alias("DENOM_SOCIAL")
)

print("Agregação concluída. Exibindo resultado:")
df_primeira_aparicao.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: Salvar Relatório na Camada Silver

# CELL ********************

print(f"Salvando o relatório simples na tabela {target_lakehouse}.{target_table}...")

# Renomear a coluna para o padrão desejado
df_relatorio_final = df_primeira_aparicao.withColumnRenamed("CNPJ_FUNDO_LIMPO", "CNPJ_FUNDO")

# Salvar a tabela no Lakehouse Silver
df_relatorio_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.{target_table}")

print("Relatório simples salvo com sucesso!")
df_relatorio_final.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
