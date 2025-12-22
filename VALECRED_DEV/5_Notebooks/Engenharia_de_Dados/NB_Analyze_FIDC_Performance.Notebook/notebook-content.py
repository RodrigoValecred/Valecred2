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

# CELL ********************

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a992da48-c3a7-4552-873b-5517173e4f3a",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "a992da48-c3a7-4552-873b-5517173e4f3a"
# META         },
# META         {
# META           "id": "553c2931-573b-4db0-838d-a70a01306d32"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook de Análise de Performance de FIDCs
# **Objetivo:** Este notebook analisa a performance mensal de Fundos de Investimento em Direitos Creditórios (FIDCs) com base nos informes mensais da CVM.
# **Processos realizados:**
# 1.  **Carregar Dados da Camada Bronze:** Lê a tabela `cvm_fidc_informe_mensal` do Lakehouse `LH_Bronze`.
# 2.  **Filtrar Período de Análise:** Seleciona os dados de agosto e setembro de 2025.
# 3.  **Calcular Variação Mensal:** Calcula a variação percentual do Patrimônio Líquido (`TAB_IV_VL_PATRIM_LIQ`) de um mês para o outro.
# 4.  **Salvar Resultado na Camada Ouro:** Armazena a análise em uma nova tabela no Lakehouse `LH_Gold`.

# MARKDOWN ********************

# ## Seção 0: Configuração do Ambiente

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Definição dos nomes dos Lakehouses
source_lakehouse = "LH_Bronze"
target_lakehouse = "LH_Gold"
table_name = "analise_fidc_performance_mensal"

print("Ambiente configurado.")

# MARKDOWN ********************

# ## Seção 1: Carregar Dados da CVM

# CELL ********************

# Carregar a tabela de informes mensais da camada Bronze
df_informe_mensal = spark.read.table(f"{source_lakehouse}.cvm_fidc_informe_mensal")

# Filtrar para os meses de agosto e setembro de 2025
df_fidc = df_informe_mensal.filter(
    (F.col("DT_COMPTC") >= "2025-08-01") & (F.col("DT_COMPTC") <= "2025-09-30")
)

print("Dados da CVM para o período de análise carregados.")

# MARKDOWN ********************

# ## Seção 2: Calcular a Variação do Patrimônio Líquido

# CELL ********************

# Janela para particionar por fundo e ordenar por data
window_spec = Window.partitionBy("CNPJ_FUNDO_CLASSE").orderBy("DT_COMPTC")

# Adicionar uma coluna com o valor do patrimônio líquido do mês anterior
df_com_valor_anterior = df_fidc.withColumn(
    "VL_PATRIM_LIQ_ANTERIOR",
    F.lag("TAB_IV_VL_PATRIM_LIQ").over(window_spec)
)

# Filtrar apenas os registros de setembro, que agora contêm os dados de agosto
df_setembro = df_com_valor_anterior.filter(F.month("DT_COMPTC") == 9)

# Calcular a variação percentual e renomear colunas para o schema final
df_analise = df_setembro.withColumn(
    "VARIACAO_PERC_PATRIMONIO",
    (
        (F.col("TAB_IV_VL_PATRIM_LIQ") - F.col("VL_PATRIM_LIQ_ANTERIOR")) / F.col("VL_PATRIM_LIQ_ANTERIOR")
    ) * 100
).select(
    "DT_COMPTC",
    F.col("CNPJ_FUNDO_CLASSE").alias("CNPJ_FUNDO"),
    "DENOM_SOCIAL",
    F.col("VL_PATRIM_LIQ_ANTERIOR"),
    F.col("TAB_IV_VL_PATRIM_LIQ").alias("VL_PATRIM_LIQ"),
    "VARIACAO_PERC_PATRIMONIO"
).orderBy(F.desc("VARIACAO_PERC_PATRIMONIO"))

print("Análise de variação do patrimônio concluída.")

# MARKDOWN ********************

# ## Seção 3: Salvar Análise na Camada Ouro

# CELL ********************

# Salvar a tabela de análise no Lakehouse de Ouro
output_path = f"{target_lakehouse}.{table_name}"

df_analise.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path)

print(f"Tabela de análise salva com sucesso em: {output_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
