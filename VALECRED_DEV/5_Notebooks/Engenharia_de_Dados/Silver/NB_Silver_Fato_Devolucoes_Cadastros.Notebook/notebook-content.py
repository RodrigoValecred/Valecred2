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
# META         },
# META         {
# META           "id": "553c2931-573b-4db0-838d-a70a01306d32"
# META         },
# META         {
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook: Fato Devoluções de Cadastro
# 
# **Objetivo:** Identificar pareceres que indicam "devolutiva de cadastro" ou "cadastro devolvido" e classificar os motivos da devolução.
# 
# **Critérios de Identificação:**
# - Texto contém: "DEVOLUTIVA DE CADASTRO" ou "CADASTRO DEVOLVIDO"
# 
# **Motivos Mapeados (Flags):**
# - Ramo de atividade
# - Fora do Raio de Atuação
# - Documentos Pendentes

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, upper, regexp_replace, translate, trim, date_format

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Definição dos Lakehouses
source_lakehouse = "LH_Bronze"
target_lakehouse = "LH_Silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leitura das tabelas
df_pareceres = spark.read.table(f"{source_lakehouse}.cad_geral_pareceres")
df_usuarios = spark.read.table(f"{source_lakehouse}.cad_usuarios")
df_clientes_pj = spark.read.table(f"{source_lakehouse}.cad_geral_pf_pj")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Joins para enriquecer os dados
# Join com Usuários
df_joined_users = df_pareceres.join(
    df_usuarios.select(col("CODUSUARIO").alias("CODUSUARIO_JOIN"), col("NOME").alias("NOME_USUARIO_INCLUSAO")),
    col("USUAINCLUSAO") == col("CODUSUARIO_JOIN"),
    "left"
).drop("CODUSUARIO_JOIN")

# Join com Clientes (PF/PJ) para obter o nome
df_enriched = df_joined_users.join(
    df_clientes_pj.select(col("CPFCNPJ").alias("CPFCNPJ_JOIN"), col("NOME").alias("NOME_CLIENTE")),
    col("CPFCNPJ") == col("CPFCNPJ_JOIN"),
    "left"
).drop("CPFCNPJ_JOIN")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Limpeza e Normalização do Texto do Parecer

# Converter OBS (binary/string) para string, remover HTML e normalizar
df_clean = df_enriched.withColumn(
    "obs_str", col("OBS").cast("string")
).withColumn(
    "obs_no_html", regexp_replace(col("obs_str"), "<[^>]+>", " ") # Remove HTML tags
).withColumn(
    "obs_clean", trim(regexp_replace(col("obs_no_html"), "\\s+", " ")) # Remove extra spaces
).withColumn(
    "obs_normalized", 
    upper(translate(col("obs_clean"), "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ", "AAAAAEEEEIIIIOOOOOUUUUCAAAAAEEEEIIIIOOOOOUUUUC"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filtragem por Termos de Devolução
df_devolucoes = df_clean.filter(
    col("obs_normalized").contains("DEVOLUTIVA DE CADASTRO") | 
    col("obs_normalized").contains("CADASTRO DEVOLVIDO")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Definição dos Motivos de Devolução (Flags)
motivos = {
    "motivo_ramo_atividade": "RAMO DE ATIVIDADE",
    "motivo_fora_raio_atuacao": "FORA DO RAIO DE ATUACAO",
    "motivo_documentos_pendentes": "DOCUMENTOS PENDENTES"
}

df_flags = df_devolucoes
for col_name, search_term in motivos.items():
    df_flags = df_flags.withColumn(
        col_name,
        when(col("obs_normalized").contains(search_term), 1).otherwise(0)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Seleção e Renomeação das Colunas Finais
df_final = df_flags.select(
    col("CODPARECER").alias("cod_parecer"),
    col("CPFCNPJ").alias("cpf_cnpj"),
    col("NOME_CLIENTE").alias("nome_cliente"),
    col("CODTIPOPARECER").alias("cod_tipo_parecer"),
    col("DATAINCLUSAO").alias("data_inclusao_parecer"),
    col("USUAINCLUSAO").alias("cod_usuario_inclusao"),
    lit("DEVOLUCAO DE CADASTRO").alias("descricao_tipo_parecer"), # Descrição fixa
    col("NOME_USUARIO_INCLUSAO").alias("nome_usuario_inclusao"),
    col("obs_clean").alias("parecer_texto"), 
    
    # Flags de Motivos
    col("motivo_ramo_atividade"),
    col("motivo_fora_raio_atuacao"),
    col("motivo_documentos_pendentes")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Escrita da Tabela no Silver
print("Escrevendo tabela LH_Silver.fato_devolucoes_cadastro...")
df_final.write.format("delta").mode("overwrite").saveAsTable(f"{target_lakehouse}.fato_devolucoes_cadastro")
print("Concluído.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
