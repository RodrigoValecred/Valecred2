# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "528f869c-46e3-448f-8d26-663857d42813",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "4950666d-1768-4503-ad37-567475308694"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook: Análise de Keywords em Pareceres
#
# **Objetivo:** Processar pareceres de alteração de status, limpando o texto e identificando a presença de keywords específicas solicitadas.
#
# **Keywords:** POLITICA, INSTABILIDADE FINANCEIRA, APONTAMENTOS RELEVANTES, ORIGEM DA EMPRESA, RAMO DE ATIVIDADE, INFORMACAO DE MERCADO NEGATIVA, ESTRUTURA DE CAPITAL, HISTORICO INTERNO NEGATIVO, PERFIL DA OPERACAO.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, upper, regexp_replace, translate, trim, date_format

# CELL ********************

# Definição dos Lakehouses
source_lakehouse = "LH_Bronze"
target_lakehouse = "LH_Silver"

# CELL ********************

# Leitura das tabelas
df_pareceres = spark.read.table(f"{source_lakehouse}.cad_geral_pareceres")
df_usuarios = spark.read.table(f"{source_lakehouse}.cad_usuarios")
df_clientes_pj = spark.read.table(f"{source_lakehouse}.cad_geral_pf_pj")

# Filtrar Pareceres de Alteração de Status (Tipo 1)
# Opcional: Filtrar datas recentes se necessário, mas o pedido não especificou.
df_pareceres_filtered = df_pareceres.filter(col("CODTIPOPARECER") == 1)

# CELL ********************

# Joins para enriquecer os dados
# Join com Usuários
df_joined_users = df_pareceres_filtered.join(
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

# CELL ********************

# Definição das Keywords e criação das colunas (0/1)
keywords = {
    "POLITICA": "POLITICA",
    "INSTABILIDADE_FINANCEIRA": "INSTABILIDADE FINANCEIRA",
    "APONTAMENTOS_RELEVANTES": "APONTAMENTOS RELEVANTES",
    "ORIGEM_DA_EMPRESA": "ORIGEM DA EMPRESA",
    "RAMO_DE_ATIVIDADE": "RAMO DE ATIVIDADE",
    "INFORMACAO_DE_MERCADO_NEGATIVA": "INFORMACAO DE MERCADO NEGATIVA",
    "ESTRUTURA_DE_CAPITAL": "ESTRUTURA DE CAPITAL",
    "HISTORICO_INTERNO_NEGATIVO": "HISTORICO INTERNO NEGATIVO",
    "PERFIL_DA_OPERACAO": "PERFIL DA OPERACAO"
}

df_keywords = df_clean
for col_name, search_term in keywords.items():
    # O termo de busca já está normalizado (maísculo, sem acentos)
    df_keywords = df_keywords.withColumn(
        col_name.lower(), # Coluna em snake_case (ex: politica, instabilidade_financeira)
        when(col("obs_normalized").contains(search_term), 1).otherwise(0)
    )

# CELL ********************

# Seleção e Renomeação das Colunas Finais
df_final = df_keywords.select(
    col("CODPARECER").alias("cod_parecer"),
    col("CPFCNPJ").alias("cpf_cnpj"),
    col("NOME_CLIENTE").alias("nome_cliente"),
    col("CODTIPOPARECER").alias("cod_tipo_parecer"),
    col("DATAINCLUSAO").alias("data_inclusao_parecer"),
    col("USUAINCLUSAO").alias("cod_usuario_inclusao"),
    lit("ALTERACAO DE STATUS").alias("descricao_tipo_parecer"), # Descrição fixa para o tipo
    col("NOME_USUARIO_INCLUSAO").alias("nome_usuario_inclusao"),
    col("obs_clean").alias("parecer_texto"), # O Parecer (tratado como texto)

    # Colunas de Keywords
    col("politica"),
    col("instabilidade_financeira"),
    col("apontamentos_relevantes"),
    col("origem_da_empresa"),
    col("ramo_de_atividade"),
    col("informacao_de_mercado_negativa"),
    col("estrutura_de_capital"),
    col("historico_interno_negativo"),
    col("perfil_da_operacao")
)

# CELL ********************

# Escrita da Tabela no Silver
print("Escrevendo tabela LH_Silver.analise_pareceres_keywords...")
df_final.write.format("delta").mode("overwrite").saveAsTable(f"{target_lakehouse}.analise_pareceres_keywords")
print("Concluído.")
