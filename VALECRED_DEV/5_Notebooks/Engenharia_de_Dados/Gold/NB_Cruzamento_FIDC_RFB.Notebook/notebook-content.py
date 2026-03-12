# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "385c2931-573b-4db0-838d-a70a01306d32",
# META       "default_lakehouse_name": "LH_Gold",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "385c2931-573b-4db0-838d-a70a01306d32"
# META         },
# META         {
# META           "id": "553c2931-573b-4db0-838d-a70a01306d32"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook de Cruzamento CVM (FIDC) x Receita Federal (Global)
# **Objetivo:** Cruzar todos os clientes informados pelos FIDCs nos arquivos da CVM com a base completa da Receita Federal.

# CELL ********************

from pyspark.sql.functions import col, expr, array, struct, explode, regexp_replace

print("Iniciando carregamento de dados...")
df_cvm = spark.read.table("LH_Bronze.cvm_fidc_informe_mensal")
df_empresas = spark.read.table("LH_Bronze.rfb_empresas_full")
df_estab = spark.read.table("LH_Bronze.rfb_estabelecimentos_full")

# 1. Preparar Base da Receita Federal
# Join para pegar Razão Social, Capital Social, Natureza Jurídica
# Chave: cnpj_basico
df_rfb_joined = df_estab.join(df_empresas, "cnpj_basico", "inner")

# Criar coluna CNPJ Completo formatado (apenas números para join)
df_rfb_completo = df_rfb_joined.withColumn("cnpj_completo", expr("concat(cnpj_basico, cnpj_ordem, cnpj_dv)"))

# 2. Desaninhar e Limpar Base CVM (FIDCs)
cols_existentes = set(df_cvm.columns)

cedentes_cols = [
    struct(
        col(f"TAB_I2A12_CPF_CNPJ_CEDENTE_{i}").alias("cnpj_cedente"),
        col(f"TAB_I2A12_PR_CEDENTE_{i}").cast("double").alias("percentual_concentracao")
    )
    for i in range(1, 10)
    if f"TAB_I2A12_CPF_CNPJ_CEDENTE_{i}" in cols_existentes
    and f"TAB_I2A12_PR_CEDENTE_{i}" in cols_existentes
]

if cedentes_cols:
    df_cvm_unpivoted = df_cvm.withColumn("cedentes", array(*cedentes_cols)) \
                             .select("CNPJ_FUNDO_CLASSE", "DENOM_SOCIAL", "DT_COMPTC", explode("cedentes").alias("cedente")) \
                             .select(
                                 col("CNPJ_FUNDO_CLASSE"),
                                 col("DENOM_SOCIAL").alias("fundo_denom_social"),
                                 col("DT_COMPTC"),
                                 col("cedente.cnpj_cedente").alias("cnpj_cedente_raw"),
                                 col("cedente.percentual_concentracao")
                             )

    # Limpar o CNPJ da CVM (remover pontuações)
    df_cvm_unpivoted = df_cvm_unpivoted.withColumn("cnpj_cedente_limpo", regexp_replace(col("cnpj_cedente_raw"), "[^0-9]", ""))

    # Filtrar CNPJs válidos
    df_cvm_clean = df_cvm_unpivoted.filter(col("cnpj_cedente_limpo") != "")

    # 3. Cruzamento
    print("Executando join FIDC x RFB...")
    df_cruzamento = df_cvm_clean.join(
        df_rfb_completo,
        df_cvm_clean.cnpj_cedente_limpo == df_rfb_completo.cnpj_completo,
        "inner"
    )

    # Selecionar colunas finais
    cols_final = [
        "CNPJ_FUNDO_CLASSE",
        "fundo_denom_social",
        "DT_COMPTC",
        "percentual_concentracao",
        "cnpj_completo",
        "razao_social",
        "nome_fantasia",
        "uf",
        "municipio",
        "situacao_cadastral",
        "cnae_fiscal_principal",
        "data_inicio_atividade",
        "ddd_1",
        "telefone_1",
        "correio_eletronico"
    ]

    df_resultado_final = df_cruzamento.select(cols_final)

    # 4. Salvar resultado
    table_target = "LH_Gold.fato_empresas_target_cvm_concentracao"
    print(f"Salvando resultados do cruzamento global na tabela: {table_target}")

    df_resultado_final.write.format("delta").mode("overwrite").saveAsTable(table_target)
    print("Cruzamento concluído com sucesso!")

else:
    print("Colunas de cedente não encontradas no schema da CVM. Operação abortada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }