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

# # Notebook de Curadoria da Camada Gold (Refatorado)
# **Objetivo:** Orquestrar a criação dos modelos dimensionais na camada Gold.
# **Refatoração:** A lógica de transformação foi movida para `NB_Curadoria_Shared`.

# CELL ********************

# Import Shared Library
%run NB_Curadoria_Shared

# Configuração da Sessão Spark
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# Execute Incremental Check
check_incremental_gold(spark)

# MARKDOWN ********************

# ## Seção 0: Leitura de Dados (Silver Source)

# CELL ********************

print("Iniciando leitura da Silver...")

# --- 1. Titulos ---
df_titulos_limpa = spark.read.table(TableNames.SILVER_STAGING_TITULOS_LIMPA).cache()

# --- 2. Operacoes ---
df_operacoes_limpa = spark.read.table(TableNames.SILVER_STAGING_OPERACOES_LIMPA)

# --- 3. Baixas ---
df_baixas_staging = spark.read.table(TableNames.SILVER_STAGING_BAIXAS_LIMPA)

# --- 4. Cadastros ---
df_clientes_staging = spark.read.table(TableNames.SILVER_STAGING_CLIENTES_LIMPA)
df_geral_pf_pj_limpa = spark.read.table(TableNames.SILVER_STAGING_CAD_GERAL_PF_PJ_LIMPA)
df_enderecos_limpa = spark.read.table(TableNames.SILVER_STAGING_ENDERECOS_LIMPA).select(col("cpf_cnpj"), col("cidade"), col("uf"), col("cep"))
df_bridge_gerente = spark.read.table(TableNames.SILVER_BRIDGE_CLIENTE_GERENTE)
df_gerentes = spark.read.table(TableNames.SILVER_STAGING_GERENTES)
df_plataformas = spark.read.table(TableNames.SILVER_STAGING_PLATAFORMAS)
df_emails_agg = spark.read.table(TableNames.SILVER_STAGING_EMAILS_AGG)
df_telefones_agg = spark.read.table(TableNames.SILVER_STAGING_TELEFONES_AGG)

# --- 5. Support Tables ---
df_dim_pago_por = spark.read.table(TableNames.SILVER_SUP_PAGO_PELO)
df_dim_forma_pagamento = spark.read.table(TableNames.SILVER_SUP_FORMA_DE_PAGAMENTO)
df_dim_tipo_taxa = spark.read.table(TableNames.SILVER_SUP_TIPO_DE_BAIXA)
df_dim_motivo_baixa = spark.read.table(TableNames.SILVER_SUP_MOTIVO_BAIXA)

# --- 6. Other Lookups ---
df_cad_geral_arquivos = spark.read.table(TableNames.BRONZE_CAD_GERAL_ARQUIVOS)
df_limites = spark.read.table(TableNames.SILVER_STAGING_RLC_CLIENTES_SACADOS_LIMITES)
df_devolucoes = spark.read.table(TableNames.SILVER_STAGING_OPERACOES_DEVOLUCOES_LIMPA)
df_protestos = spark.read.table(TableNames.SILVER_STAGING_PROTESTOS)
df_ultima_conf = spark.read.table(TableNames.SILVER_FACT_ULTIMA_CONFIRMACAO)
df_dim_calendario = spark.read.table(TableNames.GOLD_DIM_CALENDARIO).cache()
df_contratos = spark.read.table(TableNames.SILVER_STAGING_CONTRATOS_CLIENTES_LIMPA)
df_grupos_economicos = spark.read.table(TableNames.SILVER_SUP_GRUPOS_ECONOMICOS)
df_usuarios = spark.read.table(TableNames.SILVER_STAGING_USUARIOS)
df_estudo_operacoes = spark.read.table(TableNames.SILVER_STAGING_ESTUDO_OPERACOES)
df_cad_clientes_bronze = spark.read.table(TableNames.BRONZE_CAD_CLIENTES)
df_sup_status = spark.read.table(TableNames.SILVER_SUP_STATUS_DE_CLIENTES_DA_ESTEIRA)

# Safe Reads / Fallbacks
df_limites_obs_silver = safe_read_table(spark, TableNames.SILVER_STG_LIMITES_CONTRATOS_SILVER, schema=StructType([
    StructField("codcliente", LongType(), True),
    StructField("limite_geral", DoubleType(), True),
    StructField("limite_intercompany", DoubleType(), True),
    StructField("limite_extra_desconto_formal", DoubleType(), True),
    StructField("limite_extra_desconto_informal", DoubleType(), True)
]))

df_dim_produto = safe_read_table(spark, TableNames.GOLD_DIM_PRODUTOS, schema=StructType([
    StructField("sk_produto", LongType(), True),
    StructField("chave_produto", StringType(), True),
    StructField("produto_informacao_de_mercado", StringType(), True)
])).cache()

df_limites_extra_plus = safe_read_table(spark, TableNames.SILVER_SUP_LIMITES_EXTRA_PLUS, schema=StructType([
    StructField("nome", StringType(), True),
    StructField("cnpj", StringType(), True),
    StructField("limite", DoubleType(), True),
    StructField("limite_extra", DoubleType(), True),
    StructField("limite_plus", DoubleType(), True)
]))

df_relatorio_juridico = safe_read_table(spark, TableNames.SILVER_RELATORIO_TITULOS_JURIDICO, schema=StructType([
    StructField("cod_titulo", LongType(), True)
]))

df_motivos_indeferimento = safe_read_table(spark, TableNames.SILVER_SUP_MOTIVOS_DE_INDEFERIMENTO, schema=StructType([
    StructField("cod_indeferimento", LongType(), True),
    StructField("motivo_indeferimento", StringType(), True),
    StructField("grupo_motivo_indeferimento", StringType(), True)
]))

try:
    df_escrow = spark.read.table(TableNames.SILVER_STAGING_OPERACOES_ESCROW).groupBy("cod_operacao").agg(max("ESCROW").alias("ESCROW"))
except Exception as e:
    print(f"AVISO: Tabela {TableNames.SILVER_STAGING_OPERACOES_ESCROW} não encontrada ({e}). Criando dataframe vazio.")
    df_escrow = spark.createDataFrame([], schema=StructType([StructField("cod_operacao", LongType(), True), StructField("ESCROW", BooleanType(), True)]))

print("Leitura da Silver concluída.")

# CELL ********************

# 1. Enriquecimento
print("\nIniciando etapa de enriquecimento...")
df_cad_geral_enriquecido = create_cad_geral_enriquecido(df_geral_pf_pj_limpa, df_enderecos_limpa, df_emails_agg, df_telefones_agg)

df_gerentes_enrich = create_gerentes_enriched(df_gerentes, df_usuarios, df_geral_pf_pj_limpa, df_plataformas)

df_operacoes_enriquecida = create_operacoes_enriquecida(
    df_operacoes_limpa, df_bridge_gerente, df_cad_geral_arquivos, df_titulos_limpa,
    df_usuarios, df_motivos_indeferimento, df_estudo_operacoes, df_gerentes_enrich,
    df_escrow, df_contratos
).cache()

# 2. Camada Gold (Fatos)
print("\nIniciando construção das Fatos...")

# Fato Operacoes
df_fato_operacoes = create_fato_operacoes(df_operacoes_enriquecida, df_dim_calendario, df_dim_produto).cache()
df_fato_operacoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_FATO_OPERACOES)
print(f"Tabela 'fato_operacoes' salva.")

# Fato Baixas
df_fato_baixas = create_fato_baixas(
    df_baixas_staging, df_titulos_limpa, df_dim_pago_por,
    df_dim_forma_pagamento, df_dim_tipo_taxa, df_dim_motivo_baixa
)
df_fato_baixas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_FATO_BAIXAS)
print(f"Tabela 'fato_baixas' salva.")

# Fato Titulos
df_fato_titulos_final = create_fato_titulos(
    df_titulos_limpa, df_operacoes_enriquecida, df_limites, df_dim_produto,
    df_devolucoes, df_ultima_conf, df_protestos, df_relatorio_juridico, df_dim_calendario
).cache()
df_fato_titulos_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_FATO_TITULOS)
print(f"Tabela 'fato_titulos' salva.")

# 3. Outras Fatos
print("\nIniciando construção de Fatos Adicionais...")

# Fato Tarifas Esporadicas (Copia Simples)
spark.read.table(TableNames.SILVER_STAGING_TARIFAS_ESPORADICAS).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_FATO_TARIFAS_ESPORADICAS)
print(f"Tabela 'fato_tarifas_esporadicas' salva.")

# Fato Prorrogacoes
df_final_prorrogacao = create_fato_prorrogacoes(spark, df_titulos_limpa, df_operacoes_limpa)
df_final_prorrogacao.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_FATO_PRORROGACOES_DE_TITULOS)
print(f"Tabela 'fato_prorrogacoes_de_titulos' salva.")

# Fato Operacoes Prorrogacao
df_final_pr = create_fato_operacoes_prorrogacao(spark, df_operacoes_limpa, df_titulos_limpa)
df_final_pr.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_FATO_OPERACOES_PRORROGACAO)
print(f"Tabela 'fato_operacoes_prorrogacao' salva.")

# Fato Recompra
df_final_rc = create_fato_recompra(spark, df_operacoes_limpa, df_titulos_limpa)
df_final_rc.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_FATO_OPERACOES_RECOMPRA)
print(f"Tabela 'fato_operacoes_recompra' salva.")

# 4. Dim Clientes e Fato Limites
print("\nIniciando construção da Dimensão Clientes e Fato Limites...")

# Fato Limites Credito
df_fato_limites = create_fato_limites_credito(
    df_contratos, df_limites_obs_silver, df_grupos_economicos, df_limites_extra_plus,
    df_clientes_staging, df_geral_pf_pj_limpa
)
df_fato_limites.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_FATO_LIMITES_CREDITO)
print(f"Tabela 'fato_limites_credito' salva.")

# Dim Clientes
df_dim_clientes, df_score_clientes, df_analise_prazos = create_dim_clientes(
    spark, df_clientes_staging, df_cad_geral_enriquecido, df_fato_operacoes, df_fato_titulos_final,
    df_grupos_economicos, df_bridge_gerente, df_gerentes_enrich, df_contratos,
    df_limites_extra_plus, df_limites_obs_silver, df_cad_clientes_bronze, df_sup_status,
    df_geral_pf_pj_limpa
)

df_score_clientes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_ANALISE_SCORE_CLIENTES)
print(f"Tabela 'analise_score_clientes' salva.")

df_analise_prazos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_ANALISE_PRAZOS_ESTEIRA)
print(f"Tabela 'analise_prazos_esteira' salva.")

df_dim_clientes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_DIM_CLIENTES)
print(f"Tabela 'dim_clientes' salva.")

# 5. HHI
print("\nIniciando cálculo do HHI...")
df_hhi_final = calculate_hhi_metrics(spark, df_fato_titulos_final)
df_hhi_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_METRICAS_CARTEIRA_HHI)
print(f"Métricas HHI salvas.")
