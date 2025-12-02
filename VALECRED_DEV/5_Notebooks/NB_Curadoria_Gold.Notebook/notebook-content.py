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
# **Objetivo:** Aplicar regras de negócio, realizar joins e criar os modelos dimensionais (Fatos e Dimensões) na camada **Gold**.
# **Refatoração:** Este notebook consome dados tratados da camada **Silver**, preparados pelos notebooks `NB_Prepara_Tabela_Cadastros`, `NB_Prepara_Tabela_Operacoes` e `NB_Prepara_Tabela_Titulos`.
# **Origem de Dados:** `LH_Silver` (Tabelas Staging Limpas) e `LH_Bronze` (Lookups/Cadastros menores).

# MARKDOWN ********************

# ## Seção 0: Configuração e Leitura de Dados (Silver Source)

# CELL ********************

# Célula 0.1: Configuração da Sessão Spark
# ------------------------------------
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, when, lit, concat, length, regexp_replace,
    collect_list, concat_ws, upper, greatest, substring, year,
    lead, date_add, lag, max, coalesce, broadcast, dayofweek, date_sub, trim
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from delta.tables import *
from functools import reduce
import datetime

# Célula 0.2: Leitura das Tabelas Preparadas (Silver)
# ----------------------------------------------------------------
print("Iniciando leitura da Silver...")

# --- 1. Titulos (Origem: LH_Silver.staging_titulos_limpa) ---
print("Carregando Titulos (Silver)...")
df_titulos_limpa = spark.read.table("LH_Silver.staging_titulos_limpa")
# A tabela já está limpa, desduplicada e com colunas renomeadas para snake_case.

# --- 2. Operacoes (Origem: LH_Silver.staging_operacoes_limpa) ---
print("Carregando Operacoes (Silver)...")
df_operacoes_limpa = spark.read.table("LH_Silver.staging_operacoes_limpa")
# Já possui snake_case e chave_produto.

# --- 3. Baixas (Origem: LH_Silver.staging_baixas_limpa) ---
print("Carregando Baixas (Silver)...")
# A tabela staging_baixas_limpa (Silver) mantém colunas em Uppercase do Bronze. Vamos renomear para snake_case aqui.
df_baixas_staging = spark.read.table("LH_Silver.staging_baixas_limpa").select(
    col("CODTITULOBAIXAS").alias("cod_titulo_baixas"),
    col("CODTITULO").alias("cod_titulo"),
    col("DATAINCLUSAO").alias("data_inclusao"),
    col("DATAALTERACAO").alias("data_alteracao"),
    col("VALORPAGO").alias("valor_pago"),
    col("DATABAIXA").alias("data_baixa"),
    col("DATABAIXASIST").alias("data_baixa_sist"),
    col("DESCONTO").alias("desconto"),
    col("JUROS").alias("juros"),
    col("TARIFARECOMPRA").alias("tarifa_recompra"),
    col("DATAVENCIMENTO").alias("data_vencimento"),
    col("PAGOPELO").alias("pago_pelo"),
    col("FORMA").alias("forma"),
    col("TIPOBAIXA").alias("tipo_baixa"),
    col("MOTIVO").alias("motivo"),
    col("CODOPERACAO") # Needed for join
)

# --- 4. Cadastros (Origem: LH_Silver...) ---
print("Carregando Cadastros (Silver)...")
# Clientes
df_clientes_staging = spark.read.table("LH_Silver.staging_clientes_limpa") # cod_cliente, cpf_cnpj

# Geral PF/PJ
df_geral_pf_pj_limpa = spark.read.table("LH_Silver.staging_cad_geral_pf_pj_limpa") # cpf_cnpj, nome, razao_social, nome_fantasia

# Endereços
df_enderecos_limpa = spark.read.table("LH_Silver.staging_enderecos_limpa").select(
    col("cpf_cnpj"), col("cidade"), col("uf"), col("cep")
)

# Bridge Gerente
df_bridge_gerente = spark.read.table("LH_Silver.bridge_cliente_gerente")

# Emails & Telefones Agg
df_emails_agg = spark.read.table("LH_Silver.staging_emails_agg")
df_telefones_agg = spark.read.table("LH_Silver.staging_telefones_agg")

# --- 5. Support Tables ---
df_dim_pago_por = spark.read.table("LH_Silver.sup_pago_pelo")
df_dim_forma_pagamento = spark.read.table("LH_Silver.sup_forma_de_pagamento")
df_dim_tipo_taxa = spark.read.table("LH_Silver.sup_tipo_de_baixa")
df_dim_motivo_baixa = spark.read.table("LH_Silver.sup_motivo_baixa")
df_status_clientes_esteira = spark.read.table("LH_Silver.sup_status_de_clientes_da_esteira")

# --- 6. Other Lookups ---
df_cad_geral_arquivos = spark.read.table("LH_Bronze.cad_geral_arquivos")
df_tipo_op_bronze = spark.read.table("LH_Bronze.tab_tipooperacao")
df_subtipo_op_bronze = spark.read.table("LH_Bronze.tab_subtipooperacao")
df_feriados = spark.read.table("LH_Bronze.tab_feriados")
df_pareceres_raw = spark.read.table("LH_Bronze.cad_geral_pareceres")
df_usuarios_raw = spark.read.table("LH_Bronze.cad_usuarios")

# Limites (Silver)
df_limites = spark.read.table("LH_Silver.staging_rlc_clientes_sacados_limites")

# Devolucoes (Silver) - Silver mantém Uppercase? NB_Prepara_Tabela_Operacoes não renomeia explicitamente.
# Vamos garantir rename.
try:
    df_devolucoes = spark.read.table("LH_Silver.staging_operacoes_devolucoes_limpa").withColumnRenamed("CODTITULO", "cod_titulo").withColumnRenamed("CODOPERACAO", "cod_operacao")
except:
    # Fallback to Bronze if Silver not populated yet
    df_devolucoes = spark.read.table("LH_Bronze.tab_operacoes_devolucoes").withColumnRenamed("CODTITULO", "cod_titulo").withColumnRenamed("CODOPERACAO", "cod_operacao")

# Protestos (Silver)
# NB_Prepara_Tabela_Titulos cria staging_protestos com CODTITULO, STATUS_PROTESTO.
df_protestos = spark.read.table("LH_Silver.staging_protestos").withColumnRenamed("CODTITULO", "cod_titulo").withColumnRenamed("STATUS_PROTESTO", "status_protesto")

df_ultima_conf = spark.read.table("LH_Silver.fact_ultima_confirmacao")

# Calendario (Gold)
df_dim_calendario = spark.read.table("LH_Gold.dim_calendario").cache()

print("Leitura da Silver concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Geração de DataFrames Intermediários (Enriquecimento)
# **Objetivo:** Criar as visões enriquecidas (`cad_geral` e `operacoes`) em memória.

# CELL ********************

# Célula 1.1: Cadastro Geral Enriquecido
# -----------------------------------------------------------------
print("Criando DataFrame intermediário: Cadastro Geral Enriquecido...")
df_cad_geral_enriquecido = df_geral_pf_pj_limpa \
    .join(df_enderecos_limpa, on="cpf_cnpj", how="left") \
    .join(df_emails_agg, on="cpf_cnpj", how="left") \
    .join(df_telefones_agg, on="cpf_cnpj", how="left")

# Célula 1.2: Operações Enriquecidas
# -----------------------------------------------------------
print("Criando DataFrame intermediário: Operações Enriquecidas...")
# Enriquecimento com Gerente (Broker)
# Silver bridge tem "ClienteID", Ops tem "cod_cliente".
df_operacoes_com_historico = df_operacoes_limpa.join(
    df_bridge_gerente,
    (df_operacoes_limpa["cod_cliente"] == df_bridge_gerente["ClienteID"]) &
    (df_operacoes_limpa["data_analise"].cast("date") >= df_bridge_gerente["DataInicioVigencia"]) &
    (df_operacoes_limpa["data_analise"].cast("date") <= df_bridge_gerente["DataFimVigencia"]),
    "left"
)
df_operacoes_com_gerente = df_operacoes_com_historico.withColumn(
    "cod_broker",
    when((col("cod_broker").isNotNull()) & (col("cod_broker") != 0), col("cod_broker")).otherwise(col("GerenteID"))
).drop("ClienteID", "GerenteID", "DataInicioVigencia", "DataFimVigencia")

# Identificação de Operações Informais
df_chave_danfe = df_cad_geral_arquivos.filter(col("DESCRICAO") == 'CHAVEDANFE')
df_titulos_com_chave = df_titulos_limpa.join(df_chave_danfe, df_titulos_limpa.cod_titulo == df_chave_danfe.CODTITULO, how="inner")
df_operacoes_com_chave_base = df_operacoes_com_gerente.join(df_titulos_com_chave, on="cod_operacao", how="inner")
df_operacoes_com_chave_filtrado = df_operacoes_com_chave_base.filter(
    (df_operacoes_com_gerente["nota_servico"] == 'N') &
    (df_operacoes_com_gerente["status_analise"] == 'D') &
    (df_operacoes_com_gerente["cod_empresa"] == 14) &
    (df_operacoes_com_gerente["status_aceite"] == 'A') &
    (df_operacoes_com_gerente["tto"].isin(['NO','CM','FC']))
)
df_vcount = df_operacoes_com_chave_filtrado.groupBy(df_operacoes_com_gerente["cod_operacao"]).count()
df_com_vcount = df_operacoes_com_gerente.join(df_vcount, on="cod_operacao", how="left")

df_operacoes_enriquecida = df_com_vcount.withColumn(
    "operacao_informal",
    when(
        ((col("count").isNull()) | (col("count") == 0)) & (col("cod_empresa") == 14) & (col("nota_servico") == 'N'),
        lit(True)
    ).otherwise(lit(False))
).drop("count").cache()
print("DataFrames intermediários criados e cacheados.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Construção das Tabelas da Camada Gold

# CELL ********************

# Célula 2.1: Construção da Fato Operações
# ----------------------------------------
print("\nIniciando construção da fato_operacoes...")

# Adicionando a sk_data para join com dim_calendario
df_fato_operacoes_joined = df_operacoes_enriquecida.join(
    broadcast(df_dim_calendario.select("data", "sk_data")),
    df_operacoes_enriquecida["data_inclusao"].cast("date") == df_dim_calendario["data"],
    "left"
)

df_fato_operacoes = df_fato_operacoes_joined.select(
    col("cod_operacao"),
    col("cod_cliente"),
    col("cod_empresa"),
    col("data_inclusao"),
    col("data_analise"),
    col("status_aceite"),
    col("status_analise"),
    col("cod_broker"),
    col("tto"),
    col("stto"),
    col("chave_produto"),
    col("operacao_informal"),
    col("valor_retido"),
    col("valor_desembolsado"),
    col("valor_de_face"),
    col("desagio"),
    col("total_de_tarifas"),
    col("sk_data"),
    col("valor_recomprado")
)
output_path_fato_operacoes = "LH_Gold.fato_operacoes"
df_fato_operacoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_fato_operacoes)
print(f"Tabela 'fato_operacoes' salva em: {output_path_fato_operacoes}")


# Célula 2.2: Construção da Fato Baixas
# -------------------------------------
print("\nIniciando construção da fato_baixas...")
# Apply manual fixes (Mantido para correções de negócio específicas)
df_baixas_corrigido = df_baixas_staging.withColumn("juros",
    when(col("juros") == -858005.8, 3912.5).when(col("juros") == -4948525.71, -56747.24)
    .when(col("juros") == -4140.75, 0).when(col("juros") == -1447.5, 52.5)
    .when(col("juros") == -1825.72, 66.28).when(col("juros") == -965, 35)
    .when(col("juros") == -26000, 0).otherwise(col("juros"))
)
df_enriquecido_baixas = df_baixas_corrigido \
    .join(df_titulos_limpa.select("cod_titulo", "cod_operacao"), on="cod_titulo", how="left") \
    .join(broadcast(df_dim_pago_por), df_baixas_corrigido.pago_pelo == df_dim_pago_por.id, how="left") \
    .join(broadcast(df_dim_forma_pagamento), df_baixas_corrigido.forma == df_dim_forma_pagamento.id, how="left") \
    .join(broadcast(df_dim_tipo_taxa), df_baixas_corrigido.tipo_baixa == df_dim_tipo_taxa.id, how="left") \
    .join(broadcast(df_dim_motivo_baixa), df_baixas_corrigido.motivo == df_dim_motivo_baixa.id, how="left")

df_fato_baixas = df_enriquecido_baixas.select(
    "cod_titulo_baixas", "cod_titulo", "data_baixa", "data_baixa_sist", "valor_pago",
    "desconto", "juros", "tarifa_recompra", "data_vencimento", df_baixas_corrigido["CODOPERACAO"],
    df_dim_pago_por["descricao"].alias("PagoPor"), df_dim_forma_pagamento["descricao"].alias("Forma"),
    df_dim_tipo_taxa["descricao"].alias("TipoBaixa"), df_dim_motivo_baixa["descricao"].alias("Motivo")
)
output_path_fato_baixas = "LH_Gold.fato_baixas"
df_fato_baixas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_fato_baixas)
print(f"Tabela 'fato_baixas' salva em: {output_path_fato_baixas}")


# Célula 2.3: Construção da Dimensão Produto
# ------------------------------------------
print("\nIniciando construção da dim_produto...")
df_produtos_base = df_operacoes_enriquecida.select("stto", "tto").distinct()
df_com_tipo = df_produtos_base.join(broadcast(df_tipo_op_bronze.select("CODTTO", "DESCRICAO")), df_produtos_base.tto == df_tipo_op_bronze.CODTTO, "left").withColumnRenamed("DESCRICAO", "TipoProduto")
df_com_subtipo = df_com_tipo.join(broadcast(df_subtipo_op_bronze.select("CODSTTO", "DESCRICAO")), df_com_tipo.stto == df_subtipo_op_bronze.CODSTTO, "left").withColumnRenamed("DESCRICAO", "SubTipoProduto")
df_com_chaves = df_com_subtipo.withColumn("chave_produto", concat(col("tto"), col("stto"))).withColumn("Produto", when(col("SubTipoProduto").isNull(), col("TipoProduto")).otherwise(concat(col("SubTipoProduto"), lit(" - "), col("TipoProduto"))))
df_nomes_limpos = df_com_chaves.withColumn("Produto", regexp_replace(col("Produto"), "COMISSÁRIA", "COMISSARIA SIMPLES")).withColumn("Produto", regexp_replace(col("Produto"), "COMISSARIA SIMPLES - COMISSARIA SIMPLES", "COMISSARIA SIMPLES"))
df_info_mercado = df_nomes_limpos.withColumn("ProdutoInformacaoMercado", col("Produto")).withColumn("ProdutoInformacaoMercado", regexp_replace(col("ProdutoInformacaoMercado"), "NORMAL", "DESCONTO"))
df_staging_produto_lbfactor = df_info_mercado.select("ProdutoInformacaoMercado", "Produto", "chave_produto")
df_filtrado = df_staging_produto_lbfactor.filter(col("Produto").isNotNull() & (col("Produto") != ""))
window_dedup = Window.partitionBy("chave_produto").orderBy(col("Produto").asc())
df_deduplicado = df_filtrado.withColumn("rn", row_number().over(window_dedup)).filter(col("rn") == 1).drop("rn")
window_spec_sk = Window.orderBy("chave_produto")
df_com_sk = df_deduplicado.sort("chave_produto").withColumn("sk_produto", row_number().over(window_spec_sk))
df_dim_produto_final = df_com_sk.select(col("sk_produto"), col("chave_produto"), col("Produto").alias("produto"), col("ProdutoInformacaoMercado").alias("produto_informacao_de_mercado"))

output_path_dim_produto = "LH_Gold.dim_produto"
df_dim_produto_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_dim_produto)
df_dim_produto = spark.read.table(output_path_dim_produto).cache()
print(f"Tabela 'dim_produto' salva e em cache em: {output_path_dim_produto}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: Construção da Fato Títulos (Otimizada)

# CELL ********************

print("\nIniciando construção da fato_titulos...")
# 3.1 Preparação e Enriquecimento
# --------------------------------------------------------------
df_titulos_base = df_titulos_limpa.filter(~col("t_doc").isin("BL", "RC")) \
    .withColumn("TipoDocumentoSacado", when(length(col("cpf_cnpj_sacado")) == 11, "CPF").when(length(col("cpf_cnpj_sacado")) == 14, "CNPJ").otherwise("Inválido")) \
    .withColumn("RaizCNPJ", when(col("TipoDocumentoSacado") == "CNPJ", substring(col("cpf_cnpj_sacado"), 1, 8)).otherwise(col("cpf_cnpj_sacado")))

df_operacoes_small = df_operacoes_enriquecida.select("cod_operacao", "cod_cliente", "data_analise", "status_aceite", "status_analise", "chave_produto")
df_limites_small = df_limites.select("chave_cliente_sacado", "tipo")
df_produtos_small = df_dim_produto.select(col("chave_produto"), col("produto_informacao_de_mercado").alias("produto_temp"))
df_devolucoes_small = df_devolucoes.select(col("cod_titulo"), col("cod_operacao").alias("cod_operacao_recompra"))
df_ultima_conf_small = df_ultima_conf.select(col("cod_titulo"), col("confirmacao").alias("confirmado_por"))
df_protestos_small = df_protestos.select("cod_titulo", "status_protesto")

df_titulos_com_chave_sacado = df_titulos_base.join(broadcast(df_operacoes_small), "cod_operacao", "left").withColumn("chave_cliente_sacado", concat(col("cod_cliente").cast("string"), lit("-"), col("RaizCNPJ")))

df_enriquecido = df_titulos_com_chave_sacado \
    .join(broadcast(df_limites_small), "chave_cliente_sacado", "left") \
    .join(broadcast(df_produtos_small), "chave_produto", "left") \
    .join(broadcast(df_devolucoes_small), "cod_titulo", "left") \
    .join(broadcast(df_ultima_conf_small), "cod_titulo", "left") \
    .join(broadcast(df_protestos_small), "cod_titulo", "left") \
    .na.fill({"amortizacoes": 0})

# 3.2 Cálculos de Negócio
# -----------------------
df_com_calcs = df_enriquecido \
    .withColumn("intercompany", when(col("tipo") == "INTERCIA", "SIM").otherwise("NÃO")) \
    .withColumn("status_protesto", coalesce(col("status_protesto"), lit("NÃO PROTESTADO"))) \
    .withColumn("valor_vezes_prazo", col("prazo") * col("valor")) \
    .withColumn("produto_com_intercia", when((col("intercompany") == "SIM") & (col("chave_produto").isin("NO", "CM")), "INTERCOMPANY").otherwise(col("produto_temp")))

# Data Vencimento Útil
try:
    df_dim_cal_dates = df_dim_calendario.select(col("data"), col("proximo_dia_util"))
    df_dates_final = df_com_calcs.join(broadcast(df_dim_cal_dates), df_com_calcs.venc_prorrogado == df_dim_cal_dates.data, "left").withColumnRenamed("proximo_dia_util", "data_vencimento_util").drop("data")
except Exception as e:
    print(f"AVISO: Erro ao ler dim_calendario: {e}.")
    df_dates_final = df_com_calcs.withColumn("data_vencimento_util", col("venc_prorrogado"))

df_status_1 = df_dates_final.withColumn("status_deferimento", when((col("aceito") == "S") & (col("status_aceite") == "A") & (col("status_analise") == "D"), "Sim").otherwise("Não"))
df_status_2 = df_status_1.withColumn("status_clean", when(col("produto_com_intercia") == "DESCONTO", "NORMAL").otherwise("CLEAN"))

# Confirmacao Logic using Bronze column or Fallback
df_conf = df_status_2.withColumn("confirmacao", when(col("doc_confirmado") == "N", "Atenção").when(col("doc_confirmado") == "S", None).when(col("doc_confirmado") == "C", "Positivo").when(col("doc_confirmado") == "P", "Problema").when(col("doc_confirmado") == "A", "Alerta").when(col("doc_confirmado").isNull(), "Não Contatado").when(col("doc_confirmado").isin("E", "AZ"), "Eletrônico").otherwise(col("doc_confirmado")))
df_ordem = df_conf.withColumn("ordem_confirmacao", when(col("confirmacao") == "Não Contatado", 5).when(col("confirmacao") == "Atenção", 2).when(col("confirmacao") == "Eletrônico", 0).when(col("confirmacao") == "Positivo", 1).when(col("confirmacao") == "Alerta", 3).when(col("confirmacao") == "Problema", 4).otherwise(None))

# 3.3 Seleção Final e Persistência
# ---------------------------------
df_fato_titulos_final = df_ordem.select(
    col("cod_titulo"), col("cod_operacao"), col("t_doc"), col("n_doc"), col("cpf_cnpj_sacado"), col("vencimento"), col("venc_prorrogado"), col("valor"),
    col("prazo"), col("aceito"), col("data_inclusao"), col("usua_conf").alias("usua_inclusao"), col("data_alteracao"), col("amortizacoes"),
    "chave_produto", "status_protesto", "TipoDocumentoSacado", "RaizCNPJ", "valor_vezes_prazo",
    "produto_com_intercia", "data_vencimento_util", "status_deferimento", "status_clean",
    "confirmacao", "ordem_confirmacao", "cod_operacao_recompra", "confirmado_por", "intercompany"
)
output_path_titulos_final = "LH_Gold.fato_titulos"
df_fato_titulos_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_titulos_final)
print(f"Tabela 'fato_titulos' salva em: {output_path_titulos_final}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 4: Esteira de Propostas (Lógica Incremental)

# CELL ********************

# Célula 4.1: Configuração e Watermark
# ------------------------------------------------
print("\nIniciando o processamento incremental de pareceres...")
target_pareceres_status_table_name = "LH_Silver.pareceres_de_alteracao_de_status"
target_esteira_table_name = "LH_Gold.esteira_de_propostas"
watermark_table_name = "LH_Silver.etl_watermark_control"
notebook_name = "NB_Curadoria_Gold"

try:
    df_watermark = spark.read.table(watermark_table_name)
    last_watermark_str = df_watermark.filter(col("TableName") == notebook_name).select("LastWatermarkValue").collect()[0][0]
    last_watermark = datetime.datetime.strptime(last_watermark_str.split('.')[0], "%Y-%m-%d %H:%M:%S")
    print(f"Watermark encontrado: {last_watermark}")
except Exception:
    last_watermark = datetime.datetime(1900, 1, 1)
    print(f"Usando watermark padrão: {last_watermark}.")

# Célula 4.2: Leitura e Processamento Incremental
# ------------------------------------------------
df_pareceres_incremental = df_pareceres_raw.filter((col("DATAINCLUSAO") > last_watermark) | (col("DATAALTERACAO") > last_watermark)).cache()
record_count = df_pareceres_incremental.count()

if record_count > 0:
    new_watermark = df_pareceres_incremental.agg(max(greatest(coalesce(col("DATAINCLUSAO"), lit(datetime.datetime(1900,1,1))), coalesce(col("DATAALTERACAO"), lit(datetime.datetime(1900,1,1)))))).collect()[0][0]
    print(f"Registros incrementais: {record_count}. Novo watermark: {new_watermark}")

    df_replica_pareceres_delta = df_pareceres_incremental.filter(year(col("DATAINCLUSAO")) >= 2024).drop("ENCAMINHAR", "ALERTA", "CODPASTA", "CODTAREFA", "USUAALTERACAO", "DATAALTERACAO").withColumn("OBS", col("OBS").substr(1, 255)).withColumn("codTipoParecer", col("CODTIPOPARECER").cast(LongType())).filter((col("codTipoParecer") == 1) & (col("CPFCNPJ").isNotNull()) & (col("CPFCNPJ") != "") & (col("OBS").isNotNull()) & (col("OBS") != "") & (col("USUAINCLUSAO").isNotNull()) & (col("DATAINCLUSAO").isNotNull())).filter(col("OBS").startswith("STATUS ALTERADO PARA ")).withColumn("STATUS_DO_CLIENTE", trim(substring(col("OBS"), 22, 100))).withColumn("BASE", lit(40).cast(LongType())).select("CODPARECER", "CPFCNPJ", "CODOPERACAO", "DATAINCLUSAO", "USUAINCLUSAO", "STATUS_DO_CLIENTE", "BASE")

    # Recuperar MAX INDICE por Cliente da tabela alvo, se existir, para continuar a sequencia
    if spark.catalog.tableExists(target_pareceres_status_table_name):
        df_max_indices = spark.read.table(target_pareceres_status_table_name).groupBy("cod_cliente").agg(max("INDICE").alias("max_indice"))
    else:
        df_max_indices = None

    # Enriquecimento e Calculo de Indice
    df_joined = df_replica_pareceres_delta.join(df_clientes_staging.select("cpf_cnpj", "cod_cliente"), df_replica_pareceres_delta.CPFCNPJ == df_clientes_staging.cpf_cnpj, "left")

    if df_max_indices:
        df_joined = df_joined.join(df_max_indices, "cod_cliente", "left").withColumn("start_index", coalesce(col("max_indice"), lit(0)))
    else:
        df_joined = df_joined.withColumn("start_index", lit(0))

    window_cliente_data_delta = Window.partitionBy("cod_cliente").orderBy(col("DATAINCLUSAO").asc())

    df_pareceres_enriquecidos_delta = df_joined \
        .withColumn("chave_base_cliente", concat(col("BASE").cast("string"), lit("-"), col("cod_cliente").cast("string"))) \
        .join(df_usuarios_raw.select("CODUSUARIO", "NOME"), col("USUAINCLUSAO") == col("CODUSUARIO"), "left") \
        .withColumnRenamed("NOME", "USUARIO") \
        .join(df_status_clientes_esteira, "STATUS_DO_CLIENTE", "left") \
        .filter(col("cod_cliente").isNotNull() & (col("cod_cliente") != "")) \
        .withColumn("INDICE", col("start_index") + row_number().over(window_cliente_data_delta)) \
        .withColumn("chave_original", (col("INDICE") * 1000000000 + col("cod_cliente")).cast(LongType())) \
        .withColumnRenamed("DATAINCLUSAO", "DATALOG") \
        .select("CODPARECER", "cod_cliente", "STATUS_DO_CLIENTE", "DATALOG", "BASE", "USUARIO", "chave_base_cliente", "INDICE", "chave_original", "MACROPROCESSO", "FASE")

    if spark.catalog.tableExists(target_pareceres_status_table_name):
        DeltaTable.forName(spark, target_pareceres_status_table_name).alias("t").merge(df_pareceres_enriquecidos_delta.alias("s"), "t.CODPARECER = s.CODPARECER").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df_pareceres_enriquecidos_delta.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_pareceres_status_table_name)
else:
    new_watermark = last_watermark
    print("Nenhum dado novo encontrado.")

df_pareceres_incremental.unpersist()

# Célula 4.3: Reconstrução da Esteira e Atualização do Watermark
# -------------------------------------------------------------
if record_count > 0:
    print("Reconstruindo esteira_de_propostas...")
    df_pareceres_completa = spark.read.table(target_pareceres_status_table_name)
    window_lag = Window.partitionBy("cod_cliente").orderBy("DATALOG")
    df_com_lag = df_pareceres_completa.withColumn("STATUS_DO_CLIENTE_ANTERIOR", lag("STATUS_DO_CLIENTE").over(window_lag)).withColumn("DATALOG_ANTERIOR", lag("DATALOG").over(window_lag)).withColumn("MACROPROCESSO_ANTERIOR", lag("MACROPROCESSO").over(window_lag)).withColumn("FASE_ANTERIOR", lag("FASE").over(window_lag))
    df_transicoes = df_com_lag.filter(col("STATUS_DO_CLIENTE") != col("STATUS_DO_CLIENTE_ANTERIOR")).na.drop(subset=["STATUS_DO_CLIENTE_ANTERIOR"])
    df_esteira_final = df_transicoes.withColumn("DEVOLUCAO", when((col("MACROPROCESSO_ANTERIOR") == "CREDITO") & (col("MACROPROCESSO") == "COMERCIAL"), True).otherwise(False)).withColumn("RECEBIDA", when((col("MACROPROCESSO_ANTERIOR") == "COMERCIAL") & (col("MACROPROCESSO") == "CREDITO"), True).otherwise(False)).select("INDICE", "cod_cliente", "BASE", "DATALOG_ANTERIOR", "DATALOG", "chave_base_cliente", "STATUS_DO_CLIENTE_ANTERIOR", "STATUS_DO_CLIENTE", "MACROPROCESSO_ANTERIOR", "MACROPROCESSO", "FASE_ANTERIOR", "FASE", "USUARIO", "DEVOLUCAO", "RECEBIDA")
    df_esteira_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_esteira_table_name)
    print("Esteira reconstruída.")

    print("Atualizando watermark...")
    df_new_watermark = spark.createDataFrame([(notebook_name, new_watermark.strftime("%Y-%m-%d %H:%M:%S.%f"))], ["TableName", "LastWatermarkValue"])
    if spark.catalog.tableExists(watermark_table_name):
        DeltaTable.forName(spark, watermark_table_name).alias("t").merge(df_new_watermark.alias("s"), "t.TableName = s.TableName").whenMatchedUpdate(set={"LastWatermarkValue": "s.LastWatermarkValue"}).whenNotMatchedInsert(values={"TableName": "s.TableName", "LastWatermarkValue": "s.LastWatermarkValue"}).execute()
    else:
        df_new_watermark.write.mode("overwrite").saveAsTable(watermark_table_name)

print("Processo Gold Otimizado concluído.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
