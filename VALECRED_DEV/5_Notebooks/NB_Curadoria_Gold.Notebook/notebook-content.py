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

# # Notebook de Curadoria da Camada Gold (Otimizado)
# **Objetivo:** Aplicar regras de negócio, realizar joins e criar os modelos dimensionais (Fatos e Dimensões) na camada **Gold**.
# **Arquitetura:** Este notebook lê das camadas **Bronze** e **Silver** e escreve primariamente na camada **Gold**. Ele também atualiza a tabela de controle de pareceres (`pareceres_de_alteracao_de_status`) na camada Silver como parte da lógica incremental da esteira de propostas.
# **Otimizações:**
# 1.  **Leitura Centralizada:** Todas as fontes de dados são lidas no início para evitar I/O repetido.
# 2.  **Cache Estratégico:** DataFrames reutilizados são armazenados em cache.
# 3.  **Broadcast Joins:** Joins entre tabelas grandes (fatos) e pequenas (dimensões) são otimizados com a técnica de broadcast.
# 4.  **Limpeza de Cache:** O cache é liberado no final para otimizar o uso de recursos.

# MARKDOWN ********************

# ## Seção 0: Configuração e Leitura de Dados

# CELL ********************

# Célula 0.1: Configuração da Sessão Spark
# ------------------------------------
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, when, lit, concat, length, regexp_replace,
    collect_list, concat_ws, upper, greatest, substring, year,
    lead, date_add, lag, max, coalesce, broadcast, dayofweek
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from delta.tables import *
import datetime

# Célula 0.2: Leitura e Cache das Fontes de Dados
# ------------------------------------------------
print("Iniciando leitura e cache das fontes de dados...")

# Lista para limpeza de cache no final
dataframes_to_uncache = []

# ---- Camada Silver (Tabelas de Staging) ----
df_geral_pf_pj_limpa = spark.read.table("LH_Silver.staging_cad_geral_pf_pj_limpa")
df_enderecos_limpa = spark.read.table("LH_Silver.staging_enderecos_limpa")
df_emails_agg = spark.read.table("LH_Silver.staging_emails_agg")
df_telefones_agg = spark.read.table("LH_Silver.staging_telefones_agg")
df_operacoes_limpa = spark.read.table("LH_Silver.staging_operacoes_limpa")
df_bridge_gerente = spark.read.table("LH_Silver.bridge_cliente_gerente")
df_baixas_staging = spark.read.table("LH_Silver.staging_baixas_limpa")
df_limites = spark.read.table("LH_Silver.staging_rlc_clientes_sacados_limites")
df_devolucoes = spark.read.table("LH_Silver.staging_operacoes_devolucoes_limpa")
df_protestos = spark.read.table("LH_Silver.staging_protestos")
df_ultima_conf = spark.read.table("LH_Silver.fact_ultima_confirmacao")
df_clientes_staging = spark.read.table("LH_Silver.staging_clientes_limpa").cache()
dataframes_to_uncache.append("df_clientes_staging")
df_titulos_limpa = spark.read.table("LH_Silver.staging_titulos_limpa").cache()
dataframes_to_uncache.append("df_titulos_limpa")


# ---- Tabelas de Suporte (Silver) ----
df_dim_pago_por = spark.read.table("LH_Silver.sup_pago_pelo")
df_dim_forma_pagamento = spark.read.table("LH_Silver.sup_forma_de_pagamento")
df_dim_tipo_taxa = spark.read.table("LH_Silver.sup_tipo_de_baixa")
df_dim_motivo_baixa = spark.read.table("LH_Silver.sup_motivo_baixa")
df_status_clientes_esteira = spark.read.table("LH_Silver.sup_status_de_clientes_da_esteira")

# ---- Camada Bronze (Dados Brutos para Lógicas Específicas) ----
df_cad_geral_arquivos = spark.read.table("LH_Bronze.cad_geral_arquivos")
df_tipo_op_bronze = spark.read.table("LH_Bronze.tab_tipooperacao")
df_subtipo_op_bronze = spark.read.table("LH_Bronze.tab_subtipooperacao")
df_feriados = spark.read.table("LH_Bronze.tab_feriados")
df_pareceres_raw = spark.read.table("LH_Bronze.cad_geral_pareceres")
df_usuarios_raw = spark.read.table("LH_Bronze.cad_usuarios")

print("Leitura e cache iniciais concluídos.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Geração de DataFrames Intermediários (em Memória)
# **Objetivo:** Criar as visões enriquecidas (`cad_geral` e `operacoes`) em memória. Estes DataFrames serão usados para construir as tabelas Gold, mas **não serão persistidos na camada Silver**, garantindo a conformidade com a arquitetura.

# CELL ********************

# Célula 1.1: DataFrame Intermediário: Cadastro Geral Enriquecido
# -----------------------------------------------------------------
print("Criando DataFrame intermediário: Cadastro Geral Enriquecido...")
df_cad_geral_enriquecido = df_geral_pf_pj_limpa \
    .join(df_enderecos_limpa.select("CPFCNPJ", "CIDADE", "UF", "CEP"), on="CPFCNPJ", how="left") \
    .join(df_emails_agg, on="CPFCNPJ", how="left") \
    .join(df_telefones_agg, on="CPFCNPJ", how="left")

# Célula 1.2: DataFrame Intermediário: Operações Enriquecidas
# -----------------------------------------------------------
print("Criando DataFrame intermediário: Operações Enriquecidas...")
# Enriquecimento com Gerente (Broker)
df_operacoes_com_historico = df_operacoes_limpa.join(
    df_bridge_gerente,
    (df_operacoes_limpa["CODCLIENTE"] == df_bridge_gerente["ClienteID"]) &
    (df_operacoes_limpa["DATAANALISE"].cast("date") >= df_bridge_gerente["DataInicioVigencia"]) &
    (df_operacoes_limpa["DATAANALISE"].cast("date") <= df_bridge_gerente["DataFimVigencia"]),
    "left"
)
df_operacoes_com_gerente = df_operacoes_com_historico.withColumn(
    "CODBROKER",
    when((col("CODBROKER").isNotNull()) & (col("CODBROKER") != 0), col("CODBROKER")).otherwise(col("GerenteID"))
).drop("ClienteID", "GerenteID", "DataInicioVigencia", "DataFimVigencia")

# Identificação de Operações Informais
df_chave_danfe = df_cad_geral_arquivos.filter(col("DESCRICAO") == 'CHAVEDANFE')
df_titulos_com_chave = df_titulos_limpa.join(df_chave_danfe, on="CODTITULO", how="inner")
df_operacoes_com_chave_base = df_operacoes_com_gerente.join(df_titulos_com_chave, on="CODOPERACAO", how="inner")
df_operacoes_com_chave_filtrado = df_operacoes_com_chave_base.filter(
    (df_operacoes_com_gerente["NOTASERVICO"] == 'N') &
    (df_operacoes_com_gerente["STATUSANALISE"] == 'D') &
    (df_operacoes_com_gerente["CODEMPRESA"] == 14) &
    (df_operacoes_com_gerente["STATUSACEITE"] == 'A') &
    (df_operacoes_com_gerente["TTO"].isin(['NO','CM','FC']))
)
df_vcount = df_operacoes_com_chave_filtrado.groupBy(df_operacoes_com_gerente["CODOPERACAO"]).count()
df_com_vcount = df_operacoes_com_gerente.join(df_vcount, on="CODOPERACAO", how="left")

df_operacoes_enriquecida = df_com_vcount.withColumn(
    "operacao_informal",
    when(
        ((col("count").isNull()) | (col("count") == 0)) & (col("CODEMPRESA") == 14) & (col("NOTASERVICO") == 'N'),
        lit(True)
    ).otherwise(lit(False))
).drop("count").cache() # Cache: reutilizado em Fato Operações, Dim Produto e Fato Títulos
dataframes_to_uncache.append("df_operacoes_enriquecida")
print("DataFrames intermediários criados e cacheados com sucesso.")

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
df_fato_operacoes = df_operacoes_enriquecida.select(
    col("CODOPERACAO").alias("cod_operacao"),
    col("CODCLIENTE").alias("cod_cliente"),
    col("CODEMPRESA").alias("cod_empresa"),
    col("DATAINCLUSAO").alias("data_inclusao"),
    col("DATAANALISE").alias("data_analise"),
    col("STATUSACEITE").alias("status_aceite"),
    col("STATUSANALISE").alias("status_analise"),
    col("CODBROKER").alias("cod_broker"),
    col("TTO"),
    col("STTO"),
    col("chave_produto"),
    col("operacao_informal")
)
output_path_fato_operacoes = "LH_Gold.fato_operacoes"
df_fato_operacoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_fato_operacoes)
print(f"Tabela 'fato_operacoes' salva em: {output_path_fato_operacoes}")


# Célula 2.2: Construção da Fato Baixas
# -------------------------------------
print("\nIniciando construção da fato_baixas...")
df_baixas_corrigido = df_baixas_staging.withColumn("JUROS",
    when(col("JUROS") == -858005.8, 3912.5).when(col("JUROS") == -4948525.71, -56747.24)
    .when(col("JUROS") == -4140.75, 0).when(col("JUROS") == -1447.5, 52.5)
    .when(col("JUROS") == -1825.72, 66.28).when(col("JUROS") == -965, 35)
    .when(col("JUROS") == -26000, 0).otherwise(col("JUROS"))
)
df_enriquecido_baixas = df_baixas_corrigido \
    .join(df_titulos_limpa.select("CODTITULO", "CODOPERACAO"), on="CODTITULO", how="left") \
    .join(broadcast(df_dim_pago_por), df_baixas_corrigido.PAGOPELO == df_dim_pago_por.id, how="left") \
    .join(broadcast(df_dim_forma_pagamento), df_baixas_corrigido.FORMA == df_dim_forma_pagamento.id, how="left") \
    .join(broadcast(df_dim_tipo_taxa), df_baixas_corrigido.TIPOBAIXA == df_dim_tipo_taxa.id, how="left") \
    .join(broadcast(df_dim_motivo_baixa), df_baixas_corrigido.MOTIVO == df_dim_motivo_baixa.id, how="left")

df_fato_baixas = df_enriquecido_baixas.select(
    "CODTITULOBAIXAS", "CODTITULO", "DATABAIXA", "DATABAIXASIST", "VLPAGO",
    "DESCONTO", "JUROS", "TARIFARECOMPRA", "DATAVENCIMENTO", df_baixas_corrigido["CODOPERACAO"],
    df_dim_pago_por["descricao"].alias("PagoPor"), df_dim_forma_pagamento["descricao"].alias("Forma"),
    df_dim_tipo_taxa["descricao"].alias("TipoBaixa"), df_dim_motivo_baixa["descricao"].alias("Motivo")
)
output_path_fato_baixas = "LH_Gold.fato_baixas"
df_fato_baixas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_fato_baixas)
print(f"Tabela 'fato_baixas' salva em: {output_path_fato_baixas}")


# Célula 2.3: Construção da Dimensão Produto
# ------------------------------------------
print("\nIniciando construção da dim_produto...")
df_produtos_base = df_operacoes_enriquecida.select("STTO", "TTO").distinct()
df_com_tipo = df_produtos_base.join(broadcast(df_tipo_op_bronze.select("CODTTO", "DESCRICAO")), df_produtos_base.TTO == df_tipo_op_bronze.CODTTO, "left").withColumnRenamed("DESCRICAO", "TipoProduto")
df_com_subtipo = df_com_tipo.join(broadcast(df_subtipo_op_bronze.select("CODSTTO", "DESCRICAO")), df_com_tipo.STTO == df_subtipo_op_bronze.CODSTTO, "left").withColumnRenamed("DESCRICAO", "SubTipoProduto")
df_com_chaves = df_com_subtipo.withColumn("chave_produto", concat(col("TTO"), col("STTO"))).withColumn("Produto", when(col("SubTipoProduto").isNull(), col("TipoProduto")).otherwise(concat(col("SubTipoProduto"), lit(" - "), col("TipoProduto"))))
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
df_dim_produto = spark.read.table(output_path_dim_produto).cache() # Cache para uso na Fato Títulos
dataframes_to_uncache.append("df_dim_produto")
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
# 3.1 Preparação e Enriquecimento (Otimizado com Broadcast Joins)
# --------------------------------------------------------------
df_titulos_base = df_titulos_limpa.filter(~col("TDOC").isin("BL", "RC")) \
    .withColumn("TipoDocumentoSacado", when(length(col("CPFCNPJSACADO")) == 11, "CPF").when(length(col("CPFCNPJSACADO")) == 14, "CNPJ").otherwise("Inválido")) \
    .withColumn("RaizCNPJ", when(col("TipoDocumentoSacado") == "CNPJ", substring(col("CPFCNPJSACADO"), 1, 8)).otherwise(col("CPFCNPJSACADO")))

df_operacoes_small = df_operacoes_enriquecida.select("CODOPERACAO", "CODCLIENTE", "DATAANALISE", "STATUSACEITE", "STATUSANALISE", "chave_produto")
df_limites_small = df_limites.select("chave_cliente_sacado", "TIPO")
df_produtos_small = df_dim_produto.select(col("chave_produto"), col("produto_informacao_de_mercado").alias("produto_temp"))
df_devolucoes_small = df_devolucoes.select(col("CODTITULO"), col("CODOPERACAO").alias("cod_operacao_recompra"))
df_ultima_conf_small = df_ultima_conf.select(col("cod_titulo").alias("CODTITULO"), col("confirmacao").alias("confirmado_por"))
df_protestos_small = df_protestos.select("CODTITULO", "STATUS_PROTESTO")

df_titulos_com_chave_sacado = df_titulos_base.join(broadcast(df_operacoes_small), "CODOPERACAO", "left").withColumn("chave_cliente_sacado", concat(col("CODCLIENTE").cast("string"), lit("-"), col("RaizCNPJ")))

df_enriquecido = df_titulos_com_chave_sacado \
    .join(broadcast(df_limites_small), "chave_cliente_sacado", "left") \
    .join(broadcast(df_produtos_small), "chave_produto", "left") \
    .join(broadcast(df_devolucoes_small), "CODTITULO", "left") \
    .join(broadcast(df_ultima_conf_small), "CODTITULO", "left") \
    .join(broadcast(df_protestos_small), "CODTITULO", "left") \
    .na.fill({"AMORTIZACOES": 0})

# 3.2 Cálculos de Negócio
# -----------------------
df_com_calcs = df_enriquecido \
    .withColumn("intercompany", when(col("TIPO") == "INTERCIA", "SIM").otherwise("NÃO")) \
    .withColumn("status_protesto", coalesce(col("STATUS_PROTESTO"), lit("NÃO PROTESTADO"))) \
    .withColumn("valor_vezes_prazo", col("PRAZO") * col("VALOR")) \
    .withColumn("produto_com_intercia", when((col("intercompany") == "SIM") & (col("chave_produto").isin("NO", "CM")), "INTERCOMPANY").otherwise(col("produto_temp")))

# Data Vencimento Útil
try:
    df_dim_calendario = spark.read.table("LH_Gold.dim_calendario").select(col("data"), col("proximo_dia_util"))
    df_dates_final = df_com_calcs.join(broadcast(df_dim_calendario), df_com_calcs.VENCPRORROGADO == df_dim_calendario.data, "left").withColumnRenamed("proximo_dia_util", "data_vencimento_util").drop("data")
except Exception as e:
    print(f"AVISO: Erro ao ler LH_Gold.dim_calendario: {e}. Usando lógica de cálculo manual.")
    df_feriados_sel = df_feriados.select(col("DATAFERIADO").alias("data_feriado"), col("TFERIADO").alias("tipo_feriado"))
    df_dates_1 = df_com_calcs.withColumn("dia_da_semana", dayofweek(col("VENCPRORROGADO"))).withColumn("data_vencimento_util_temp", when(col("dia_da_semana") == 1, date_add(col("VENCPRORROGADO"), 1)).when(col("dia_da_semana") == 7, date_add(col("VENCPRORROGADO"), 2)).otherwise(col("VENCPRORROGADO")))
    df_dates_2 = df_dates_1.join(broadcast(df_feriados_sel), df_dates_1.data_vencimento_util_temp == df_feriados_sel.data_feriado, "left")
    df_dates_final = df_dates_2.withColumn("data_vencimento_util", when(col("tipo_feriado") == "L", col("data_vencimento_util_temp")).when((col("tipo_feriado") == "N") & (col("dia_da_semana") == 6), date_add(col("data_vencimento_util_temp"), 3)).when((col("tipo_feriado") == "N"), date_add(col("data_vencimento_util_temp"), 1)).otherwise(col("data_vencimento_util_temp"))).drop("data_vencimento_util_temp", "tipo_feriado", "data_feriado", "dia_da_semana")

df_status_1 = df_dates_final.withColumn("status_deferimento", when((col("ACEITO") == "S") & (col("STATUSACEITE") == "A") & (col("STATUSANALISE") == "D"), "Sim").otherwise("Não"))
df_status_2 = df_status_1.withColumn("status_clean", when(col("produto_com_intercia") == "DESCONTO", "NORMAL").otherwise("CLEAN"))
df_conf = df_status_2.withColumn("confirmacao", when(col("DOCCONFIRMADO") == "N", "Atenção").when(col("DOCCONFIRMADO") == "S", None).when(col("DOCCONFIRMADO") == "C", "Positivo").when(col("DOCCONFIRMADO") == "P", "Problema").when(col("DOCCONFIRMADO") == "A", "Alerta").when(col("DOCCONFIRMADO").isNull(), "Não Contatado").when(col("DOCCONFIRMADO").isin("E", "AZ"), "Eletrônico").otherwise(col("DOCCONFIRMADO")))
df_ordem = df_conf.withColumn("ordem_confirmacao", when(col("confirmacao") == "Não Contatado", 5).when(col("confirmacao") == "Atenção", 2).when(col("confirmacao") == "Eletrônico", 0).when(col("confirmacao") == "Positivo", 1).when(col("confirmacao") == "Alerta", 3).when(col("confirmacao") == "Problema", 4).otherwise(None))

# 3.3 Seleção Final e Persistência
# ---------------------------------
df_fato_titulos_final = df_ordem.select(
    "CODTITULO", "CODOPERACAO", "TDOC", "NDOC", "CPFCNPJSACADO", "VENCIMENTO", "VENCPRORROGADO", "VALOR",
    "PRAZO", "ACEITO", "DATAINCLUSAO", "USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "AMORTIZACOES",
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
notebook_name = "NB_Prepare_Silver_Staging_Pareceres"

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

    df_replica_pareceres_delta = df_pareceres_incremental.filter(year(col("DATAINCLUSAO")) >= 2024).drop("ENCAMINHAR", "ALERTA", "CODPASTA", "CODTAREFA", "USUAALTERACAO", "DATAALTERACAO").withColumn("OBS", col("OBS").substr(1, 255)).withColumn("codTipoParecer", col("CODTIPOPARECER").cast(LongType())).filter((col("codTipoParecer") == 1) & (col("CPFCNPJ").isNotNull()) & (col("CPFCNPJ") != "") & (col("OBS").isNotNull()) & (col("OBS") != "") & (col("USUAINCLUSAO").isNotNull()) & (col("DATAINCLUSAO").isNotNull())).filter(col("OBS").startswith("STATUS ALTERADO PARA ")).withColumn("STATUS_DO_CLIENTE", substring(col("OBS"), 22, 100)).withColumn("BASE", lit(40).cast(LongType())).select("CODPARECER", "CPFCNPJ", "CODOPERACAO", "DATAINCLUSAO", "USUAINCLUSAO", "STATUS_DO_CLIENTE", "BASE")
    window_cliente_data_delta = Window.partitionBy("CODCLIENTE").orderBy(col("DATAINCLUSAO").asc())
    df_pareceres_enriquecidos_delta = df_replica_pareceres_delta.join(df_clientes_staging.select("CPFCNPJ", "CODCLIENTE"), ["CPFCNPJ"], "left").withColumn("chave_base_cliente", concat(col("BASE"), lit("-"), col("CODCLIENTE"))).join(df_usuarios_raw.select("CODUSUARIO", "NOME"), col("USUAINCLUSAO") == col("CODUSUARIO"), "left").withColumnRenamed("NOME", "USUARIO").join(df_status_clientes_esteira, "STATUS_DO_CLIENTE", "left").filter(col("CODCLIENTE").isNotNull() & (col("CODCLIENTE") != "")).withColumn("INDICE", row_number().over(window_cliente_data_delta)).withColumn("chave_original", (col("INDICE") * 1000000000 + col("CODCLIENTE")).cast(LongType())).withColumnRenamed("DATAINCLUSAO", "DATALOG").select("CODPARECER", "CODCLIENTE", "STATUS_DO_CLIENTE", "DATALOG", "BASE", "USUARIO", "chave_base_cliente", "INDICE", "chave_original", "MACROPROCESSO", "FASE")

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
    window_lag = Window.partitionBy("CODCLIENTE").orderBy("DATALOG")
    df_com_lag = df_pareceres_completa.withColumn("STATUS_DO_CLIENTE_ANTERIOR", lag("STATUS_DO_CLIENTE").over(window_lag)).withColumn("DATALOG_ANTERIOR", lag("DATALOG").over(window_lag)).withColumn("MACROPROCESSO_ANTERIOR", lag("MACROPROCESSO").over(window_lag)).withColumn("FASE_ANTERIOR", lag("FASE").over(window_lag))
    df_transicoes = df_com_lag.filter(col("STATUS_DO_CLIENTE") != col("STATUS_DO_CLIENTE_ANTERIOR")).na.drop(subset=["STATUS_DO_CLIENTE_ANTERIOR"])
    df_esteira_final = df_transicoes.withColumn("DEVOLUCAO", when((col("MACROPROCESSO_ANTERIOR") == "CREDITO") & (col("MACROPROCESSO") == "COMERCIAL"), True).otherwise(False)).withColumn("RECEBIDA", when((col("MACROPROCESSO_ANTERIOR") == "COMERCIAL") & (col("MACROPROCESSO") == "CREDITO"), True).otherwise(False)).select("INDICE", "CODCLIENTE", "BASE", "DATALOG_ANTERIOR", "DATALOG", "chave_base_cliente", "STATUS_DO_CLIENTE_ANTERIOR", "STATUS_DO_CLIENTE", "MACROPROCESSO_ANTERIOR", "MACROPROCESSO", "FASE_ANTERIOR", "FASE", "USUARIO", "DEVOLUCAO", "RECEBIDA")
    df_esteira_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_esteira_table_name)
    print("Esteira reconstruída.")

    print("Atualizando watermark...")
    df_new_watermark = spark.createDataFrame([(notebook_name, new_watermark.strftime("%Y-%m-%d %H:%M:%S.%f"))], ["TableName", "LastWatermarkValue"])
    if spark.catalog.tableExists(watermark_table_name):
        DeltaTable.forName(spark, watermark_table_name).alias("t").merge(df_new_watermark.alias("s"), "t.TableName = s.TableName").whenMatchedUpdate(set={"LastWatermarkValue": "s.LastWatermarkValue"}).whenNotMatchedInsert(values={"TableName": "s.TableName", "LastWatermarkValue": "s.LastWatermarkValue"}).execute()
    else:
        df_new_watermark.write.mode("overwrite").saveAsTable(watermark_table_name)

print("Processo incremental de pareceres concluído.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 5: Limpeza do Cache
# **Objetivo:** Liberar da memória os DataFrames que foram armazenados em cache.


# CELL ********************

print("\nLimpando os DataFrames do cache...")
for df_name_str in dataframes_to_uncache:
    try:
        globals()[df_name_str].unpersist()
        print(f"Cache de '{df_name_str}' liberado.")
    except Exception as e:
        print(f"Não foi possível liberar o cache de '{df_name_str}': {e}")
print("Limpeza do cache concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
