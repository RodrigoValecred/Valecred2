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
    lead, date_add, lag, max, coalesce, broadcast, dayofweek, dayofmonth, date_sub, trim, to_date,
    datediff, sum, min, count, round, floor, least, current_date, split
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from delta.tables import *
from functools import reduce
import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
df_baixas_staging = spark.read.table("LH_Silver.staging_baixas_limpa")

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

# Gerentes e Plataformas
df_gerentes = spark.read.table("LH_Silver.staging_gerentes")
df_plataformas = spark.read.table("LH_Silver.staging_plataformas")

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

# Devolucoes (Silver)
df_devolucoes = spark.read.table("LH_Silver.staging_operacoes_devolucoes_limpa")

# Protestos (Silver)
df_protestos = spark.read.table("LH_Silver.staging_protestos")

df_ultima_conf = spark.read.table("LH_Silver.fact_ultima_confirmacao")

# Calendario (Gold)
df_dim_calendario = spark.read.table("LH_Gold.dim_calendario").cache()

# Sacados (Silver)
df_sacados_enriquecida = spark.read.table("LH_Silver.staging_sacados_enriquecida")

# Contratos (Silver) - Para Limites
df_contratos = spark.read.table("LH_Silver.staging_contratos_clientes_limpa")

# Grupos Economicos (Silver)
df_grupos_economicos = spark.read.table("LH_Silver.sup_grupos_economicos")

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 1.2: Operações Enriquecidas
# -----------------------------------------------------------
print("Criando DataFrame intermediário: Operações Enriquecidas...")
# PASSO 1: Tratamento de Ambiguidade
# Renomeamos o cod_cliente da bridge para garantir unicidade no join
df_bridge_prep = df_bridge_gerente.withColumnRenamed("cod_cliente", "cod_cliente_bridge")

# Enriquecimento com Gerente (Broker)
df_operacoes_com_historico = df_operacoes_limpa.join(
    df_bridge_prep,
    (df_operacoes_limpa["cod_cliente"] == df_bridge_prep["cod_cliente_bridge"]) &
    (df_operacoes_limpa["data_analise"].cast("date") >= df_bridge_prep["data_inicio_vigencia"]) &
    (df_operacoes_limpa["data_analise"].cast("date") <= df_bridge_prep["data_fim_vigencia"]),
    "left"
)

df_operacoes_com_gerente = df_operacoes_com_historico.withColumn(
    "cod_broker",
    when((col("cod_broker").isNotNull()) & (col("cod_broker") != 0), col("cod_broker")).otherwise(col("cod_gerente"))
).drop("cod_cliente_bridge","cod_gerente", "data_inicio_vigencia", "data_fim_vigencia")

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

# Check de Sanidade
print("Colunas disponíveis em df_operacoes_enriquecida:")
print(df_operacoes_enriquecida.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 2.1: Construção da Fato Operações
# ----------------------------------------
print("\nIniciando construção da fato_operacoes (Otimizada)...")

# 1. PREPARAÇÃO (Engenharia):
# Criamos a coluna de junção ANTES. Isso permite que o Spark entenda a distribuição dos dados.
# Se 'data_inclusao' for timestamp, to_date corta a hora. Se for string, ele converte.
df_operacoes_prep = df_operacoes_enriquecida.withColumn(
    "data_join_calendario", 
    to_date(col("data_inclusao"))
)

# Adicionando a sk_data para join com dim_calendario
df_fato_operacoes_joined = df_operacoes_prep.join(
    broadcast(df_dim_calendario.select("data", "sk_data")),
    col("data_join_calendario") == col("data"),
    "left"
)

# 3. SELEÇÃO FINAL
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
    "desconto", "juros", "tarifa_recompra", "data_vencimento", df_baixas_corrigido["cod_operacao"],
    df_dim_pago_por["descricao"].alias("pago_por"), df_dim_forma_pagamento["descricao"].alias("forma"),
    df_dim_tipo_taxa["descricao"].alias("tipo_baixa"), df_dim_motivo_baixa["descricao"].alias("motivo")
)
output_path_fato_baixas = "LH_Gold.fato_baixas"
df_fato_baixas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_fato_baixas)
print(f"Tabela 'fato_baixas' salva em: {output_path_fato_baixas}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# CELL ********************

# Célula 2.4: Construção da Dimensão Sacados
# ------------------------------------------
print("\nIniciando construção da dim_sacados...")
# A tabela já vem tratada da camada Silver (NB_Prepara_Tabela_Cadastros)
# Selecionamos as colunas e salvamos na Gold.
df_dim_sacados = df_sacados_enriquecida.select(
    col("cpf_cnpj"),
    col("nome_sacado"),
    col("emails"),
    col("telefones"),
    col("endereco"),
    col("numero"),
    col("complemento"),
    col("bairro"),
    col("cidade"),
    col("uf"),
    col("cep"),
    col("regiao")
)

output_path_dim_sacados = "LH_Gold.dim_sacados"
df_dim_sacados.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_dim_sacados)
print(f"Tabela 'dim_sacados' salva em: {output_path_dim_sacados}")

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
    .withColumn("tipo_documento_sacado", when(length(col("cpf_cnpj_sacado")) == 11, "CPF").when(length(col("cpf_cnpj_sacado")) == 14, "CNPJ").otherwise("Inválido")) \
    .withColumn("raiz_cnpj", when(col("tipo_documento_sacado") == "CNPJ", substring(col("cpf_cnpj_sacado"), 1, 8)).otherwise(col("cpf_cnpj_sacado")))

df_operacoes_small = df_operacoes_enriquecida.select("cod_operacao", "cod_cliente", "data_analise", "status_aceite", "status_analise", "chave_produto", "tto")
df_limites_small = df_limites.select("chave_cliente_sacado", "tipo")
df_produtos_small = df_dim_produto.select(col("chave_produto"), col("produto_informacao_de_mercado").alias("produto_temp"))
df_devolucoes_small = df_devolucoes.select(col("cod_titulo"), col("cod_operacao").alias("cod_operacao_recompra"))
df_ultima_conf_small = df_ultima_conf.select(col("cod_titulo"), col("confirmacao").alias("confirmado_por"))
df_protestos_small = df_protestos.select("cod_titulo", "status_protesto")

df_titulos_com_chave_sacado = df_titulos_base.join(broadcast(df_operacoes_small), "cod_operacao", "left").withColumn("chave_cliente_sacado", concat(col("cod_cliente").cast("string"), lit("-"), col("raiz_cnpj")))

df_enriquecido = df_titulos_com_chave_sacado \
    .join(broadcast(df_limites_small), "chave_cliente_sacado", "left") \
    .join(broadcast(df_produtos_small), "chave_produto", "left") \
    .join(broadcast(df_devolucoes_small), "cod_titulo", "left") \
    .join(broadcast(df_ultima_conf_small), "cod_titulo", "left") \
    .join(broadcast(df_protestos_small), "cod_titulo", "left") \
    .na.fill({"amortizacoes": 0})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# Classificação de Risco e Atraso
df_classificacao = df_dates_final.withColumn("dias_atraso", datediff(current_date(), col("data_vencimento_util"))) \
    .withColumn("status_risco",
        when((col("tto") == "RN") & (col("data_vencimento_util") < current_date()), "CRÍTICO (Vermelho)")
        .when(col("data_vencimento_util") < current_date(), "ATENÇÃO (Amarelo)")
        .otherwise("NO PRAZO (Verde)")
    )

df_status_1 = df_classificacao.withColumn("status_deferimento", when((col("aceito") == "S") & (col("status_aceite") == "A") & (col("status_analise") == "D"), "Sim").otherwise("Não"))
df_status_2 = df_status_1.withColumn("status_clean", when(col("produto_com_intercia") == "DESCONTO", "NORMAL").otherwise("CLEAN"))

# Confirmacao Logic using Bronze column or Fallback
df_conf = df_status_2.withColumn("confirmacao", when(col("doc_confirmado") == "N", "Atenção").when(col("doc_confirmado") == "S", None).when(col("doc_confirmado") == "C", "Positivo").when(col("doc_confirmado") == "P", "Problema").when(col("doc_confirmado") == "A", "Alerta").when(col("doc_confirmado").isNull(), "Não Contatado").when(col("doc_confirmado").isin("E", "AZ"), "Eletrônico").otherwise(col("doc_confirmado")))
df_ordem = df_conf.withColumn("ordem_confirmacao", when(col("confirmacao") == "Não Contatado", 5).when(col("confirmacao") == "Atenção", 2).when(col("confirmacao") == "Eletrônico", 0).when(col("confirmacao") == "Positivo", 1).when(col("confirmacao") == "Alerta", 3).when(col("confirmacao") == "Problema", 4).otherwise(None))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3.3 Seleção Final e Persistência
# ---------------------------------
df_fato_titulos_final = df_ordem.select(
    col("cod_titulo"), col("cod_operacao"), col("t_doc"), col("n_doc"), col("cpf_cnpj_sacado"), col("vencimento"), col("venc_prorrogado"), col("valor"),
    col("prazo"), col("aceito"), col("data_inclusao"), col("usua_conf").alias("usua_inclusao"), col("data_alteracao"), col("amortizacoes"),
    "chave_produto", "status_protesto", "tipo_documento_sacado", "raiz_cnpj", "valor_vezes_prazo",
    "produto_com_intercia", "data_vencimento_util", "status_deferimento", "status_clean",
    "confirmacao", "ordem_confirmacao", "cod_operacao_recompra", "confirmado_por", "intercompany",
    col("liquidacao"), col("valor_devido"), col("motivo"),
    col("status_risco"), col("dias_atraso")
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 4.2: Leitura e Processamento Incremental
# ------------------------------------------------
df_pareceres_incremental = df_pareceres_raw.filter((col("DATAINCLUSAO") > last_watermark) | (col("DATAALTERACAO") > last_watermark)).cache()
record_count = df_pareceres_incremental.count()

print("mostrando colunas da df_pareceres_incremental:")
print(df_pareceres_incremental.columns)

if record_count > 0:
    new_watermark = df_pareceres_incremental.agg(max(greatest(coalesce(col("DATAINCLUSAO"), lit(datetime.datetime(1900,1,1))), coalesce(col("DATAALTERACAO"), lit(datetime.datetime(1900,1,1)))))).collect()[0][0]
    print(f"Registros incrementais: {record_count}. Novo watermark: {new_watermark}")

    df_replica_pareceres_delta = df_pareceres_incremental.filter(year(col("DATAINCLUSAO")) >= 2024).drop("ENCAMINHAR", "ALERTA", "CODPASTA", "CODTAREFA", "USUAALTERACAO", "DATAALTERACAO").withColumn("OBS", col("OBS").substr(1, 255)).withColumn("codTipoParecer", col("CODTIPOPARECER").cast(LongType())).filter((col("codTipoParecer") == 1) & (col("CPFCNPJ").isNotNull()) & (col("CPFCNPJ") != "") & (col("OBS").isNotNull()) & (col("OBS") != "") & (col("USUAINCLUSAO").isNotNull()) & (col("DATAINCLUSAO").isNotNull())).filter(col("OBS").startswith("STATUS ALTERADO PARA ")).withColumn("STATUS_DO_CLIENTE", trim(substring(col("OBS"), 22, 100))).withColumn("BASE", lit(40).cast(LongType())).select("CODPARECER", "CPFCNPJ", "CODOPERACAO", "DATAINCLUSAO", "USUAINCLUSAO", "STATUS_DO_CLIENTE", "BASE")

    # Recuperar MAX INDICE por Cliente da tabela alvo, se existir, para continuar a sequencia
    if spark.catalog.tableExists(target_pareceres_status_table_name):
        df_max_indices = spark.read.table(target_pareceres_status_table_name).groupBy("CODCLIENTE").agg(max("INDICE").alias("max_indice"))
        # Garantir snake_case para join
        if "CODCLIENTE" in df_max_indices.columns:
            df_max_indices = df_max_indices.withColumnRenamed("CODCLIENTE", "cod_cliente")
    else:
        df_max_indices = None

    # Enriquecimento e Calculo de Indice
    df_joined = df_replica_pareceres_delta.join(df_clientes_staging.select("cpf_cnpj", "cod_cliente"), df_replica_pareceres_delta.CPFCNPJ == df_clientes_staging.cpf_cnpj, "left")

    if df_max_indices:
        df_joined = df_joined.join(df_max_indices, "cod_cliente", "left").withColumn("start_index", coalesce(col("max_indice"), lit(0)))
    else:
        df_joined = df_joined.withColumn("start_index", lit(0))

    window_cliente_data_delta = Window.partitionBy("cod_cliente").orderBy(col("DATAINCLUSAO").asc())

    # --- AQUI ESTAVA O ERRO E AQUI ESTÁ A CORREÇÃO ---
    df_pareceres_enriquecidos_delta = df_joined \
        .withColumn("chave_base_cliente", concat(col("BASE").cast("string"), lit("-"), col("cod_cliente").cast("string"))) \
        .join(df_usuarios_raw.select("CODUSUARIO", "NOME"), col("USUAINCLUSAO") == col("CODUSUARIO"), "left") \
        .withColumnRenamed("NOME", "USUARIO") \
        .join(df_status_clientes_esteira, "STATUS_DO_CLIENTE", "left") \
        .filter(col("cod_cliente").isNotNull() & (col("cod_cliente") != "")) \
        .withColumn("INDICE", col("start_index") + row_number().over(window_cliente_data_delta)) \
        .withColumn("chave_original", (col("INDICE") * 1000000000 + col("cod_cliente")).cast(LongType())) \
        .withColumnRenamed("DATAINCLUSAO", "DATALOG") \
        .select(
            col("CODPARECER"),
            col("cod_cliente").alias("CODCLIENTE"), # <--- CORREÇÃO: Renomeando explicitamente para o Merge encontrar
            col("STATUS_DO_CLIENTE"),
            col("DATALOG"),
            col("BASE"),
            col("USUARIO"),
            col("chave_base_cliente"),
            col("INDICE"),
            col("chave_original"),
            col("MACROPROCESSO"),
            col("FASE")
        )

    if spark.catalog.tableExists(target_pareceres_status_table_name):
        print(f"Executando Merge na tabela {target_pareceres_status_table_name}...")
        # Schema Evolution: Se houver colunas novas, permite a evolução
        delta_table = DeltaTable.forName(spark, target_pareceres_status_table_name)
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        delta_table.alias("t").merge(
            df_pareceres_enriquecidos_delta.alias("s"), 
            "t.CODPARECER = s.CODPARECER"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        print(f"Criando tabela {target_pareceres_status_table_name} pela primeira vez...")
        df_pareceres_enriquecidos_delta.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_pareceres_status_table_name)
else:
    new_watermark = last_watermark
    print("Nenhum dado novo encontrado.")

if 'df_pareceres_incremental' in locals():
    df_pareceres_incremental.unpersist()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 4.3: Reconstrução da Esteira e Atualização do Watermark
# -------------------------------------------------------------
if record_count > 0:
    print("Reconstruindo esteira_de_propostas...")
    df_pareceres_completa = spark.read.table(target_pareceres_status_table_name)
    window_lag = Window.partitionBy("CODCLIENTE").orderBy("DATALOG")
    df_com_lag = df_pareceres_completa.withColumn("STATUS_DO_CLIENTE_ANTERIOR", lag("STATUS_DO_CLIENTE").over(window_lag)).withColumn("DATALOG_ANTERIOR", lag("DATALOG").over(window_lag)).withColumn("MACROPROCESSO_ANTERIOR", lag("MACROPROCESSO").over(window_lag)).withColumn("FASE_ANTERIOR", lag("FASE").over(window_lag))
    df_transicoes = df_com_lag.filter(col("STATUS_DO_CLIENTE") != col("STATUS_DO_CLIENTE_ANTERIOR")).na.drop(subset=["STATUS_DO_CLIENTE_ANTERIOR"])
    df_esteira_final = df_transicoes.withColumn("DEVOLUCAO", when((col("MACROPROCESSO_ANTERIOR") == "CREDITO") & (col("MACROPROCESSO") == "COMERCIAL"), True).otherwise(False)).withColumn("RECEBIDA", when((col("MACROPROCESSO_ANTERIOR") == "COMERCIAL") & (col("MACROPROCESSO") == "CREDITO"), True).otherwise(False)).select(col("INDICE").alias("indice"), col("CODCLIENTE").alias("cod_cliente"), col("BASE").alias("base"), col("DATALOG_ANTERIOR").alias("datalog_anterior"), col("DATALOG").alias("datalog"), "chave_base_cliente", col("STATUS_DO_CLIENTE_ANTERIOR").alias("status_do_cliente_anterior"), col("STATUS_DO_CLIENTE").alias("status_do_cliente"), col("MACROPROCESSO_ANTERIOR").alias("macroprocesso_anterior"), col("MACROPROCESSO").alias("macroprocesso"), col("FASE_ANTERIOR").alias("fase_anterior"), col("FASE").alias("fase"), col("USUARIO").alias("usuario"), col("DEVOLUCAO").alias("devolucao"), col("RECEBIDA").alias("recebida"))
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

# CELL ********************

# Célula 5.1: Construção da Fato Operações Prorrogação
# ---------------------------------------------------
print("\nIniciando construção da fato_operacoes_prorrogacao...")

# Leitura da Staging Limpa (Silver)
df_prorrogacao_silver = spark.read.table("LH_Silver.staging_operacoes_prorrogacao_limpa")

# Leitura das tabelas auxiliares (Silver/Gold) já carregadas no início (df_titulos_limpa, df_operacoes_limpa)
# Mas garantindo a seleção correta
df_titulos_join = df_titulos_limpa.select(col("cod_titulo"), col("valor").alias("VALOR_TITULO"))
df_operacoes_join = df_operacoes_limpa.select(col("cod_operacao"), col("status_analise").alias("status_analise"), col("status_aceite").alias("status_aceite"))

# Join
# Etapa 2: Mesclar dados de títulos
df_joined_titulos = df_prorrogacao_silver.join(df_titulos_join, "cod_titulo", "left_outer")

# Etapa 4: Mesclar dados de operações
df_joined_full = df_joined_titulos.join(df_operacoes_join, "cod_operacao", "left_outer")

# Etapa 6: Remover colunas desnecessárias
cols_to_remove = ["tarifa", "usuainclusao", "dataalteracao", "usuaalteracao", "valordevido", "valorpror", "valorboleto"]
# Nota: As colunas originais do bronze foram convertidas para lower case no Silver (staging_operacoes_prorrogacao_limpa)
# Portanto, removemos as versões lower case.

df_cleaned = df_joined_full.drop(*cols_to_remove)

# Tratamento do campo VALOR (Prioridade para o valor vindo de Títulos, se expandido)
# Se existir 'valor' na tabela original, ele pode conflitar ou ser substituído.
# No M script: Table.ExpandTableColumn(..., {"VALOR"}, {"VALOR"}) sugere que usamos o valor do título.
if "valor" in df_cleaned.columns:
    df_cleaned = df_cleaned.drop("valor")
df_cleaned = df_cleaned.withColumnRenamed("VALOR_TITULO", "valor")

# Etapas 7, 8, 9: Transformações (Removido colunas legado: base e chave_base_titulo)
df_final_prorrogacao = df_cleaned \
    .withColumn("data", to_date(col("data_inclusao"))) \
    .withColumn("dias_prorrogados", datediff(col("vencimentonov"), col("vencimentoant")))

target_fato_prorrogacao = "LH_Gold.fato_operacoes_prorrogacao"
df_final_prorrogacao.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_fato_prorrogacao)
print(f"Tabela '{target_fato_prorrogacao}' criada com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 6: Construção da Dimensão Clientes Enriquecida
# **Objetivo:** Unificar dados cadastrais com métricas agregadas de Operações, Títulos e Esteira.

# CELL ********************

print("\nIniciando construção da dim_clientes enriquecida...")
from pyspark.sql.functions import sum, min, count, current_date, round, floor, dayofmonth

# 6.0: Preparação de Dados Auxiliares
# -----------------------------------
# Grupos Econômicos
df_grupos_prep = df_grupos_economicos.withColumnRenamed("nomegrupo", "grupo_economico")
if "cod_cliente" not in df_grupos_prep.columns and "codcliente" in df_grupos_prep.columns:
     df_grupos_prep = df_grupos_prep.withColumnRenamed("codcliente", "cod_cliente")
df_grupos_prep = df_grupos_prep.select("cod_cliente", "grupo_economico")

# 6.0.1: Info Gestor (Para join final)
df_bridge_atual = df_bridge_gerente.filter(col("data_fim_vigencia") == "9999-12-31")
df_info_gestor = df_bridge_atual \
    .join(df_gerentes, df_bridge_atual.cod_gerente == df_gerentes.cod_broker, "left") \
    .join(df_plataformas, "cod_agencia", "left") \
    .select(df_bridge_atual.cod_cliente, df_plataformas.gestor_da_plataforma)

# 6.1: Métricas de Operações
# --------------------------
# Usamos df_fato_operacoes criada na Seção 2.1
df_ops_validas = df_fato_operacoes.filter(col("status_analise") == "D")

# VOP por Dia da Semana (Top 1)
df_vop_semana = df_ops_validas.withColumn("dia_semana", dayofweek("data_analise")) \
    .groupBy("cod_cliente", "dia_semana").agg(sum("valor_de_face").alias("vop"))
w_rank_semana = Window.partitionBy("cod_cliente").orderBy(col("vop").desc())
df_dia_semana_top = df_vop_semana.withColumn("rn", row_number().over(w_rank_semana)).filter(col("rn") == 1) \
    .select(col("cod_cliente"), col("dia_semana").alias("dia_semana_mais_vop"))

# VOP por Dia do Mês (Top 1)
df_vop_mes = df_ops_validas.withColumn("dia_mes", dayofmonth("data_analise")) \
    .groupBy("cod_cliente", "dia_mes").agg(sum("valor_de_face").alias("vop"))
w_rank_mes = Window.partitionBy("cod_cliente").orderBy(col("vop").desc())
df_dia_mes_top = df_vop_mes.withColumn("rn", row_number().over(w_rank_mes)).filter(col("rn") == 1) \
    .select(col("cod_cliente"), col("dia_mes").alias("dia_mes_mais_vop"))

# Métricas Gerais Operações
df_metrics_ops = df_ops_validas.groupBy("cod_cliente").agg(
    max("data_analise").alias("data_ultima_operacao"),
    max("cod_operacao").alias("bordero_ultima_operacao"),
    min(when(col("status_aceite") == "A", col("data_analise"))).alias("data_primeira_operacao"),
    min(when((col("status_aceite") == "A") & (col("status_analise") == "D"), col("data_analise"))).alias("data_cliente_desde")
)

df_metrics_ops_final = df_metrics_ops.join(df_dia_semana_top, "cod_cliente", "left").join(df_dia_mes_top, "cod_cliente", "left")

# 6.2: Métricas de Títulos (Risco)
# --------------------------------
# Usamos df_fato_titulos_final criada na Seção 3.3
# Join com Operações para pegar cod_cliente
df_titulos_cliente = df_fato_titulos_final.join(
    df_fato_operacoes.select("cod_operacao", "cod_cliente"), "cod_operacao", "left"
)

today_date = current_date()

# Filtro Base Risco: Aceito=S, StatusAnalise=D (status_deferimento=Sim), Liquidacao=Null
df_risco_base = df_titulos_cliente.filter((col("status_deferimento") == "Sim") & (col("liquidacao").isNull()))

df_metrics_titulos = df_risco_base.groupBy("cod_cliente").agg(
    sum("valor_devido").alias("risco"),
    sum(when(col("produto_com_intercia") != "COMISSÁRIA", col("valor_devido")).otherwise(0)).alias("risco_exceto_comissaria"),
    sum(when(col("produto_com_intercia") == "COMISSÁRIA", col("valor_devido")).otherwise(0)).alias("risco_comissaria"),
    sum(when(col("confirmacao") == "Atenção", col("valor_devido")).otherwise(0)).alias("confirmado_atencao"),
    sum(when(col("confirmacao") == "Positivo", col("valor_devido")).otherwise(0)).alias("confirmado_positivo"),
    sum(when(col("confirmacao") == "Problema", col("valor_devido")).otherwise(0)).alias("problemas_checagem"),
    # Inadimplencia: VencProrrogado < Hoje-14
    sum(when(col("venc_prorrogado") < date_sub(today_date, 14), col("valor_devido")).otherwise(0)).alias("inadimplencia"),
    # Dates
    min(when(datediff(today_date, col("venc_prorrogado")) >= 15, col("venc_prorrogado"))).alias("data_vencido_mais_antigo"),
    max(when(datediff(today_date, col("venc_prorrogado")) >= 15, col("venc_prorrogado"))).alias("data_vencido_mais_recente"),

    # NOVAS COLUNAS: Qualidade do Cliente (Risco Clean, Renegociacao, etc.)
    sum(when(col("status_clean") == "CLEAN", col("valor_devido")).otherwise(0)).alias("risco_clean"),
    sum(when(col("produto_com_intercia") == "RENEGOCIAÇÃO", col("valor_devido")).otherwise(0)).alias("risco_renegociacao"),
    sum(when(col("produto_com_intercia") != "RENEGOCIAÇÃO", col("valor_devido")).otherwise(0)).alias("risco_sem_renegociacao"),
    sum(when(col("data_vencimento_util") < today_date, col("valor_devido")).otherwise(0)).alias("vencidos")
)

# Perdas (VOP - VlrPagoLiquido) where Motivo='PR' (Assume proxy for LIQUIDEZ='L5')
# Need a separate aggregation from Fato Baixas or Titulos if column exists
# Assuming 'motivo' in fato_titulos (from previous steps) can be used.
# Logic: sum(valor) - sum(liquidacao value? No, liquidacao is date).
# We need `valor_pago` from somewhere. `fato_titulos` usually has `valor` and `valor_pago` comes from `baixas`.
# But `fato_titulos` in Seção 3 doesn't explicitly have `valor_pago`. It has `amortizacoes`.
# Let's use `amortizacoes` as proxy for `valor_pago_liquido` if strict. Or better, check if we joined Baixas.
# In `Seção 3`, `df_fato_titulos_final` has `amortizacoes`. Let's use that.
# Perdas = (Valor - Amortizacoes) where motivo='PR'.
df_perdas_agg = df_titulos_cliente.filter(col("motivo") == "PR") \
    .groupBy("cod_cliente").agg(
        (sum("valor") - sum("amortizacoes")).alias("perdas")
    )

df_metrics_titulos_final = df_metrics_titulos.join(df_perdas_agg, "cod_cliente", "left").na.fill({"perdas": 0})

# Risco Grupo e Risco Comissaria Grupo
# Precisamos das metricas individuais antes
df_risco_ind = df_metrics_titulos_final.select("cod_cliente", "risco", "risco_comissaria")
df_risco_grupo_agg = df_risco_ind.join(df_grupos_prep, "cod_cliente", "inner") \
    .groupBy("grupo_economico").agg(
        sum("risco").alias("risco_grupo"),
        sum("risco_comissaria").alias("risco_comissaria_grupo")
    )

# 6.3: Esteira Dates e Funnel
# ---------------------------
df_esteira = spark.read.table("LH_Gold.esteira_de_propostas")

# Normalização de colunas (Compatibilidade com Legado/Schema antigo)
# Se a tabela não foi regerada (incremental = 0), ela pode estar com colunas em UpperCase.
# Forçamos o rename para snake_case esperado.
if "CODCLIENTE" in df_esteira.columns:
    df_esteira = df_esteira.withColumnRenamed("CODCLIENTE", "cod_cliente")
if "STATUS_DO_CLIENTE" in df_esteira.columns:
    df_esteira = df_esteira.withColumnRenamed("STATUS_DO_CLIENTE", "status_do_cliente")
if "DATALOG" in df_esteira.columns:
    df_esteira = df_esteira.withColumnRenamed("DATALOG", "datalog")

# Status esperados para o Pivot (para evitar erros de coluna inexistente)
expected_status = [
    "CHECKLIST", "ASSINATURA", "COMITE", "CONCLUIDO", "BIZAGI",
    "RENOVAÇÃO", "RESERVA", "START", "CREDITO",
    "PROPOSTA", "REVISÃO COMERCIAL", "DIR COMERCIAL"
]

# Pivot Simples das Datas Maximas por Status
# Atualizado para usar colunas snake_case da esteira Gold
df_esteira_pivot = df_esteira \
    .groupBy("cod_cliente") \
    .pivot("status_do_cliente", expected_status) \
    .agg(max("datalog"))

# Renomeando colunas do pivot para evitar ambiguidade com outras tabelas e padronizar
status_mapping = {
    "CHECKLIST": "checklist", "ASSINATURA": "assinatura", "COMITE": "comite",
    "CONCLUIDO": "concluido", "BIZAGI": "bizagi", "RENOVAÇÃO": "renovacao",
    "RESERVA": "reserva", "START": "start", "CREDITO": "credito",
    "PROPOSTA": "proposta", "REVISÃO COMERCIAL": "revisao_comercial",
    "DIR COMERCIAL": "dir_comercial"
}

for status, clean_name in status_mapping.items():
    if status in df_esteira_pivot.columns:
        df_esteira_pivot = df_esteira_pivot.withColumnRenamed(status, f"pivot_{clean_name}")

# Funnel: Data Primeira Proposta (Lógica Sequencial por Cliente)
# Precisamos calcular as MIN datas para cada status, mas respeitando a sequencia de eventos (funnel)
# Como PySpark SQL é limitado para isso, vamos usar Window Functions e Self-Joins Simplificados.
# Passo 1: Min Datas puras
df_esteira_min = df_esteira.groupBy("cod_cliente").pivot("status_do_cliente", expected_status).agg(min("datalog"))

# 6.4: Limites
# ------------
df_limites_agg = df_contratos.filter(col("status") == "A") \
    .withColumn("limite_total", coalesce(col("limite_fomento"), lit(0)) + coalesce(col("limite_comissaria"), lit(0))) \
    .groupBy("cod_cliente").agg(
        sum("limite_total").alias("limite"),
        sum("limite_comissaria").alias("limite_comissaria_contrato"),
        max("validade_limite").alias("vencimento_limite"),
        max("tranche").alias("tranche"),
        max("perc_confirmacao").alias("percentual_exigido")
    )

# 6.5: Join Final e Colunas Calculadas
# ------------------------------------
# Base: Clientes Staging
# Atualização: Incluindo data_inclusao (requeridas para idade_cliente e idade_cliente_em_dias)
df_base = df_clientes_staging.select("cod_cliente", "cpf_cnpj", "data_inclusao")

# Prepare Esteira Min Dates for Funnel (Joining back to main flow)
# Renaming for clarity
df_esteira_min_renamed = df_esteira_min \
    .withColumnRenamed("PROPOSTA", "min_proposta") \
    .withColumnRenamed("REVISÃO COMERCIAL", "min_revisao") \
    .withColumnRenamed("DIR COMERCIAL", "min_dir_comercial") \
    .withColumnRenamed("CREDITO", "min_credito") \
    .withColumnRenamed("CHECKLIST", "min_checklist") \
    .withColumnRenamed("CONCLUIDO", "min_concluido") \
    .select("cod_cliente", "min_proposta", "min_revisao", "min_dir_comercial", "min_credito", "min_checklist", "min_concluido")

# Renomeando chaves para evitar ambiguidade nos joins
df_esteira_pivot_prep = df_esteira_pivot.withColumnRenamed("cod_cliente", "cod_cliente_pivot")
df_esteira_min_prep = df_esteira_min_renamed.withColumnRenamed("cod_cliente", "cod_cliente_min")

# Join Chain
df_join_1 = df_base.join(df_cad_geral_enriquecido, "cpf_cnpj", "left") \
    .join(df_metrics_ops_final, "cod_cliente", "left") \
    .join(df_metrics_titulos_final, "cod_cliente", "left") \
    .join(df_esteira_pivot_prep, df_base.cod_cliente == df_esteira_pivot_prep.cod_cliente_pivot, "left").drop("cod_cliente_pivot") \
    .join(df_esteira_min_prep, df_base.cod_cliente == df_esteira_min_prep.cod_cliente_min, "left").drop("cod_cliente_min") \
    .join(df_limites_agg, "cod_cliente", "left") \
    .join(df_grupos_prep, "cod_cliente", "left") \
    .join(df_risco_grupo_agg, "grupo_economico", "left") \
    .join(df_info_gestor, "cod_cliente", "left")

# Implementando Lógica Funnel Sequencial (Aproximação)
# Data 1: Primeira Proposta Comercial = MIN(Proposta, Revisao, Diretoria)
# Data 2: Credito (Min data credito >= data 1) - Aqui assumimos Min Credito geral, pois PySpark SQL row-level logic é complexa.
# Data 3: Formalizacao (Checklist >= Credito)
# Data 4: Concluido (Concluido >= Formalizacao)

df_funnel = df_join_1 \
    .withColumn("data_primeira_proposta_comercial", least(col("min_proposta"), col("min_revisao"), col("min_dir_comercial"))) \
    .withColumn("data_primeira_proposta_credito",
        when(col("min_credito") >= col("data_primeira_proposta_comercial"), col("min_credito"))
    ) \
    .withColumn("data_primeira_proposta_formalizacao",
        when(col("min_checklist") >= col("data_primeira_proposta_credito"), col("min_checklist"))
    ) \
    .withColumn("data_primeira_proposta_concluida",
        when(col("min_concluido") >= col("data_primeira_proposta_formalizacao"), col("min_concluido"))
    )

# Colunas Calculadas
df_final = df_funnel \
    .withColumn("data_aprovacao", greatest(col("pivot_checklist"), col("pivot_assinatura"))) \
    .withColumn("data_conclusao", coalesce(col("pivot_bizagi"), col("pivot_concluido"))) \
    .withColumn("data_comite", col("pivot_comite")) \
    .withColumn("data_reserva", greatest(col("pivot_renovacao"), col("pivot_reserva"))) \
    .withColumn("data_entrada", coalesce(
        greatest(col("pivot_dir_comercial"), col("pivot_proposta"), col("pivot_revisao_comercial")),
        col("data_comite")
    )) \
    .withColumn("risco", coalesce(col("risco"), lit(0))) \
    .withColumn("risco_grupo", coalesce(col("risco_grupo"), lit(0))) \
    .withColumn("risco_comissaria_grupo", coalesce(col("risco_comissaria_grupo"), lit(0))) \
    .withColumn("limite", coalesce(col("limite"), lit(0))) \
    .withColumn("limite_comissaria_contrato", coalesce(col("limite_comissaria_contrato"), lit(0))) \
    .withColumn("risco_comissaria", coalesce(col("risco_comissaria"), lit(0))) \
    .withColumn("risco_exceto_comissaria", coalesce(col("risco_exceto_comissaria"), lit(0))) \
    .withColumn("risco_total", col("risco") + col("risco_grupo")) \
    .withColumn("limite_disponivel", col("limite") - col("risco_total")) \
    .withColumn("risco_subtotal_comissaria", col("risco_comissaria") + col("risco_comissaria_grupo")) \
    .withColumn("disponivel_comissaria", greatest(
        least(col("limite_disponivel"), col("limite_comissaria_contrato") - col("risco_subtotal_comissaria")),
        lit(0)
    )) \
    .withColumn("limite_maximo_disponivel", greatest(col("disponivel_comissaria"), col("limite_disponivel"))) \
    .withColumn("dias_sem_operar", datediff(today_date, greatest(coalesce(col("data_ultima_operacao"), lit("1900-01-01")), coalesce(col("data_conclusao"), lit("1900-01-01"))))) \
    .withColumn("dias_vencidos", datediff(today_date, col("data_vencido_mais_antigo"))) \
    .withColumn("inadimplencia", coalesce(col("inadimplencia"), lit(0))) \
    .withColumn("faixa_pdd",
        when(col("dias_vencidos") > 180, 1)
        .when(col("dias_vencidos") > 150, 0.7)
        .when(col("dias_vencidos") > 120, 0.4)
        .when(col("dias_vencidos") > 90, 0.2)
        .when(col("dias_vencidos") > 60, 0.1)
        .when(col("dias_vencidos") > 30, 0.05)
        .otherwise(0)
    ) \
    .withColumn("pdd", col("faixa_pdd") * col("inadimplencia")) \
    .withColumn("status_atividade",
        when(col("dias_sem_operar") > 120, "INATIVO")
        .when(col("data_ultima_operacao").isNull(), "NUNCA OPEROU")
        .otherwise("ATIVO")
    ) \
    .withColumn("status_limite",
        when((col("limite").isNull()) | (col("limite") == 0), "SEM LIMITE")
        .when(col("vencimento_limite") < today_date, "LIMITE VENCIDO")
        .when(col("risco") == 0, "LIMITE INATIVO")
        .when(col("risco") > col("limite"), "LIMITE EXCEDIDO")
        .otherwise("LIMITE DISPONIVEL")
    ) \
    .withColumn("percentual_cm", col("limite_comissaria_contrato") / col("limite")) \
    .withColumn("falta_checar", greatest(
        (coalesce(col("percentual_exigido"), lit(0.5)) * (col("risco_exceto_comissaria") + col("risco_grupo") - col("risco_subtotal_comissaria")))
        - coalesce(col("confirmado_positivo"), lit(0)) - coalesce(col("confirmado_atencao"), lit(0)),
        lit(0)
    )) \
    .withColumn("data_primeira_operacao_apos_aprovacao",
        when(col("data_primeira_operacao") >= col("data_aprovacao"), col("data_primeira_operacao"))
    ) \
    .withColumn("dias_proposta_comercial", datediff(col("data_primeira_proposta_credito"), col("data_primeira_proposta_comercial"))) \
    .withColumn("dias_proposta_credito", datediff(col("data_primeira_proposta_formalizacao"), col("data_primeira_proposta_credito"))) \
    .withColumn("dias_proposta_formalizacao", datediff(col("data_primeira_proposta_concluida"), col("data_primeira_proposta_formalizacao"))) \
    .withColumn("tempo_conclusao", datediff(col("data_conclusao"), col("data_aprovacao"))) \
    .withColumn("tempo_analise", datediff(col("data_aprovacao"), col("data_entrada"))) \
    .withColumn("idade_cliente", floor(datediff(today_date, to_date(substring(col("data_inclusao").cast("string"), 1, 10))) / 365)) \
    .withColumn("idade_cliente_em_dias", coalesce(datediff(today_date, col("data_primeira_operacao")), lit(0))) \
    .withColumn("tipo_proposta",
        when(col("dias_sem_operar") > 120, "REATIVAÇÃO")
        .when(col("data_ultima_operacao").isNull(), "PROSPECÇÃO")
        .when(col("idade_cliente_em_dias") > 90, "RENOVAÇÃO")
        .otherwise("PROSPECÇÃO")
    ) \
    .withColumn("pais", lit("Brasil")) \
    .withColumn("primeiro_nome_gerente", split(col("gestor_da_plataforma"), " ")[0]) \
    .withColumn("status_operando_vencido",
        when(
            (col("data_ultima_operacao") > col("vencimento_limite")) &
            (col("vencimento_limite").isNotNull()) &
            (date_add(col("data_ultima_operacao"), 120) >= today_date),
            "OPERANDO VENCIDO"
        ).otherwise("OPERANDO NORMAL")
    ) \
    .withColumn("taxa_minima_exigida",
        (when(col("faixa_pdd") == 0.05, 0.0025) # Rating A (0.05 PDD)
         .when(col("faixa_pdd") == 0.7, 0.0075) # Rating C (0.7 PDD?) - DAX Logic vague on ratingPdd mapping, using best guess from PDD
         .otherwise(0.0050) # Rating B
        ) + 0.0150 + 0.0010
    ) \
    .withColumn("pendencias_desc", concat_ws(", ",
        when(col("status_atividade") == "INATIVO", "Cliente inativo"),
        when(col("falta_checar") > 0, "Confirmação desenquadrada"),
        when(col("vencimento_limite") < today_date, "Limite vencido"),
        when(col("limite_maximo_disponivel") <= 0, "Sem limite disponível"),
        when(col("problemas_checagem") > 0, "Problemas de checagem"),
        when(col("inadimplencia") > 0, "Títulos vencidos")
    )) \
    .withColumn("qtd_pendencias",
        (when(col("status_atividade") == "INATIVO", 1).otherwise(0) +
         when(col("falta_checar") > 0, 1).otherwise(0) +
         when(col("vencimento_limite") < today_date, 1).otherwise(0) +
         when(col("limite_maximo_disponivel") <= 0, 1).otherwise(0) +
         when(col("problemas_checagem") > 0, 1).otherwise(0) +
         when(col("inadimplencia") > 0, 1).otherwise(0))
    ) \
    .withColumn("perc_risco_clean", col("risco_clean") / col("risco_sem_renegociacao")) \
    .withColumn("penalidade_clean", when(col("perc_risco_clean") > 0.5, 1).otherwise(0)) \
    .withColumn("penalidade_inativo",
        when(col("dias_sem_operar") > 120, 3)
        .when(col("dias_sem_operar") > 60, 2)
        .when(col("dias_sem_operar") > 30, 1)
        .otherwise(0)
    ) \
    .withColumn("penalidade_perdas", when(col("perdas") > 0, 10).otherwise(0)) \
    .withColumn("perc_vencidos_risco", col("vencidos") / col("risco")) \
    .withColumn("penalidade_inadimplencia",
        when(col("perc_vencidos_risco") > 0.50, 4)
        .when(col("perc_vencidos_risco") > 0.25, 3)
        .when(col("perc_vencidos_risco") > 0.10, 2)
        .when(col("perc_vencidos_risco") > 0.05, 1)
        .otherwise(0)
    ) \
    .withColumn("perc_risco_renegociacao", col("risco_renegociacao") / col("risco")) \
    .withColumn("penalidade_renegociacao",
        when(col("perc_risco_renegociacao") == 1, 3)
        .when(col("perc_risco_renegociacao") > 0.4, 2)
        .when(col("perc_risco_renegociacao") > 0.01, 1)
        .otherwise(0)
    ) \
    .withColumn("qualidade_cliente",
        when(col("penalidade_perdas") > 0, 0)
        .otherwise(
            round(
                lit(10) - (
                    col("penalidade_inadimplencia") +
                    col("penalidade_renegociacao") +
                    col("penalidade_clean") +
                    col("penalidade_inativo")
                ), 0
            )
        )
    )

# Salvar
output_path_dim_clientes = "LH_Gold.dim_clientes"
df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_dim_clientes)
print(f"Tabela 'dim_clientes' recriada em: {output_path_dim_clientes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
