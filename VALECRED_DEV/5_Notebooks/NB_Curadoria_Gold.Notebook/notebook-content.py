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

# # Notebook de Curadoria da Camada Gold
# **Objetivo:** Este notebook é responsável por aplicar regras de negócio complexas, realizar joins entre tabelas limpas da camada Silver (Staging), e criar tabelas enriquecidas e modelos dimensionais (Fatos e Dimensões) para a camada **Gold** (ou tabelas finais refinadas da Silver).
# **Origem dos Dados:** Tabelas "staging" limpas geradas pelo notebook `NB_Preparacao_Silver` e algumas tabelas da camada Bronze que requerem processamento complexo direto.
# **Processos realizados:**
# 1.  **Configuração do Ambiente:** Define configurações do Spark e importa as bibliotecas necessárias.
# 2.  **Enriquecimento do Cadastro Geral:** Reconstrói a visão completa do cliente juntando dados de perfil, endereços, emails e telefones.
# 3.  **Processamento de Status de Protesto:** Calcula o status de protesto dos títulos.
# 4.  **Enriquecimento de Operações:** Adiciona informações de gerentes (brokers) e marcadores de operações informais.
# 5.  **Construção da Fato Baixas:** Cria a tabela fato de baixas enriquecida com dimensões.
# 6.  **Processamento Incremental de Pareceres:** Constrói a esteira de propostas e histórico de status.


# MARKDOWN ********************

# ## Seção 0: Configuração do ambiente Python

# CELL ********************

# Célula 0: Configuração da Sessão Spark
# ------------------------------------

# Corrige o problema de LEITURA de datas antigas (formato LEGACY do parquet)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

# Corrige o problema de ESCRITA de datas antigas (formato LEGACY do parquet)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# Importando as funções necessárias do PySpark
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, when, lit, concat, length, regexp_replace,
    collect_list, concat_ws, upper, greatest, substring, year,
    lead, date_add, lag, max, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from delta.tables import *
import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Enriquecimento do Cadastro Geral
# **Objetivo:** Consolidar as tabelas limpas de cadastro geral, endereços, emails e telefones em uma única visão enriquecida do cliente (`staging_cad_geral_limpa`).

# CELL ********************

# Célula 1.1: Leitura das Tabelas Staging Limpas
# ------------------------------------------------
print("Lendo tabelas de staging limpas para composição do cadastro geral...")
df_geral_limpa = spark.read.table("LH_Silver.staging_cad_geral_pf_pj_limpa")
df_enderecos_limpa = spark.read.table("LH_Silver.staging_enderecos_limpa")
df_emails_agg = spark.read.table("LH_Silver.staging_emails_agg")
df_telefones_agg = spark.read.table("LH_Silver.staging_telefones_agg")

# Célula 1.2: Join e Enriquecimento
# ------------------------------------------------
print("Realizando joins para enriquecimento...")
df_enriquecido = df_geral_limpa \
    .join(df_enderecos_limpa.select("CPFCNPJ", "CIDADE", "UF", "CEP"), on="CPFCNPJ", how="left") \
    .join(df_emails_agg, on="CPFCNPJ", how="left") \
    .join(df_telefones_agg, on="CPFCNPJ", how="left")

# Célula 1.3: Salvar Resultado
# ------------------------------------------------
output_path_geral = "LH_Silver.staging_cad_geral_limpa"
df_enriquecido.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_geral)
print(f"Tabela de cadastro geral enriquecida salva com sucesso em: {output_path_geral}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Processamento de Status de Protesto
# **Objetivo:** Calcular o status de protesto mais recente para cada título, baseando-se em ocorrências de cobrança. A tabela resultante `staging_protestos` enriquece a dimensão de títulos.

# CELL ********************

print("\nIniciando o processamento de status de protesto de títulos...")

# 2.1 Leitura das tabelas de origem (Bronze)
df_ocorrencias_bronze = spark.read.table("LH_Bronze.rlc_titulos_ocorrencias_cobranca")
df_titulos_cobranca_bronze = spark.read.table("LH_Bronze.tab_titulos_cobranca")
print("Tabelas de ocorrências e cobrança lidas da camada Bronze.")

# 2.2 Pré-cálculos e Lógica de Negócio
# ------------------------------------
# SQL: WHEN A.CODTITULO IN (SELECT CODTITULO FROM tab_titulos_cobranca WHERE ... AND CODOCORCOBRANCA = 1015)
df_titulos_para_protesto_cobranca = df_titulos_cobranca_bronze \
    .filter(col("CODOCORCOBRANCA") == 1015) \
    .select("CODTITULO") \
    .distinct() \
    .withColumn("flag_protesto_cobranca", lit(True))

# SQL: A.CODTITULO IN (SELECT CODTITULO FROM rlc_titulos_ocorrencias_cobranca WHERE ... AND A.CODOCORINTERNA = 2)
df_subquery_ocorrencia = df_ocorrencias_bronze \
    .filter(col("CODOCORINTERNA").isin(8, 34) & col("CODOCORCOBRBANCO").isin(19, 23)) \
    .select("CODTITULO") \
    .distinct() \
    .withColumn("flag_subquery_ocorrencia", lit(True))

# Filtragem principal
df_ocorrencias_filtradas = df_ocorrencias_bronze.filter(
    (
        (col("CODOCORINTERNA").isin(8, 17, 34, 2, 82)) &
        (col("CODOCORCOBRBANCO").isin(6, 19, 23, 10, 43)) &
        (col("TOCORRENCIA") == 2)
    ) |
    (
        (col("CODOCORINTERNA") == 8) &
        (col("CODOCORCOBRBANCO") == 9) &
        (col("TOCORRENCIA") == 1)
    )
)

# Isolar ocorrência mais recente
window_spec_latest = Window.partitionBy("CODTITULO").orderBy(col("CODTITULOOCORCOB").desc())

df_latest_ocorrencia = df_ocorrencias_filtradas \
    .withColumn("row_num", row_number().over(window_spec_latest)) \
    .filter(col("row_num") == 1) \
    .drop("row_num") \
    .join(df_titulos_para_protesto_cobranca, "CODTITULO", "left") \
    .join(df_subquery_ocorrencia, "CODTITULO", "left") \
    .fillna(False, subset=["flag_protesto_cobranca", "flag_subquery_ocorrencia"])
df_latest_ocorrencia.cache()

# Calcular Status Code e Descrição
cond_p1 = (substring(col("MOTIVOCODOCORCOBRBANCO"), 1, 2) == '14')
cond_p2 = (col("CODOCORINTERNA") == 2) & (col("flag_subquery_ocorrencia") == True)
cond_p3 = (col("CODOCORINTERNA") == 82)
cond_p4 = (col("flag_protesto_cobranca") == True)
cond_e = (col("CODOCORINTERNA") == 8) & (col("CODOCORCOBRBANCO") == 9)
cond_i = (col("CODOCORINTERNA") == 8)
cond_c = (col("CODOCORINTERNA") == 34)

df_com_status_code = df_latest_ocorrencia.withColumn("STATUSPROTESTO",
    when(cond_p1 | cond_p2 | cond_p3 | cond_p4, lit("P"))
    .when(cond_e, lit("E"))
    .when(cond_i, lit("I"))
    .when(cond_c, lit("C"))
    .otherwise(lit("N"))
)

df_com_status_desc = df_com_status_code.withColumn("STATUS_PROTESTO",
    when(col("STATUSPROTESTO") == 'P', lit("Protestado"))
    .when(col("STATUSPROTESTO") == 'E', lit("Instrução Protesto Enviada"))
    .when(col("STATUSPROTESTO") == 'I', lit("Instrução Protesto"))
    .when(col("STATUSPROTESTO") == 'C', lit("Em Cartório"))
    .otherwise(lit("N/A"))
).filter(col("STATUS_PROTESTO") != "N/A")

# 2.3 Salvar Resultado
# ------------------------------------------------
df_final_protestos = df_com_status_desc.select(
    "CODTITULO",
    "STATUS_PROTESTO",
    col("DATAINCLUSAO").alias("DATA_OCORRENCIA_PROTESTO")
)

output_path_protestos = "LH_Silver.staging_protestos"
df_final_protestos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_protestos)
print(f"Tabela de staging para protestos salva com sucesso em: {output_path_protestos}")

df_latest_ocorrencia.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: Enriquecimento da Tabela Operações
# **Objetivo:** Enriquecer a tabela base de operações com o histórico correto de gerentes (Broker) e identificar operações informais.

# CELL ********************

# Célula 3.1: Leitura da Base Limpa
# ------------------------------------------------
print("\nIniciando enriquecimento de operações...")
df_operacoes_base = spark.read.table("LH_Silver.staging_operacoes_limpa")

# Célula 3.2: Enriquecimento com Gerente (Broker)
# ------------------------------------------------
print("Enriquecendo com histórico de gerentes...")
df_bridge_gerente = spark.read.table("LH_Silver.bridge_cliente_gerente")

df_operacoes_com_historico = df_operacoes_base.join(
    df_bridge_gerente,
    (df_operacoes_base["CODCLIENTE"] == df_bridge_gerente["ClienteID"]) &
    (df_operacoes_base["DATAANALISE"].cast("date") >= df_bridge_gerente["DataInicioVigencia"]) &
    (df_operacoes_base["DATAANALISE"].cast("date") <= df_bridge_gerente["DataFimVigencia"]),
    "left"
)

df_operacoes_com_gerente_final = df_operacoes_com_historico.withColumn(
    "CODBROKER",
    when(
        (col("CODBROKER").isNotNull()) & (col("CODBROKER") != 0),
        col("CODBROKER")
    ).otherwise(col("GerenteID"))
).drop("ClienteID", "GerenteID", "DataInicioVigencia", "DataFimVigencia")

# Célula 3.3: Identificação de Operações Informais
# ------------------------------------------------
print("Identificando operações informais...")
df_titulos_limpa = spark.read.table("LH_Silver.staging_titulos_limpa")
df_cad_geral_arquivos = spark.read.table("LH_Bronze.cad_geral_arquivos")

df_chave_danfe = df_cad_geral_arquivos.filter(col("DESCRICAO") == 'CHAVEDANFE')
df_titulos_com_chave = df_titulos_limpa.join(df_chave_danfe, on="CODTITULO", how="inner")
df_operacoes_com_chave_base = df_operacoes_com_gerente_final.join(df_titulos_com_chave, on="CODOPERACAO", how="inner")

df_operacoes_com_chave_filtrado = df_operacoes_com_chave_base.filter(
    (df_operacoes_com_gerente_final["NOTASERVICO"] == 'N') &
    (df_operacoes_com_gerente_final["STATUSANALISE"] == 'D') &
    (df_operacoes_com_gerente_final["CODEMPRESA"] == 14) &
    (df_operacoes_com_gerente_final["STATUSACEITE"] == 'A') &
    (df_operacoes_com_gerente_final["TTO"].isin(['NO','CM','FC']))
)

df_vcount = df_operacoes_com_chave_filtrado.groupBy(df_operacoes_com_gerente_final["CODOPERACAO"]).count()
df_com_vcount = df_operacoes_com_gerente_final.join(df_vcount, on="CODOPERACAO", how="left")

df_final_com_informal = df_com_vcount.withColumn(
    "operacao_informal",
    when(
        ((col("count").isNull()) | (col("count") == 0)) &
        (col("CODEMPRESA") == 14) &
        (col("NOTASERVICO") == 'N'),
        lit(True)
    ).otherwise(lit(False))
).drop("count")

# Célula 3.4: Salvar Resultado
# ------------------------------------------------
output_path_operacoes = "LH_Silver.staging_operacoes_enriquecida"
df_final_com_informal.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_operacoes)
print(f"Tabela de operações enriquecida salva em: {output_path_operacoes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3.5: Construção da Fato Operações
# **Objetivo:** Selecionar e renomear as colunas da tabela de operações enriquecida para criar a `fato_operacoes` na camada Gold.

# CELL ********************

# Célula 3.5.1: Leitura e Transformação
# ------------------------------------------------
print("\nIniciando construção da fato_operacoes...")
df_operacoes_enriquecida = spark.read.table("LH_Silver.staging_operacoes_enriquecida")

# Seleção e renomeação de colunas para o padrão do Data Warehouse
df_fato_operacoes = df_operacoes_enriquecida.select(
    col("CODOPERACAO").alias("cod_operacao"),
    col("CODCLIENTE").alias("cod_cliente"),
    col("CODEMPRESA").alias("cod_empresa"),
    col("DATAINCLUSAO").alias("data_inclusao"),
    col("DATAANALISE").alias("data_analise"),
    col("DATAPAGAMENTO").alias("data_pagamento"),
    col("VALORBRUTO").alias("valor_bruto"),
    col("VALORLIQUIDO").alias("valor_liquido"),
    col("STATUSACEITE").alias("status_aceite"),
    col("STATUSANALISE").alias("status_analise"),
    col("CODBROKER").alias("cod_broker"),
    col("TTO"),
    col("STTO"),
    col("chave_produto"),
    col("operacao_informal")
)

# Célula 3.5.2: Salvar o Resultado
# ------------------------------------------------
output_path_fato_operacoes = "LH_Gold.fato_operacoes"
df_fato_operacoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_fato_operacoes)
print(f"Tabela 'fato_operacoes' construída e salva com sucesso em: {output_path_fato_operacoes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 4: Construção da Fato Baixas
# **Objetivo:** Unir a tabela de baixas limpa com as tabelas de dimensão para criar a tabela fato.

# CELL ********************

# Célula 4.1: Leitura e Carga de Dimensões
# ------------------------------------------------
print("\nIniciando construção da fato_baixas...")
df_baixas_staging = spark.read.table("LH_Silver.staging_baixas_limpa")
df_titulos_staging = spark.read.table("LH_Silver.staging_titulos_limpa")

df_dim_pago_por = spark.read.table("LH_Silver.sup_pago_pelo")
df_dim_forma_pagamento = spark.read.table("LH_Silver.sup_forma_de_pagamento")
df_dim_tipo_taxa = spark.read.table("LH_Silver.sup_tipo_de_baixa")
df_dim_motivo_baixa = spark.read.table("LH_Silver.sup_motivo_baixa")

# Célula 4.2: Correção de Dados e Joins
# ------------------------------------------------
df_baixas_corrigido = df_baixas_staging.withColumn("JUROS",
    when(col("JUROS") == -858005.8, 3912.5)
    .when(col("JUROS") == -4948525.71, -56747.24)
    .when(col("JUROS") == -4140.75, 0)
    .when(col("JUROS") == -1447.5, 52.5)
    .when(col("JUROS") == -1825.72, 66.28)
    .when(col("JUROS") == -965, 35)
    .when(col("JUROS") == -26000, 0)
    .otherwise(col("JUROS")))

df_enriquecido_baixas = df_baixas_corrigido \
    .join(df_titulos_staging, on="CODTITULO", how="left") \
    .join(df_dim_pago_por, df_baixas_corrigido.PAGOPELO == df_dim_pago_por.id, how="left") \
    .join(df_dim_forma_pagamento, df_baixas_corrigido.FORMA == df_dim_forma_pagamento.id, how="left") \
    .join(df_dim_tipo_taxa, df_baixas_corrigido.TIPOBAIXA == df_dim_tipo_taxa.id, how="left") \
    .join(df_dim_motivo_baixa, df_baixas_corrigido.MOTIVO == df_dim_motivo_baixa.id, how="left")

# Célula 4.3: Seleção e Persistência
# ------------------------------------------------
df_fato_baixas = df_enriquecido_baixas.select(
    df_baixas_corrigido["CODTITULOBAIXAS"], df_baixas_corrigido["CODTITULO"],
    df_baixas_corrigido["DATABAIXA"], df_baixas_corrigido["DATABAIXASIST"],
    df_baixas_corrigido["VLPAGO"], df_baixas_corrigido["DESCONTO"],
    df_baixas_corrigido["JUROS"], df_baixas_corrigido["TARIFARECOMPRA"],
    df_baixas_corrigido["DATAVENCIMENTO"], df_baixas_corrigido["CODOPERACAO"],
    df_dim_pago_por["descricao"].alias("PagoPor"),df_dim_forma_pagamento["descricao"].alias("Forma"),
    df_dim_tipo_taxa["descricao"].alias("TipoBaixa"), df_dim_motivo_baixa["descricao"].alias("Motivo"))

output_path_fato_baixas = "LH_Gold.fato_baixas"
df_fato_baixas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_fato_baixas)
print(f"Tabela 'fato_baixas' construída e salva com sucesso em: {output_path_fato_baixas}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 6: Construção da Dimensão Produto
# **Objetivo:** Construir a tabela de dimensão `dim_produto` a partir dos dados de operações e salvá-la na camada Gold. Esta lógica foi migrada do `DF_Produto_Gold.Dataflow` para centralizar as transformações em notebooks.

# CELL ********************

print("\nIniciando construção da dim_produto...")
from pyspark.sql.functions import concat, lit, when, col, sort_array, struct, monotonically_increasing_id

# 6.1: Leitura das tabelas de origem
# -----------------------------------
df_operacoes_silver = spark.read.table("LH_Silver.staging_operacoes_enriquecida")
df_tipo_op_bronze = spark.read.table("LH_Bronze.tab_tipooperacao")
df_subtipo_op_bronze = spark.read.table("LH_Bronze.tab_subtipooperacao")

# 6.2: Lógica de transformação (replicando o Power Query)
# --------------------------------------------------------
# Seleciona e remove duplicatas de TTO/STTO
df_produtos_base = df_operacoes_silver.select("STTO", "TTO").distinct()

# Join com tipo de operação
df_com_tipo = df_produtos_base.join(
    df_tipo_op_bronze.select("CODTTO", "DESCRICAO"),
    df_produtos_base.TTO == df_tipo_op_bronze.CODTTO,
    "left"
).withColumnRenamed("DESCRICAO", "TipoProduto")

# Join com subtipo de operação
df_com_subtipo = df_com_tipo.join(
    df_subtipo_op_bronze.select("CODSTTO", "DESCRICAO"),
    df_com_tipo.STTO == df_subtipo_op_bronze.CODSTTO,
    "left"
).withColumnRenamed("DESCRICAO", "SubTipoProduto")

# Criação de colunas de chave e nome do produto
df_com_chaves = df_com_subtipo \
    .withColumn("chave_produto", concat(col("TTO"), col("STTO"))) \
    .withColumn("Produto",
                when(col("SubTipoProduto").isNull(), col("TipoProduto"))
                .otherwise(concat(col("SubTipoProduto"), lit(" - "), col("TipoProduto")))
    )

# Limpeza e padronização de nomes
df_nomes_limpos = df_com_chaves \
    .withColumn("Produto", regexp_replace(col("Produto"), "COMISSÁRIA", "COMISSARIA SIMPLES")) \
    .withColumn("Produto", regexp_replace(col("Produto"), "COMISSARIA SIMPLES - COMISSARIA SIMPLES", "COMISSARIA SIMPLES"))

# Criação da coluna de informação de mercado
df_info_mercado = df_nomes_limpos \
    .withColumn("ProdutoInformacaoMercado", col("Produto")) \
    .withColumn("ProdutoInformacaoMercado", regexp_replace(col("ProdutoInformacaoMercado"), "NORMAL", "DESCONTO"))

# Seleção das colunas finais da staging
df_staging_produto_lbfactor = df_info_mercado.select("ProdutoInformacaoMercado", "Produto", "chave_produto")

# 6.3: Adição de produtos manuais
# ---------------------------------
# Criando o DataFrame para produtos manuais (replicando a tabela hardcoded do Dataflow)
schema_manual = StructType([
    StructField("chave_produto", StringType(), True),
    StructField("Produto", StringType(), True),
    StructField("ProdutoInformacaoMercado", StringType(), True)
])
# A tabela manual no dataflow original está vazia, então criamos um DF vazio.
# Se houvesse dados, seriam adicionados aqui. Ex: data_manual = [("CHAVE1", "Produto Manual", "Info Manual")]
data_manual = []
df_produto_manual = spark.createDataFrame(data_manual, schema_manual)

# Unindo a base com os produtos manuais
df_combinado = df_staging_produto_lbfactor.unionByName(df_produto_manual)

# 6.4: Adição de Surrogate Key e persistência
# ---------------------------------------------
# Filtra nulos e prepara para deduplicação
df_filtrado = df_combinado.filter(col("Produto").isNotNull() & (col("Produto") != ""))

# Deduplicação determinística:
# Particiona pela chave de negócio e ordena por uma coluna (pode ser a mesma chave)
# para garantir que a mesma linha seja sempre escolhida.
window_dedup = Window.partitionBy("chave_produto").orderBy(col("Produto").asc())

df_deduplicado = df_filtrado \
    .withColumn("rn", row_number().over(window_dedup)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# Adição de Surrogate Key:
# Agora que os dados são únicos, ordenamos a tabela final e adicionamos a SK.
window_spec_sk = Window.orderBy("chave_produto")
df_com_sk = df_deduplicado \
    .sort("chave_produto") \
    .withColumn("sk_produto", row_number().over(window_spec_sk))

# Seleciona e renomeia colunas para o padrão do DW
df_final_dim_produto = df_com_sk.select(
    col("sk_produto"),
    col("chave_produto"),
    col("Produto").alias("produto"),
    col("ProdutoInformacaoMercado").alias("produto_informacao_de_mercado")
)

# Salva a dimensão no Data Warehouse Gold
output_path_dim_produto = "LH_Gold.dim_produto"
df_final_dim_produto.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_dim_produto)
print(f"Tabela 'dim_produto' construída e salva com sucesso em: {output_path_dim_produto}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 7: Construção da Fato Títulos
# **Objetivo:** Criar a tabela `fato_titulos` enriquecida, consolidando dados de títulos, operações, produtos e regras de negócio de datas e status.

# CELL ********************

print("\nIniciando construção da fato_titulos...")
from pyspark.sql.functions import dayofweek

# 6.1 Leitura das Tabelas
# -----------------------
df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa")
df_limites = spark.read.table("LH_Silver.staging_rlc_clientes_sacados_limites")
df_devolucoes = spark.read.table("LH_Silver.staging_operacoes_devolucoes_limpa")
df_operacoes = spark.read.table("LH_Silver.staging_operacoes_enriquecida")
df_protestos = spark.read.table("LH_Silver.staging_protestos")
df_feriados = spark.read.table("LH_Bronze.tab_feriados")

# Tabelas de Dimensão/Fato auxiliares
# A dim_produto agora é lida da camada Gold, onde é construída por este notebook.
df_produtos = spark.read.table("LH_Gold.dim_produto")
df_ultima_conf = spark.read.table("LH_Silver.fact_ultima_confirmacao")

# 6.2 Preparação e Enriquecimento
# -------------------------------

# Filtro inicial
df_titulos_filt = df_titulos.filter(~col("TDOC").isin("BL", "RC"))

# Preparação de chave de cliente e RaizCNPJ
# Nota: staging_titulos_limpa (Prep Silver) já limpa dados básicos, mas RaizCNPJ específica para regra de chave_cliente_sacado
# é recalculada aqui para garantir consistência com a lógica do Dataflow.
df_titulos_prep = df_titulos_filt \
    .withColumn("TipoDocumentoSacado",
                when(length(col("CPFCNPJSACADO")) == 11, "CPF")
                .when(length(col("CPFCNPJSACADO")) == 14, "CNPJ")
                .otherwise("Inválido")) \
    .withColumn("RaizCNPJ",
                when(col("TipoDocumentoSacado") == "CNPJ", substring(col("CPFCNPJSACADO"), 1, 8))
                .otherwise(col("CPFCNPJSACADO")))

# Join com Operações para obter CODCLIENTE (necessário para chave_cliente_sacado) e outros campos
df_titulos_ops = df_titulos_prep.join(
    df_operacoes.select("CODOPERACAO", "CODCLIENTE", "DATAANALISE", "STATUSACEITE", "STATUSANALISE", "chave_produto", "CODEMPRESA"),
    "CODOPERACAO",
    "left"
)

# Criação da chave_cliente_sacado
df_titulos_keys = df_titulos_ops \
    .withColumn("chave_cliente_sacado", concat(col("CODCLIENTE").cast("string"), lit("-"), col("RaizCNPJ")))

# Join com Limites (Intercompany)
df_titulos_limites = df_titulos_keys.join(
    df_limites.select("chave_cliente_sacado", "TIPO"),
    "chave_cliente_sacado",
    "left"
).withColumn("intercompany",
             when(col("TIPO") == "INTERCIA", "SIM").otherwise("NÃO")
).drop("TIPO")

# Join com Produtos
df_titulos_prod = df_titulos_limites.join(
    df_produtos.select(col("chave_produto"), col("produto_informacao_de_mercado").alias("produto_temp")),
    "chave_produto",
    "left"
)

# Join com Devoluções (Recompra)
df_titulos_recompra = df_titulos_prod.join(
    df_devolucoes.select(col("CODTITULO"), col("CODOPERACAO").alias("cod_operacao_recompra")),
    "CODTITULO",
    "left"
)

# Join com Última Confirmação
df_titulos_conf = df_titulos_recompra.join(
    df_ultima_conf.select(col("cod_titulo").alias("CODTITULO"), col("confirmacao").alias("confirmado_por")),
    "CODTITULO",
    "left"
).na.fill({"AMORTIZACOES": 0})

# Join com Protestos
df_titulos_prot = df_titulos_conf.join(
    df_protestos.select("CODTITULO", "STATUS_PROTESTO"),
    "CODTITULO",
    "left"
).withColumn("status_protesto", coalesce(col("STATUS_PROTESTO"), lit("NÃO PROTESTADO")))

# 6.3 Cálculos de Negócio
# -----------------------

# valor_vezes_prazo
df_calc_1 = df_titulos_prot.withColumn("valor_vezes_prazo", col("PRAZO") * col("VALOR"))

# produto_com_intercia
df_calc_2 = df_calc_1.withColumn("produto_com_intercia",
    when((col("intercompany") == "SIM") & (col("chave_produto").isin("NO", "CM")), "INTERCOMPANY")
    .otherwise(col("produto_temp"))
)

# Data Vencimento Útil
# PySpark dayofweek: 1=Domingo, 7=Sábado.
# Ajuste: Domingo (+1), Sábado (+2)
df_dates_1 = df_calc_2.withColumn("dia_da_semana", dayofweek(col("VENCPRORROGADO"))) \
    .withColumn("data_vencimento_util_temp",
                when(col("dia_da_semana") == 1, date_add(col("VENCPRORROGADO"), 1))
                .when(col("dia_da_semana") == 7, date_add(col("VENCPRORROGADO"), 2))
                .otherwise(col("VENCPRORROGADO")))

# Data Vencimento Útil - Substituição por Join com dim_calendario (Gold)
# Lógica anterior: Cálculo manual considerando dia da semana e feriados (L/N)
# Nova lógica: Join com dim_calendario que já possui proximo_dia_util calculado
# -----------------------------------------------------------------------

# Carregar dim_calendario (Gold)
try:
    df_dim_calendario = spark.read.table("LH_Gold.dim_calendario") \
        .select(col("data"), col("proximo_dia_util"))

    # Realizar Join para buscar o próximo dia útil baseado no VENCPRORROGADO
    # Nota: Usamos VENCPRORROGADO como a data base para verificar se é útil.
    # Se VENCPRORROGADO for útil, proximo_dia_util será ele mesmo (na dim_calendario).
    # Se não for, será o próximo.

    df_dates_final = df_calc_2.join(
        df_dim_calendario,
        df_calc_2.VENCPRORROGADO == df_dim_calendario.data,
        "left"
    ).withColumnRenamed("proximo_dia_util", "data_vencimento_util") \
     .drop("data") # Remove a coluna de data do join (que é igual a VENCPRORROGADO)

except Exception as e:
    print(f"Erro ao ler LH_Gold.dim_calendario: {e}. Mantendo lógica antiga de cálculo.")

    # Fallback para lógica antiga caso a tabela não exista ou ocorra erro
    df_feriados_sel = df_feriados.select(col("DATAFERIADO").alias("data_feriado"), col("TFERIADO").alias("tipo_feriado"))

    df_dates_1 = df_calc_2.withColumn("dia_da_semana", dayofweek(col("VENCPRORROGADO"))) \
        .withColumn("data_vencimento_util_temp",
                    when(col("dia_da_semana") == 1, date_add(col("VENCPRORROGADO"), 1))
                    .when(col("dia_da_semana") == 7, date_add(col("VENCPRORROGADO"), 2))
                    .otherwise(col("VENCPRORROGADO")))

    df_dates_2 = df_dates_1.join(
        df_feriados_sel,
        df_dates_1.data_vencimento_util_temp == df_feriados_sel.data_feriado,
        "left"
    )

    df_dates_final = df_dates_2.withColumn("data_vencimento_util",
        when(col("tipo_feriado") == "L", col("data_vencimento_util_temp"))
        .when((col("tipo_feriado") == "N") & (col("dia_da_semana") == 6), date_add(col("data_vencimento_util_temp"), 3))
        .when((col("tipo_feriado") == "N"), date_add(col("data_vencimento_util_temp"), 1))
        .otherwise(col("data_vencimento_util_temp"))
    ).drop("data_vencimento_util_temp", "tipo_feriado", "data_feriado", "dia_da_semana")

# Status Deferimento
df_status_1 = df_dates_final.withColumn("status_deferimento",
    when((col("ACEITO") == "S") & (col("STATUSACEITE") == "A") & (col("STATUSANALISE") == "D"), "Sim")
    .otherwise("Não")
)

# Status Clean
df_status_2 = df_status_1.withColumn("status_clean",
    when(col("produto_com_intercia") == "DESCONTO", "NORMAL").otherwise("CLEAN")
)

# Confirmação
df_conf = df_status_2.withColumn("confirmacao",
    when(col("DOCCONFIRMADO") == "N", "Atenção")
    .when(col("DOCCONFIRMADO") == "S", None)
    .when(col("DOCCONFIRMADO") == "C", "Positivo")
    .when(col("DOCCONFIRMADO") == "P", "Problema")
    .when(col("DOCCONFIRMADO") == "A", "Alerta")
    .when(col("DOCCONFIRMADO").isNull(), "Não Contatado")
    .when(col("DOCCONFIRMADO").isin("E", "AZ"), "Eletrônico")
    .otherwise(col("DOCCONFIRMADO"))
)

# Ordem Confirmação
df_ordem = df_conf.withColumn("ordem_confirmacao",
    when(col("confirmacao") == "Não Contatado", 5)
    .when(col("confirmacao") == "Atenção", 2)
    .when(col("confirmacao") == "Eletrônico", 0)
    .when(col("confirmacao") == "Positivo", 1)
    .when(col("confirmacao") == "Alerta", 3)
    .when(col("confirmacao") == "Problema", 4)
    .otherwise(None)
)

# 6.4 Seleção e Renomeação Final
# ------------------------------
df_final = df_ordem.select(
    col("CODTITULO").alias("cod_titulo"),
    col("CODOPERACAO").alias("cod_operacao"),
    col("TDOC").alias("tipo_documento"),
    col("NDOC").alias("numero_documento"),
    col("CPFCNPJSACADO").alias("cpf_cnpj_sacado"),
    col("VENCIMENTO").alias("vencimento"),
    col("VENCPRORROGADO").alias("vencimento_prorrogado"),
    col("VALOR").alias("valor"),
    col("PRAZO").alias("prazo"),
    col("ACEITO").alias("aceito"),
    col("DATAINCLUSAO").alias("data_inclusao"),
    col("USUAINCLUSAO").alias("usuario_inclusao"),
    col("DATAALTERACAO").alias("data_alteracao"),
    col("USUAALTERACAO").alias("usuario_alteracao"),
    col("AMORTIZACOES").alias("amortizacoes"),
    col("chave_produto"),
    col("status_protesto"),
    col("TipoDocumentoSacado").alias("tipo_documento_sacado"),
    col("RaizCNPJ").alias("raiz_cnpj"),
    "valor_vezes_prazo",
    "produto_com_intercia",
    "data_vencimento_util",
    "status_deferimento",
    "status_clean",
    "confirmacao",
    "ordem_confirmacao",
    "cod_operacao_recompra",
    "confirmado_por",
    "intercompany"
)

output_path_titulos_final = "LH_Gold.fato_titulos"
df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_titulos_final)
print(f"Tabela 'fato_titulos' construída e salva em: {output_path_titulos_final}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 5: Processamento Incremental de Pareceres
# **Objetivo:** Processar a tabela `cad_geral_pareceres` de forma incremental para reconstruir a `esteira_de_propostas`.

# CELL ********************

# Célula 5.1: Configuração e Watermark
# ------------------------------------------------
print("\nIniciando o processamento incremental de pareceres.")
source_table_name_pareceres = "LH_Bronze.cad_geral_pareceres"
target_pareceres_status_table_name = "LH_Silver.pareceres_de_alteracao_de_status"
target_esteira_table_name = "LH_Gold.esteira_de_propostas"
watermark_table_name = "LH_Silver.etl_watermark_control"
notebook_name = "NB_Prepare_Silver_Staging_Pareceres" # Mantendo o nome original do processo para compatibilidade do watermark

# Leitura do Watermark
try:
    df_watermark = spark.read.table(watermark_table_name)
    last_watermark_str = df_watermark.filter(col("TableName") == notebook_name).select("LastWatermarkValue").collect()[0][0]
    try:
        last_watermark = datetime.datetime.strptime(last_watermark_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        last_watermark = datetime.datetime.strptime(last_watermark_str, "%Y-%m-%d %H:%M:%S")
    print(f"Watermark encontrado: {last_watermark}")
except Exception as e:
    last_watermark = datetime.datetime(1900, 1, 1)
    print(f"Usando watermark padrão: {last_watermark}. Erro: {e}")

# Célula 5.2: Leitura Incremental
# ------------------------------------------------
df_pareceres_raw = spark.read.table(source_table_name_pareceres)
df_clientes_staging = spark.read.table("LH_Silver.staging_clientes_limpa")
df_usuarios_raw = spark.read.table("LH_Bronze.cad_usuarios")
df_status_clientes_esteira = spark.read.table("LH_Silver.sup_status_de_clientes_da_esteira")

df_pareceres_incremental = df_pareceres_raw.filter(
    (col("DATAINCLUSAO") > last_watermark) | (col("DATAALTERACAO") > last_watermark)
).cache()

record_count = df_pareceres_incremental.count()

if record_count > 0:
    new_watermark_df = df_pareceres_incremental.withColumn(
        "latest_date",
        greatest(
            coalesce(col("DATAINCLUSAO"), lit(datetime.datetime(1900, 1, 1))),
            coalesce(col("DATAALTERACAO"), lit(datetime.datetime(1900, 1, 1)))
        )
    ).agg(max("latest_date").alias("NewWatermark"))
    new_watermark = new_watermark_df.collect()[0]["NewWatermark"]
    print(f"Registros incrementais: {record_count}. Novo watermark: {new_watermark}")
else:
    new_watermark = last_watermark
    print("Nenhum dado novo encontrado.")

# Célula 5.3: Transformação e Merge
# ------------------------------------------------
print("Aplicando lógica de transformação...")
df_replica_pareceres_delta = df_pareceres_incremental \
    .filter(year(col("DATAINCLUSAO")) >= 2024) \
    .drop("ENCAMINHAR", "ALERTA", "CODPASTA", "CODTAREFA", "USUAALTERACAO", "DATAALTERACAO") \
    .withColumn("OBS", col("OBS").substr(1, 255)) \
    .withColumn("codTipoParecer", col("CODTIPOPARECER").cast(LongType())) \
    .filter(col("codTipoParecer") == 1) \
    .filter((col("CPFCNPJ").isNotNull() & (col("CPFCNPJ") != "")) & (col("OBS").isNotNull() & (col("OBS") != "")) & (col("USUAINCLUSAO").isNotNull()) & (col("DATAINCLUSAO").isNotNull())) \
    .filter(col("OBS").startswith("STATUS ALTERADO PARA ")) \
    .withColumn("STATUS_DO_CLIENTE", substring(col("OBS"), 22, 100)) \
    .withColumn("BASE", lit(40).cast(LongType())) \
    .select("CODPARECER", "CPFCNPJ", "CODOPERACAO", "DATAINCLUSAO", "USUAINCLUSAO", "STATUS_DO_CLIENTE", "BASE")

window_cliente_data_delta = Window.partitionBy("CODCLIENTE").orderBy(col("DATAINCLUSAO").asc())

df_pareceres_enriquecidos_delta = df_replica_pareceres_delta \
    .join(df_clientes_staging.select("CPFCNPJ", "CODCLIENTE"), ["CPFCNPJ"], "left") \
    .withColumn("chave_base_cliente", concat(col("BASE"), lit("-"), col("CODCLIENTE"))) \
    .join(df_usuarios_raw.select("CODUSUARIO", "NOME"), col("USUAINCLUSAO") == col("CODUSUARIO"), "left") \
    .withColumnRenamed("NOME", "USUARIO") \
    .join(df_status_clientes_esteira, "STATUS_DO_CLIENTE", "left") \
    .filter(col("CODCLIENTE").isNotNull() & (col("CODCLIENTE") != "")) \
    .withColumn("INDICE", row_number().over(window_cliente_data_delta)) \
    .withColumn("chave_original", (col("INDICE") * 1000000000 + col("CODCLIENTE")).cast(LongType())) \
    .withColumnRenamed("DATAINCLUSAO", "DATALOG") \
    .select("CODPARECER", "CODCLIENTE", "STATUS_DO_CLIENTE", "DATALOG", "BASE", "USUARIO", "chave_base_cliente", "INDICE", "chave_original", "MACROPROCESSO", "FASE")

if spark.catalog.tableExists(target_pareceres_status_table_name):
    delta_table = DeltaTable.forName(spark, target_pareceres_status_table_name)
    delta_table.alias("t").merge(df_pareceres_enriquecidos_delta.alias("s"), "t.CODPARECER = s.CODPARECER").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_pareceres_enriquecidos_delta.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_pareceres_status_table_name)
df_pareceres_incremental.unpersist()

# Célula 5.4: Reconstrução da Esteira de Propostas
# ------------------------------------------------
print("Reconstruindo esteira_de_propostas...")
df_pareceres_completa = spark.read.table(target_pareceres_status_table_name)
window_lag = Window.partitionBy("CODCLIENTE").orderBy("DATALOG")
df_com_lag = df_pareceres_completa \
    .withColumn("STATUS_DO_CLIENTE_ANTERIOR", lag("STATUS_DO_CLIENTE").over(window_lag)) \
    .withColumn("DATALOG_ANTERIOR", lag("DATALOG").over(window_lag)) \
    .withColumn("MACROPROCESSO_ANTERIOR", lag("MACROPROCESSO").over(window_lag)) \
    .withColumn("FASE_ANTERIOR", lag("FASE").over(window_lag))

df_transicoes = df_com_lag.filter(col("STATUS_DO_CLIENTE") != col("STATUS_DO_CLIENTE_ANTERIOR")).na.drop(subset=["STATUS_DO_CLIENTE_ANTERIOR"])

df_esteira_final = df_transicoes \
    .withColumn("DEVOLUCAO", when((col("MACROPROCESSO_ANTERIOR") == "CREDITO") & (col("MACROPROCESSO") == "COMERCIAL"), True).otherwise(False)) \
    .withColumn("RECEBIDA", when((col("MACROPROCESSO_ANTERIOR") == "COMERCIAL") & (col("MACROPROCESSO") == "CREDITO"), True).otherwise(False)) \
    .select("INDICE", "CODCLIENTE", "BASE", "DATALOG_ANTERIOR", "DATALOG", "chave_base_cliente", "STATUS_DO_CLIENTE_ANTERIOR", "STATUS_DO_CLIENTE", "MACROPROCESSO_ANTERIOR", "MACROPROCESSO", "FASE_ANTERIOR", "FASE", "USUARIO", "DEVOLUCAO", "RECEBIDA")

df_esteira_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_esteira_table_name)
print("Esteira reconstruída.")

# Célula 5.5: Atualização do Watermark
# ------------------------------------------------
print("Atualizando watermark...")
new_watermark_str = new_watermark.strftime("%Y-%m-%d %H:%M:%S.%f")
new_watermark_data = [(notebook_name, new_watermark_str)]
df_new_watermark = spark.createDataFrame(new_watermark_data, ["TableName", "LastWatermarkValue"])

if spark.catalog.tableExists(watermark_table_name):
    delta_watermark_table = DeltaTable.forName(spark, watermark_table_name)
    delta_watermark_table.alias("t").merge(df_new_watermark.alias("s"), "t.TableName = s.TableName") \
        .whenMatchedUpdate(set={"LastWatermarkValue": "s.LastWatermarkValue"}) \
        .whenNotMatchedInsert(values={"TableName": "s.TableName", "LastWatermarkValue": "s.LastWatermarkValue"}) \
        .execute()
else:
    df_new_watermark.write.mode("overwrite").saveAsTable(watermark_table_name)
print("Processo concluído.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
