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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook de Preparação da Camada Silver (Staging - Limpeza)
# **Objetivo:** Este notebook é responsável por ler os dados brutos da camada **Bronze**, aplicar transformações de **limpeza e desduplicação**, e salvar os dados resultantes na camada **Silver**.
# 
# **Observação:** As etapas de enriquecimento (joins) foram movidas para o notebook `NB_Curadoria_Gold` para separar a limpeza da construção de modelos de negócio.
# 
# **Processos realizados:**
# 1.  **Configuração do Ambiente:** Define configurações do Spark e importa as bibliotecas necessárias.
# 2.  **Limpeza de `tab_titulos`:** Remove duplicatas para garantir que cada título seja único.
# 3.  **Limpeza de `cad_clientes`:** Desduplica a tabela base para a dimensão de clientes.
# 4.  **Limpeza de Componentes do Cadastro Geral:** Limpa e salva individualmente tabelas de telefones, emails, endereços e cadastro geral (PF/PJ), preparando-as para enriquecimento posterior.
# 5.  **Limpeza de `tab_operacoes`:** Remove duplicatas da tabela de operações.
# 6.  **Processamento da Chave DANFE:** Extrai informações detalhadas da chave da nota fiscal.
# 7.  **Limpeza de `tab_titulos_baixas`:** Limpa os dados de baixas de títulos.
# 8.  **Processamento de Contratos de Clientes:** Limpa a tabela de contratos.
# 9.  **Processamento da Bridge Cliente-Gerente:** Cria tabela ponte de histórico de gerentes e tabela de relacionamento atual.
# 10. **Limpeza de Cache:** Libera os DataFrames da memória.


# MARKDOWN ********************

# ## Seção 0: Configuração do ambiente Python
# **Descrição:** Esta célula prepara a sessão Spark e importa todas as funções e bibliotecas que serão utilizadas ao longo do notebook.

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
    lead, date_add, lag, max, coalesce, date_sub
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from functools import reduce
from delta.tables import *
import datetime

def upsert_silver_table(spark, df_incoming, target_path, merge_key):
    """
    Executa um MERGE INTO (upsert) se a tabela de destino existir, caso contrario faz o overwrite inicial.
    """
    if spark.catalog.tableExists(target_path):
        print(f"Executando MERGE (Upsert) na tabela {target_path} usando a chave {merge_key}...")
        delta_table = DeltaTable.forName(spark, target_path)
        delta_table.alias("target") \
            .merge(
                df_incoming.alias("source"),
                f"target.{merge_key} = source.{merge_key}"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        print(f"Tabela {target_path} nao existe. Criando nova tabela (Overwrite)...")
        df_incoming.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_path)
    return spark.read.table(target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Limpeza da Tabela tab_titulos
# **Objetivo:** A tabela `tab_titulos` na camada Bronze pode conter múltiplos registros para o mesmo título. Esta seção isola apenas o registro mais recente e válido.

# CELL ********************

# Célula 1.1: Parâmetros e Leitura
# ------------------------------------------------
source_lakehouse = "LH_Bronze"
target_lakehouse = "LH_Silver"
source_table_titulos = "tab_titulos"
target_table_titulos = "staging_titulos_limpa"

print(f"Iniciando a limpeza da tabela: {source_lakehouse}.{source_table_titulos}")
df_bronze_titulos = spark.read.table(f"{source_lakehouse}.{source_table_titulos}")

# Célula 1.2: Lógica de Desduplicação
# ----------------------------------------------------
key_columns_titulos = ["CODTITULO"]
df_with_latest_date = df_bronze_titulos.withColumn(
    "DATA_MAIS_RECENTE",
    greatest(col("DATAALTERACAO"), col("DATAINCLUSAO"), col("LIQUIDACAO"))
)
windowSpec_titulos = Window.partitionBy([col(c) for c in key_columns_titulos]).orderBy(col("DATA_MAIS_RECENTE").desc())
df_ranked_titulos = df_with_latest_date.withColumn("row_num", row_number().over(windowSpec_titulos))
df_deduplicated_titulos = df_ranked_titulos.filter(col("row_num") == 1).drop("row_num", "DATA_MAIS_RECENTE")

# Célula 1.3: Seleção e Renomeação de Colunas
# ------------------------------------------------------
df_titulos_final = df_deduplicated_titulos.select(
    col("CODTITULO").alias("cod_titulo"),
    col("CODOPERACAO").alias("cod_operacao"),
    col("NDOC").alias("n_doc"),
    col("TDOC").alias("t_doc"),
    col("VENCIMENTO").alias("vencimento"),
    col("VENCPRORROGADO").alias("venc_prorrogado"),
    col("PRAZO").alias("prazo"),
    col("CPFCNPJSACADO").alias("cpf_cnpj_sacado"),
    col("CPFCNPJCEDENTE").alias("cpf_cnpj_cedente"),
    col("VALOR").alias("valor"),
    col("DESAGIO").alias("desagio"),
    col("LIQUIDO").alias("liquido"),
    col("AMORTIZACOES").alias("amortizacoes"),
    col("VALORDEVIDO").alias("valor_devido"),
    col("LIQUIDACAO").alias("liquidacao"),
    col("ACEITO").alias("aceito"),
    col("CODBANCOCOBR").alias("cod_banco_cobr"),
    col("DATACONF").alias("data_conf"),
    col("USUACONF").alias("usua_conf"),
    col("DATAINCLUSAO").alias("data_inclusao"),
    col("DOCCONFIRMADO").alias("doc_confirmado"),
    col("MOTIVO").alias("motivo"),
    col("PRACA").alias("praca"),
    col("CHAVEDANFE").alias("chave_danfe"),
    col("NOSSONUMERO").alias("nosso_numero"),
    col("CODFUNDO").alias("cod_fundo"),
    col("TTO").alias("tipo_cobranca"),
    col("FILIAL").alias("raiz_cnpj"),
    col("CODEMISSAO").alias("cod_emissao"),
    col("STATUSCONFIRMACAO").alias("status_confirmacao"),
    col("SEUNUMERO").alias("seu_numero_bancario"),
    col("CODREMESSA").alias("cod_remessa")
).orderBy(col("data_inclusao").desc())

# Célula 1.4: Salvar e Armazenar em Cache
# ------------------------------------------------------
output_path_titulos = f"{target_lakehouse}.{target_table_titulos}"
df_titulos_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_titulos)

# Armazena o resultado em cache
spark.table(output_path_titulos).cache()
print(f"Tabela limpa salva e em cache: {output_path_titulos}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Limpeza da Tabela cad_clientes
# **Objetivo:** Desduplicar a tabela `cad_clientes`.

# CELL ********************

# Célula 2.1: Parâmetros e Leitura
# ------------------------------------------------
source_table_clientes = "cad_clientes"
target_table_clientes = "staging_clientes_limpa"

print(f"\nIniciando a limpeza da tabela: {source_lakehouse}.{source_table_clientes}")
df_bronze_clientes = spark.read.table(f"{source_lakehouse}.{source_table_clientes}")

# Célula 2.2: Lógica de Desduplicação
# ----------------------------------------------------
key_columns_clientes = ["CODCLIENTE"]
order_by_column_clientes = "DATAALTERACAO"
windowSpec_clientes = Window.partitionBy([col(c) for c in key_columns_clientes]).orderBy(col(order_by_column_clientes).desc())
df_ranked_clientes = df_bronze_clientes.withColumn("row_num", row_number().over(windowSpec_clientes))
df_deduplicated_clientes = df_ranked_clientes.filter(col("row_num") == 1).drop("row_num")

# Célula 2.3: Salvar o Resultado
# ------------------------------------------------------
output_path_clientes = f"{target_lakehouse}.{target_table_clientes}"
# 🧠 Tensor: Implementacao de Upsert (MERGE) nas Dimensoes Silver
# 💡 O que: Substituida a logica de gravacao em modo overwrite por operacoes incrementais de MERGE INTO (upsert) nas tabelas staging_clientes_limpa, staging_telefones_agg, staging_emails_agg, staging_enderecos_limpa e staging_sacados_enriquecida.
# 🎯 Por que: Garantir processamento eficiente das cargas de dados, inserindo registros novos e atualizando os existentes sem a necessidade de reescrever toda a tabela diariamente.
# 📊 Impacto: Otimizacao severa de I/O de disco para o storage Delta Lake, viabilizando cargas mais rapidas e com menor overhead do Spark.
# 🔬 Medicao: O log do cluster Spark detalhara a operacao de Merge command com numeros de "numTargetRowsUpdated" e "numTargetRowsInserted" em vez de recalculo total.
upsert_silver_table(spark, df_deduplicated_clientes, output_path_clientes, "CODCLIENTE")
print(f"Tabela limpa salva com sucesso em: {output_path_clientes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: Limpeza de Componentes do Cadastro Geral
# **Objetivo:** Limpar e padronizar as tabelas de telefones, emails, endereços e cadastro geral (PF/PJ) individualmente, salvando-as como tabelas de staging para posterior enriquecimento.

# CELL ********************

# Célula 3.1: Processamento de Telefones
# --------------------------------------------------------------------------------
print("\nIniciando o tratamento de telefones...")
df_telefones_bronze = spark.read.table("LH_Bronze.cad_telefones")
df_telefones_agg = df_telefones_bronze \
    .filter((col("FONE").isNotNull() & (col("FONE") != "")) & (col("DDD").isNotNull() & (col("DDD") != ""))) \
    .withColumn("FONE_limpo", regexp_replace(col("FONE"), "-", "")) \
    .withColumn("FONE_COMPLETO", regexp_replace(concat(col("DDD"), col("FONE_limpo")), " ", "")) \
    .filter((length(col("FONE_COMPLETO")) >= 10) & (length(col("FONE_COMPLETO")) <= 11)) \
    .select(col("CPFCNPJ"), col("FONE_COMPLETO").alias("FONE"), col("CONTATO")).distinct() \
    .groupBy("CPFCNPJ").agg(concat_ws("; ", collect_list("FONE")).alias("Telefones"))

output_path_telefones = "LH_Silver.staging_telefones_agg"
# 🧠 Tensor: Implementacao de Upsert (MERGE) nas Dimensoes Silver
# 💡 O que: Substituida a logica de gravacao em modo overwrite por operacoes incrementais de MERGE INTO (upsert) nas tabelas staging_clientes_limpa, staging_telefones_agg, staging_emails_agg, staging_enderecos_limpa e staging_sacados_enriquecida.
# 🎯 Por que: Garantir processamento eficiente das cargas de dados, inserindo registros novos e atualizando os existentes sem a necessidade de reescrever toda a tabela diariamente.
# 📊 Impacto: Otimizacao severa de I/O de disco para o storage Delta Lake, viabilizando cargas mais rapidas e com menor overhead do Spark.
# 🔬 Medicao: O log do cluster Spark detalhara a operacao de Merge command com numeros de "numTargetRowsUpdated" e "numTargetRowsInserted" em vez de recalculo total.
upsert_silver_table(spark, df_telefones_agg, output_path_telefones, "CPFCNPJ")
print(f"Telefones agregados salvos em: {output_path_telefones}")

# Célula 3.2: Processamento de Emails
# --------------------------------------------------------------------------------
print("\nIniciando o tratamento de emails...")
df_emails_bronze = spark.read.table("LH_Bronze.cad_email")
df_emails_agg = df_emails_bronze \
    .filter(col("EMAIL").isNotNull() & (col("EMAIL") != "")) \
    .select("CPFCNPJ", "EMAIL").distinct() \
    .groupBy("CPFCNPJ").agg(concat_ws("; ", collect_list("EMAIL")).alias("Emails"))

output_path_emails = "LH_Silver.staging_emails_agg"
# 🧠 Tensor: Implementacao de Upsert (MERGE) nas Dimensoes Silver
# 💡 O que: Substituida a logica de gravacao em modo overwrite por operacoes incrementais de MERGE INTO (upsert) nas tabelas staging_clientes_limpa, staging_telefones_agg, staging_emails_agg, staging_enderecos_limpa e staging_sacados_enriquecida.
# 🎯 Por que: Garantir processamento eficiente das cargas de dados, inserindo registros novos e atualizando os existentes sem a necessidade de reescrever toda a tabela diariamente.
# 📊 Impacto: Otimizacao severa de I/O de disco para o storage Delta Lake, viabilizando cargas mais rapidas e com menor overhead do Spark.
# 🔬 Medicao: O log do cluster Spark detalhara a operacao de Merge command com numeros de "numTargetRowsUpdated" e "numTargetRowsInserted" em vez de recalculo total.
upsert_silver_table(spark, df_emails_agg, output_path_emails, "CPFCNPJ")
print(f"Emails agregados salvos em: {output_path_emails}")

# Célula 3.3: Processamento de Endereços
# --------------------------------------------------------------------------------
print("\nIniciando o tratamento de endereços...")
schema_regioes = StructType([
    StructField("Sigla", StringType(), True), StructField("Estado", StringType(), True),
    StructField("Capital", StringType(), True), StructField("Regiao", StringType(), True)
])
data_regioes = [("AC", "Acre", "Rio Branco", "Norte"),("AL", "Alagoas", "Maceió", "Nordeste"),("AP", "Amapá", "Macapá", "Norte"),("AM", "Amazonas", "Manaus", "Norte"),("BA", "Bahia", "Salvador", "Nordeste"),("CE", "Ceará", "Fortaleza", "Nordeste"),("DF", "Distrito Federal", "Brasília", "Centro-Oeste"),("ES", "Espírito Santo", "Vitória", "Sudeste"),("GO", "Goiás", "Goiânia", "Centro-Oeste"),("MA", "Maranhão", "São Luís", "Nordeste"),("MT", "Mato Grosso", "Cuiabá", "Centro-Oeste"),("MS", "Mato Grosso do Sul", "Campo Grande", "Centro-Oeste"),("MG", "Minas Gerais", "Belo Horizonte", "Sudeste"),("PA", "Pará", "Belém", "Norte"),("PB", "Paraíba", "João Pessoa", "Nordeste"),("PR", "Paraná", "Curitiba", "Sul"),("PE", "Pernambuco", "Recife", "Nordeste"),("PI", "Piauí", "Teresina", "Nordeste"),("RJ", "Rio de Janeiro", "Rio de Janeiro", "Sudeste"),("RN", "Rio Grande do Norte", "Natal", "Nordeste"),("RS", "Rio Grande do Sul", "Porto Alegre", "Sul"),("RO", "Rondônia", "Porto Velho", "Norte"),("RR", "Roraima", "Boa Vista", "Norte"),("SC", "Santa Catarina", "Florianópolis", "Sul"),("SP", "São Paulo", "São Paulo", "Sudeste"),("SE", "Sergipe", "Aracaju", "Nordeste"),("TO", "Tocantins", "Palmas", "Norte")]
df_regioes = spark.createDataFrame(data=data_regioes, schema=schema_regioes).cache()

df_enderecos_bronze = spark.read.table("LH_Bronze.cad_enderecos")
colunas_para_remover = ["PAIS", "FONE", "FAX", "TIPO", "DATAINCLUSAO", "USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODMUNICIPIO", "CODENDERECO"]
df_enderecos_filtrado = df_enderecos_bronze.drop(*colunas_para_remover) \
    .filter(~col("ENDERECO").isin(["END PENDENTE", "Rua Teste", "S/ EDEREÇO", "S/ ENDERECO", "S/ ENDEREÇO", "test2", "teste"]) & col("CIDADE").isNotNull() & (col("CIDADE") != "") & (col("CIDADE") != "0"))
cols_to_upper = ["ENDERECO", "COMPLEMENTO", "BAIRRO", "CIDADE", "UF"]
df_upper = reduce(lambda df, c: df.withColumn(c, upper(col(c))), cols_to_upper, df_enderecos_filtrado)
replacements_uf = {"RP": "PR", "SÃ": "SP", " C": "CE", "G": "GO", "11": "SP", "DP": "SP", "SA": "SP", "S": "SP", "MF": "MG", "ED": "ES", "31": None, "0": None, "A": None, "1": None, "3": None, "..": None, "7": None, "-": None, "+": None, ".": None, "SS": None, "9": None, "2": None, "GU": None, "": None, "68": None}
df_enderecos_final = df_upper \
    .filter(col("CEP").isNotNull() & ~col("CEP").isin(["00      ", "0000000", "00000000", "0"])).distinct() \
    .replace(replacements_uf, subset=['UF']).filter(col("UF").isNotNull()) \
    .dropDuplicates(["CPFCNPJ"]) \
    .filter(col("CPFCNPJ") != "00000000000000") \
    .withColumn("CIDADE_limpa", regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(col("CIDADE"), "[ÃÂÁ]", "A"), "[É]", "E"), "[Í]", "I"), "[Ó]", "O"), "[Ú]", "U"), "-", " ")) \
    .drop("CIDADE").withColumnRenamed("CIDADE_limpa", "CIDADE") \
    .join(df_regioes, col("UF") == df_regioes.Sigla, "left") \
    .withColumn("CPFCNPJ_valido", col("CPFCNPJ").cast("long")).filter(col("CPFCNPJ_valido").isNotNull()) \
    .select("CPFCNPJ", "ENDERECO", "NUMERO", "COMPLEMENTO", "BAIRRO", "CIDADE", "UF", "CEP", "Estado", "Capital", "Regiao")

output_path_enderecos = "LH_Silver.staging_enderecos_limpa"
# 🧠 Tensor: Implementacao de Upsert (MERGE) nas Dimensoes Silver
# 💡 O que: Substituida a logica de gravacao em modo overwrite por operacoes incrementais de MERGE INTO (upsert) nas tabelas staging_clientes_limpa, staging_telefones_agg, staging_emails_agg, staging_enderecos_limpa e staging_sacados_enriquecida.
# 🎯 Por que: Garantir processamento eficiente das cargas de dados, inserindo registros novos e atualizando os existentes sem a necessidade de reescrever toda a tabela diariamente.
# 📊 Impacto: Otimizacao severa de I/O de disco para o storage Delta Lake, viabilizando cargas mais rapidas e com menor overhead do Spark.
# 🔬 Medicao: O log do cluster Spark detalhara a operacao de Merge command com numeros de "numTargetRowsUpdated" e "numTargetRowsInserted" em vez de recalculo total.
upsert_silver_table(spark, df_enderecos_final, output_path_enderecos, "CPFCNPJ")
df_regioes.unpersist()
print(f"Endereços limpos salvos em: {output_path_enderecos}")

# Célula 3.4: Limpeza do Cadastro Geral (Base)
# --------------------------------------------------------------------------------
print("\nIniciando a limpeza da tabela de cadastro geral (base).")
df_geral_bronze = spark.read.table("LH_Bronze.cad_geral_pf_pj")
key_cols_geral = ["CPFCNPJ"]
order_by_col_geral = "DATAALTERACAO"
window_geral = Window.partitionBy([col(c) for c in key_cols_geral]).orderBy(col(order_by_col_geral).desc())
df_geral_deduplicated = df_geral_bronze.withColumn("row_num", row_number().over(window_geral)) \
                                     .filter(col("row_num") == 1) \
                                     .drop("row_num")

output_path_geral = "LH_Silver.staging_cad_geral_pf_pj_limpa"
df_geral_deduplicated.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_geral)
print(f"Tabela de cadastro geral desduplicada salva em: {output_path_geral}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 4: Limpeza da Tabela tab_operacoes
# **Objetivo:** Limpar e desduplicar a tabela `tab_operacoes`.

# CELL ********************

# Célula 4.1: Parâmetros e Leitura
# ------------------------------------------------
source_table_operacoes = "tab_operacoes"
target_table_operacoes = "staging_operacoes_limpa"
print(f"\nIniciando a limpeza da tabela: {source_lakehouse}.{source_table_operacoes}")
df_bronze_operacoes = spark.read.table(f"{source_lakehouse}.{source_table_operacoes}")

# Célula 4.2: Lógica de Correção e Desduplicação
# ----------------------------------------------------
df_corrigido = df_bronze_operacoes.withColumn("TTO_corrigido", when(col("CODOPERACAO") == 3042074, lit("CS")).otherwise(col("TTO"))).drop("TTO").withColumnRenamed("TTO_corrigido", "TTO")
key_columns_operacoes = ["CODOPERACAO"]
order_by_column_operacoes = "DATAALTERACAO"
windowSpec_operacoes = Window.partitionBy([col(c) for c in key_columns_operacoes]).orderBy(col(order_by_column_operacoes).desc())
df_ranked_operacoes = df_corrigido.withColumn("row_num", row_number().over(windowSpec_operacoes))
df_deduplicated_operacoes = df_ranked_operacoes.filter(col("row_num") == 1).drop("row_num")

# Adiciona a chave do produto
df_com_chave_produto = df_deduplicated_operacoes.withColumn("chave_produto", concat(col("TTO"), col("STTO")))

# Célula 4.3: Salvar o Resultado
# ------------------------------------------------------
# Seleciona explicitamente as colunas para garantir um schema de saída estável
# e incluir as novas colunas solicitadas.
df_silver_operacoes_final = df_com_chave_produto.select(
    "CODOPERACAO",
    "CODCLIENTE",
    "CODEMPRESA",
    "DATAINCLUSAO",
    "DATAALTERACAO", # Mantido para referência e rastreabilidade
    "DATAANALISE",
    "STATUSACEITE",
    "STATUSANALISE",
    "CODBROKER",
    "NOTASERVICO", # Necessário para a lógica de 'operação informal' no Gold
    "TTO",
    "STTO",
    "chave_produto",
    "TOTRETENCAO",
    "TOTDES",
    "TOTFAC",
    "TOTDCP",
    "TOTTAR",
    "TOTRECOMPRA"
)

output_path_operacoes = f"{target_lakehouse}.{target_table_operacoes}"
df_silver_operacoes_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_operacoes)
print(f"Tabela desduplicada salva com sucesso em: {output_path_operacoes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 5: Processamento e Detalhamento da Chave da DANFE
# **Objetivo:** Extrair e decodificar as informações contidas na `CHAVEDANFE` dos títulos.

# CELL ********************

# Célula 5.1: Parâmetros e Leitura da Tabela em Cache
# ------------------------------------------------
danfe_source_table = "staging_titulos_limpa"
danfe_target_table = "staging_chave_danfe_detalhada"
print(f"\nIniciando o processamento da CHAVEDANFE da tabela: {target_lakehouse}.{danfe_source_table}")
df_titulos_danfe = spark.table(f"{target_lakehouse}.{danfe_source_table}") # Usando a tabela em cache

# Célula 5.2: Lógica de Transformação da CHAVEDANFE
# ------------------------------------------------
# Nota: A tabela staging_titulos_limpa agora usa snake_case (chave_danfe)
df_chave_filtrada = df_titulos_danfe \
    .select(col("chave_danfe").alias("CHAVEDANFE")) \
    .na.drop(subset=["CHAVEDANFE"]) \
    .filter((col("CHAVEDANFE") != "") & (length(col("CHAVEDANFE")) == 44)) \
    .filter(~col("CHAVEDANFE").contains("XML NF-E 495 MOMENTUM OP. 149717.XML de NF-E")) \
    .withColumn("CHAVEDANFE_limpa", regexp_replace(col("CHAVEDANFE"), " ", "0")) \
    .select("CHAVEDANFE_limpa") \
    .withColumnRenamed("CHAVEDANFE_limpa", "CHAVEDANFE") \
    .distinct()

df_detalhada = df_chave_filtrada \
    .withColumn("UF", substring(col("CHAVEDANFE"), 1, 2)) \
    .withColumn("AAMM", substring(col("CHAVEDANFE"), 3, 4)) \
    .withColumn("CNPJ", substring(col("CHAVEDANFE"), 7, 14)) \
    .withColumn("Modelo", substring(col("CHAVEDANFE"), 21, 2)) \
    .withColumn("Serie", substring(col("CHAVEDANFE"), 23, 3)) \
    .withColumn("NumeroNF", substring(col("CHAVEDANFE"), 26, 9)) \
    .withColumn("CodigoNF", substring(col("CHAVEDANFE"), 35, 9)) \
    .withColumn("DV", substring(col("CHAVEDANFE"), 44, 1))

# Célula 5.3: Salvar o Resultado
# ------------------------------------------------------
output_path_danfe = f"{target_lakehouse}.{danfe_target_table}"
df_detalhada.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_danfe)
print(f"Tabela detalhada da CHAVEDANFE salva com sucesso em: {output_path_danfe}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 6: Limpeza da Tabela `tab_titulos_baixas`
# **Objetivo:** Processar as "baixas" de títulos, corrigindo dados inconsistentes e desduplicando.

# CELL ********************

# Célula 6.1: Leitura e Limpeza de Baixas
# ------------------------------------------------
print("\nIniciando o processamento da tab_titulos_baixas")
df_baixas = spark.read.table("LH_Bronze.tab_titulos_baixas")
key_cols_baixa = ["CODTITULOBAIXAS"]
order_by_col_baixa = "DATAINCLUSAO"
window_baixa = Window.partitionBy([col(c) for c in key_cols_baixa]).orderBy(col(order_by_col_baixa).desc())
df_baixas_desduplicada = df_baixas.withColumn("row_num", row_number().over(window_baixa)) \
                                    .filter(col("row_num") == 1).drop("row_num")

# Correções manuais de juros (Valor Incorreto -> Valor Correto)
JUROS_CORRECTIONS = {
    -858005.8: 3912.5,
    -4948525.71: -56747.24,
    -4140.75: 0,
    -1447.5: 52.5,
    -1825.72: 66.28,
    -965: 35,
    -26000: 0
}

def apply_juros_corrections(df, corrections=None):
    """
    Aplica correções pontuais na coluna 'JUROS' baseada em um dicionário de mapeamento.
    Nota: A coluna original no Bronze é 'JUROS' (upper case).
    """
    if corrections is None:
        corrections = JUROS_CORRECTIONS

    if not corrections:
        return df

    keys = list(corrections.keys())
    if not keys:
        return df

    # Verifica se a coluna existe (verificação insensível a maiúsculas/minúsculas)
    col_name = "JUROS"
    if "JUROS" not in df.columns and "juros" in df.columns:
         col_name = "juros"
    elif "JUROS" not in df.columns:
         print("AVISO: Coluna de juros não encontrada para aplicação de correções.")
         return df

    # Inicia a cadeia de condições
    expr = when(col(col_name) == keys[0], corrections[keys[0]])
    for k in keys[1:]:
        expr = expr.when(col(col_name) == k, corrections[k])

    expr = expr.otherwise(col(col_name))

    return df.withColumn(col_name, expr)

# Aplica as correções na tabela desduplicada
df_baixas_corrigida = apply_juros_corrections(df_baixas_desduplicada)

output_path_baixas_staging = "LH_Silver.staging_baixas_limpa"
df_baixas_corrigida.write.mode("overwrite").option("overwriteSchema","true").saveAsTable(output_path_baixas_staging)
print(f"Tabela de baixas limpa e salva em: {output_path_baixas_staging}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 7: Processamento de Contratos de Clientes
# **Objetivo:** Limpar e transformar os dados da tabela `cad_contratos_clientes` para criar uma tabela de staging.

# CELL ********************

# Célula 7.1: Parâmetros e Leitura
# ------------------------------------------------
source_table_contratos = "cad_contratos_clientes"
target_table_contratos = "staging_contratos_clientes_limpa"
print(f"\nIniciando o processamento da tabela: {source_lakehouse}.{source_table_contratos}")
df_bronze_contratos = spark.read.table(f"{source_lakehouse}.{source_table_contratos}")

# Célula 7.2: Lógica de Transformação
# ----------------------------------------------------
df_transformed_contratos = df_bronze_contratos \
    .withColumn("PERCCONFIRMACAO", col("PERCCONFIRMACAO") / 100) \
    .withColumn("STATUSDIRETORIA", when(col("OBSERVACOES").like("%#STATUSDIRETORIA%"), 1).otherwise(0)) \
    .select(
        "CODCONTRATO",
        "CODCLIENTE",
        "DTINICONTRATO",
        "VALIDADELIMITE",
        "FATOR",
        "LIMITEFOMENTO",
        "LIMITECOMISSARIA",
        "STATUS",
        "PERCCONFIRMACAO",
        "TRANCHE",
        "STATUSDIRETORIA"
    ) \
    .orderBy(col("CODCONTRATO").desc())

# Célula 7.3: Salvar o Resultado
# ------------------------------------------------------
output_path_contratos = f"{target_lakehouse}.{target_table_contratos}"
df_transformed_contratos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_contratos)
print(f"Tabela de staging de contratos salva com sucesso em: {output_path_contratos}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 8: Processamento da Bridge Cliente-Gerente
# **Objetivo:** Criar a tabela ponte (bridge) que mapeia o relacionamento histórico entre Clientes e Gerentes, e a tabela de relacionamento atual.

# CELL ********************

# Célula 8.1: Processamento da Bridge Cliente-Gerente
# ------------------------------------------------
print("\nIniciando o processamento da Bridge Cliente-Gerente...")

# 1. Leitura
df_historico = spark.read.table("LH_Bronze.rlc_brokers_clientes_historico")
df_atual = spark.read.table("LH_Bronze.rlc_brokers_clientes")

# 2. União e Limpeza
df_unificado = df_historico.unionByName(df_atual, allowMissingColumns=True)

df_preparado = df_unificado.withColumn(
    "DataInicioVigencia",
    coalesce(col("DATAINICIO"), col("DATAINCLUSAO")).cast("date")
).select(
    col("CODCLIENTE").alias("ClienteID"),
    col("CODBROKER").alias("GerenteID"),
    "DataInicioVigencia"
).filter(
    col("ClienteID").isNotNull() & col("GerenteID").isNotNull() &
    col("DataInicioVigencia").isNotNull()
).distinct()

# 3. Lógica de Vigência (Bridge)
windowSpec_bridge = Window.partitionBy("ClienteID").orderBy(col("DataInicioVigencia").asc())

df_com_data_fim = df_preparado.withColumn(
    "DataFimVigencia_temp",
    lead("DataInicioVigencia", 1, datetime.date(9999, 12, 31)).over(windowSpec_bridge)
)

df_final_bridge = df_com_data_fim.withColumn(
    "DataFimVigencia",
    when(
        col("DataFimVigencia_temp") == datetime.date(9999, 12, 31),
        lit("9999-12-31").cast("date")
    ).otherwise(
        date_sub(col("DataFimVigencia_temp"), 1)
    )
).select("ClienteID", "GerenteID", "DataInicioVigencia", "DataFimVigencia")

# 4. Salvar Bridge
output_path_bridge = "LH_Silver.bridge_cliente_gerente"
df_final_bridge.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_bridge)
print(f"Tabela ponte '{output_path_bridge}' salva com sucesso.")

# 5. Filtrar e Salvar Relacionamento Atual
df_relacionamento_atual = df_final_bridge.filter(col("DataFimVigencia") == "9999-12-31") \
    .select("ClienteID", "GerenteID", "DataInicioVigencia")

output_path_atual = "LH_Silver.relacionamento_cliente_gerente_atual"
df_relacionamento_atual.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_atual)
print(f"Tabela de relacionamento atual '{output_path_atual}' salva com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 9: Processamento de Limites de Clientes e Sacados
# **Objetivo:** Limpar e padronizar a tabela `rlc_clientes_sacados_limites` para uso no enriquecimento de títulos.

# CELL ********************

# Célula 9.1: Parâmetros e Leitura
# ------------------------------------------------
source_table_limites = "rlc_clientes_sacados_limites"
target_table_limites = "staging_rlc_clientes_sacados_limites"
print(f"\nIniciando o processamento da tabela: {source_lakehouse}.{source_table_limites}")
df_bronze_limites = spark.read.table(f"{source_lakehouse}.{source_table_limites}")

# Célula 9.2: Lógica de Transformação
# ----------------------------------------------------
df_transformed_limites = df_bronze_limites \
    .withColumn("TIPO", regexp_replace(col("TIPO"), "^I$", "INTERCIA")) \
    .withColumn("TipoDocumentoSacado",
                when(length(col("CPFCNPJ")) == 11, "CPF")
                .when(length(col("CPFCNPJ")) == 14, "CNPJ")
                .otherwise("Inválido")) \
    .withColumn("RaizCNPJ",
                when(col("TipoDocumentoSacado") == "CNPJ", substring(col("CPFCNPJ"), 1, 8))
                .otherwise(col("CPFCNPJ"))) \
    .withColumn("chave_cliente_sacado", concat(col("CODCLIENTE").cast("string"), lit("-"), col("RaizCNPJ")))

# Desduplicação determinística
window_limites = Window.partitionBy("chave_cliente_sacado").orderBy(col("DATAINCLUSAO").desc())
df_transformed_limites = df_transformed_limites \
    .withColumn("row_num", row_number().over(window_limites)) \
    .filter(col("row_num") == 1).drop("row_num")

# Célula 9.3: Salvar o Resultado
# ------------------------------------------------------
output_path_limites = f"{target_lakehouse}.{target_table_limites}"
df_transformed_limites.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_limites)
print(f"Tabela de staging de limites salva com sucesso em: {output_path_limites}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 10: Processamento de Devoluções de Operações
# **Objetivo:** Limpar a tabela `tab_operacoes_devolucoes` removendo colunas desnecessárias e duplicatas.

# CELL ********************

# Célula 10.1: Parâmetros e Leitura
# ------------------------------------------------
source_table_devolucoes = "tab_operacoes_devolucoes"
target_table_devolucoes = "staging_operacoes_devolucoes_limpa"
print(f"\nIniciando o processamento da tabela: {source_lakehouse}.{source_table_devolucoes}")
df_bronze_devolucoes = spark.read.table(f"{source_lakehouse}.{source_table_devolucoes}")

# Célula 10.2: Lógica de Transformação
# ----------------------------------------------------
# Desduplicação determinística antes de remover colunas de data
window_devolucoes = Window.partitionBy("CODTITULO").orderBy(col("DATAALTERACAO").desc())
df_transformed_devolucoes = df_bronze_devolucoes \
    .withColumn("row_num", row_number().over(window_devolucoes)) \
    .filter(col("row_num") == 1).drop("row_num") \
    .drop("USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODTITULOBAIXA")

# Célula 10.3: Salvar o Resultado
# ------------------------------------------------------
output_path_devolucoes = f"{target_lakehouse}.{target_table_devolucoes}"
df_transformed_devolucoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_devolucoes)
print(f"Tabela de staging de devoluções salva com sucesso em: {output_path_devolucoes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 11: Processamento de Status de Protesto
# **Objetivo:** Calcular o status de protesto mais recente para cada título, baseando-se em ocorrências de cobrança. A tabela resultante `staging_protestos` enriquece a dimensão de títulos.

# CELL ********************

print("\\nIniciando o processamento de status de protesto de títulos...")

# Leitura das tabelas de origem (Bronze)
df_ocorrencias_bronze = spark.read.table("LH_Bronze.rlc_titulos_ocorrencias_cobranca")
df_titulos_cobranca_bronze = spark.read.table("LH_Bronze.tab_titulos_cobranca")
print("Tabelas de ocorrências e cobrança lidas da camada Bronze.")

# Pré-cálculos e Lógica de Negócio
df_titulos_para_protesto_cobranca = df_titulos_cobranca_bronze \
    .filter(col("CODOCORCOBRANCA") == 1015) \
    .select("CODTITULO") \
    .distinct() \
    .withColumn("flag_protesto_cobranca", lit(True))

df_subquery_ocorrencia = df_ocorrencias_bronze \
    .filter(col("CODOCORINTERNA").isin(8, 34) & col("CODOCORCOBRBANCO").isin(19, 23)) \
    .select("CODTITULO") \
    .distinct() \
    .withColumn("flag_subquery_ocorrencia", lit(True))

df_ocorrencias_filtradas = df_ocorrencias_bronze.filter(
    ((col("CODOCORINTERNA").isin(8, 17, 34, 2, 82)) & (col("CODOCORCOBRBANCO").isin(6, 19, 23, 10, 43)) & (col("TOCORRENCIA") == 2)) |
    ((col("CODOCORINTERNA") == 8) & (col("CODOCORCOBRBANCO") == 9) & (col("TOCORRENCIA") == 1))
)

window_spec_latest = Window.partitionBy("CODTITULO").orderBy(col("CODTITULOOCORCOB").desc())

df_latest_ocorrencia = df_ocorrencias_filtradas \
    .withColumn("row_num", row_number().over(window_spec_latest)) \
    .filter(col("row_num") == 1) \
    .drop("row_num") \
    .join(df_titulos_para_protesto_cobranca, "CODTITULO", "left") \
    .join(df_subquery_ocorrencia, "CODTITULO", "left") \
    .fillna(False, subset=["flag_protesto_cobranca", "flag_subquery_ocorrencia"])

# Calcular Status
cond_p1 = (substring(col("MOTIVOCODOCORCOBRBANCO"), 1, 2) == '14')
cond_p2 = (col("CODOCORINTERNA") == 2) & (col("flag_subquery_ocorrencia") == True)
cond_p3 = (col("CODOCORINTERNA") == 82)
cond_p4 = (col("flag_protesto_cobranca") == True)
cond_e = (col("CODOCORINTERNA") == 8) & (col("CODOCORCOBRBANCO") == 9)
cond_i = (col("CODOCORINTERNA") == 8)
cond_c = (col("CODOCORINTERNA") == 34)

df_com_status_code = df_latest_ocorrencia.withColumn("STATUSPROTESTO",
    when(cond_p1 | cond_p2 | cond_p3 | cond_p4, lit("P"))
    .when(cond_e, lit("E")).when(cond_i, lit("I")).when(cond_c, lit("C"))
    .otherwise(lit("N"))
)
df_com_status_desc = df_com_status_code.withColumn("STATUS_PROTESTO",
    when(col("STATUSPROTESTO") == 'P', lit("Protestado"))
    .when(col("STATUSPROTESTO") == 'E', lit("Instrução Protesto Enviada"))
    .when(col("STATUSPROTESTO") == 'I', lit("Instrução Protesto"))
    .when(col("STATUSPROTESTO") == 'C', lit("Em Cartório"))
    .otherwise(lit("N/A"))
).filter(col("STATUS_PROTESTO") != "N/A")

# Salvar Resultado
df_final_protestos = df_com_status_desc.select("CODTITULO", "STATUS_PROTESTO", col("DATAINCLUSAO").alias("DATA_OCORRENCIA_PROTESTO"))
output_path_protestos = "LH_Silver.staging_protestos"
df_final_protestos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_protestos)
print(f"Tabela de staging para protestos salva com sucesso em: {output_path_protestos}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 12: Limpeza de Tabelas Adicionais (Empresas, Abatimentos, Notificações, TAC M)
# **Objetivo:** Limpar e transformar tabelas de menor complexidade mas necessárias para o processo: `cad_empresas`, `tab_titulos_abatimento`, `tab_titulos_cobranca` (notificações) e `tab_operacoes_tarifas_extras` (TAC M).

# CELL ********************

# 12.1 Processamento de Staging Empresas
# ----------------------------------------------------
print("\nIniciando processamento de staging_empresas...")
df_empresas = spark.read.table("LH_Bronze.cad_empresas")
df_empresas_filtered = df_empresas \
    .filter(col("CODEMPRESA").isin([6, 14, 24, 25])) \
    .select(
        col("CODEMPRESA").alias("cod_empresa"),
        col("CNPJ").alias("cnpj")
    )
df_empresas_filtered.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_empresas")
print("Tabela staging_empresas salva.")

# 12.2 Processamento de Staging Abatimentos
# ----------------------------------------------------
print("\nIniciando processamento de staging_abatimentos...")
df_abatimentos = spark.read.table("LH_Bronze.tab_titulos_abatimento")
df_abatimentos_final = df_abatimentos.select(
    col("CODTITULOABAT").alias("cod_titulo_abat"),
    col("CODOPERACAO").alias("cod_operacao"),
    col("CODTITULO").alias("cod_titulo"),
    col("CODOPERACAOAB").alias("cod_operacao_ab"),
    col("VALORDEVIDO").alias("valor_devido"),
    col("ABATIMENTO").alias("abatimento"),
    col("DATAINCLUSAO").alias("data_inclusao"),
    col("CODBANCOCOBR").alias("cod_banco_cobr"),
    col("USUAINCLUSAO").alias("usua_inclusao")
).withColumn("Data", col("data_inclusao").cast("date"))

df_abatimentos_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_abatimentos")
print("Tabela staging_abatimentos salva.")

# 12.3 Processamento de Staging Notificações
# ----------------------------------------------------
print("\nIniciando processamento de staging_notificacoes...")
df_notificacoes = spark.read.table("LH_Bronze.tab_titulos_cobranca")

# Função para decodificar e limpar observação
def clean_obs(col_name):
    # Assume que se for binário, spark lê como binário ou string.
    # Se for string, apenas substitui. Se for binário, precisa de decode.
    # O padrão seguro é cast para string e replaces.
    # O Dataflow usava Text.FromBinary(_, 1252), diminuindo a codificação cp1252.
    return regexp_replace(
        regexp_replace(col(col_name).cast("string"), "&ccedil;", "ç"),
        "&atilde;", "ã"
    )

df_notificacoes_final = df_notificacoes \
    .filter(col("CODOCORCOBRANCA") == 12) \
    .select(
        col("CODOPERACAO").alias("cod_operacao"),
        col("CODTITULO").alias("cod_titulo"),
        col("COBRADOAO").alias("cobrado_ao"),
        col("CODOCORCOBRANCA").alias("cod_ocor_cobranca"),
        clean_obs("OBSERVACAO").alias("observacao"),
        col("DATAINCLUSAO").alias("data_inclusao"),
        col("USUAINCLUSAO").alias("usua_inclusao")
    )

df_notificacoes_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_notificacoes")
print("Tabela staging_notificacoes salva.")

# 12.4 Processamento de Staging TAC M
# ----------------------------------------------------
print("\nIniciando processamento de staging_tac_m...")
df_tac = spark.read.table("LH_Bronze.tab_operacoes_tarifas_extras")

# Filtro de ano >= 2024 e seleção/renomeação inicial
df_tac_renamed = df_tac \
    .filter(year(col("DATAINCLUSAO")) >= 2024) \
    .select(
        col("CODTARIFAEXTRA").alias("cod_tarifa_extra"),
        col("CODOPERACAO").alias("cod_operacao"),
        col("DESCRICAO").alias("descricao"),
        col("TOTAL").alias("total"),
        col("DATAINCLUSAO").alias("data_inclusao"),
        col("USUAINCLUSAO").alias("usua_inclusao")
    )

# Limpeza da descrição (Upper, Trim, Replaces)
df_tac_cleaned = df_tac_renamed \
    .withColumn("descricao", upper(col("descricao"))) \
    .withColumn("descricao", regexp_replace(col("descricao"), "^\\s+|\\s+$", "")) \
    .withColumn("descricao",
        when(col("descricao") == "TAC  M", lit("TAC M"))
        .when(col("descricao") == "TAC MOP", lit("TAC M"))
        .when(col("descricao") == "TAC M.", lit("TAC M"))
        .when(col("descricao") == "TACM", lit("TAC M"))
        .when(col("descricao") == "TACA M", lit("TAC M"))
        .when(col("descricao") == "TAC M 300,00", lit("TAC M"))
        .when(col("descricao") == "TAC", lit("TAC M"))
        .otherwise(col("descricao"))
    ) \
    .filter(col("descricao") == "TAC M") \
    .orderBy(col("data_inclusao").desc())

df_tac_cleaned.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_tac_m")
print("Tabela staging_tac_m salva.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 13: Processamento de Tabelas Auxiliares e Relacionamentos
# **Objetivo:** Migrar tabelas restantes: `staging_gerentes`, `staging_plataformas`, `staging_boletos_titulos`, `staging_status_clientes_esteira`.

# CELL ********************

# 13.1 Processamento de Staging Gerentes
# ----------------------------------------------------
print("\nIniciando processamento de staging_gerentes...")
df_brokers = spark.read.table("LH_Bronze.cad_brokers")
df_gerentes = df_brokers.select(
    col("CODBROKER").alias("cod_broker"),
    col("CPFCNPJ").alias("cpf_cnpj"),
    col("CODAGENCIA").alias("cod_agencia"),
    col("CODUSUARIO").alias("cod_usuario")
)
df_gerentes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_gerentes")
print("Tabela staging_gerentes salva.")

# 13.2 Processamento de Staging Plataformas
# ----------------------------------------------------
print("\nIniciando processamento de staging_plataformas...")
df_agencias = spark.read.table("LH_Bronze.cad_agencias")
# Assumindo que sup_gestor_de_plataforma está no Silver, baseado nos IDs do Dataflow.
df_sup_gestor = spark.read.table("LH_Silver.sup_gestor_de_plataforma")

df_plataformas_base = df_agencias.select(
    col("CODAGENCIA").alias("cod_agencia"),
    col("NOME").alias("nome_plataforma")
)

df_plataformas_joined = df_plataformas_base.join(
    df_sup_gestor,
    on="cod_agencia",
    how="left"
)

# Transforma 'nome_plataforma' removendo a primeira palavra (geralmente "AGENCIA")
df_plataformas_final = df_plataformas_joined.withColumn(
    "plataforma",
    regexp_replace(col("nome_plataforma"), "^.*? ", "")
).select(
    "cod_agencia",
    "nome_plataforma",
    "gestor_da_plataforma",
    "plataforma"
)

df_plataformas_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_plataformas")
print("Tabela staging_plataformas salva.")

# 13.3 Processamento de Staging Boletos Títulos
# ----------------------------------------------------
print("\nIniciando processamento de staging_boletos_titulos...")
# Lê da staging_titulos_limpa que já está em cache ou salva no lakehouse
df_titulos_limpa = spark.table("LH_Silver.staging_titulos_limpa")

# Garante que usamos nomes snake_case, mas tem uma verificação de contingência caso a tabela não tenha sido atualizada
# O padrão agora é snake_case (t_doc, data_inclusao)
df_boletos = df_titulos_limpa \
    .filter(col("t_doc") == "BL") \
    .filter(col("data_inclusao").cast("string") >= "2021-01-01") \
    .drop(
        "venc_prorrogado",
        "prazo",
        "desagio",
        "data_conf",
        "usua_conf",
        "doc_confirmado",
        "chave_danfe"
    )

df_boletos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_boletos_titulos")
print("Tabela staging_boletos_titulos salva.")

# 13.4 Processamento de Status Clientes Esteira
# ----------------------------------------------------
print("\nIniciando processamento de staging_status_clientes_esteira...")
df_sup_status = spark.read.table("LH_Silver.sup_status_de_clientes_da_esteira")

# Renomeação simples
df_status_esteira = df_sup_status.withColumnRenamed("codstatuscliente", "cod_status_cliente") \
    .select("cod_status_cliente", "status_do_cliente", "macroprocesso", "fase")

df_status_esteira.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_status_clientes_esteira")
print("Tabela staging_status_clientes_esteira salva.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 14: Limpeza do Cache
# **Objetivo:** Liberar os DataFrames que foram armazenados em cache da memória do Spark.

# CELL ********************

print("\nLimpando os DataFrames do cache...")

# Libera o cache da tabela de títulos
spark.catalog.uncacheTable("LH_Silver.staging_titulos_limpa")
print("Cache de 'staging_titulos_limpa' liberado.")

print("Limpeza do cache concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
