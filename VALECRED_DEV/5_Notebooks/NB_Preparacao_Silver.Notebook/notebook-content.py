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
# **Observação:** As etapas de enriquecimento (joins) foram movidas para o notebook `NB_Curadoria_Gold` para separar a limpeza da construção de modelos de negócio.
# **Processos realizados:**
# 1.  **Configuração do Ambiente:** Define configurações do Spark e importa as bibliotecas necessárias.
# 2.  **Limpeza de `tab_titulos`:** Remove duplicatas para garantir que cada título seja único.
# 3.  **Limpeza de `cad_clientes`:** Desduplica a tabela base para a dimensão de clientes.
# 4.  **Limpeza de Componentes do Cadastro Geral:** Limpa e salva individualmente tabelas de telefones, emails, endereços e cadastro geral (PF/PJ), preparando-as para enriquecimento posterior.
# 5.  **Limpeza de `tab_operacoes`:** Remove duplicatas da tabela de operações.
# 6.  **Processamento da Chave DANFE:** Extrai informações detalhadas da chave da nota fiscal.
# 7.  **Limpeza de `tab_titulos_baixas`:** Limpa os dados de baixas de títulos.
# 8.  **Processamento de Contratos de Clientes:** Limpa a tabela de contratos.
# 9.  **Limpeza de Cache:** Libera os DataFrames da memória.


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
    lead, date_add, lag, max, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from functools import reduce
from delta.tables import *
import datetime

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

# Célula 1.4: Salvar e Armazenar em Cache
# ------------------------------------------------------
output_path_titulos = f"{target_lakehouse}.{target_table_titulos}"
df_deduplicated_titulos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_titulos)

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

# Célula 2.3: Salvar e Armazenar em Cache
# ------------------------------------------------------
output_path_clientes = f"{target_lakehouse}.{target_table_clientes}"
df_deduplicated_clientes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_clientes)

# Armazena o resultado em cache
spark.table(output_path_clientes).cache()
print(f"Tabela limpa salva e em cache: {output_path_clientes}")

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
df_telefones_agg.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_telefones)
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
df_emails_agg.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_emails)
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
df_regioes.unpersist()

output_path_enderecos = "LH_Silver.staging_enderecos_limpa"
df_enderecos_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_enderecos)
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
target_table_operacoes = "staging_operacoes_base"
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

# Célula 4.3: Salvar o Resultado
# ------------------------------------------------------
output_path_operacoes = f"{target_lakehouse}.{target_table_operacoes}"
df_deduplicated_operacoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_operacoes)
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
df_chave_filtrada = df_titulos_danfe \
    .select("CHAVEDANFE") \
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
output_path_baixas_staging = "LH_Silver.staging_baixas_limpa"
df_baixas_desduplicada.write.mode("overwrite").option("overwriteSchema","true").saveAsTable(output_path_baixas_staging)
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

# ## Seção 8: Limpeza do Cache
# **Objetivo:** Liberar os DataFrames que foram armazenados em cache da memória do Spark.

# CELL ********************

print("\nLimpando os DataFrames do cache...")

# Libera o cache da tabela de títulos
spark.catalog.uncacheTable("LH_Silver.staging_titulos_limpa")
print("Cache de 'staging_titulos_limpa' liberado.")

# Libera o cache da tabela de clientes
spark.catalog.uncacheTable("LH_Silver.staging_clientes_limpa")
print("Cache de 'staging_clientes_limpa' liberado.")

print("Limpeza do cache concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
