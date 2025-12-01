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

# # Notebook de Preparação Silver - Cadastros
# **Objetivo:** Processamento de tabelas dimensionais e cadastrais (`clientes`, `geral`, `telefones`, `enderecos`, `contratos`, `bridge`, `limites`, `empresas`, `gerentes`, `plataformas`, `status`).
# **Estratégia:** Carga Full Overwrite (devido ao volume menor e necessidade de garantir integridade referencial completa das dimensões).

# MARKDOWN ********************

# ## Seção 0: Configuração do ambiente

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, when, lit, concat, length, regexp_replace,
    collect_list, concat_ws, upper, greatest, substring, year,
    lead, date_add, lag, max, coalesce, date_sub
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from functools import reduce
import datetime

source_lakehouse = "LH_Bronze"
target_lakehouse = "LH_Silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Clientes e Cadastro Geral

# CELL ********************

# 1.1 Clientes
print("Processando Clientes...")
df_bronze_clientes = spark.read.table(f"{source_lakehouse}.cad_clientes")
windowSpec_clientes = Window.partitionBy("CODCLIENTE").orderBy(col("DATAALTERACAO").desc())
df_deduplicated_clientes = df_bronze_clientes.withColumn("row_num", row_number().over(windowSpec_clientes)) \
    .filter(col("row_num") == 1).drop("row_num")
df_deduplicated_clientes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_clientes_limpa")

# 1.2 Cadastro Geral
print("Processando Cadastro Geral...")
df_geral_bronze = spark.read.table("LH_Bronze.cad_geral_pf_pj")
window_geral = Window.partitionBy("CPFCNPJ").orderBy(col("DATAALTERACAO").desc())
df_geral_deduplicated = df_geral_bronze.withColumn("row_num", row_number().over(window_geral)) \
    .filter(col("row_num") == 1).drop("row_num")
df_geral_deduplicated.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_cad_geral_pf_pj_limpa")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Contatos e Endereços

# CELL ********************

# 2.1 Telefones
print("Processando Telefones...")
df_telefones_agg = spark.read.table("LH_Bronze.cad_telefones") \
    .filter((col("FONE").isNotNull() & (col("FONE") != "")) & (col("DDD").isNotNull() & (col("DDD") != ""))) \
    .withColumn("FONE_limpo", regexp_replace(col("FONE"), "-", "")) \
    .withColumn("FONE_COMPLETO", regexp_replace(concat(col("DDD"), col("FONE_limpo")), " ", "")) \
    .filter((length(col("FONE_COMPLETO")) >= 10) & (length(col("FONE_COMPLETO")) <= 11)) \
    .select(col("CPFCNPJ"), col("FONE_COMPLETO").alias("FONE"), col("CONTATO")).distinct() \
    .groupBy("CPFCNPJ").agg(concat_ws("; ", collect_list("FONE")).alias("Telefones"))
df_telefones_agg.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_telefones_agg")

# 2.2 Emails
print("Processando Emails...")
df_emails_agg = spark.read.table("LH_Bronze.cad_email") \
    .filter(col("EMAIL").isNotNull() & (col("EMAIL") != "")) \
    .select("CPFCNPJ", "EMAIL").distinct() \
    .groupBy("CPFCNPJ").agg(concat_ws("; ", collect_list("EMAIL")).alias("Emails"))
df_emails_agg.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_emails_agg")

# 2.3 Endereços
print("Processando Endereços...")
schema_regioes = StructType([
    StructField("Sigla", StringType(), True), StructField("Estado", StringType(), True),
    StructField("Capital", StringType(), True), StructField("Regiao", StringType(), True)
])
data_regioes = [("AC", "Acre", "Rio Branco", "Norte"),("AL", "Alagoas", "Maceió", "Nordeste"),("AP", "Amapá", "Macapá", "Norte"),("AM", "Amazonas", "Manaus", "Norte"),("BA", "Bahia", "Salvador", "Nordeste"),("CE", "Ceará", "Fortaleza", "Nordeste"),("DF", "Distrito Federal", "Brasília", "Centro-Oeste"),("ES", "Espírito Santo", "Vitória", "Sudeste"),("GO", "Goiás", "Goiânia", "Centro-Oeste"),("MA", "Maranhão", "São Luís", "Nordeste"),("MT", "Mato Grosso", "Cuiabá", "Centro-Oeste"),("MS", "Mato Grosso do Sul", "Campo Grande", "Centro-Oeste"),("MG", "Minas Gerais", "Belo Horizonte", "Sudeste"),("PA", "Pará", "Belém", "Norte"),("PB", "Paraíba", "João Pessoa", "Nordeste"),("PR", "Paraná", "Curitiba", "Sul"),("PE", "Pernambuco", "Recife", "Nordeste"),("PI", "Piauí", "Teresina", "Nordeste"),("RJ", "Rio de Janeiro", "Rio de Janeiro", "Sudeste"),("RN", "Rio Grande do Norte", "Natal", "Nordeste"),("RS", "Rio Grande do Sul", "Porto Alegre", "Sul"),("RO", "Rondônia", "Porto Velho", "Norte"),("RR", "Roraima", "Boa Vista", "Norte"),("SC", "Santa Catarina", "Florianópolis", "Sul"),("SP", "São Paulo", "São Paulo", "Sudeste"),("SE", "Sergipe", "Aracaju", "Nordeste"),("TO", "Tocantins", "Palmas", "Norte")]
df_regioes = spark.createDataFrame(data=data_regioes, schema=schema_regioes).cache()

df_enderecos_bronze = spark.read.table("LH_Bronze.cad_enderecos")
df_enderecos_filtrado = df_enderecos_bronze.drop("PAIS", "FONE", "FAX", "TIPO", "DATAINCLUSAO", "USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODMUNICIPIO", "CODENDERECO") \
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

df_enderecos_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_enderecos_limpa")
df_regioes.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: Bridge, Contratos, Limites e Outros

# CELL ********************

# 3.1 Contratos
print("Processando Contratos...")
df_bronze_contratos = spark.read.table("LH_Bronze.cad_contratos_clientes")
df_transformed_contratos = df_bronze_contratos \
    .withColumn("PERCCONFIRMACAO", col("PERCCONFIRMACAO") / 100) \
    .withColumn("STATUSDIRETORIA", when(col("OBSERVACOES").like("%#STATUSDIRETORIA%"), 1).otherwise(0)) \
    .select("CODCONTRATO", "CODCLIENTE", "DTINICONTRATO", "VALIDADELIMITE", "FATOR", "LIMITEFOMENTO", "LIMITECOMISSARIA", "STATUS", "PERCCONFIRMACAO", "TRANCHE", "STATUSDIRETORIA") \
    .orderBy(col("CODCONTRATO").desc())
df_transformed_contratos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_contratos_clientes_limpa")

# 3.2 Bridge Cliente-Gerente
print("Processando Bridge Cliente-Gerente...")
df_historico = spark.read.table("LH_Bronze.rlc_brokers_clientes_historico")
df_atual = spark.read.table("LH_Bronze.rlc_brokers_clientes")
df_unificado = df_historico.unionByName(df_atual, allowMissingColumns=True)
df_preparado = df_unificado.withColumn("DataInicioVigencia", coalesce(col("DATAINICIO"), col("DATAINCLUSAO")).cast("date")) \
    .select(col("CODCLIENTE").alias("ClienteID"), col("CODBROKER").alias("GerenteID"), "DataInicioVigencia") \
    .filter(col("ClienteID").isNotNull() & col("GerenteID").isNotNull() & col("DataInicioVigencia").isNotNull()).distinct()
windowSpec_bridge = Window.partitionBy("ClienteID").orderBy(col("DataInicioVigencia").asc())
df_com_data_fim = df_preparado.withColumn("DataFimVigencia_temp", lead("DataInicioVigencia", 1, datetime.date(9999, 12, 31)).over(windowSpec_bridge))
df_final_bridge = df_com_data_fim.withColumn("DataFimVigencia", when(col("DataFimVigencia_temp") == datetime.date(9999, 12, 31), lit("9999-12-31").cast("date")).otherwise(date_sub(col("DataFimVigencia_temp"), 1))) \
    .select("ClienteID", "GerenteID", "DataInicioVigencia", "DataFimVigencia")
df_final_bridge.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.bridge_cliente_gerente")
df_final_bridge.filter(col("DataFimVigencia") == "9999-12-31").select("ClienteID", "GerenteID", "DataInicioVigencia").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.relacionamento_cliente_gerente_atual")

# 3.3 Limites
print("Processando Limites...")
df_bronze_limites = spark.read.table("LH_Bronze.rlc_clientes_sacados_limites")
df_transformed_limites = df_bronze_limites \
    .withColumn("TIPO", regexp_replace(col("TIPO"), "^I$", "INTERCIA")) \
    .withColumn("TipoDocumentoSacado", when(length(col("CPFCNPJ")) == 11, "CPF").when(length(col("CPFCNPJ")) == 14, "CNPJ").otherwise("Inválido")) \
    .withColumn("RaizCNPJ", when(col("TipoDocumentoSacado") == "CNPJ", substring(col("CPFCNPJ"), 1, 8)).otherwise(col("CPFCNPJ"))) \
    .withColumn("chave_cliente_sacado", concat(col("CODCLIENTE").cast("string"), lit("-"), col("RaizCNPJ")))
window_limites = Window.partitionBy("chave_cliente_sacado").orderBy(col("DATAINCLUSAO").desc())
df_transformed_limites = df_transformed_limites.withColumn("row_num", row_number().over(window_limites)).filter(col("row_num") == 1).drop("row_num")
df_transformed_limites.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_rlc_clientes_sacados_limites")

# 3.4 Empresas
print("Processando Empresas...")
spark.read.table("LH_Bronze.cad_empresas").filter(col("CODEMPRESA").isin([6, 14, 24, 25])).select(col("CODEMPRESA").alias("cod_empresa"), col("CNPJ").alias("cnpj")).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_empresas")

# 3.5 Gerentes
print("Processando Gerentes...")
spark.read.table("LH_Bronze.cad_brokers").select(col("CODBROKER").alias("cod_broker"), col("CPFCNPJ").alias("cpf_cnpj"), col("CODAGENCIA").alias("cod_agencia"), col("CODUSUARIO").alias("cod_usuario")).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_gerentes")

# 3.6 Plataformas
print("Processando Plataformas...")
df_agencias = spark.read.table("LH_Bronze.cad_agencias").select(col("CODAGENCIA").alias("cod_agencia"), col("NOME").alias("nome_plataforma"))
df_sup_gestor = spark.read.table("LH_Silver.sup_gestor_de_plataforma")
df_agencias.join(df_sup_gestor, on="cod_agencia", how="left").withColumn("plataforma", regexp_replace(col("nome_plataforma"), "^.*? ", "")).select("cod_agencia", "nome_plataforma", "gestor_da_plataforma", "plataforma").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_plataformas")

# 3.7 Status Esteira
print("Processando Status Esteira...")
spark.read.table("LH_Silver.sup_status_de_clientes_da_esteira").withColumnRenamed("codstatuscliente", "cod_status_cliente").select("cod_status_cliente", "status_do_cliente", "macroprocesso", "fase").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_status_clientes_esteira")

print("Limpeza Silver - Cadastros finalizada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
