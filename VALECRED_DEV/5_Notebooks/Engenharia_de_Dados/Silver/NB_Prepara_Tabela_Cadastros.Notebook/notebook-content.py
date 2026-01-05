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
# **Objetivo:** Processamento de tabelas dimensionais e cadastrais (`clientes`, `geral`, `telefones`, `enderecos`, `contratos`, `bridge`, `limites`, `empresas`, `gerentes`, `plataformas`, `status`, `usuarios`, `pareceres`, `sacados`).
# 
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
    lead, date_add, lag, max, coalesce, date_sub, transform, 
    filter as array_filter, split, array_join, array_contains
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, IntegerType
from functools import reduce
from notebookutils import mssparkutils
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

def process_clientes():
    print("Processando Clientes...")
    df_bronze_clientes = spark.read.table(f"{source_lakehouse}.cad_clientes")
    windowSpec_clientes = Window.partitionBy("CODCLIENTE").orderBy(col("DATAALTERACAO").desc())
    df_deduplicated_clientes = df_bronze_clientes.withColumn("row_num", row_number().over(windowSpec_clientes)) \
        .filter(col("row_num") == 1).drop("row_num") \
        .select(col("CODCLIENTE").alias("cod_cliente"), col("CPFCNPJ").alias("cpf_cnpj"))
    df_deduplicated_clientes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_clientes_limpa")

def process_cadastro_geral():
    print("Processando Cadastro Geral...")
    df_geral_bronze = spark.read.table(f"{source_lakehouse}.cad_geral_pf_pj")
    window_geral = Window.partitionBy("CPFCNPJ").orderBy(col("DATAALTERACAO").desc())
    df_geral_deduplicated = df_geral_bronze.withColumn("row_num", row_number().over(window_geral)) \
        .filter(col("row_num") == 1).drop("row_num") \
        .select(col("CPFCNPJ").alias("cpf_cnpj"), col("NOME").alias("nome"), col("NOME").alias("razao_social"), col("FANTASIA").alias("nome_fantasia"))

    # 1. Definir "Stopwords" (termos que não devem compor a sigla)
    stopwords = ["DA", "DE", "DO", "DAS", "DOS", "E", "LTDA", "S.A", "SA", "ME", "EPP", "S/A"]

    # 2. Aplicar lógica vetorial (Alta Performance)
    df_geral_deduplicated = df_geral_deduplicated \
        .withColumn("nome_base", col("nome")) \
        .withColumn(
            "sigla",
            array_join(
                transform(
                    # Passo A: Limpa caracteres especiais, deixa maiúsculo e quebra em array
                    array_filter(
                        split(regexp_replace(upper(col("nome_base")), "[^A-Z0-9 ]", ""), " "),
                        # Passo B: Remove stopwords e espaços vazios
                        lambda x: (length(x) > 0) & (~x.isin(stopwords))
                    ),
                    # Passo C: Pega a primeira letra de cada palavra restante
                    lambda x: substring(x, 1, 1)
                ),
                "" # Junta tudo sem espaço (Ex: "Fundo Investimento" -> "FI")
            )
        ).drop("nome_base")

    df_cadastros_clean = df_geral_deduplicated \
        .filter(col("cpf_cnpj").rlike("^[0-9]+$")) \
        .select("cpf_cnpj", "nome", "sigla")

    # Validação rápida
    # df_cadastros_clean.show(5, truncate=False)
    df_cadastros_clean.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_cad_geral_pf_pj_limpa")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Contatos e Endereços

# CELL ********************

def process_telefones():
    print("Processando Telefones...")
    df_telefones_agg = spark.read.table(f"{source_lakehouse}.cad_telefones") \
        .filter((col("FONE").isNotNull() & (col("FONE") != "")) & (col("DDD").isNotNull() & (col("DDD") != ""))) \
        .withColumn("FONE_limpo", regexp_replace(col("FONE"), "-", "")) \
        .withColumn("FONE_COMPLETO", regexp_replace(concat(col("DDD"), col("FONE_limpo")), " ", "")) \
        .filter((length(col("FONE_COMPLETO")) >= 10) & (length(col("FONE_COMPLETO")) <= 11)) \
        .select(col("CPFCNPJ").alias("cpf_cnpj"), col("FONE_COMPLETO").alias("fone"), col("CONTATO")).distinct() \
        .groupBy("cpf_cnpj").agg(concat_ws("; ", collect_list("fone")).alias("telefones"))
    df_telefones_agg.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_telefones_agg")

def process_emails():
    print("Processando Emails...")
    df_emails_agg = spark.read.table(f"{source_lakehouse}.cad_email") \
        .filter(col("EMAIL").isNotNull() & (col("EMAIL") != "")) \
        .select(col("CPFCNPJ").alias("cpf_cnpj"), col("EMAIL").alias("email")).distinct() \
        .groupBy("cpf_cnpj").agg(concat_ws("; ", collect_list("email")).alias("emails"))
    df_emails_agg.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_emails_agg")

def process_enderecos():
    print("Processando Endereços...")
    schema_regioes = StructType([
        StructField("sigla", StringType(), True), StructField("estado", StringType(), True),
        StructField("capital", StringType(), True), StructField("regiao", StringType(), True)
    ])
    data_regioes = [("AC", "Acre", "Rio Branco", "Norte"),("AL", "Alagoas", "Maceió", "Nordeste"),("AP", "Amapá", "Macapá", "Norte"),("AM", "Amazonas", "Manaus", "Norte"),("BA", "Bahia", "Salvador", "Nordeste"),("CE", "Ceará", "Fortaleza", "Nordeste"),("DF", "Distrito Federal", "Brasília", "Centro-Oeste"),("ES", "Espírito Santo", "Vitória", "Sudeste"),("GO", "Goiás", "Goiânia", "Centro-Oeste"),("MA", "Maranhão", "São Luís", "Nordeste"),("MT", "Mato Grosso", "Cuiabá", "Centro-Oeste"),("MS", "Mato Grosso do Sul", "Campo Grande", "Centro-Oeste"),("MG", "Minas Gerais", "Belo Horizonte", "Sudeste"),("PA", "Pará", "Belém", "Norte"),("PB", "Paraíba", "João Pessoa", "Nordeste"),("PR", "Paraná", "Curitiba", "Sul"),("PE", "Pernambuco", "Recife", "Nordeste"),("PI", "Piauí", "Teresina", "Nordeste"),("RJ", "Rio de Janeiro", "Rio de Janeiro", "Sudeste"),("RN", "Rio Grande do Norte", "Natal", "Nordeste"),("RS", "Rio Grande do Sul", "Porto Alegre", "Sul"),("RO", "Rondônia", "Porto Velho", "Norte"),("RR", "Roraima", "Boa Vista", "Norte"),("SC", "Santa Catarina", "Florianópolis", "Sul"),("SP", "São Paulo", "São Paulo", "Sudeste"),("SE", "Sergipe", "Aracaju", "Nordeste"),("TO", "Tocantins", "Palmas", "Norte")]
    df_regioes = spark.createDataFrame(data=data_regioes, schema=schema_regioes).cache()

    df_enderecos_bronze = spark.read.table(f"{source_lakehouse}.cad_enderecos")
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
        .join(df_regioes, col("UF") == df_regioes.sigla, "left") \
        .withColumn("CPFCNPJ_valido", col("CPFCNPJ").cast("long")).filter(col("CPFCNPJ_valido").isNotNull()) \
        .select(col("CPFCNPJ").alias("cpf_cnpj"), col("ENDERECO").alias("endereco"), col("NUMERO").alias("numero"), col("COMPLEMENTO").alias("complemento"), col("BAIRRO").alias("bairro"), col("CIDADE").alias("cidade"), col("UF").alias("uf"), col("CEP").alias("cep"), col("estado"), col("capital"), col("regiao"))

    df_enderecos_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_enderecos_limpa")
    df_regioes.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: Bridge, Contratos, Limites e Outros

# CELL ********************

def process_contratos():
    print("Processando Contratos...")
    df_bronze_contratos = spark.read.table(f"{source_lakehouse}.cad_contratos_clientes")
    df_transformed_contratos = df_bronze_contratos \
        .withColumn("PERCCONFIRMACAO", col("PERCCONFIRMACAO") / 100) \
        .withColumn("STATUSDIRETORIA", when(col("OBSERVACOES").like("%#STATUSDIRETORIA%"), 1).otherwise(0)) \
        .select(col("CODCONTRATO").alias("cod_contrato"), col("CODCLIENTE").alias("cod_cliente"), col("DTINICONTRATO").alias("dt_ini_contrato"), col("VALIDADELIMITE").alias("validade_limite"), col("FATOR").alias("fator"), col("LIMITEFOMENTO").alias("limite_fomento"), col("LIMITECOMISSARIA").alias("limite_comissaria"), col("STATUS").alias("status"), col("PERCCONFIRMACAO").alias("perc_confirmacao"), col("TRANCHE").alias("tranche"), col("STATUSDIRETORIA").alias("status_diretoria")) \
        .orderBy(col("cod_contrato").desc())
    df_transformed_contratos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_contratos_clientes_limpa")

def process_bridge_cliente_gerente():
    print("Processando Bridge Cliente-Gerente...")
    df_historico = spark.read.table(f"{source_lakehouse}.rlc_brokers_clientes_historico")
    df_atual = spark.read.table(f"{source_lakehouse}.rlc_brokers_clientes")
    df_unificado = df_historico.unionByName(df_atual, allowMissingColumns=True)
    df_preparado = df_unificado.withColumn("data_inicio_vigencia", coalesce(col("DATAINICIO"), col("DATAINCLUSAO")).cast("date")) \
        .select(col("CODCLIENTE").alias("cod_cliente"), col("CODBROKER").alias("cod_gerente"), "data_inicio_vigencia") \
        .filter(col("cod_cliente").isNotNull() & col("cod_gerente").isNotNull() & col("data_inicio_vigencia").isNotNull()).distinct()
    windowSpec_bridge = Window.partitionBy("cod_cliente").orderBy(col("data_inicio_vigencia").asc())
    df_com_data_fim = df_preparado.withColumn("data_fim_vigencia_temp", lead("data_inicio_vigencia", 1, datetime.date(9999, 12, 31)).over(windowSpec_bridge))
    df_final_bridge = df_com_data_fim.withColumn("data_fim_vigencia", when(col("data_fim_vigencia_temp") == datetime.date(9999, 12, 31), lit("9999-12-31").cast("date")).otherwise(date_sub(col("data_fim_vigencia_temp"), 1))) \
        .select("cod_cliente", "cod_gerente", "data_inicio_vigencia", "data_fim_vigencia")
    df_final_bridge.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.bridge_cliente_gerente")
    df_final_bridge.filter(col("data_fim_vigencia") == "9999-12-31").select("cod_cliente", "cod_gerente", "data_inicio_vigencia").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.relacionamento_cliente_gerente_atual")

def process_limites():
    print("Processando Limites...")
    df_bronze_limites = spark.read.table(f"{source_lakehouse}.rlc_clientes_sacados_limites")
    df_transformed_limites = df_bronze_limites \
        .withColumn("tipo", regexp_replace(col("TIPO"), "^I$", "INTERCIA")) \
        .withColumn("tipo_documento_sacado", when(length(col("CPFCNPJ")) == 11, "CPF").when(length(col("CPFCNPJ")) == 14, "CNPJ").otherwise("Inválido")) \
        .withColumn("raiz_cnpj", when(col("tipo_documento_sacado") == "CNPJ", substring(col("CPFCNPJ"), 1, 8)).otherwise(col("CPFCNPJ"))) \
        .withColumn("chave_cliente_sacado", concat(col("CODCLIENTE").cast("string"), lit("-"), col("raiz_cnpj"))) \
        .withColumnRenamed("CODCLIENTE", "cod_cliente") \
        .withColumnRenamed("CPFCNPJ", "cpf_cnpj") \
        .withColumnRenamed("DATAINCLUSAO", "data_inclusao")

    # Garantir snake_case em todas as colunas
    df_transformed_limites = df_transformed_limites.select([col(c).alias(c.lower()) for c in df_transformed_limites.columns])

    window_limites = Window.partitionBy("chave_cliente_sacado").orderBy(col("data_inclusao").desc())
    df_transformed_limites = df_transformed_limites.withColumn("row_num", row_number().over(window_limites)).filter(col("row_num") == 1).drop("row_num")
    df_transformed_limites.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_rlc_clientes_sacados_limites")

def process_empresas():
    print("Processando Empresas...")
    spark.read.table(f"{source_lakehouse}.cad_empresas").filter(col("CODEMPRESA").isin([6, 14, 24, 25])).select(col("CODEMPRESA").alias("cod_empresa"), col("CNPJ").alias("cnpj")).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_empresas")

def process_gerentes():
    print("Processando Gerentes...")
    spark.read.table(f"{source_lakehouse}.cad_brokers").select(col("CODBROKER").alias("cod_broker"), col("CPFCNPJ").alias("cpf_cnpj"), col("CODAGENCIA").alias("cod_agencia"), col("CODUSUARIO").alias("cod_usuario")).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_gerentes")

def process_plataformas():
    print("Processando Plataformas...")
    df_agencias = spark.read.table(f"{source_lakehouse}.cad_agencias").select(col("CODAGENCIA").alias("cod_agencia"), col("NOME").alias("nome_plataforma"))
    df_sup_gestor = spark.read.table(f"{target_lakehouse}.sup_gestor_de_plataforma")
    df_agencias.join(df_sup_gestor, on="cod_agencia", how="left").withColumn("plataforma", regexp_replace(col("nome_plataforma"), "^.*? ", "")).select("cod_agencia", "nome_plataforma", "gestor_da_plataforma", "plataforma").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_plataformas")

def process_status_esteira():
    print("Processando Status Esteira...")
    spark.read.table(f"{target_lakehouse}.sup_status_de_clientes_da_esteira").withColumnRenamed("codstatuscliente", "cod_status_cliente").select("cod_status_cliente", "status_do_cliente", "macroprocesso", "fase").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_status_clientes_esteira")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 4: Usuários
# **Objetivo:** Processamento de `cad_usuarios` e enriquecimento com níveis.

# CELL ********************

def process_usuarios():
    print("Processando Usuários...")
    # Dados manuais de Nivel de Usuario (do Dataflow)
    data_nivel = [
        (1, "ADMINISTRADOR"), (2, "DIRETORIA"), (3, "GERENTE"), (4, "SUPERVISOR"),
        (5, "OPERADOR"), (6, "SEM ACESSO"), (7, "CONSULTA"), (8, "JURIDICO"),
        (9, "COMERCIAL"), (10, "BACKOFFICE")
    ]
    # Rename column to avoid ambiguity with 'nivel' in df_usuarios
    df_nivel = spark.createDataFrame(data_nivel, ["id_nivel", "descricao_nivel"])

    df_usuarios = spark.read.table(f"{source_lakehouse}.cad_usuarios") \
        .select(
            col("CODUSUARIO").alias("cod_usuario"),
            col("NOME").alias("nome"),
            col("FUNCAO").alias("funcao"),
            col("NIVEL").alias("nivel"),
            col("CPFCNPJ").alias("cpf_cnpj"),
            col("APELIDO").alias("apelido")
        ) \
        .withColumn("nome", upper(col("nome"))) \
        .withColumn("funcao", upper(col("funcao")))

    # Update join condition to use new column name
    df_usuarios_final = df_usuarios.join(df_nivel, col("nivel") == df_nivel.id_nivel, "left") \
        .drop("id_nivel") \
        .select("cod_usuario", "nome", "funcao", "nivel", "descricao_nivel", "cpf_cnpj", "apelido")

    df_usuarios_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_usuarios")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 5: Pareceres Clientes (Esteira)
# **Objetivo:** Análise de mudança de status de clientes.

# CELL ********************

def process_pareceres_clientes_esteira():
    print("Processando Pareceres Clientes (Esteira)...")
    df_pareceres = spark.read.table(f"{source_lakehouse}.cad_geral_pareceres")
    df_clientes = spark.read.table(f"{source_lakehouse}.cad_clientes")
    df_status = spark.read.table(f"{target_lakehouse}.staging_status_clientes_esteira")

    # Filtragem inicial
    df_pareceres_cli = df_pareceres.filter(
        (col("CODTIPOPARECER") == 1) &
        (col("OBS").startswith("STATUS ALTERADO PARA ")) &
        col("CPFCNPJ").isNotNull()
    )

    # Extração de Status
    df_extracted = df_pareceres_cli.withColumn(
        "status_cliente",
        substring(col("OBS"), 22, 100) # Length arbitrary large to get rest of string
    ).withColumn("data_log", col("DATAINCLUSAO"))

    # Join com Clientes (para pegar CODCLIENTE)
    df_joined = df_extracted.join(df_clientes, "CPFCNPJ", "left") \
        .select("CODCLIENTE", "status_cliente", "data_log", "USUAINCLUSAO") \
        .filter(col("CODCLIENTE").isNotNull())

    # Join com Status (Enriquecimento)
    df_enriched = df_joined.join(df_status, df_joined.status_cliente == df_status.status_do_cliente, "left") \
        .select("CODCLIENTE", "status_cliente", "data_log", "USUAINCLUSAO", "macroprocesso", "fase")

    # Window Functions para Esteira (Anterior/Posterior)
    window_esteira = Window.partitionBy("CODCLIENTE").orderBy("data_log")
    df_esteira = df_enriched.withColumn("status_anterior", lag("status_cliente").over(window_esteira)) \
        .withColumn("data_anterior", lag("data_log").over(window_esteira)) \
        .withColumn("macroprocesso_anterior", lag("macroprocesso").over(window_esteira)) \
        .withColumn("fase_anterior", lag("fase").over(window_esteira)) \
        .filter(col("status_cliente") != col("status_anterior")) # Remove duplicatas consecutivas de mesmo status

    # Flags Devolução/Recebida
    df_final_esteira = df_esteira.withColumn(
            "devolucao",
            (col("macroprocesso") == "CREDITO") & (col("macroprocesso_anterior") == "COMERCIAL")
        ).withColumn(
            "recebida",
            (col("macroprocesso") == "COMERCIAL") & (col("macroprocesso_anterior") == "CREDITO")
        ).withColumnRenamed("CODCLIENTE", "cod_cliente") \
         .withColumnRenamed("USUAINCLUSAO", "usua_inclusao")

    df_final_esteira.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_pareceres_clientes_esteira")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 6: Sacados Enriquecida
# **Objetivo:** Tabela unificada de sacados com contatos.

# CELL ********************

def process_sacados_enriquecida():
    print("Processando Sacados Enriquecida...")
    # Distinct Sacados from Titulos
    df_titulos_sacados = spark.read.table(f"{source_lakehouse}.tab_titulos") \
        .filter(year(col("DATAINCLUSAO")) >= 2021) \
        .select(col("CPFCNPJSACADO").alias("cpf_cnpj")).distinct()

    # Joins
    df_geral = spark.read.table(f"{target_lakehouse}.staging_cad_geral_pf_pj_limpa")
    df_enderecos = spark.read.table(f"{target_lakehouse}.staging_enderecos_limpa")
    df_emails = spark.read.table(f"{target_lakehouse}.staging_emails_agg")
    df_telefones = spark.read.table(f"{target_lakehouse}.staging_telefones_agg")

    df_sacados = df_titulos_sacados \
        .join(df_geral, "cpf_cnpj", "left") \
        .join(df_enderecos, "cpf_cnpj", "left") \
        .join(df_emails, "cpf_cnpj", "left") \
        .join(df_telefones, "cpf_cnpj", "left") \
        .select(
            "cpf_cnpj",
            col("nome").alias("nome_sacado"),
            "endereco", "numero", "complemento", "bairro", "cidade", "uf", "cep", "regiao",
            "emails", "telefones"
        )

    df_sacados.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_sacados_enriquecida")

# Execução
process_clientes()
process_cadastro_geral()
process_telefones()
process_emails()
process_enderecos()
process_contratos()
process_bridge_cliente_gerente()
process_limites()
process_empresas()
process_gerentes()
process_plataformas()
process_status_esteira()
process_usuarios()
process_pareceres_clientes_esteira()
process_sacados_enriquecida()

print("Limpeza Silver - Cadastros finalizada.")
mssparkutils.notebook.exit("Success")
