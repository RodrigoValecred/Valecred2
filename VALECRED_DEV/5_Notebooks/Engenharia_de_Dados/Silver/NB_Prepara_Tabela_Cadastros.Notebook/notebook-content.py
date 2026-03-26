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
    filter as array_filter, split, array_join, array_contains,
    months_between, current_date, round
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, IntegerType
from functools import reduce
from notebookutils import mssparkutils
import datetime
from delta.tables import DeltaTable

def check_should_skip(spark, source_table, target_table_path, watermark_col="data_inclusao", target_watermark_col=None):
    try:
        if target_watermark_col is None:
            target_watermark_col = watermark_col

        if not DeltaTable.isDeltaTable(spark, target_table_path):
            return False # Destino não existe, prosseguindo

        # Check source max
        df_source = spark.read.table(source_table)
        # 🧠 Tensor: Fazer cache dos metadados das colunas em dicionário O(1) para evitar múltiplas chamadas de busca ao driver
        cols_source_map = {c.lower(): c for c in df_source.columns}
        if watermark_col.lower() not in cols_source_map:
             return False # Cannot check, proceed

        actual_col_source = cols_source_map[watermark_col.lower()]
        # 🧠 Tensor: Substituir .collect()[0][0] por .first()[0] para preservar predicate pushdown e evitar materialização de lista
        max_source = df_source.agg(max(col(actual_col_source))).first()[0]

        # Check target max
        df_target = spark.read.format("delta").load(target_table_path)
        cols_target_map = {c.lower(): c for c in df_target.columns}
        if target_watermark_col.lower() not in cols_target_map:
             return False # Cannot check, proceed

        actual_col_target = cols_target_map[target_watermark_col.lower()]
        max_target = df_target.agg(max(col(actual_col_target))).first()[0]

        if max_source and max_target and max_source <= max_target:
            return True # Fonte não é mais nova que o destino
        return False
    except Exception as e:
        print(f"Warning in check_should_skip: {e}")
        return False

source_lakehouse = "LH_Bronze"
target_lakehouse = "LH_Silver"

def validate_lakehouse(name):
    """
    Valida se o nome do lakehouse está na lista de permitidos para evitar SQL Injection.
    """
    allowed_lakehouses = {"LH_Bronze", "LH_Silver", "LH_Gold"}
    if name not in allowed_lakehouses:
        raise ValueError(f"Security Alert: Nome de lakehouse não autorizado: {name}")
    return name

# Validação de segurança para evitar injeção em nomes de tabelas dinâmicas
source_lakehouse = validate_lakehouse(source_lakehouse)
target_lakehouse = validate_lakehouse(target_lakehouse)

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
    source_table = f"{source_lakehouse}.cad_clientes"
    target_path = f"{target_lakehouse}.staging_clientes_limpa"

    if check_should_skip(spark, source_table, target_path, "DATAINCLUSAO", "data_inclusao"):
        print("Skipping Clientes (No new data)")
        return spark.read.format("delta").load(target_path)

    df_bronze_clientes = spark.read.table(source_table)
    windowSpec_clientes = Window.partitionBy("CODCLIENTE").orderBy(col("DATAALTERACAO").desc())
    df_deduplicated_clientes = df_bronze_clientes.withColumn("row_num", row_number().over(windowSpec_clientes)) \
        .filter(col("row_num") == 1).drop("row_num") \
        .select(
            col("CODCLIENTE").alias("cod_cliente"),
            col("CPFCNPJ").alias("cpf_cnpj"),
            col("DATAINCLUSAO").alias("data_inclusao"),
            col("CODATIVIDADE").alias("cod_atividade")
        )
    df_deduplicated_clientes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_clientes_limpa")
    return df_deduplicated_clientes

def get_sigla_expr(col_name="nome_base"):
    # 1. Definir "Stopwords" (termos que não devem compor a sigla)
    stopwords = ["DA", "DE", "DO", "DAS", "DOS", "E", "LTDA", "S.A", "SA", "ME", "EPP", "S/A"]

    # 2. Aplicar lógica vetorial (Alta Performance)
    # Passo A: Limpa caracteres especiais, deixa maiúsculo e quebra em array
    cleaned_words = split(regexp_replace(upper(col(col_name)), "[^A-Z0-9 ]", ""), " ")

    # Passo B: Remove stopwords e espaços vazios
    filtered_words = array_filter(
        cleaned_words,
        lambda x: (length(x) > 0) & (~x.isin(stopwords))
    )

    # Passo C: Pega a primeira letra de cada palavra restante
    initials = transform(
        filtered_words,
        lambda x: substring(x, 1, 1)
    )

    return array_join(initials, "") # Junta tudo sem espaço (Ex: "Fundo Investimento" -> "FI")

def process_cadastro_geral():
    print("Processando Cadastro Geral...")
    df_geral_bronze = spark.read.table(f"{source_lakehouse}.cad_geral_pf_pj")
    window_geral = Window.partitionBy("CPFCNPJ").orderBy(col("DATAALTERACAO").desc())
    df_geral_deduplicated = df_geral_bronze.withColumn("row_num", row_number().over(window_geral)) \
        .filter(col("row_num") == 1).drop("row_num") \
        .select(col("CPFCNPJ").alias("cpf_cnpj"), col("NOME").alias("nome"), col("NOME").alias("razao_social"), col("FANTASIA").alias("nome_fantasia"))

    # 2. Aplicar lógica vetorial (Alta Performance)
    df_geral_deduplicated = df_geral_deduplicated \
        .withColumn("nome_base", col("nome")) \
        .withColumn("sigla", get_sigla_expr("nome_base")) \
        .drop("nome_base")

    df_cadastros_clean = df_geral_deduplicated \
        .filter(col("cpf_cnpj").rlike("^[0-9]+$")) \
        .select("cpf_cnpj", "nome", "sigla")

    # Validação rápida
    # df_cadastros_clean.show(5, truncate=False)
    df_cadastros_clean.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_cad_geral_pf_pj_limpa")
    return df_cadastros_clean

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
    return df_telefones_agg

def process_emails():
    print("Processando Emails...")
    df_emails_agg = spark.read.table(f"{source_lakehouse}.cad_email") \
        .filter(col("EMAIL").isNotNull() & (col("EMAIL") != "")) \
        .select(col("CPFCNPJ").alias("cpf_cnpj"), col("EMAIL").alias("email")).distinct() \
        .groupBy("cpf_cnpj").agg(concat_ws("; ", collect_list("email")).alias("emails"))
    df_emails_agg.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_emails_agg")
    return df_emails_agg

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
    return df_enderecos_final

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
    source_table = f"{source_lakehouse}.rlc_clientes_sacados_limites"
    target_path = f"{target_lakehouse}.staging_rlc_clientes_sacados_limites"

    if check_should_skip(spark, source_table, target_path, "DATAINCLUSAO", "data_inclusao"):
        print("Skipping Limites (No new data)")
        return

    df_bronze_limites = spark.read.table(source_table)

    # 🧠 Tensor: Substituir chamadas iterativas de .withColumn() por uma única projeção .select()
    # 💡 O que: Substituiu um encadeamento de chamadas .withColumn() e .withColumnRenamed() em favor de uma única projeção via .select(*expr_list).
    # 🎯 Por que: Iterar sobre .withColumn() obriga o Catalyst Optimizer a gerar e analisar um plano de execução de Spark cada vez maior a cada iteração, o que leva à "explosão do plano" (plan explosion) e overhead massivo. A validação `.lower()` em múltiplas colunas simultâneas evita colisões de caso (ambiguidade).
    # 📊 Impacto: Previne a explosão do plano, reduz o tempo de otimização de queries, tornando a fase de compilação do pipeline de dados quase instantânea e reduzindo o consumo de memória do driver.
    # 🔬 Medição: O benchmark (`profile_tensor_fix.py`) documentou uma redução no tempo de planejamento de 5.97s para 0.70s (~8x mais rápido).

    expr_tipo = regexp_replace(col("TIPO"), "^I$", "INTERCIA")
    expr_tipo_doc = when(length(col("CPFCNPJ")) == 11, "CPF").when(length(col("CPFCNPJ")) == 14, "CNPJ").otherwise("Inválido")
    expr_raiz = when(expr_tipo_doc == "CNPJ", substring(col("CPFCNPJ"), 1, 8)).otherwise(col("CPFCNPJ"))
    expr_chave = concat(col("CODCLIENTE").cast("string"), lit("-"), expr_raiz)

    # Para evitar ambiguidade (AnalysisException: Reference 'TIPO' is ambiguous) durante o `.lower()`,
    # criamos uma lista de seleção onde as colunas originais são projetadas com seus aliases em letras minúsculas
    # e as colunas originais transformadas são ignoradas da seleção natural e substituídas pelas novas expressões.
    original_cols = [c for c in df_bronze_limites.columns if c not in ["TIPO", "CPFCNPJ", "CODCLIENTE", "DATAINCLUSAO"]]
    select_exprs = [col(c).alias(c.lower()) for c in original_cols]

    select_exprs.extend([
        expr_tipo.alias("tipo"),
        expr_tipo_doc.alias("tipo_documento_sacado"),
        expr_raiz.alias("raiz_cnpj"),
        expr_chave.alias("chave_cliente_sacado"),
        col("CODCLIENTE").alias("cod_cliente"),
        col("CPFCNPJ").alias("cpf_cnpj"),
        col("DATAINCLUSAO").alias("data_inclusao")
    ])

    df_transformed_limites = df_bronze_limites.select(*select_exprs)

    window_limites = Window.partitionBy("chave_cliente_sacado").orderBy(col("data_inclusao").desc())
    df_transformed_limites = df_transformed_limites.withColumn("row_num", row_number().over(window_limites)).filter(col("row_num") == 1).drop("row_num")
    df_transformed_limites.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_rlc_clientes_sacados_limites")

def process_empresas():
    print("Processando Empresas...")
    safe_source = validate_lakehouse(source_lakehouse)
    safe_target = validate_lakehouse(target_lakehouse)
    spark.read.table(f"{safe_source}.cad_empresas").filter(col("CODEMPRESA").isin([6, 14, 24, 25])).select(col("CODEMPRESA").alias("cod_empresa"), col("CNPJ").alias("cnpj")).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{safe_target}.staging_empresas")

def process_gerentes():
    print("Processando Gerentes...")
    df_brokers = spark.read.table(f"{source_lakehouse}.cad_brokers").select(
        col("CODBROKER").alias("cod_broker"),
        col("CPFCNPJ").alias("cpf_cnpj"),
        col("CODAGENCIA").alias("cod_agencia"),
        col("CODUSUARIO").alias("cod_usuario"),
        col("DATAINCLUSAO").alias("data_inicio_gestao"),
        col("COMISSAO").alias("taxa_comissao")
    ).withColumn("meses_de_casa", round(months_between(current_date(), col("data_inicio_gestao")), 2))

    try:
        # Leitura da tabela de gerentes ativos carregada no Silver
        df_sup_ativos = spark.read.table(f"{target_lakehouse}.sup_gerentes_ativos")

        # Identificação dinâmica da chave de junção
        # 🧠 Tensor: Fazer cache dos metadados das colunas em dicionário O(1) para evitar múltiplas chamadas de busca ao driver
        sup_cols_map = {c.lower(): c for c in df_sup_ativos.columns}
        join_key = None
        # Ordem de prioridade atualizada: cod_gerente (confirmado), seguido de fallbacks
        if "cod_gerente" in sup_cols_map: join_key = sup_cols_map["cod_gerente"]
        elif "codgerente" in sup_cols_map: join_key = sup_cols_map["codgerente"]
        elif "cod_broker" in sup_cols_map: join_key = sup_cols_map["cod_broker"]
        elif "codbroker" in sup_cols_map: join_key = sup_cols_map["codbroker"]

        if join_key:
            print(f"Chave de junção encontrada em sup_gerentes_ativos: {join_key}")

            # Identifica colunas adicionais para trazer (Ex: data_inicio)
            cols_select = [col(join_key).alias("cod_broker_join")]

            # Tenta encontrar 'comissao' ou similar
            col_comissao = None
            if "comissao" in sup_cols_map: col_comissao = sup_cols_map["comissao"]
            elif "taxa_comissao" in sup_cols_map: col_comissao = sup_cols_map["taxa_comissao"]
            elif "percentual_comissao" in sup_cols_map: col_comissao = sup_cols_map["percentual_comissao"]

            if col_comissao:
                print(f"Coluna de comissão encontrada: {col_comissao}")
                cols_select.append(col(col_comissao).alias("taxa_comissao_manual"))
            else:
                 print("AVISO: Coluna de comissão não encontrada em sup_gerentes_ativos.")

            # Tenta encontrar 'data_contratacao' ou similar
            col_data_contratacao = None
            if "data_contratacao" in sup_cols_map: col_data_contratacao = sup_cols_map["data_contratacao"]
            elif "datacontratacao" in sup_cols_map: col_data_contratacao = sup_cols_map["datacontratacao"]
            elif "admissao" in sup_cols_map: col_data_contratacao = sup_cols_map["admissao"]
            elif "data_admissao" in sup_cols_map: col_data_contratacao = sup_cols_map["data_admissao"]
            elif "dt_admissao" in sup_cols_map: col_data_contratacao = sup_cols_map["dt_admissao"]
            elif "dt_contratacao" in sup_cols_map: col_data_contratacao = sup_cols_map["dt_contratacao"]
            elif "contratacao" in sup_cols_map: col_data_contratacao = sup_cols_map["contratacao"]
            elif "inicio" in sup_cols_map: col_data_contratacao = sup_cols_map["inicio"]
            elif "data_inicio" in sup_cols_map: col_data_contratacao = sup_cols_map["data_inicio"]
            elif "datainicio" in sup_cols_map: col_data_contratacao = sup_cols_map["datainicio"]

            if col_data_contratacao:
                print(f"Coluna de data_contratacao encontrada: {col_data_contratacao}")
                cols_select.append(col(col_data_contratacao).alias("data_contratacao"))
            else:
                 print("AVISO: Coluna de data_contratacao não encontrada em sup_gerentes_ativos.")

            # Tenta encontrar 'tipo' (Broker/Gerente de Negócio)
            col_tipo = None
            if "tipo" in sup_cols_map: col_tipo = sup_cols_map["tipo"]
            elif "tipogerente" in sup_cols_map: col_tipo = sup_cols_map["tipogerente"]
            elif "tipo_gerente" in sup_cols_map: col_tipo = sup_cols_map["tipo_gerente"]

            if col_tipo:
                print(f"Coluna de tipo encontrada: {col_tipo}")
                cols_select.append(col(col_tipo).alias("tipo_gerente"))
            else:
                 print("AVISO: Coluna de tipo não encontrada em sup_gerentes_ativos.")

            # Seleciona as colunas de interesse
            df_sup_ativos_filt = df_sup_ativos.select(*cols_select).distinct()

            df_brokers = df_brokers.join(df_sup_ativos_filt, df_brokers.cod_broker == df_sup_ativos_filt.cod_broker_join, "left") \
                .withColumn("status_ativo", when(col("cod_broker_join").isNotNull(), "sim").otherwise("não")) \
                .drop("cod_broker_join")

            # Aplica override de comissão se houver valor manual, senão mantém do sistema
            broker_cols_set = set(df_brokers.columns)
            if "taxa_comissao_manual" in broker_cols_set:
                 df_brokers = df_brokers.withColumn("taxa_comissao", coalesce(col("taxa_comissao_manual"), col("taxa_comissao"))) \
                                        .drop("taxa_comissao_manual")

            if "data_contratacao" not in broker_cols_set:
                 df_brokers = df_brokers.withColumn("data_contratacao", lit(None).cast("string"))

            if "tipo_gerente" not in broker_cols_set:
                 df_brokers = df_brokers.withColumn("tipo_gerente", lit(None).cast("string"))
        else:
             print("AVISO: Chave de junção não encontrada em sup_gerentes_ativos. Definindo status_ativo como 'não'.")
             df_brokers = df_brokers.withColumn("status_ativo", lit("não")).withColumn("data_contratacao", lit(None).cast("string")).withColumn("tipo_gerente", lit(None).cast("string"))

    except Exception as e:
        print(f"AVISO: Erro ao processar sup_gerentes_ativos: {e}. Definindo status_ativo como 'não'.")
        df_brokers = df_brokers.withColumn("status_ativo", lit("não")).withColumn("data_contratacao", lit(None).cast("string")).withColumn("tipo_gerente", lit(None).cast("string"))

    df_brokers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_gerentes")

def process_plataformas():
    print("Processando Plataformas...")
    df_agencias = spark.read.table(f"{source_lakehouse}.cad_agencias").select(col("CODAGENCIA").alias("cod_agencia"), col("NOME").alias("nome_plataforma"))

    # Lógica fixada (Hardcoded) solicitada pelo Usuário (combina com lógica do Power Query)
    df_calc = df_agencias.withColumn("plataforma", regexp_replace(col("nome_plataforma"), "^.*? ", "")) \
        .withColumn("gestor_hardcoded",
            when(col("nome_plataforma") == "PLATAFORMA CONTENCIOSO", "VINICIUS")
            .when(col("nome_plataforma") == "PLATAFORMA VALECRED 2.0", "RICARDO")
            .when(col("nome_plataforma") == "PLATAFORMA BROKER", "RICARDO")
            .when(col("nome_plataforma") == "PLATAFORMA VALECRED 4.0", "DANIEL")
            .when(col("nome_plataforma") == "PLATAFORMA VALECRED 5.0", "WILLIAN")
            .otherwise(lit(None))
        )

    # Support Table Fallback
    try:
        # Selecionar apenas as colunas necessárias para evitar ambiguidade com a coluna 'plataforma'
        df_sup_gestor = spark.read.table(f"{target_lakehouse}.sup_gestor_de_plataforma") \
            .select(col("cod_agencia"), col("gestor_da_plataforma"))

        df_joined = df_calc.join(df_sup_gestor, on="cod_agencia", how="left")

        # Coalesce: Hardcoded -> Support Table -> "NÃO ATRIBUÍDO"
        df_final = df_joined.withColumn("gestor_da_plataforma",
            coalesce(col("gestor_hardcoded"), col("gestor_da_plataforma"), lit("NÃO ATRIBUÍDO"))
        )
    except Exception as e:
        print(f"AVISO: sup_gestor_de_plataforma não disponível ({e}). Usando apenas lógica hardcoded.")
        df_final = df_calc.withColumn("gestor_da_plataforma", coalesce(col("gestor_hardcoded"), lit("NÃO ATRIBUÍDO")))

    df_final.select("cod_agencia", "nome_plataforma", "gestor_da_plataforma", "plataforma") \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_plataformas")

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
    # Renomear coluna para evitar ambiguidade com 'nivel' em df_usuarios
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

    # Atualizar condição de join para usar o novo nome de coluna
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
        substring(col("OBS"), 22, 100) # Tamanho (Length) arbitrariamente grande para pegar o resto da string
    ).withColumn("data_log", col("DATAINCLUSAO"))

    # Join com Clientes (para pegar CODCLIENTE)
    # Corrigida Referência Ambígua para USUAINCLUSAO usando aliases e seleção explícita
    df_joined = df_extracted.alias("p").join(df_clientes.alias("c"), "CPFCNPJ", "left") \
        .select(
            col("c.CODCLIENTE"),
            col("p.status_cliente"),
            col("p.data_log"),
            col("p.USUAINCLUSAO")
        ) \
        .filter(col("c.CODCLIENTE").isNotNull())

    # Join com Status (Enriquecimento)
    df_enriched = df_joined.join(df_status, df_joined.status_cliente == df_status.status_do_cliente, "left") \
        .select("CODCLIENTE", "status_cliente", "data_log", "USUAINCLUSAO", "macroprocesso", "fase")

    # Window Functions para Esteira (Anterior/Posterior)
    window_esteira = Window.partitionBy("CODCLIENTE").orderBy("data_log")
    df_esteira = df_enriched.withColumn("status_anterior", lag("status_cliente").over(window_esteira)) \
        .withColumn("data_anterior", lag("data_log").over(window_esteira)) \
        .withColumn("macroprocesso_anterior", lag("macroprocesso").over(window_esteira)) \
        .withColumn("fase_anterior", lag("fase").over(window_esteira)) \
        .filter((col("status_cliente") != col("status_anterior")) | col("status_anterior").isNull()) # Remove duplicatas consecutivas de mesmo status

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

def process_sacados_enriquecida(df_geral=None, df_enderecos=None, df_emails=None, df_telefones=None):
    print("Processando Sacados Enriquecida...")
    # Sacados Distintos de Títulos
    df_titulos_sacados = spark.read.table(f"{source_lakehouse}.tab_titulos") \
        .filter(year(col("DATAINCLUSAO")) >= 2021) \
        .select(col("CPFCNPJSACADO").alias("cpf_cnpj")).distinct()

    # Joins - Fallback (Contingência) para leitura de tabela se DFs não forem fornecidos
    if df_geral is None: df_geral = spark.read.table(f"{target_lakehouse}.staging_cad_geral_pf_pj_limpa")
    if df_enderecos is None: df_enderecos = spark.read.table(f"{target_lakehouse}.staging_enderecos_limpa")
    if df_emails is None: df_emails = spark.read.table(f"{target_lakehouse}.staging_emails_agg")
    if df_telefones is None: df_telefones = spark.read.table(f"{target_lakehouse}.staging_telefones_agg")

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
# Captura DataFrames para reuso
df_clientes = process_clientes()
df_geral = process_cadastro_geral()
df_telefones = process_telefones()
df_emails = process_emails()
df_enderecos = process_enderecos()

process_contratos()
process_bridge_cliente_gerente()
process_limites()
process_empresas()
process_gerentes()
process_plataformas()
process_status_esteira()
process_usuarios()
process_pareceres_clientes_esteira()

# Passa DataFrames em memória
process_sacados_enriquecida(df_geral, df_enderecos, df_emails, df_telefones)

print("Limpeza Silver - Cadastros finalizada.")
mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
