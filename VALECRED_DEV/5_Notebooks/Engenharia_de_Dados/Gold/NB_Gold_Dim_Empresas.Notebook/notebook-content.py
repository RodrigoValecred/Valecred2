# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ee40705b-0100-49bc-8f35-81d71839f042",
# META       "default_lakehouse_name": "LH_Gold",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "ee40705b-0100-49bc-8f35-81d71839f042"
# META         },
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

# # Notebook de Construção da Dimensão Empresas (Gold)
# **Objetivo:** Criar a tabela `LH_Gold.dim_empresas` a partir de `staging_empresas` e dados cadastrais, aplicando regras de negócio específicas para derivação de nomes.

# CELL ********************

# Configuração e Imports
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, lit, concat, udf, regexp_replace, when
from pyspark.sql.types import StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Definição da UDF para derivação de nome de empresa
# Lógica baseada em script Power Query legado

def derive_empresa_name(nome):
    """
    Derives a short company name code based on specific string patterns.
    Logic ported from legacy Power Query.
    """
    if not nome:
        return None

    try:
        # 1. Extract from " SECURITIZADORA " prefix
        # Split by " SECURITIZADORA ", take the first character of the part BEFORE it (last part before split if multiple).
        # Original logic: split1_rev[1] corresponds to split1[-2].
        split_sec = nome.split(" SECURITIZADORA ")
        char1 = ""
        if len(split_sec) > 1:
            part1 = split_sec[-2]
            if part1:
                char1 = part1[:1]

        # 2. Extract from 4th-to-last word
        # Split by space, take the 4th word from the end, then its 10th character (index 9).
        split_space = nome.split(" ")
        char2 = ""
        if len(split_space) >= 4:
            part2 = split_space[-4]
            if len(part2) > 9:
                char2 = part2[9]

        # 3. Extract from "VALECRED " suffix parts
        # Split by "VALECRED ", take the part AFTER it (index 1).
        # Then split that by ".", reverse the parts, and take the 1st char of each.
        split_valecred = nome.split("VALECRED ")
        char3 = ""
        if len(split_valecred) > 1:
            part3 = split_valecred[1]
            split_dots = part3.split(".")
            char3 = "".join(s[:1] for s in reversed(split_dots) if s)

        # 4. Extract from 5th word
        # Split by space (original order), take the 5th word (index 4), then its 5th character (index 4).
        char4 = ""
        if len(split_space) > 4:
            part4 = split_space[4]
            if len(part4) > 4:
                char4 = part4[4]

        # 5. Extract 33rd character from the end
        # Original logic: rev_nome[32:33] corresponds to nome[-33].
        char5 = ""
        if len(nome) > 32:
            char5 = nome[-33]

        # 6. Extract from 6th word (trimmed)
        # Split by space, take 6th word (index 5). Remove last 7 characters.
        char6 = ""
        if len(split_space) > 5:
            part6 = split_space[5]
            if len(part6) > 7:
                char6 = part6[:-7]

        # 7. Extract from last part after splitting by "."
        # Split by ".", take the last part, then its 5th character (index 4).
        split_dot = nome.split(".")
        char7 = ""
        if split_dot:
            part7 = split_dot[-1]
            if len(part7) > 4:
                char7 = part7[4]

        result = char1 + char2 + char3 + char4 + char5 + char6 + char7

        # Hardcoded replacement rule
        if result == "ITCREDO":
            return "TATUHY"
        return result

    except Exception:
        return None

# Registro da UDF
derive_empresa_name_udf = udf(derive_empresa_name, StringType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Iniciando construção da dim_empresas...")

# 1. Leitura dos dados
df_empresas = spark.read.table("LH_Silver.staging_empresas")

# Explicit Safety Filter (Garante apenas IDs desejados mesmo se staging tiver mais)
df_empresas = df_empresas.filter(col("cod_empresa").isin([6, 14, 24, 25]))

df_cadastros = spark.read.table("LH_Silver.staging_cad_geral_pf_pj_limpa")

# 2. Preparação e Join
# Limpar CNPJ da tabela de empresas para garantir match numérico
df_empresas_clean = df_empresas.withColumn("cnpj_clean", regexp_replace(col("cnpj"), "[^0-9]", ""))

# Join with aliases to avoid ambiguity on 'nome' column
df_joined = df_empresas_clean.alias("e").join(
    df_cadastros.alias("c"),
    col("e.cnpj_clean") == col("c.cpf_cnpj"),
    "left"
)

# 3. Transformações
df_final = df_joined \
    .withColumn("base", lit(40)) \
    .withColumn("chave_base_empresa", concat(col("base").cast("string"), lit("-"), col("e.cod_empresa").cast("string"))) \
    .withColumn("chave_base_cadastro", concat(col("base").cast("string"), lit("-"), col("e.cnpj_clean"))) \
    .withColumn("empresa_calculada", derive_empresa_name_udf(col("c.nome"))) \
    .withColumn("TIPO", when(col("chave_base_empresa") == "40-14", "SECURITIZADORA").otherwise("FIDC")) \
    .select(
        col("base"),
        col("chave_base_empresa"),
        col("chave_base_cadastro"),
        col("e.cnpj"),
        col("e.cod_empresa"),
        col("c.nome").alias("nome_original"),
        col("empresa_calculada").alias("empresa"),
        col("TIPO")
    )

# 4. Escrita
output_path = "LH_Gold.dim_empresas"
df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path)
print(f"Tabela '{output_path}' criada com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
