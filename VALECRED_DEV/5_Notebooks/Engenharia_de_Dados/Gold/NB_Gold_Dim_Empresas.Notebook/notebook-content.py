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
    if not nome:
        return None
    try:
        # 1. Split by " SECURITIZADORA "
        split1 = nome.split(" SECURITIZADORA ")
        split1_rev = split1[::-1]
        part1 = split1_rev[1] if len(split1_rev) > 1 else None
        char1 = part1[:1] if part1 else ""

        # 2. Split by " "
        split2 = nome.split(" ")
        split2_rev = split2[::-1]
        part2 = split2_rev[3] if len(split2_rev) > 3 else None
        char2 = part2[9:10] if part2 and len(part2) > 9 else ""

        # 3. Split by "VALECRED "
        split3 = nome.split("VALECRED ")
        if len(split3) > 1:
            part3 = split3[1]
            split3_1 = part3.split(".")
            split3_1_rev = split3_1[::-1]
            char3 = "".join([s[:1] for s in split3_1_rev])
        else:
            char3 = ""

        # 4. Split by " " (again)
        # Use split2 which is already split by " ", but we need original order logic
        # PQ: splitNomedaEmpresa4{4}? -> 5th item
        part4 = split2[4] if len(split2) > 4 else None
        char4 = part4[4:5] if part4 and len(part4) > 4 else ""

        # 5. Reverse Middle Reverse (char at index 32 from end)
        rev_nome = nome[::-1]
        char5 = rev_nome[32:33] if len(rev_nome) > 32 else ""

        # 6. Split by " " (again), 6th item
        part4_5 = split2[5] if len(split2) > 5 else None
        if part4_5:
            rev_part = part4_5[::-1]
            mid = rev_part[7:]
            char6 = mid[::-1]
        else:
            char6 = ""

        # 7. Split by "."
        split5 = nome.split(".")
        split5_rev = split5[::-1]
        part5 = split5_rev[0] if len(split5_rev) > 0 else None
        char7 = part5[4:5] if part5 and len(part5) > 4 else ""

        result = char1 + char2 + char3 + char4 + char5 + char6 + char7

        # Replacement Rule
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
    .select(
        col("base"),
        col("chave_base_empresa"),
        col("chave_base_cadastro"),
        col("e.cnpj"),
        col("e.cod_empresa"),
        col("c.nome").alias("nome_original"),
        col("empresa_calculada").alias("empresa")
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
