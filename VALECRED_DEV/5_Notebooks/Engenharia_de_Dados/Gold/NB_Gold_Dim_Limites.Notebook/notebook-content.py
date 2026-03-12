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

# # Notebook de Dimensão Limites (Camada Gold)
# **Objetivo:** Criar a tabela fato_limites_credito consolidada para a camada Gold.

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, lit, coalesce, sum, max, regexp_replace, concat, upper, trim, greatest
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType, DateType, BooleanType

class TableNames:
    SILVER_STAGING_CONTRATOS_CLIENTES_LIMPA = "LH_Silver.staging_contratos_clientes_limpa"
    SILVER_STG_LIMITES_CONTRATOS_SILVER = "LH_Silver.stg_limites_contratos_silver"
    SILVER_SUP_LIMITES_EXTRA_PLUS = "LH_Silver.sup_limites_extra_plus"
    SILVER_STAGING_CLIENTES_LIMPA = "LH_Silver.staging_clientes_limpa"
    SILVER_STAGING_CAD_GERAL_PF_PJ_LIMPA = "LH_Silver.staging_cad_geral_pf_pj_limpa"
    SILVER_SUP_GRUPOS_ECONOMICOS = "LH_Silver.sup_grupos_economicos"
    GOLD_FATO_LIMITES_CREDITO = "LH_Gold.fato_limites_credito"

def safe_read_table(spark, table_name, schema=None, fallback_df=None):
    """
    Tenta ler uma tabela. Se falhar, retorna um DataFrame vazio com o schema fornecido
    ou um DataFrame de fallback.
    """
    try:
        return spark.read.table(table_name)
    except Exception as e:
        print(f"AVISO: Tabela {table_name} não encontrada ({e}).")
        if fallback_df is not None:
             print("Usando dataframe de fallback.")
             return fallback_df
        elif schema is not None:
             print("Criando dataframe vazio com schema fornecido.")
             return spark.createDataFrame([], schema=schema)
        else:
             raise e

print("Carregando Dados (Silver)...")
df_contratos = spark.read.table(TableNames.SILVER_STAGING_CONTRATOS_CLIENTES_LIMPA)

df_limites_obs_silver = safe_read_table(spark, TableNames.SILVER_STG_LIMITES_CONTRATOS_SILVER, schema=StructType([
    StructField("codcliente", LongType(), True),
    StructField("limite_geral", DoubleType(), True),
    StructField("limite_intercompany", DoubleType(), True),
    StructField("limite_extra_desconto_formal", DoubleType(), True),
    StructField("limite_extra_desconto_informal", DoubleType(), True),
    StructField("OBS_TRATADA", StringType(), True)
]))

df_limites_extra_plus = safe_read_table(spark, TableNames.SILVER_SUP_LIMITES_EXTRA_PLUS, schema=StructType([
    StructField("nome", StringType(), True),
    StructField("cnpj", StringType(), True),
    StructField("limite", DoubleType(), True),
    StructField("limite_extra", DoubleType(), True),
    StructField("limite_plus", DoubleType(), True)
]))

df_clientes_staging = spark.read.table(TableNames.SILVER_STAGING_CLIENTES_LIMPA)
df_geral_pf_pj_limpa = spark.read.table(TableNames.SILVER_STAGING_CAD_GERAL_PF_PJ_LIMPA)
df_grupos_economicos = spark.read.table(TableNames.SILVER_SUP_GRUPOS_ECONOMICOS)

print("Preparo das Bases Auxiliares...")
# Grupos Economicos Prep
df_grupos_prep = df_grupos_economicos.withColumnRenamed("nomegrupo", "grupo_economico")
if "cod_cliente" not in df_grupos_prep.columns and "codcliente" in df_grupos_prep.columns:
     df_grupos_prep = df_grupos_prep.withColumnRenamed("codcliente", "cod_cliente")
df_grupos_prep = df_grupos_prep.select("cod_cliente", "grupo_economico")


# 6.4.2 Limites Extra e Plus (Desduplicação por Grupo)
df_limites_ep_prep = df_limites_extra_plus.withColumn("cnpj_clean", regexp_replace(col("cnpj"), "[^0-9]", ""))

df_limites_ep_clientes = df_limites_ep_prep.join(
    df_clientes_staging.select(col("cpf_cnpj").alias("cnpj_clean"), "cod_cliente"),
    "cnpj_clean",
    "inner"
)

df_limites_ep_grupos = df_limites_ep_clientes.join(
    df_grupos_prep,
    "cod_cliente",
    "inner"
)

df_limites_grupo_dedup = df_limites_ep_grupos.groupBy("grupo_economico").agg(
    max("limite").alias("limite_grupo_manual"),
    max("limite_extra").alias("limite_extra_grupo"),
    max("limite_plus").alias("limite_plus_grupo")
)


# 6.4.3: Construção da Fato Limites de Crédito (Consolidada)
print("\nConstruindo Fato Limites de Crédito (Consolidada)...")

if "CODCLIENTE" in df_limites_obs_silver.columns:
    df_limites_obs_silver = df_limites_obs_silver.withColumnRenamed("CODCLIENTE", "cod_cliente")

df_limites_obs_select = df_limites_obs_silver.select(
    col("cod_cliente"),
    coalesce(col("limite_geral"), lit(0)).alias("limite_geral_obs"),
    coalesce(col("limite_intercompany"), lit(0)).alias("limite_intercompany_obs"),
    coalesce(col("limite_extra_desconto_formal"), lit(0)).alias("limite_extra_desconto_formal_obs"),
    coalesce(col("limite_extra_desconto_informal"), lit(0)).alias("limite_extra_desconto_informal_obs"),
    col("OBS_TRATADA").alias("observacoes_contrato_obs")
)

df_limites_base = df_contratos.filter(col("status") == "A").join(df_limites_obs_select, "cod_cliente", "left") \
    .select(
        col("cod_cliente"),
        coalesce(col("limite_fomento"), lit(0)).alias("limite_fomento"),
        coalesce(col("limite_comissaria"), lit(0)).alias("limite_comissaria"),
        col("validade_limite"),
        coalesce(col("limite_geral_obs"), lit(0)).alias("limite_geral"),
        coalesce(col("limite_intercompany_obs"), lit(0)).alias("limite_intercompany"),
        coalesce(col("limite_extra_desconto_formal_obs"), lit(0)).alias("limite_extra_desconto_formal"),
        coalesce(col("limite_extra_desconto_informal_obs"), lit(0)).alias("limite_extra_desconto_informal"),
        col("observacoes_contrato_obs").alias("observacoes_contrato")
    )

df_limites_base_grp = df_limites_base.join(df_grupos_prep, "cod_cliente", "left")

df_com_grupo = df_limites_base_grp.filter(col("grupo_economico").isNotNull())
df_sem_grupo = df_limites_base_grp.filter(col("grupo_economico").isNull())

df_grupo_contract_agg = df_com_grupo.groupBy("grupo_economico").agg(
    max("limite_fomento").alias("limite_fomento_auto"),
    max("limite_comissaria").alias("limite_comissaria_auto"),
    max("validade_limite").alias("validade_limite_auto"),
    max("limite_geral").alias("limite_geral_auto"),
    max("limite_intercompany").alias("limite_intercompany_auto"),
    max("limite_extra_desconto_formal").alias("limite_extra_desconto_formal_auto"),
    max("limite_extra_desconto_informal").alias("limite_extra_desconto_informal_auto"),
    max("observacoes_contrato").alias("observacoes_contrato_auto")
)

df_grupo_final = df_grupo_contract_agg.join(df_limites_grupo_dedup, "grupo_economico", "full_outer") \
    .select(
        coalesce(col("grupo_economico"), col("grupo_economico")).alias("nome_entidade"),
        lit("GRUPO").alias("tipo_entidade"),
        concat(lit("G-"), upper(trim(coalesce(col("grupo_economico"), col("grupo_economico"))))).alias("id_limite_credito"),
        greatest(coalesce(col("limite_fomento_auto"), lit(0)), coalesce(col("limite_grupo_manual"), lit(0))).alias("limite_fomento"),
        coalesce(col("limite_comissaria_auto"), lit(0)).alias("limite_comissaria"),
        coalesce(col("limite_extra_grupo"), lit(0)).alias("limite_extra"),
        coalesce(col("limite_plus_grupo"), lit(0)).alias("limite_plus"),
        col("validade_limite_auto").alias("validade_limite"),
        coalesce(col("limite_geral_auto"), lit(0)).alias("limite_geral"),
        coalesce(col("limite_intercompany_auto"), lit(0)).alias("limite_intercompany"),
        coalesce(col("limite_extra_desconto_formal_auto"), lit(0)).alias("limite_extra_desconto_formal"),
        coalesce(col("limite_extra_desconto_informal_auto"), lit(0)).alias("limite_extra_desconto_informal"),
        col("observacoes_contrato_auto").alias("observacoes_contrato")
    ).filter(col("nome_entidade").isNotNull())

df_nomes_clientes = df_clientes_staging.join(df_geral_pf_pj_limpa, "cpf_cnpj", "left").select("cod_cliente", "nome")

df_sem_grupo_named = df_sem_grupo.join(df_nomes_clientes, "cod_cliente", "left")

df_cliente_final = df_sem_grupo_named.select(
    coalesce(col("nome"), concat(lit("CLIENTE "), col("cod_cliente"))).alias("nome_entidade"),
    lit("CLIENTE").alias("tipo_entidade"),
    concat(lit("C-"), col("cod_cliente")).alias("id_limite_credito"),
    col("limite_fomento"),
    col("limite_comissaria"),
    lit(0.0).alias("limite_extra"),
    lit(0.0).alias("limite_plus"),
    col("validade_limite"),
    col("limite_geral"),
    col("limite_intercompany"),
    col("limite_extra_desconto_formal"),
    col("limite_extra_desconto_informal"),
    col("observacoes_contrato")
)

df_fato_limites = df_grupo_final.unionByName(df_cliente_final, allowMissingColumns=True) \
    .withColumn("limite_total_calculado",
        coalesce(col("limite_fomento"), lit(0)) +
        coalesce(col("limite_extra"), lit(0)) +
        coalesce(col("limite_plus"), lit(0))
    )

output_path_fato_limites = TableNames.GOLD_FATO_LIMITES_CREDITO
df_fato_limites.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_fato_limites)
print(f"Tabela 'fato_limites_credito' criada em: {output_path_fato_limites}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }