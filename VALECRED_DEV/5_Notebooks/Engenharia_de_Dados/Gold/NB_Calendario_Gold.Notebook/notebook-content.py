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

# # Notebook de Geração da Dimensão Calendário (Gold)
# **Objetivo:** Substituir o Dataflow `DF_Calendario_Gold` por um processo PySpark mais robusto e performático.
# **Melhorias:**
# *   Inclusão de feriados bancários a partir da tabela `tab_feriados` (Bronze).
# *   Cálculo do `proximo_dia_util` para fins de vencimento de boletos.
# *   Geração de calendário estendido (até 2030).
# *   Persistência em formato Delta na camada Gold (`LH_Gold`).

# CELL ********************

# Configurações iniciais
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

from pyspark.sql.functions import (
    col, lit, date_format, dayofweek, expr, when,
    date_add, to_date, year, month, dayofmonth, quarter,
    first, row_number, concat, min, max, sequence, explode
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Leitura e Preparação dos Feriados
# Lendo a tabela de feriados da camada Bronze e preparando para o join.
# Consideramos feriados bancários aqueles marcados como 'N' (Nacional) ou que impactam operações.
# Assumimos que a tabela `tab_feriados` possui a coluna `DATAFERIADO` e `TFERIADO`.

# CELL ********************

print("Lendo tabela de feriados...")
try:
    df_feriados_raw = spark.read.table("LH_Bronze.tab_feriados")
    
    # Padronização e remoção de duplicatas por data
    # Se houver múltiplos registros para o mesmo dia, priorizamos 'N' (Nacional)
    df_feriados = df_feriados_raw \
        .select(
            to_date(col("DATAFERIADO")).alias("data_feriado"),
            col("TFERIADO").alias("tipo_feriado_codigo"),
            col("DESCRICAO").alias("descricao_feriado")
        ) \
        .withColumn("prioridade", 
                    when(col("tipo_feriado_codigo") == "N", 1)
                    .when(col("tipo_feriado_codigo") == "E", 2)
                    .when(col("tipo_feriado_codigo") == "M", 3)
                    .otherwise(4))
    
    # Deduplicando mantendo a maior prioridade (menor número)
    w_dedup = Window.partitionBy("data_feriado").orderBy("prioridade")
    df_feriados_unicos = df_feriados.withColumn("rn", row_number().over(w_dedup)) \
        .filter(col("rn") == 1) \
        .drop("rn", "prioridade") \
        .withColumn("eh_feriado_bancario", 
                    when(col("tipo_feriado_codigo") == "N", lit(True)) # Assumindo 'N' como feriado bancário nacional
                    .otherwise(lit(False))) # Ajustar regra se Estaduais/Municipais contarem
                    
    # NOTA: Em muitas regras de negócio bancárias nacionais, apenas feriados Nacionais ('N') param o processamento bancário geral (TED/DOC/Boleto).
    # Se feriados locais ('L' ou 'M') também pararem, alterar a lógica acima.
    
    print("Feriados lidos e deduplicados.")

except Exception as e:
    print(f"Erro ao ler feriados ou tabela inexistente: {e}")
    # Fallback para criar dataframe vazio se falhar, para não quebrar o notebook (mas idealmente deve existir)
    from pyspark.sql.types import StructType, StructField, DateType, StringType, BooleanType
    schema = StructType([
        StructField("data_feriado", DateType(), True),
        StructField("tipo_feriado_codigo", StringType(), True),
        StructField("descricao_feriado", StringType(), True),
        StructField("eh_feriado_bancario", BooleanType(), True)
    ])
    df_feriados_unicos = spark.createDataFrame([], schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Geração da Sequência de Datas
# Gerando datas de 2017-01-01 até 2030-12-31.

# CELL ********************

try:
    print("Calculando datas dinâmicas...")

    # Min Data Inclusão de Operações
    df_ops = spark.read.table("LH_Silver.staging_operacoes_limpa")
    min_date_row = df_ops.agg(min("data_inclusao")).collect()

    # Max Data Vencimento (Efetivo) de Títulos
    df_titles = spark.read.table("LH_Silver.staging_titulos_limpa")
    max_date_row = df_titles.agg(max("vencimento_efetivo")).collect()

    start_date = "2017-01-01"
    end_date = "2030-12-31"

    if min_date_row and min_date_row[0][0]:
        start_date = str(min_date_row[0][0]).split(' ')[0] # Garantir aaaa-MM-dd se para carimbo de data/hora

    if max_date_row and max_date_row[0][0]:
        end_date = str(max_date_row[0][0]).split(' ')[0]

    print(f"Intervalo dinâmico definido: {start_date} até {end_date}")

except Exception as e:
    print(f"Erro ao calcular datas dinâmicas: {e}. Usando fallback.")
    start_date = "2017-01-01"
    end_date = "2030-12-31"

print(f"Gerando calendário de {start_date} a {end_date}...")

# Gerar sequência de números (dias)
df_dates = spark.range(1).select(
    explode(
        sequence(
            to_date(lit(start_date)),
            to_date(lit(end_date))
        )
    ).alias("data")
)

# Adicionar colunas básicas de calendário
# Spark dayofweek: 1=Sunday, 2=Monday, ..., 7=Saturday
df_calendario_base = df_dates.withColumn("ano", year("data")) \
    .withColumn("mes", month("data")) \
    .withColumn("dia", dayofmonth("data")) \
    .withColumn("trimestre", quarter("data")) \
    .withColumn("nome_mes", date_format("data", "MMMM")) \
    .withColumn("mes_ano_abrev", date_format("data", "MMM/yy")) \
    .withColumn("dia_semana", date_format("data", "EEEE")) \
    .withColumn("numero_dia_semana_spark", dayofweek("data")) \
    .withColumn("sk_data", date_format("data", "yyyyMMdd").cast(IntegerType()))

# Ajuste do numero_dia_semana para corresponder à lógica do Dataflow anterior (1=Monday... mas o DF original usava Date.DayOfWeek que varia dependendo da cultura, mas geralmente 0 ou 1 based)
# O Dataflow original: Date.DayOfWeek([data], Day.Monday) + 1. Isso resulta em 1=Monday, 7=Sunday.
# Spark dayofweek: 1=Sunday, 2=Monday... 7=Saturday.
# Conversão:
# Faísca 2 (seg) -> 1
# Faísca 3 (terça-feira) -> 2
# ...
# Spark 7 (Sat) -> 6
# Spark 1 (Sun) -> 7

df_calendario_base = df_calendario_base.withColumn("numero_dia_semana", 
    when(col("numero_dia_semana_spark") == 1, 7)
    .otherwise(col("numero_dia_semana_spark") - 1)
).drop("numero_dia_semana_spark")

# Fim de semana: Sábado (6) e Domingo (7)
df_calendario_base = df_calendario_base.withColumn("fim_de_semana",
    when(col("numero_dia_semana").isin(6, 7), "Sim").otherwise("Não")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Integração com Feriados e Cálculo de Dias Úteis
# Join com a tabela de feriados e cálculo do próximo dia útil.

# CELL ********************

# Join com feriados
df_calendario_completo = df_calendario_base.join(
    df_feriados_unicos,
    df_calendario_base.data == df_feriados_unicos.data_feriado,
    "left"
).drop("data_feriado")

# Preencher nulos de feriado
df_calendario_completo = df_calendario_completo.fillna({
    "eh_feriado_bancario": False,
    "tipo_feriado_codigo": "N/A"
})

# Definir Flag de Dia Útil (Bancário)
# Não é fim de semana (6,7) E Não é feriado bancário
df_calendario_completo = df_calendario_completo.withColumn(
    "eh_dia_util",
    (col("numero_dia_semana") < 6) & (col("eh_feriado_bancario") == False)
)

# Cálculo do Próximo Dia Útil
# Lógica: Se hoje é dia útil, retorna hoje. Se não, retorna o próximo dia que for útil.
# Usamos Window function olhando para frente (rowsBetween(0, unboundedFollowing)) e pegando o primeiro valor não nulo.
# Criamos uma coluna auxiliar que tem a DATA se for dia útil, e NULL se não for.

w_forward = Window.orderBy("data").rowsBetween(0, Window.unboundedFollowing)

df_calculo_dia_util = df_calendario_completo.withColumn(
    "data_se_util",
    when(col("eh_dia_util") == True, col("data")).otherwise(None)
)

df_final = df_calculo_dia_util.withColumn(
    "proximo_dia_util",
    first("data_se_util", ignorenulls=True).over(w_forward)
).drop("data_se_util")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Salvando na Camada Gold
# Salvando a tabela `dim_calendario` no Lakehouse Gold.


# CELL ********************

target_table = "LH_Gold.dim_calendario"

print(f"Salvando tabela em {target_table}...")

df_final.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable(target_table)

print("Tabela salva com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Verificação
# Exibindo amostra para validar finais de semana e feriados.

# CELL ********************

print("Verificação de dias úteis (Amostra incluindo fins de semana e feriados):")
df_final.filter(
    (col("fim_de_semana") == "Sim") | (col("eh_feriado_bancario") == True)
).select(
    "data", "dia_semana", "fim_de_semana", "eh_feriado_bancario", "descricao_feriado", "proximo_dia_util"
).orderBy("data").limit(20).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
