# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from notebookutils import mssparkutils

# 1. Configuração de conexão (Substitua pelos seus dados do MySQL)
# No Fabric, você pode usar o conector nativo ou ler da camada Bronze
# SECURE: Usando Azure Key Vault para recuperar senha
password = mssparkutils.credentials.getSecret("SeuKeyVault", "SenhaMySQL")

df_raw = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://seu_servidor:3306/seu_db") \
    .option("dbtable", "tab_operacoes") \
    .option("user", "seu_usuario") \
    .option("password", password) \
    .load()

# 2. Filtro de Janela (Últimas 24h para manter o dash leve)
df_filtrado = df_raw.filter(
    (F.col("TTO").isin('CM', 'FC', 'NO', 'GR', 'NC')) & 
    (F.col("DATAINCLUSAO") >= F.date_sub(F.current_timestamp(), 1)) &
    (~F.col("STATUSANALISE").isin('D', 'I'))
)

# 3. Regra de Negócio: Ajuste das 09h e Cálculo do Tempo (SLA)
df_final = df_filtrado.withColumn(
    "DataInicioAjustada",
    F.when(
        F.hour("DATAINCLUSAO") < 9,
        F.to_timestamp(F.concat(F.date_format("DATAINCLUSAO", "yyyy-MM-dd"), F.lit(" 09:00:00")))
    ).otherwise(F.col("DATAINCLUSAO"))
).withColumn(
    "TEMPO_MINUTOS",
    (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("DataInicioAjustada")) / 60
)

# 4. Escrita Otimizada para Direct Lake (V-Order)
# O modo 'overwrite' garante que o Power BI veja sempre o dado mais atual
df_final.write.format("delta").mode("overwrite").saveAsTable("fato_operacoes_analise")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
