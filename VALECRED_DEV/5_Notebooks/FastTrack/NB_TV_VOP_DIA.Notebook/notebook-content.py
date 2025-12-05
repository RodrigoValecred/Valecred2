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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Notebook: 5_Notebooks/FastTrack/NB_TV_Volume_Dia.ipynb
from pyspark.sql.functions import current_date
import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Data Dinâmica (Formato do banco)
# Ajuste o formato se necessário (%Y-%m-%d ou %Y%m%d)
hoje_formatado = datetime.date.today().strftime("%Y%m%d") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Query Otimizada (Sem Joins, focada no TOTFAC)
# Trazemos as colunas de status para você poder filtrar no visual do Power BI
query_sql = f"""
(
    SELECT 
        NBORDERO,
        TOTFAC AS VOLUME_OPERADO,
        TTO, -- Tipo (CM, FC, NO)
        STATUSACEITE,
        STATUSANALISE,
        ACEITO,
        DATAANALISE
    FROM tab_operacoes
    WHERE CAST(DATAANALISE AS DATE) = '{hoje_formatado}' 
      AND TTO IN ('CM','FC','NO')
    -- Se quiser filtrar apenas os aprovados já na query, descomente abaixo:
    -- AND ACEITO = 'S' 
) as monitor_tv
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Leitura do MySQL
jdbc_url = "jdbc:mysql://56.125.147.185:3306/lbfactor"
jdbc_properties = {
    "user": "admin_bi",
    "password": "S6n$nIL7H*gdl@",
    "driver": "com.mysql.cj.jdbc.Driver"
}

df_tv = spark.read.jdbc(url=jdbc_url, table=query_sql, properties=jdbc_properties)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Salvar no Lakehouse
# Overwrite garante que a tabela sempre tenha apenas o retrato atual do dia
df_tv.write.format("delta").mode("overwrite").saveAsTable("TV_KPI_Volume_Dia")

print(f"Carga de Volume TV realizada. Data base: {hoje_formatado}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
