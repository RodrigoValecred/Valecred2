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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "abfss://VALECRED_DEV@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables/tab_titulos")
df_historico = delta_table.history() 
display(df_historico)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_versao_antiga = spark.read \
    .format("delta") \
    .option("versionAsOf", 425) \
    .load("abfss://VALECRED_DEV@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables/tab_titulos")
display(df_versao_antiga)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ontem = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2026-05-10 10:00:00") \
    .load("abfss://VALECRED_DEV@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables/tab_titulos")
display(df_ontem)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
