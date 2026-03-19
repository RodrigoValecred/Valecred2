# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8f85c372-56ad-4f3f-acf9-3be2e9b99513",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
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

# CELL ********************

# 1. A Importação que faltava (Onde definimos quem é o "F")
from pyspark.sql import functions as F
from pyspark.sql.functions import col, datediff, current_date, coalesce, when

# 2. Carregar a tabela de Títulos
df_titulos = spark.table("LH_Silver.staging_titulos_limpa")

# 3. Filtrar e Calcular o Atraso na hora
# IMPORTANTE: Calculamos o atraso AGORA para ter o dado fresco, 
# em vez de confiar em coluna velha gravada na tabela.
df_aberto = df_titulos.filter("liquidacao IS NULL") \
                      .withColumn("vencimento_efetivo", coalesce(col("venc_prorrogado"), col("vencimento"))) \
                      .withColumn("atraso_dias", datediff(current_date(), col("vencimento_efetivo")))

# 4. Filtrar Apenas Aceitos (Sua regra de negócio)
df_aceitos = df_aberto.filter("aceito = 'S'")

# 5. Criar o PERFIL DO SACADO
df_perfil = df_aceitos.groupBy("cpf_cnpj_sacado").agg(
    F.sum("valor_devido").alias("exposicao_total_d1"), # Soma tudo que ele deve
    F.max("atraso_dias").alias("maior_atraso_atual"),  # Pega o pior atraso dele hoje
    F.count("cod_titulo").alias("qtd_titulos_aberto")  # Quantos boletos ele tem na praça
)

# 6. Salvar (Overwrite na tabela de apoio)
df_perfil.write.mode("overwrite").format("delta").saveAsTable("LH_Gold.Perfil_Risco_Sacado")

print("Perfil de Risco atualizado com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
