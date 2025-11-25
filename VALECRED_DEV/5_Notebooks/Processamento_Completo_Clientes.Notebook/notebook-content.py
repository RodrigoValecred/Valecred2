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

# CELL ********************

# --- PARTE 1: SILVER (A Limpeza) ---
from pyspark.sql import functions as F

# 1. Lemos o dado sujo (Bronze)
df_clientes_bronze = spark.read.format("parquet").load("LH_Bronze.cad_clientes")

# 2. Limpamos (Tirar duplicatas)
# Note que eu criei uma variável 'df_clientes_limpo'. O dado está VIVO aqui dentro.
df_clientes_limpo = df_clientes_bronze.dropDuplicates(["CODCLIENTE"])

# 3. (Opcional) Salvamos uma cópia na Silver para segurança, se quiser
df_clientes_limpo.write.format("delta").mode("overwrite").save("LH_Silver.silver_cad_clientes")

# O TRUQUE: Damos um "Apelido" para essa tabela na memória do Spark
# Agora o Spark sabe que "v_clientes" é esse dado limpo que acabamos de gerar
df_clientes_limpo.createOrReplaceTempView("v_clientes")

print("Silver de Clientes pronta e na memória!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- PARTE 1.1: SILVER (Tabelas de Apoio) ---

# Limpa Telefones
df_tel = spark.read.format("parquet").load("Files/bronze/tab_telefones")
df_tel = df_tel.dropDuplicates(["CODTELEFONE"])
df_tel.createOrReplaceTempView("v_telefones") # Apelido: v_telefones

# Limpa Endereços
df_end = spark.read.format("parquet").load("Files/bronze/tab_enderecos")
df_end = df_end.dropDuplicates(["CODENDERECO"])
df_end.createOrReplaceTempView("v_enderecos") # Apelido: v_enderecos

print("Tabelas de apoio na memória!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- PARTE 2: GOLD (O Cruzamento) ---

# Note que usamos SQL direto nas Views que criamos ali em cima!
# Não tem nenhum comando "read" ou "load" aqui. É super rápido.

df_cliente_gold = spark.sql("""
    SELECT 
        c.CODCLIENTE,
        c.CPFCNPJ,
        t.DDD,
        t.FONE,
        e.ENDERECO,
        e.CIDADE
    FROM v_clientes c
    LEFT JOIN v_telefones t ON c.CODCLIENTE = t.CODCLIENTE
    LEFT JOIN v_enderecos e ON c.CODCLIENTE = e.CODCLIENTE
""")

# Agora sim, salvamos o resultado final para o Power BI ler
df_cliente_gold.write.format("delta").mode("overwrite").save("Tables/gold_cliente_completo")

print("Gold gerada com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
