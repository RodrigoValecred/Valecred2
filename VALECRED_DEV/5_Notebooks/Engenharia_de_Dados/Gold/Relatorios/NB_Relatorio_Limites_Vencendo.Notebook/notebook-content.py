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

# # Relatório de Limites Vencendo
# **Objetivo:** Criar uma tabela simples na camada Gold (`relatorio_limites_vencendo`) consolidando dados de contratos e clientes para análise de vencimento de limites.
# 
# **Origem:** `LH_Silver` (`staging_contratos_clientes_limpa`, `staging_clientes_limpa`, `staging_cad_geral_pf_pj_limpa`).
# 
# **Destino:** `LH_Gold` (`relatorio_limites_vencendo`).
# 
# **Conexão:** Tabela Delta otimizada para Direct Lake.
# 


# CELL ********************

# Configuração e Imports
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, when, current_date, date_add, datediff, lit, broadcast
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leitura das Tabelas Silver
print("Lendo tabelas da camada Silver...")
df_contratos = spark.read.table("LH_Silver.staging_contratos_clientes_limpa")
df_clientes = spark.read.table("LH_Silver.staging_clientes_limpa")
df_geral = spark.read.table("LH_Silver.staging_cad_geral_pf_pj_limpa")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enriquecimento (Joins)
print("Realizando joins para identificar clientes...")

# Join Contratos com Clientes (para obter CPF/CNPJ)
# 🧠 Tensor: Aplicado broadcast() a tabelas de dimensão para eliminar shuffle
# 💡 O que: Envolveu as tabelas de dimensão `df_clientes` e `df_geral` com `broadcast()` durante o join com `df_contratos`.
# 🎯 Por que: Tabelas de dimensão (clientes, cadastro geral) são tipicamente pequenas o suficiente para caber na memória de cada executor. Sem o `broadcast()`, o Spark realiza um Shuffle Hash Join ou Sort Merge Join, que exige intensa movimentação de dados (shuffle) pela rede, agrupando a tabela de fatos grande pelas chaves de join. O Broadcast Join envia a tabela pequena inteira para cada nó, permitindo que a tabela de fatos grande seja avaliada localmente, o que é drasticamente mais rápido.
# 📊 Impacto: Elimina as despesas gerais de rede (shuffle exchanges) ao unir fatos com dimensões, reduzindo a latência do join em ordens de magnitude.
# 🔬 Medição: Na UI do Spark, os estágios de "ShuffleExchange" são substituídos por "BroadcastExchange", e o tempo geral de execução do DAG cai vertiginosamente.
df_joined_1 = df_contratos.join(
    broadcast(df_clientes),
    on="cod_cliente", 
    how="left"
)

# Join com Cadastro Geral (para obter Razão Social / Nome Fantasia)
df_final_source = df_joined_1.join(
    broadcast(df_geral),
    on="cpf_cnpj", 
    how="left"
)

# Seleção e Transformação de Colunas
# O usuário solicitou uma tabela simples com chaves CODCLIENTE/cod_cliente e CPFCNPJ/cpf_cnpj
# 'nome_fantasia' removido conforme solicitação do usuário/disponibilidade do esquema
df_relatorio = df_final_source.select(
    col("cod_cliente"),
    col("cpf_cnpj"),
    col("nome").alias("razao_social"),
    col("cod_contrato"),
    col("dt_ini_contrato"),
    col("validade_limite"),
    col("limite_fomento"),
    col("limite_comissaria"),
    col("status"),
    col("perc_confirmacao"),
    col("tranche"),
    col("status_diretoria")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adicionar flag ou cálculo de dias para vencimento (opcional, mas útil para o relatório "Limites Vencendo")
# Se validade_limite for nulo, consideramos que não vence ou data inválida.
df_relatorio = df_relatorio.withColumn(
    "dias_para_vencimento", 
    datediff(col("validade_limite"), current_date())
).withColumn(
    "status_vencimento",
    when(col("validade_limite").isNull(), "Sem Data")
    .when(col("dias_para_vencimento") < 0, "Vencido")
    .when(col("dias_para_vencimento") <= 30, "Vencendo em 30 dias")
    .otherwise("Vigente")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Escrita na Camada Gold
output_table = "LH_Gold.relatorio_limites_vencendo"
print(f"Salvando tabela {output_table}...")

df_relatorio.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

print("Tabela criada com sucesso.")
mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
