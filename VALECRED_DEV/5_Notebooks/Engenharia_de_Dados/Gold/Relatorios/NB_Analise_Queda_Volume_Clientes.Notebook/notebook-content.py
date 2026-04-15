# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1839f042-81d7-8f35-49bc-0100ee40705b",
# META       "default_lakehouse_name": "LH_Gold",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Monitoramento de Queda de Volume de Clientes (Churn Alert)
# **Objetivo:** Identificar clientes que tiveram redução significativa no volume de operações nos últimos 30 dias em comparação aos 30 dias anteriores.
# **Tabelas de Origem:** `LH_Gold.fato_operacoes`, `LH_Gold.dim_clientes`
# **Tabela de Saída:** `LH_Gold.analise_queda_volume_clientes`

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("Iniciando análise de queda de volume operado por cliente...")

# 1. Carregar Dados
# Assumimos que o ambiente já prove o SparkSession globalmente
df_ops = spark.table("LH_Gold.fato_operacoes")
df_clientes = spark.table("LH_Silver.staging_clientes_limpa") if spark._jsparkSession.catalog().tableExists("LH_Silver", "staging_clientes_limpa") else spark.table("LH_Gold.dim_clientes")

# CELL ********************

def calcular_queda_volume(df_operacoes):
    """
    Função extraível para calcular a queda de volume dos clientes.
    Calcula o volume dos últimos 30 dias versus o período de 31 a 60 dias atrás.
    """
    # 2. Definir Períodos (Referência baseada na max_date disponível ou data atual)
    # Pegamos a data máxima de operação para não nos basearmos apenas na data física de execução
    max_date_row = df_operacoes.agg(F.max("data_analise").alias("max_date")).collect()[0]
    data_ref = F.to_date(F.lit(max_date_row["max_date"])) if max_date_row["max_date"] else F.current_date()

    # 3. Marcar períodos
    df_periodos = df_operacoes.withColumn(
        "periodo",
        F.when(
            (F.to_date(F.col("data_analise")) > F.date_sub(data_ref, 30)) &
            (F.to_date(F.col("data_analise")) <= data_ref), "recentes_30d"
        ).when(
            (F.to_date(F.col("data_analise")) > F.date_sub(data_ref, 60)) &
            (F.to_date(F.col("data_analise")) <= F.date_sub(data_ref, 30)), "anteriores_30d"
        ).otherwise("fora_janela")
    ).filter(F.col("periodo") != "fora_janela")

    # 4. Calcular Volume Operado por Cliente em cada período
    # Utilizamos o valor_de_face, comum em fato_operacoes
    col_valor = "valor_de_face" if "valor_de_face" in df_operacoes.columns else "valor"

    # Agrupamento base
    df_agg = df_periodos.groupBy("cod_cliente").pivot("periodo", ["anteriores_30d", "recentes_30d"]).agg(
        F.sum(F.col(col_valor)).alias("vop"),
        F.count("cod_operacao").alias("qtd")
    ).fillna(0)

    # 5. Calcular Queda
    df_final = df_agg.withColumn(
        "vop_anteriores_30d", F.col("anteriores_30d_vop")
    ).withColumn(
        "vop_recentes_30d", F.col("recentes_30d_vop")
    ).withColumn(
        "qtd_anteriores_30d", F.col("anteriores_30d_qtd")
    ).withColumn(
        "qtd_recentes_30d", F.col("recentes_30d_qtd")
    ).withColumn(
        "queda_absoluta", F.col("vop_anteriores_30d") - F.col("vop_recentes_30d")
    ).withColumn(
        "queda_percentual",
        F.when(F.col("vop_anteriores_30d") > 0,
               (F.col("queda_absoluta") / F.col("vop_anteriores_30d")) * 100
        ).otherwise(0)
    )

    # Filtro: Apenas clientes que tiveram alguma queda
    df_queda = df_final.filter(F.col("queda_absoluta") > 0).select(
        "cod_cliente", "vop_anteriores_30d", "vop_recentes_30d",
        "qtd_anteriores_30d", "qtd_recentes_30d",
        "queda_absoluta", "queda_percentual"
    ).orderBy(F.col("queda_absoluta").desc())

    return df_queda

# CELL ********************

# Executar a transformação principal
df_resultado = calcular_queda_volume(df_ops)

# Opcional: Enriquecer com dados do cliente
df_completo = df_resultado.join(
    df_clientes.select("cod_cliente", "nome", "grupo_economico").dropDuplicates(["cod_cliente"]),
    "cod_cliente", "left"
).select(
    "cod_cliente", "nome", "grupo_economico",
    "vop_anteriores_30d", "vop_recentes_30d",
    "queda_absoluta", "queda_percentual",
    "qtd_anteriores_30d", "qtd_recentes_30d"
).orderBy(F.col("queda_absoluta").desc())

# CELL ********************

# 6. Salvar na camada Gold
print("Salvando tabela LH_Gold.analise_queda_volume_clientes...")
df_completo.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Gold.analise_queda_volume_clientes")

# 7. Dashboard Rápido (UX)
# ⚡ Bolt Optimization: Combine scalar aggregations
metrics = df_completo.select(
    F.count("*").alias("qtd_clientes_com_queda"),
    F.sum("queda_absoluta").alias("volume_total_perdido")
).collect()[0]

print("\n" + "="*50)
print(" 📊 DASHBOARD: CLIENTES COM QUEDA DE VOLUME ")
print("="*50)
print(f" Clientes identificados: {metrics['qtd_clientes_com_queda']}")
volume_str = f"R$ {metrics['volume_total_perdido']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if metrics['volume_total_perdido'] else "R$ 0,00"
print(f" Risco/Volume Reduzido Global: {volume_str}")
print("="*50 + "\n")

print("Relatório salvo e atualizado com sucesso!")
