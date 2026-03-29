# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# Fabric notebook source

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Relatório Mensal de Fechamento de Prorrogação
# **Objetivo:** Gerar um relatório detalhado de prorrogações, identificando operações Deferidas, Indeferidas e Recuperadas (Indeferidas seguidas de Deferimento para o mesmo título).
# **Solicitação:** Puxar informações de Indeferidos e Deferidos, considerando casos de "Instruções que voltam indeferidas e fazemos boletos avulsos que gera outra instrução sendo essa deferida".

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import (
    col, sum, max, min, lit, when, coalesce, year, month, trunc, datediff, to_date, concat, broadcast, trim, regexp_extract
)
from pyspark.sql.window import Window
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Carregamento e Preparação de Dados
def load_and_prepare_data(spark):
    print("Carregando Fato Prorrogações de Títulos (Gold)...")
    df_prorrog = spark.read.table("LH_Gold.fato_prorrogacoes_de_titulos")

    print("Carregando Dimensão Clientes (Gold)...")
    df_clientes = spark.read.table("LH_Gold.dim_clientes") \
        .select("cod_cliente", "nome", "grupo_economico", "nome_gerente") \
        .dropDuplicates(["cod_cliente"])

    print("Carregando e Processando Pareceres (Silver)...")
    df_pareceres = spark.read.table("LH_Silver.staging_pareceres_operacoes")

    # Extrair Bordero Indeferido
    # Regex: Procura por 'BORDERO INDEFERIDO:' seguido de digitos
    df_pareceres_ref = df_pareceres \
        .withColumn("bordero_referencia_indeferido", regexp_extract(col("Parecer"), r"(?i)BORDERO\s+INDEFERIDO:\s*(\d+)", 1)) \
        .filter(col("bordero_referencia_indeferido") != "") \
        .groupBy("cod_operacao") \
        .agg(max("bordero_referencia_indeferido").alias("bordero_referencia_indeferido"))

    # Fazer join Prorrog com Pareceres Ref
    df_prorrog_enriched = df_prorrog.join(df_pareceres_ref, "cod_operacao", "left")

    # Normalizar datas e status
    df_prorrog_prep = df_prorrog_enriched \
        .withColumn("data_referencia", to_date(col("data_inclusao"))) \
        .withColumn("status_analise_norm", 
            when(col("status_analise") == "D", "DEFERIDO")
            .otherwise("INDEFERIDO")
        )

    return df_prorrog_prep, df_clientes

def process_fechamento_prorrogacao(df_prorrog, df_clientes):
    print("Processando lógica de Fechamento (Indeferidos vs Recuperados)...")

    # Identificar se houve deferimento eventual para o mesmo título
    # Agrupando por Título (cod_titulo)
    w_titulo = Window.partitionBy("cod_titulo")

    # Flag: 1 se status_analise == 'D' (DEFERIDO)
    df_flagged = df_prorrog.withColumn("is_deferido", 
        when(col("status_analise_norm") == "DEFERIDO", 1).otherwise(0)
    )

    # Calcular se houve algum deferimento no histórico (ou no mês, se quisermos restringir, mas geralmente o título é único)
    # Assumindo que cod_titulo é único globalmente ou por cliente.
    df_calculated = df_flagged.withColumn("foi_deferido_eventualmente", 
        max("is_deferido").over(w_titulo)
    )

    # Categorização Final
    # DEFERIDO: status_analise == 'D'
    # RECUPERADA: status_analise != 'D' mas foi_deferido_eventualmente == 1
    # INDEFERIDO: status_analise != 'D' e foi_deferido_eventualmente == 0
    df_categorized = df_calculated.withColumn("status_final_prorrogacao",
        when(col("status_analise_norm") == "DEFERIDO", "DEFERIDO")
        .when((col("status_analise_norm") != "DEFERIDO") & (col("foi_deferido_eventualmente") == 1), "RECUPERADA")
        .otherwise("INDEFERIDO")
    )

    # Adicionar Mês de Referência para Agrupamento
    df_enrich = df_categorized.withColumn("mes_referencia", trunc(col("data_referencia"), "MM"))

    # Join com Clientes
    df_final = df_enrich.join(df_clientes, "cod_cliente", "left") \
        .select(
            col("mes_referencia"),
            col("cod_cliente"),
            coalesce(col("nome"), concat(lit("CLIENTE "), col("cod_cliente"))).alias("nome_cliente"),
            col("grupo_economico"),
            col("nome_gerente"),
            col("cod_operacao"),
            col("cod_titulo"),
            col("nbordero"),
            col("bordero_referencia_indeferido"),
            col("data_referencia").alias("data_operacao"),
            col("valor"),
            col("dias_prorrogados"),
            col("status_analise").alias("status_analise_orig"),
            col("status_final_prorrogacao")
        ) \
        .orderBy("mes_referencia", "nome_cliente", "data_operacao")

    return df_final

# Execução
print("Iniciando Relatório de Fechamento de Prorrogação...")
df_prorrog_prep, df_clientes = load_and_prepare_data(spark)

# ⚡ Bolt: Cache do DataFrame principal do relatório
# 💡 O que: Adicionado `.cache()` ao DataFrame `df_relatorio`.
# 🎯 Por que: O DataFrame `df_relatorio` é usado múltiplas vezes em ações Spark subsequentes (`count().collect()` para o resumo e `saveAsTable()` para salvar na tabela Gold). Sem o cache, o Catalyst reavalia o plano lógico inteiro (incluindo leitura de tabelas, joins e window functions) para cada ação.
# 📊 Impacto: Evita full table scans redundantes e recálculos custosos, reduzindo significativamente o tempo de execução do notebook.
# 🔬 Medição: Elimina um job Spark duplicado que processava as mesmas regras de negócio.
df_relatorio = process_fechamento_prorrogacao(df_prorrog_prep, df_clientes).cache()

# Exibição (Amostra)
# df_relatorio.show(10, truncate=False)

# DASHBOARD RÁPIDO DE SAÍDA (UX)
def display_summary(df):
    try:
        counts = df.groupBy("status_final_prorrogacao").count().collect()
        total = sum(row['count'] for row in counts)

        print("\n" + "═"*52)
        print(f"{' 📊 RESUMO DE PRORROGAÇÕES ':^52}")
        print("═"*52)

        if total == 0:
            print(f"{'⚠️ NENHUMA PRORROGAÇÃO ENCONTRADA':^52}")
            print("═"*52 + "\n")
            return

        print(f"  Total de Operações Processadas: {total}")
        print("-" * 52)

        icons = {"DEFERIDO": "✅", "RECUPERADA": "🔄", "INDEFERIDO": "❌"}
        counts_sorted = sorted(counts, key=lambda x: str(x['status_final_prorrogacao']))
        for row in counts_sorted:
            status = str(row['status_final_prorrogacao'])
            qtd = row['count']
            icon = icons.get(status, "🔹")
            print(f"  {icon} {status:<15} : {qtd:>10}")
        print("═"*52 + "\n")
    except Exception as e:
        print(f"Resumo indisponível: {e}")

display_summary(df_relatorio)

# Salvar
output_table = "LH_Gold.relatorio_fechamento_prorrogacao"
df_relatorio.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)
print(f"Relatório salvo em: {output_table}")

# 🧠 OTIMIZAÇÃO BOLT: Liberar memória
df_relatorio.unpersist()
print("⚡ Bolt: Cache cleared.")

mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
