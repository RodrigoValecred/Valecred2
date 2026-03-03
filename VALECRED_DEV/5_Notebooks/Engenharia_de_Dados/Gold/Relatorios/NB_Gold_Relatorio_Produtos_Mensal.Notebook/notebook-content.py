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

# MARKDOWN ********************

# # Relatório Mensal de Produtos por Cliente
# **Objetivo:** Analisar a performance mensal de cada cliente segmentada por produto (Operações, Prorrogações, Mora).
# **Métricas:** Volume, Prazo Médio, Taxa Média, Receita.
# **Granularidade:** Detalhado por Operação/Bordero.

# CELL ********************

# Configuração e Imports
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import (
    col, sum, avg, count, max, min, lit, when, round, coalesce, year, month, trunc, datediff, to_date, concat, broadcast, trim
)
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Helper Function para Resolver Ambiguidade Dinamicamente
def resolve_columns(df, target_cols):
    """
    Para cada coluna alvo, verifica se existe no DF.
    Se existir, faz coalesce com a versão '_op' (priorizando a do evento).
    Se não existir, renomeia a '_op' para o nome alvo.
    """
    df_resolved = df
    for col_name in target_cols:
        col_op = f"{col_name}_op"
        if col_name in df.columns:
            # Se existe (ex: nbordero na fato prorrogacao), usamos coalesce para garantir preenchimento caso nulo,
            # mas priorizando o valor do evento.

            # Tratar string vazia como nulo para evitar buracos no relatório quando a tabela fato tem a coluna mas ela está vazia
            col_target = when(trim(col(col_name)) == "", None).otherwise(col(col_name))

            df_resolved = df_resolved.withColumn(col_name, coalesce(col_target, col(col_op)))
        elif col_op in df.columns:
            # Se não existe na fato, pegamos do map (op)
            df_resolved = df_resolved.withColumnRenamed(col_op, col_name)
    return df_resolved

# 1. Carregamento e Preparação de Dados
def load_and_prepare_data(spark):
    print("Carregando tabelas Fato e Dimensão...")

    # Dimensão Clientes
    # FIX: Incluindo nome_gerente para reportar gestor
    df_clientes = spark.read.table("LH_Gold.dim_clientes") \
        .select("cod_cliente", "nome", "grupo_economico", "nome_gerente") \
        .dropDuplicates(["cod_cliente"])

    # Fato Operações (Raw para Mapping Completo)
    df_ops_raw = spark.read.table("LH_Gold.fato_operacoes")

    # Fato Operações Filtrada (Para Stream de Operações - Apenas Aceitas/Deferidas)
    df_ops_full = df_ops_raw \
        .filter(col("status_aceite") == "A") \
        .filter(col("status_analise") == "D")

    # Para Stream de Operações (Novas), filtramos apenas 2025+
    df_ops = df_ops_full.filter(year(col("data_deferimento")) >= 2025)

    # Fato Títulos (para prazo das operações e mora em aberto se necessário, mas mora aqui será via Baixas)
    df_titulos = spark.read.table("LH_Gold.fato_titulos") \
        .filter(col("aceito") == "S")

    # Fato Prorrogações de Títulos (Preferencialmente Gold)
    df_prorrog = spark.read.table("LH_Gold.fato_prorrogacoes_de_titulos")

    # Fato Baixas (para Mora Realizada)
    df_baixas = spark.read.table("LH_Gold.fato_baixas")

    # Dados para Fallback de Plataforma (Quando a operação não tem informação)
    print("Carregando tabelas para fallback de plataforma (Silver)...")
    df_bridge = spark.read.table("LH_Silver.bridge_cliente_gerente").filter(col("data_fim_vigencia") == "9999-12-31")
    df_gerentes = spark.read.table("LH_Silver.staging_gerentes")
    df_plataformas = spark.read.table("LH_Silver.staging_plataformas")

    # Criar mapa Cliente -> Plataforma Atual
    # Join: Bridge -> Gerente -> Plataforma
    df_cli_plat_map = df_bridge.join(df_gerentes.alias("g"), df_bridge.cod_gerente == col("g.cod_broker"), "left") \
        .join(df_plataformas.alias("p"), col("g.cod_agencia") == col("p.cod_agencia"), "left") \
        .select(df_bridge.cod_cliente, col("p.nome_plataforma").alias("nome_plataforma_cli")) \
        .filter(col("nome_plataforma_cli").isNotNull()) \
        .dropDuplicates(["cod_cliente"])

    # Tabela de Mapeamento para Enriquecer Prorrogações e Mora
    # FIX: Usamos df_ops_raw (sem filtro de status/ano) para garantir que Prorrogacoes/Mora de ops antigas ou com status diversos sejam mapeadas.
    df_map_ops = df_ops_raw.select(
        col("cod_operacao"),
        col("nbordero").alias("nbordero_op"),
        col("nome_plataforma").alias("nome_plataforma_op"),
        col("chave_produto").alias("chave_produto_op"),
        col("data_deferimento").alias("data_deferimento_op"),
        col("cod_cliente").alias("cod_cliente_op")
    ).dropDuplicates(["cod_operacao"])

    granular_cols = ["nbordero", "nome_plataforma", "chave_produto", "data_deferimento", "cod_cliente"]

    print("Dados carregados.")

    return {
        "df_clientes": df_clientes,
        "df_ops": df_ops,
        "df_titulos": df_titulos,
        "df_prorrog": df_prorrog,
        "df_baixas": df_baixas,
        "df_map_ops": df_map_ops,
        "df_cli_plat_map": df_cli_plat_map,
        "granular_cols": granular_cols
    }

# -------------------------------------------------------------------------
# STREAM 1: OPERAÇÕES (Novas Operações no Mês)
# -------------------------------------------------------------------------
def process_operacoes_stream(df_ops, df_titulos):
    print("Processando Operações...")

    # Preparar Títulos para cálculo de Prazo Ponderado da Operação
    # Agregamos por operação primeiro

    # Enriquecer Títulos com Data de Deferimento da Operação (para cálculo do Prazo Original)
    df_titulos_dates = df_titulos.join(df_ops.select("cod_operacao", "data_aceite"), "cod_operacao", "inner") \
        .withColumn("prazo_original_dias", datediff(col("vencimento"), col("data_aceite"))) \
        .withColumn("valor_vezes_prazo_original", col("valor") * col("prazo_original_dias"))

    df_titulos_agg_op = df_titulos_dates.groupBy("cod_operacao").agg(
        sum(col("valor") * col("prazo")).alias("soma_valor_prazo_op"),
        sum("valor").alias("soma_valor_titulos_op"),
        sum("valor_vezes_prazo_original").alias("soma_valor_prazo_original_op")
    )

    # Join Ops com Títulos Agg
    df_ops_enrich = df_ops.join(df_titulos_agg_op, "cod_operacao", "left")

    # Calcular Receita Total da Operação (Desagio + Tarifas)
    df_ops_enrich = df_ops_enrich.withColumn("receita_total_op",
        coalesce(col("desagio"), lit(0)) + coalesce(col("total_de_tarifas"), lit(0))
    )

    # Agregar por Mês e Cliente e DETALHES
    df_stream_ops = df_ops_enrich \
        .withColumn("mes_ref", trunc(col("data_deferimento"), "MM")) \
        .groupBy("mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento", "floating") \
        .agg(
            sum("valor_de_face").alias("volume"),
            sum("soma_valor_prazo_op").alias("total_valor_prazo_mes"),
            sum("soma_valor_prazo_original_op").alias("total_valor_prazo_original_mes"),
            sum("receita_total_op").alias("receita"),
            count("cod_operacao").alias("qtd_eventos")
        ) \
        .withColumn("tipo_produto", lit("OPERACOES")) \
        .withColumn("prazo_medio",
                    when(col("volume") > 0, col("total_valor_prazo_mes") / col("volume")).otherwise(0)) \
        .withColumn("prazo_medio_original",
                    when(col("volume") > 0, col("total_valor_prazo_original_mes") / col("volume")).otherwise(0)) \
        .withColumn("taxa_media",
                    when(col("total_valor_prazo_mes") > 0,
                        (col("receita") / (col("total_valor_prazo_mes") / 30)) * 100
                    ).otherwise(0)) \
        .withColumnRenamed("chave_produto", "sub_tipo_produto") \
        .drop("total_valor_prazo_mes", "total_valor_prazo_original_mes")

    return df_stream_ops

# -------------------------------------------------------------------------
# STREAM 2: PRORROGAÇÕES (Eventos de Prorrogação no Mês)
# -------------------------------------------------------------------------
def process_prorrogacoes_stream(df_prorrog, df_map_ops, df_cli_plat_map, granular_cols):
    print("Processando Prorrogações...")

    # Filtrar ano relevante (2025+)
    df_prorrog_filtered = df_prorrog.filter(year(col("data_inclusao")) >= 2025)

    # Join com Mapeamento de Operações para obter detalhes (Granularidade)
    # Fato Prorrogacoes tem cod_operacao
    df_prorrog_joined = df_prorrog_filtered.join(df_map_ops, "cod_operacao", "left")

    # Resolver Ambiguidade de Colunas (nbordero, plataforma, etc.)
    df_prorrog_enrich = resolve_columns(df_prorrog_joined, granular_cols)

    # FIX: Fallback de Atributos Faltantes (Data, Plataforma)
    # Estratégia Plataforma: 1. Operação Original, 2. Plataforma Atual do Cliente, 3. "N/D"
    df_prorrog_enrich = df_prorrog_enrich \
        .join(df_cli_plat_map, "cod_cliente", "left") \
        .withColumn("data_deferimento", coalesce(col("data_deferimento"), to_date(col("data_inclusao")))) \
        .withColumn("nome_plataforma", coalesce(col("nome_plataforma"), col("nome_plataforma_cli"), lit("N/D"))) \
        .drop("nome_plataforma_cli")

    # Calcular Peso do Prazo (Valor * Dias Prorrogados)
    df_prorrog_calc = df_prorrog_enrich.withColumn("valor_vezes_dias", col("valor") * col("dias_prorrogados"))

    df_stream_prorrog = df_prorrog_calc \
        .withColumn("mes_ref", trunc(col("data_inclusao"), "MM")) \
        .groupBy("mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento") \
        .agg(
            sum("valor").alias("volume"),
            sum("valor_vezes_dias").alias("total_valor_dias_mes"),
            sum("juros").alias("receita"),
            count("cod_titulo").alias("qtd_eventos")
        ) \
        .withColumn("tipo_produto", lit("PRORROGACOES")) \
        .withColumn("prazo_medio",
                    when(col("volume") > 0, col("total_valor_dias_mes") / col("volume")).otherwise(0)) \
        .withColumn("prazo_medio_original", lit(None).cast("double")) \
        .withColumn("floating", lit(None).cast("double")) \
        .withColumn("taxa_media",
                    when(col("total_valor_dias_mes") > 0,
                        (col("receita") / (col("total_valor_dias_mes") / 30)) * 100
                    ).otherwise(0)) \
        .withColumn("sub_tipo_produto", lit("PR")) \
        .drop("chave_produto", "total_valor_dias_mes")

    return df_stream_prorrog

# -------------------------------------------------------------------------
# STREAM 3: MORA (Juros Pagos no Mês)
# -------------------------------------------------------------------------
def process_mora_stream(df_baixas, df_map_ops, df_cli_plat_map, df_titulos, granular_cols):
    print("Processando Mora...")

    # Filtrar Baixas com Juros > 0 e Ano >= 2025
    df_mora_filtered = df_baixas \
        .filter(year(col("data_baixa")) >= 2025) \
        .filter(col("juros") > 0)

    # Join com Mapeamento de Operações para obter detalhes
    df_mora_joined = df_mora_filtered.join(df_map_ops, "cod_operacao", "left")

    # Resolver Ambiguidade e Colunas Faltantes (cod_cliente pode vir do map)
    df_mora_enrich = resolve_columns(df_mora_joined, granular_cols)

    # AJUSTE SOLICITADO: Para Mora, data_deferimento deve ser a data do pagamento (data_baixa)
    # Aplicar também fallback de plataforma
    df_mora_enrich = df_mora_enrich \
        .join(df_cli_plat_map, "cod_cliente", "left") \
        .withColumn("data_deferimento", col("data_baixa")) \
        .withColumn("nome_plataforma", coalesce(col("nome_plataforma"), col("nome_plataforma_cli"), lit("N/D"))) \
        .drop("nome_plataforma_cli")

    # Calcular Atraso (Data Baixa - Data Vencimento)
    # Baixas tem data_baixa e data_vencimento
    # FIX: Verificar datas nulas ou inválidas (ex: ano 0001) para evitar prazos gigantes
    # FIX: Usar Vencimento Prorrogado se disponível (via join com titulos)
    df_titulos_dates = df_titulos.select(col("cod_titulo"), col("venc_prorrogado"))
    df_mora_enrich_venc = df_mora_enrich.join(df_titulos_dates, "cod_titulo", "left")

    df_mora_calc = df_mora_enrich_venc \
        .withColumn("data_referencia_mora", coalesce(col("venc_prorrogado"), col("data_vencimento"))) \
        .withColumn("dias_atraso",
                    when(col("data_baixa").isNull() | col("data_referencia_mora").isNull(), 0)
                    .when(year(col("data_baixa")) < 1900, 0)
                    .when(year(col("data_referencia_mora")) < 1900, 0)
                    .otherwise(datediff(col("data_baixa"), col("data_referencia_mora")))
        ) \
        .withColumn("valor_vezes_atraso", col("valor_pago") * col("dias_atraso"))

    df_stream_mora = df_mora_calc \
        .withColumn("mes_ref", trunc(col("data_baixa"), "MM")) \
        .groupBy("mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento") \
        .agg(
            sum("valor_pago").alias("volume"),
            sum("valor_vezes_atraso").alias("total_valor_atraso_mes"),
            sum("juros").alias("receita"),
            count("cod_titulo").alias("qtd_eventos")
        ) \
        .withColumn("tipo_produto", lit("MORA")) \
        .withColumn("prazo_medio",
                    when(col("volume") > 0, col("total_valor_atraso_mes") / col("volume")).otherwise(0)) \
        .withColumn("prazo_medio_original", lit(None).cast("double")) \
        .withColumn("floating", lit(None).cast("double")) \
        .withColumn("taxa_media",
                    when(col("total_valor_atraso_mes") > 0,
                        (col("receita") / (col("total_valor_atraso_mes") / 30)) * 100
                    ).otherwise(0)) \
        .withColumnRenamed("chave_produto", "sub_tipo_produto") \
        .drop("total_valor_atraso_mes")

    return df_stream_mora

# Main Execution Flow
print("Iniciando Relatório de Produtos Mensal...")
data = load_and_prepare_data(spark)

# Unpack
df_ops = data["df_ops"]
df_titulos = data["df_titulos"]
df_prorrog = data["df_prorrog"]
df_baixas = data["df_baixas"]
df_map_ops = data["df_map_ops"]
df_cli_plat_map = data["df_cli_plat_map"]
granular_cols = data["granular_cols"]
df_clientes = data["df_clientes"]

# Process
df_stream_ops = process_operacoes_stream(df_ops, df_titulos)
df_stream_prorrog = process_prorrogacoes_stream(df_prorrog, df_map_ops, df_cli_plat_map, granular_cols)
df_stream_mora = process_mora_stream(df_baixas, df_map_ops, df_cli_plat_map, df_titulos, granular_cols)

print("Streams processados.")

# 3. Consolidação e Enriquecimento
print("Consolidando dados...")

# Union dos Streams
df_union = df_stream_ops.unionByName(df_stream_prorrog).unionByName(df_stream_mora)

df_final = df_union \
    .select(
        col("mes_ref").alias("mes_referencia"),
        col("cod_operacao"),
        col("nbordero"),
        col("sub_tipo_produto"),
        col("nome_plataforma"),
        col("data_deferimento"),
        col("tipo_produto"),
        round(col("volume"), 2).alias("volume"),
        round(col("prazo_medio"), 2).alias("prazo_medio_dias"),
        round(col("prazo_medio_original"), 2).alias("prazo_medio_original_dias"),
        col("floating"),
        round(col("taxa_media"), 4).alias("taxa_media_mensal_pct"),
        round(col("receita"), 2).alias("receita"),
        col("qtd_eventos")
    ) \
    .orderBy("mes_referencia", "tipo_produto")

# Salvar
output_table = "LH_Gold.relatorio_produtos_mensal"
df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)
print(f"Relatório salvo em: {output_table}")

mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
