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
    # Incluindo nome_gerente para reportar gestor
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
    # Usamos df_ops_raw (sem filtro de status/ano) para garantir que Prorrogacoes/Mora de ops antigas ou com status diversos sejam mapeadas.
    df_map_ops = df_ops_raw.select(
        col("cod_operacao"),
        col("nbordero").alias("nbordero_op"),
        col("nome_plataforma").alias("nome_plataforma_op"),
        col("chave_produto").alias("chave_produto_op"),
        col("data_deferimento").alias("data_deferimento_op"),
        col("cod_cliente").alias("cod_cliente_op"),
        col("floating").alias("floating_op")
    ).dropDuplicates(["cod_operacao"])

    granular_cols = ["nbordero", "nome_plataforma", "chave_produto", "data_deferimento", "cod_cliente", "floating"]

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
    df_titulos_dates = df_titulos.join(df_ops.select("cod_operacao", "data_deferimento"), "cod_operacao", "inner") \
        .withColumn("prazo_original_dias", datediff(col("vencimento"), col("data_deferimento"))) \
        .withColumn("valor_vezes_prazo_original", col("valor") * col("prazo_original_dias"))

    df_titulos_agg_op = df_titulos_dates.groupBy("cod_operacao").agg(
        sum(col("valor") * col("prazo")).alias("soma_valor_prazo_op"),
        sum("valor").alias("soma_valor_titulos_op"),
        sum("valor_vezes_prazo_original").alias("soma_valor_prazo_original_op"),
        min("vencimento").alias("menor_vencimento_titulos"),
        max("vencimento").alias("maior_vencimento_titulos")
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
            count("cod_operacao").alias("qtd_eventos"),
            min(coalesce(col("menor_vencimento_titulos"), col("menor_vencimento"))).alias("menor_vencimento"),
            max(coalesce(col("maior_vencimento_titulos"), col("maior_vencimento"))).alias("maior_vencimento")
        ) \
        .withColumn("tipo_produto", lit("OPERACOES")) \
        .withColumn("prazo_medio",
                    when(col("volume") > 0, col("total_valor_prazo_mes") / col("volume")).otherwise(0)) \
        .withColumn("prazo_medio_original",
                    when(col("volume") > 0, col("total_valor_prazo_original_mes") / col("volume")).otherwise(0)) \
        .withColumn("prazo_medio_total", col("prazo_medio") + coalesce(col("floating"), lit(0))) \
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

    # Fallback de Atributos Faltantes (Data, Plataforma)
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
        .groupBy("mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento", "floating") \
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
        .withColumn("prazo_medio_total", col("prazo_medio") + coalesce(col("floating"), lit(0))) \
        .withColumn("menor_vencimento", lit(None).cast("date")) \
        .withColumn("maior_vencimento", lit(None).cast("date")) \
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
    # Verificar datas nulas ou inválidas (ex: ano 0001) para evitar prazos gigantes
    # Usar Vencimento Prorrogado se disponível (via join com titulos)
    df_titulos_dates = df_titulos.select(col("cod_titulo"), col("venc_prorrogado"))
    df_mora_enrich_venc = df_mora_enrich.join(df_titulos_dates, "cod_titulo", "left")

    df_mora_calc = df_mora_enrich_venc \
        .withColumn("data_referencia_mora",
                    when(year(col("venc_prorrogado")) > 1900, col("venc_prorrogado"))
                    .otherwise(col("data_vencimento"))) \
        .withColumn("dias_atraso",
                    when(col("data_baixa").isNull() | col("data_referencia_mora").isNull(), 0)
                    .when(year(col("data_baixa")) <= 1900, 0)
                    .when(year(col("data_referencia_mora")) <= 1900, 0)
                    .otherwise(datediff(col("data_baixa"), col("data_referencia_mora")))
        ) \
        .withColumn("valor_vezes_atraso", col("valor_pago") * col("dias_atraso"))

    df_stream_mora = df_mora_calc \
        .withColumn("mes_ref", trunc(col("data_baixa"), "MM")) \
        .groupBy("mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento", "floating") \
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
        .withColumn("prazo_medio_total", col("prazo_medio") + coalesce(col("floating"), lit(0))) \
        .withColumn("menor_vencimento", lit(None).cast("date")) \
        .withColumn("maior_vencimento", lit(None).cast("date")) \
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
        round(col("prazo_medio_total"), 2).alias("prazo_medio_total_dias"),
        col("menor_vencimento").alias("menor_vencimento_op"),
        col("maior_vencimento").alias("maior_vencimento_op"),
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

# MARKDOWN ********************

# # Relatório de Rentabilidade e Risco de Clientes (Safra 2025)
# **Objetivo:** Analisar a taxa média ponderada, receitas (tarifas, juros de mora) e risco dos clientes que operaram em 2025.
# 
# **Contexto:** A taxa média de 2025 apresentou queda. Este relatório identifica os clientes com menores taxas e cruza com perfil de risco e rentabilidade total.


# CELL ********************

# Configuração e Imports
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import (
    col, sum, avg, count, max, min, lit, when, round, desc, asc, broadcast, coalesce, year, datediff, to_date, current_date
)
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Iniciando Análise de Rentabilidade 2025...")

# 1. Carregamento de Dados (Gold)
print("Carregando tabelas Fato e Dimensão...")

# Operações (Base da Análise - Safra 2025)
# Dedup by cod_operacao to be safe
# Incluindo nbordero e cod_operacao na selecao
df_ops_normal = spark.read.table("LH_Gold.fato_operacoes") \
    .filter(col("data_deferimento") >= "2025-01-01") \
    .filter(col("data_deferimento") <= "2025-12-31") \
    .filter(col("status_aceite") == "A") \
    .filter(col("status_analise") == "D") \
    .dropDuplicates(["cod_operacao"])

# Operações de Recompra (Para incluir Receita de Recompra)
# Fonte: LH_Gold.fato_operacoes_recompra (Granularidade: Título expandido)
# Agregado por Operação

# Garantir que tarifa_de_recompra exista em df_ops_normal antes do select
if "tarifa_de_recompra" not in df_ops_normal.columns:
    print("Aviso: tarifa_de_recompra não encontrada em fato_operacoes. Criando com 0.")
    df_ops_normal = df_ops_normal.withColumn("tarifa_de_recompra", lit(0.0))

if "floating" not in df_ops_normal.columns:
    if "float" in df_ops_normal.columns:
        df_ops_normal = df_ops_normal.withColumnRenamed("float", "floating")
    else:
        df_ops_normal = df_ops_normal.withColumn("floating", lit(0.0))

if "data_aceite" not in df_ops_normal.columns:
    df_ops_normal = df_ops_normal.withColumn("data_aceite", to_date(col("data_deferimento")))

try:
    # Load Dim Gerentes for Platform Info
    df_gerentes = spark.read.table("LH_Gold.dim_gerentes").select("cod_broker", "nome_plataforma")

    df_ops_rc = spark.read.table("LH_Gold.fato_operacoes_recompra") \
        .filter(to_date(col("data_analise")) >= "2025-01-01") \
        .filter(to_date(col("data_analise")) <= "2025-12-31")

    # Join with Dim Gerentes
    df_ops_rc = df_ops_rc.join(df_gerentes, "cod_broker", "left")

    # Deduplicar/Agregar por Operação
    df_ops_rc_agg = df_ops_rc.groupBy("cod_operacao").agg(
        max("cod_cliente").alias("cod_cliente"),
        max("nbordero").alias("nbordero"),
        to_date(max("data_analise")).alias("data_deferimento"),
        sum("valor").alias("valor_de_face"), # Volume da recompra (soma dos titulos)
        max("chave_produto").alias("chave_produto"),
        (max("tarifa_recompra") * max("n_docs_recompra")).alias("tarifa_de_recompra"), # Taxa da operação
        max("nome_plataforma").alias("nome_plataforma")
    ).withColumn("desagio", lit(0.0)) \
     .withColumn("total_de_tarifas", lit(0.0)) \
     .withColumn("floating", lit(0.0)) \
     .withColumn("data_aceite", to_date(col("data_deferimento")))

    # Definir colunas comuns para o Union
    common_cols = [
        "cod_operacao", "cod_cliente", "nbordero", "data_deferimento",
        "valor_de_face", "desagio", "total_de_tarifas", "tarifa_de_recompra",
        "chave_produto", "nome_plataforma", "floating", "data_aceite"
    ]

    # Union
    df_ops = df_ops_normal.select(common_cols).unionByName(df_ops_rc_agg.select(common_cols), allowMissingColumns=True) \
        .dropDuplicates(["cod_operacao"])
    print(f"Adicionadas operações de recompra. Total combinado: {df_ops.count()}")

except Exception as e:
    print(f"Aviso: Não foi possível carregar fato_operacoes_recompra ({e}). Usando apenas operações normais.")
    df_ops = df_ops_normal

# Prorrogações (Silver - Source of Truth for Revenue per Title)
# Join by cod_titulo requested by user
try:
    df_prorrogacao_silver = spark.read.table("LH_Silver.staging_operacoes_prorrogacao_limpa")

    # Check column name (codtitulo vs cod_titulo) - Standardizing to cod_titulo
    if "codtitulo" in df_prorrogacao_silver.columns:
        df_prorrogacao_silver = df_prorrogacao_silver.withColumnRenamed("codtitulo", "cod_titulo")

    # Deduplicate rows (exact full row duplication from Source)
    df_prorrogacao_silver = df_prorrogacao_silver.dropDuplicates()

    # Filter Valid Prorogations (Only 'D' - Deferido)
    # Removing 'I' (Indeferido) which causes inflated revenue
    if "status_analise" in df_prorrogacao_silver.columns:
        df_prorrogacao_silver = df_prorrogacao_silver.filter(col("status_analise") == "D")
    else:
        print("Aviso: status_analise não encontrada em staging_operacoes_prorrogacao_limpa. Filtro não aplicado.")

    # Aggregate by Title (to avoid exploding rows in Title join)
    # 1. Total Revenue per Title
    # 2. 2025 Revenue per Title (for Client Deduction Logic)
    df_prorrogacao_silver_agg = df_prorrogacao_silver.groupBy("cod_titulo").agg(
        sum("juros").alias("receita_prorrogacao_titulo"),
        sum(when(year(col("data_inclusao")) == 2025, col("juros")).otherwise(0)).alias("receita_prorrogacao_titulo_2025")
    )
    print("Tabela Silver de Prorrogações carregada e agregada.")
except Exception as e:
    print(f"Erro ao carregar LH_Silver.staging_operacoes_prorrogacao_limpa: {e}. Usando placeholder (0).")
    df_prorrogacao_silver_agg = None

# Títulos (Para cálculo da Taxa Ponderada: Valor * Prazo)
# Agregado por Operação
df_titulos = spark.read.table("LH_Gold.fato_titulos") \
    .filter(col("aceito") == "S") \
    .filter(col("t_doc") != "BL") \
    .dropDuplicates(["cod_titulo"]) \
    .join(df_ops.select("cod_operacao", "data_deferimento", "data_aceite", "floating"), "cod_operacao", "left") \
    .withColumn("data_final_real",
                when(col("liquidacao").isNotNull(), col("liquidacao"))
                .when(col("venc_prorrogado").isNotNull(), col("venc_prorrogado"))
                .otherwise(col("vencimento"))) \
    .withColumn("dias_final_epoch", datediff(col("data_final_real"), lit("1970-01-01"))) \
    .withColumn("valor_vezes_data_final", col("valor") * col("dias_final_epoch")) \
    .withColumn("dias_prorrogacao", datediff(coalesce(col("venc_prorrogado"), col("vencimento")), col("vencimento"))) \
    .withColumn("valor_vezes_prorrogacao", col("valor") * col("dias_prorrogacao")) \
    .withColumn("data_vencimento_ajustado", coalesce(col("venc_prorrogado"), col("vencimento"))) \
    .withColumn("dias_atraso_real",
                when(col("liquidacao").isNotNull(), datediff(col("liquidacao"), col("data_vencimento_ajustado")))
                .otherwise(datediff(current_date(), col("data_vencimento_ajustado")))) \
    .withColumn("em_mora", col("dias_atraso_real") > 0) \
    .withColumn("valor_vezes_atraso", when(col("em_mora"), col("valor") * col("dias_atraso_real")).otherwise(0)) \
    .withColumn("valor_em_mora", when(col("em_mora"), col("valor")).otherwise(0)) \
    .withColumn("dias_prazo_total", datediff(col("vencimento"), col("data_deferimento"))) \
    .withColumn("valor_vezes_prazo_total", col("valor") * col("dias_prazo_total")) \
    .withColumn("valor_vezes_prazo", col("valor") * col("prazo"))

if df_prorrogacao_silver_agg:
    df_titulos = df_titulos.join(df_prorrogacao_silver_agg, "cod_titulo", "left") \
        .withColumn("receita_prorrogacao_titulo", coalesce(col("receita_prorrogacao_titulo"), lit(0))) \
        .withColumn("receita_prorrogacao_titulo_2025", coalesce(col("receita_prorrogacao_titulo_2025"), lit(0)))
else:
    df_titulos = df_titulos.withColumn("receita_prorrogacao_titulo", lit(0)) \
        .withColumn("receita_prorrogacao_titulo_2025", lit(0))

df_titulos_agg = df_titulos.groupBy("cod_operacao").agg(
    sum("valor_vezes_prazo").alias("total_valor_prazo_op"),
    sum("valor").alias("valor_face_titulos_op"),
    sum("custo_financeiro").alias("custo_financeiro_op"),
    sum("spread").alias("spread_op"),
    sum("valor_vezes_data_final").alias("soma_produto_valor_data_final"),
    sum("valor_vezes_prorrogacao").alias("total_valor_prorrogacao_op"),
    sum("valor_vezes_atraso").alias("total_valor_atraso_op"),
    sum("valor_em_mora").alias("total_valor_mora_op"),
    sum("receita_prorrogacao_titulo").alias("receita_prorrogacao_op"),
    sum("receita_prorrogacao_titulo_2025").alias("receita_prorrogacao_op_2025"),
    sum("valor_vezes_prazo_total").alias("soma_produto_valor_prazo_total")
)

# Baixas (Para cálculo de Receita de Juros de Mora Pagos)
# Agregado por Operação
df_baixas = spark.read.table("LH_Gold.fato_baixas")
df_baixas_agg = df_baixas.groupBy("cod_operacao").agg(
    sum("juros").alias("total_juros_mora_pago_op")
)

# Prorrogações (Receita de Prorrogação = Juros da tabela de prorrogações)
# Fonte: LH_Gold.fato_prorrogacoes_de_titulos
# Agregado por Cliente
try:
    # A tabela fato_prorrogacoes_de_titulos deve ter cod_cliente (adicionado no NB_Curadoria)
    df_prorrogacao = spark.read.table("LH_Gold.fato_prorrogacoes_de_titulos")

    # 1. Filtro Básico: Apenas Prorrogações Reais (data vencimento novo != data vencimento antigo)
    # Não filtramos ano aqui para garantir que pegamos todo o histórico da operação (Lifetime)
    df_prorrogacao_clean = df_prorrogacao.filter(col("vencimentonov") != col("vencimentoant"))

    # 2. Agregado por Cliente (Existente - Calendário 2025)
    # Mantém filtro de ano 2025 para compatibilidade com a métrica "Receita Cliente 2025"
    df_prorrogacao_agg = df_prorrogacao_clean \
        .filter(year(col("data_inclusao")) == 2025) \
        .groupBy("cod_cliente").agg(sum("juros").alias("receita_tarifa_prorrogacao_cliente"))

    # 3. Agregado por Operação (Lifetime - Safra 2025)
    # SUBSTITUÍDO: Agora calculado via Join com Titles (Silver Source) para maior precisão (join by cod_titulo)
    df_prorrogacao_agg_op = None

    # 4. Agregado por Operação (Calendário 2025 - Para Deduplicação)
    # SUBSTITUÍDO: Agora calculado via Join com Titles (Silver Source)
    df_prorrogacao_agg_op_2025 = None

except Exception as e:
    print(f"Aviso: Tabela fato_prorrogacoes_de_titulos não encontrada ou erro ({e}). Usando placeholder.")
    df_prorrogacao_agg = None
    df_prorrogacao_agg_op = None
    df_prorrogacao_agg_op_2025 = None

# Dimensão Clientes (Para Nome e Risco Atual)
df_clientes = spark.read.table("LH_Gold.dim_clientes") \
    .select("cod_cliente", "nome", "risco", "risco_comissaria", "status_risco", "grupo_economico") \
    .dropDuplicates(["cod_cliente"])

# Análise Score Clientes (Para Qualidade/Classificação Detalhada se existir)
try:
    df_score = spark.read.table("LH_Gold.analise_score_clientes") \
        .select("cod_cliente", "qualidade_cliente") \
        .dropDuplicates(["cod_cliente"])
except:
    print("Aviso: Tabela analise_score_clientes não encontrada ou esquema diferente. Usando placeholder.")
    df_score = None

# Dimensão Produtos (Para Nome Amigável e Categorização)
# Force Dedup on chave_produto to prevent join explosion
df_produtos = spark.read.table("LH_Gold.dim_produtos") \
    .select("chave_produto", "produto_informacao_de_mercado") \
    .dropDuplicates(["chave_produto"])

print("Dados carregados.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Enriquecimento e Cálculos
print("Realizando joins e cálculos de métricas...")

# Join Operações com Agregados
# Usamos LEFT JOIN com titulos para garantir que operações de Recompra (que não têm títulos na fato_titulos) sejam mantidas.
df_base = df_ops.join(df_titulos_agg, "cod_operacao", "left") \
    .join(df_baixas_agg, "cod_operacao", "left") \
    .join(broadcast(df_produtos), "chave_produto", "left")

# Prorrogação (Operação): Já incluído via df_titulos_agg (Silver Source)
# Garantir que colunas existam (caso df_titulos esteja vazio ou erro no silver)
if "receita_prorrogacao_op" not in df_base.columns:
    df_base = df_base.withColumn("receita_prorrogacao_op", lit(0))

if "receita_prorrogacao_op_2025" not in df_base.columns:
    df_base = df_base.withColumn("receita_prorrogacao_op_2025", lit(0))

# Join com Dados do Cliente (Risco e Nome)
df_base_cliente = df_base.join(df_clientes, "cod_cliente", "left")

if df_score:
    df_base_cliente = df_base_cliente.join(df_score, "cod_cliente", "left")
else:
    df_base_cliente = df_base_cliente.withColumn("qualidade_cliente", lit(None))

if df_prorrogacao_agg:
    df_base_cliente = df_base_cliente.join(df_prorrogacao_agg, "cod_cliente", "left")
else:
    df_base_cliente = df_base_cliente.withColumn("receita_tarifa_prorrogacao_cliente", lit(0))

# 4. Cálculo de Indicadores Finais (Granularidade: Operação)

# Optimization (Bolt ⚡): Replace Window functions with GroupBy + Join for client aggregations
# This avoids expensive window shuffling across all operations of a client (O(N*W) -> O(N)).

# 4.1 Pre-aggregation calculations (Row-level)
df_calcs = df_base_cliente \
    .withColumn("produto_final", coalesce(col("produto_informacao_de_mercado"), lit("PRODUTO NÃO IDENTIFICADO"))) \
    .withColumn("prazo_medio_mora_op",
                when(col("valor_face_titulos_op") > 0,
                     (col("soma_produto_valor_data_final") / col("valor_face_titulos_op")) - datediff(col("data_deferimento"), lit("1970-01-01"))
                ).otherwise(0)) \
    .withColumn("receita_real_op_calc",
                coalesce(col("desagio"), lit(0)) +
                coalesce(col("total_juros_mora_pago_op"), lit(0)) +
                coalesce(col("total_de_tarifas"), lit(0))) \
    .withColumn("vol_prazo_real_op_calc",
                col("valor_de_face") * col("prazo_medio_mora_op")) \
    .withColumn("receita_total_op", 
                coalesce(col("desagio"), lit(0)) + 
                coalesce(col("total_de_tarifas"), lit(0)) + 
                coalesce(col("total_juros_mora_pago_op"), lit(0)) +
                coalesce(col("tarifa_de_recompra"), lit(0)) +
                coalesce(col("receita_prorrogacao_op"), lit(0))) \
    .withColumn("prazo_medio_total",
                (when(col("valor_face_titulos_op") > 0,
                      col("soma_produto_valor_prazo_total") / col("valor_face_titulos_op")
                 ).otherwise(0) + coalesce(col("floating"), lit(0))).cast("float"))

# 4.2 Aggregation by Client
df_cliente_agg = df_calcs.groupBy("cod_cliente").agg(
    sum("total_valor_prazo_op").alias("soma_valor_prazo_cliente"),
    sum("desagio").alias("receita_desagio_cliente"),
    sum("receita_total_op").alias("soma_receita_total_op"), # Intermediate sum
    sum("receita_prorrogacao_op").alias("soma_prorrogacao_op_cliente"),
    sum("receita_prorrogacao_op_2025").alias("soma_prorrogacao_op_2025_cliente"), # Soma da receita de prorrogação (Safra 2025) das operações (para deduzir da receita cliente 2025 e evitar contagem dupla)
    sum("receita_real_op_calc").alias("soma_receita_real_cliente"),
    sum("vol_prazo_real_op_calc").alias("soma_vol_prazo_real_cliente"),
    sum("custo_financeiro_op").alias("custo_financeiro_cliente_sum"),
    sum("spread_op").alias("spread_cliente_sum"),
    sum("valor_de_face").alias("volume_operado_cliente"),
    count("cod_operacao").alias("qtd_operacoes_cliente")
)

# 4.3 Join and Final Calculations
df_report = df_calcs.join(df_cliente_agg, "cod_cliente", "left") \
    .withColumn("receita_total_cliente", col("soma_receita_total_op") + (coalesce(col("receita_tarifa_prorrogacao_cliente"), lit(0)) - coalesce(col("soma_prorrogacao_op_2025_cliente"), lit(0)))) \
    .withColumn("custo_financeiro_cliente", coalesce(col("custo_financeiro_cliente_sum"), lit(0))) \
    .withColumn("spread_cliente", coalesce(col("spread_cliente_sum"), lit(0))) \
    .withColumn("taxa_media_ponderada_mensal_cliente", 
                when(col("soma_valor_prazo_cliente") > 0, 
                     (col("receita_desagio_cliente") / col("soma_valor_prazo_cliente")) * 30 * 100
                ).otherwise(0)) \
    .withColumn("taxa_media_real_mensal_cliente",
                when(col("soma_vol_prazo_real_cliente") > 0,
                     (col("soma_receita_real_cliente") / col("soma_vol_prazo_real_cliente")) * 30 * 100
                ).otherwise(0)) \
    .withColumn("rentabilidade_percentual_cliente", 
                when(col("volume_operado_cliente") > 0, 
                     (col("receita_total_cliente") / col("volume_operado_cliente")) * 100
                ).otherwise(0)) \
    .withColumn("taxa_operacao",
                when(col("total_valor_prazo_op") > 0,
                     (col("desagio") / col("total_valor_prazo_op")) * 30 * 100
                ).otherwise(0)) \
    .withColumn("taxa_media_real_mensal_op",
                when(col("vol_prazo_real_op_calc") > 0,
                     (col("receita_real_op_calc") / col("vol_prazo_real_op_calc")) * 30 * 100
                ).otherwise(0)) \
    .withColumn("prazo_medio_operacao",
                when(col("valor_de_face") > 0,
                     col("total_valor_prazo_op") / col("valor_de_face")
                ).otherwise(0)) \
    .withColumn("prazo_medio_prorrogado_op",
                when(col("valor_face_titulos_op") > 0,
                     col("total_valor_prorrogacao_op") / col("valor_face_titulos_op")
                ).otherwise(0)) \
    .withColumn("prazo_verdadeiro_real_medio_ponderado_op",
                coalesce(col("prazo_medio_operacao"), lit(0)) + coalesce(col("prazo_medio_prorrogado_op"), lit(0))) \
    .withColumn("prazo_medio_atraso_titulos_mora",
                when(col("total_valor_mora_op") > 0,
                     col("total_valor_atraso_op") / col("total_valor_mora_op")
                ).otherwise(0)) \
    .select(
        # Identificadores da Operação
        col("cod_operacao"),
        col("nbordero"),
        col("data_deferimento"),
        col("cod_cliente"),
        col("nome").alias("nome_cliente"),
        col("nome_plataforma"),
        col("grupo_economico"),
        col("produto_final").alias("produto"),
        # Perfil Cliente
        col("qualidade_cliente"),
        col("status_risco"),
        col("risco").alias("risco_total_atual"),
        col("risco_comissaria").alias("risco_comissaria_atual"),
        # Métricas da Operação Individual
        col("valor_de_face").alias("volume_operacao"),
        col("desagio").alias("receita_desagio_op"),
        col("total_de_tarifas").alias("receita_tarifas_op"),
        col("total_juros_mora_pago_op").alias("receita_juros_mora_op"),
        coalesce(col("tarifa_de_recompra"), lit(0)).alias("receita_recompra_op"),
        coalesce(col("receita_prorrogacao_op"), lit(0)).alias("receita_prorrogacao_op"),
        col("receita_total_op"),
        col("custo_financeiro_op").alias("custo_financeiro"),
        col("spread_op").alias("spread"),
        round(col("taxa_operacao"), 4).alias("taxa_operacao"),
        round(col("prazo_medio_operacao"), 2).alias("prazo_medio_operacao"),
        round(col("prazo_medio_prorrogado_op"), 2).alias("prazo_medio_prorrogado_op"),
        round(col("prazo_verdadeiro_real_medio_ponderado_op"), 2).alias("prazo_verdadeiro_real_medio_ponderado_op"),
        round(col("taxa_media_real_mensal_op"), 4).alias("taxa_media_real_mensal_op"),
        col("prazo_medio_total"),
        col("floating").cast("float").alias("floating"),
        # Métricas Agregadas do Cliente (Repetidas nas linhas)
        col("volume_operado_cliente"),
        col("qtd_operacoes_cliente"),
        round(col("taxa_media_ponderada_mensal_cliente"), 4).alias("taxa_media_pond_2025_cliente"),
        round(col("taxa_media_real_mensal_cliente"), 4).alias("taxa_media_real_mensal_cliente"),
        round(col("prazo_medio_atraso_titulos_mora"), 2).alias("prazo_medio_atraso_titulos_mora"),
        round(col("rentabilidade_percentual_cliente"), 4).alias("rentabilidade_perc_cliente"),
        coalesce(col("receita_tarifa_prorrogacao_cliente"), lit(0)).alias("receita_tarifa_prorrogacao_cliente"),
        round(col("receita_total_cliente"), 2).alias("receita_total_cliente"),
        round(col("custo_financeiro_cliente"), 2).alias("custo_financeiro_cliente"),
        round(col("spread_cliente"), 2).alias("spread_cliente")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Output e Análise
print("Gerando output...")

# Para visualização (Top 20 Clientes), agregamos para evitar duplicatas visuais
df_top_clientes = df_report.select(
    "cod_cliente", "nome_cliente", "grupo_economico", 
    "volume_operado_cliente", "qtd_operacoes_cliente", 
    "taxa_media_pond_2025_cliente", "rentabilidade_perc_cliente", "receita_total_cliente",
    "custo_financeiro_cliente", "spread_cliente"
).dropDuplicates(["cod_cliente"])

# Ordenar por Taxa Média Cliente (Menores Taxas Primeiro)
df_menores_taxas = df_top_clientes.filter(col("volume_operado_cliente") > 10000) \
    .orderBy(col("taxa_media_pond_2025_cliente").asc())

# Salvar Tabela Gold (Granularidade: Operação)
output_table = "LH_Gold.relatorio_rentabilidade_clientes_2025"
df_report.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)
print(f"Relatório detalhado salvo em: {output_table}")

mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
