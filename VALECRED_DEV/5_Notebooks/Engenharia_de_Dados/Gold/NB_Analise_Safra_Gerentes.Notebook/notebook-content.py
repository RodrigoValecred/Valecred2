# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "553c2931-573b-4db0-838d-a70a01306d32",
# META       "default_lakehouse_name": "LH_Gold",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "553c2931-573b-4db0-838d-a70a01306d32"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Análise de Safra de Gerentes (Vintage Analysis)
# **Objetivo:** Analisar a performance dos gerentes normalizada pelo tempo de casa (MOB - Month on Book).
# **Metodologia:**
# 1. Calcular ROGm (Retorno Operacional sobre Gerente).
# 2. Criar curvas de referência (Top Performers vs Média).
# 3. Projetar crescimento para gerentes novos.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, DateType
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from datetime import date, timedelta

# Configurações
CUSTO_FIXO_MENSAL_PADRAO = 5000.0  # Exemplo: Custo de mesa/salário base rateado
COMISSAO_PCT = 0.05               # Exemplo: 5% sobre a Receita (Spread)
TARGET_TABLE = "LH_Gold.analise_safra_gerentes"

# Leitura dos Dados
print("Lendo tabelas...")
df_gerentes = spark.read.table("LH_Silver.staging_gerentes") # Espera-se col: cod_broker, data_contratacao
df_ops = spark.read.table("LH_Gold.fato_operacoes")
df_titulos = spark.read.table("LH_Gold.fato_titulos")
df_bridge = spark.read.table("LH_Silver.bridge_cliente_gerente")

# Garantir que data_contratacao existe (Fallback se não foi criado no Silver ainda)
if "data_contratacao" not in df_gerentes.columns:
    print("AVISO: data_contratacao não encontrada. Usando dummy para teste ou tentando inferir.")
    # Fallback: tentar usar join com usuario ou data default
    df_gerentes = df_gerentes.withColumn("data_contratacao", F.lit("2023-01-01").cast(DateType()))

# CELL ********************

# 1. Definição do Eixo de Tempo (Mensal)
# Vamos pegar os últimos 36 meses até hoje
print("Gerando eixo de tempo...")
max_date = date.today()
months = []
curr = max_date.replace(day=1)
for i in range(36):
    months.append(curr)
    # Go to previous month
    first_of_month = curr.replace(day=1)
    prev_month = first_of_month - timedelta(days=1)
    curr = prev_month.replace(day=1)

df_calendar_months = spark.createDataFrame([(m,) for m in months], ["data_referencia"]).withColumn("data_referencia", F.to_date("data_referencia"))
df_calendar_months = df_calendar_months.withColumn("ano_mes", F.date_format("data_referencia", "yyyy-MM"))
df_calendar_months = df_calendar_months.withColumn("ultimo_dia_mes", F.last_day("data_referencia"))

# 2. Receita (Spread + Tarifas) por Gerente e Mês
# Baseado na data de analise da operação
print("Calculando Receita...")
df_receita = df_ops.withColumn("data_referencia_op", F.trunc("data_analise", "MM")) \
    .groupBy("cod_broker", "data_referencia_op") \
    .agg(
        F.sum("desagio").alias("spread"),
        F.sum("total_de_tarifas").alias("tarifas")
    ) \
    .withColumnRenamed("data_referencia_op", "data_referencia") \
    .withColumn("receita_total", F.col("spread") + F.col("tarifas"))

# 3. Carteira Ativa e Risco (PDD) por Gerente e Mês
# Complexidade: Reconstruir o histórico
print("Calculando Carteira e Risco (Histórico)...")

# 3.1 Expandir Bridge de Clientes no Tempo
# Join Calendar com Bridge
# Cliente C foi atendido por Gerente G no Mês M se [Inicio, Fim] engloba Mês M
df_bridge_hist = df_bridge.crossJoin(F.broadcast(df_calendar_months)) \
    .filter(
        (F.col("data_referencia") >= F.trunc("data_inicio_vigencia", "MM")) &
        (F.col("data_referencia") <= F.col("data_fim_vigencia"))
    ) \
    .select("cod_cliente", "cod_gerente", "data_referencia", "ultimo_dia_mes")

# 3.2 Títulos Abertos no Mês
# Titulo T do Cliente C estava aberto em M se:
# Inclusao <= UltimoDiaMes AND (Liquidacao > UltimoDiaMes OR Liquidacao IS NULL)
# E Status != Cancelado/Recusado (status_deferimento='Sim')
df_titulos_hist = df_titulos.filter(F.col("status_deferimento") == "Sim") \
    .select("cod_titulo", "cod_operacao", "valor_devido", "venc_prorrogado", "data_inclusao", "liquidacao", "cod_cliente") \
    .crossJoin(F.broadcast(df_calendar_months)) \
    .filter(
        (F.col("data_inclusao") <= F.col("ultimo_dia_mes")) &
        ((F.col("liquidacao") > F.col("ultimo_dia_mes")) | (F.col("liquidacao").isNull()))
    )

# 3.3 Calcular Atraso e PDD do Título naquele Mês
df_titulos_risk = df_titulos_hist.withColumn("dias_atraso_mes", F.datediff(F.col("ultimo_dia_mes"), F.col("venc_prorrogado"))) \
    .withColumn("faixa_pdd",
        F.when(F.col("dias_atraso_mes") > 180, 1.0)
         .when(F.col("dias_atraso_mes") > 150, 0.7)
         .when(F.col("dias_atraso_mes") > 120, 0.4)
         .when(F.col("dias_atraso_mes") > 90, 0.2)
         .when(F.col("dias_atraso_mes") > 60, 0.1)
         .when(F.col("dias_atraso_mes") > 30, 0.05)
         .otherwise(0.0)
    ) \
    .withColumn("pdd_valor", F.col("valor_devido") * F.col("faixa_pdd"))

# Agrupar Títulos por Cliente e Mês
df_titulos_agg = df_titulos_risk.groupBy("cod_cliente", "data_referencia") \
    .agg(
        F.sum("valor_devido").alias("carteira_ativa"),
        F.sum("pdd_valor").alias("perda_esperada")
    )

# 3.4 Join Bridge com Titulos Agregados
df_portfolio_gerente = df_bridge_hist.join(
    df_titulos_agg,
    on=["cod_cliente", "data_referencia"],
    how="left"
) \
    .groupBy("cod_gerente", "data_referencia") \
    .agg(
        F.sum("carteira_ativa").alias("carteira_ativa"),
        F.sum("perda_esperada").alias("perda_esperada")
    ) \
    .na.fill(0)

# 4. Consolidar Base Analítica
print("Consolidando Metrics...")
df_full = df_receita.join(
    df_portfolio_gerente,
    (df_receita.cod_broker == df_portfolio_gerente.cod_gerente) &
    (df_receita.data_referencia == df_portfolio_gerente.data_referencia),
    "full_outer"
) \
.withColumn("id_gerente", F.coalesce(F.col("cod_broker"), F.col("cod_gerente"))) \
.withColumn("mes_ref", F.coalesce(F.col("data_referencia"), F.col("df_portfolio_gerente.data_referencia"))) \
.select("id_gerente", "mes_ref", "spread", "tarifas", "receita_total", "carteira_ativa", "perda_esperada") \
.na.fill(0)

# Join com Dados do Gerente (Contratação)
df_analise = df_full.join(df_gerentes, df_full.id_gerente == df_gerentes.cod_broker, "inner") \
    .select(
        F.col("id_gerente"),
        F.col("mes_ref"),
        F.col("data_contratacao"),
        F.col("receita_total"),
        F.col("carteira_ativa"),
        F.col("perda_esperada"),
        F.col("cpf_cnpj")
    )

# 5. Calcular Indicadores (MOB, ROGm)
df_final_metrics = df_analise.withColumn(
    "mob",
    F.floor(F.months_between(F.col("mes_ref"), F.col("data_contratacao")))
).filter(F.col("mob") >= 0) # Ignorar dados anteriores à contratação (migração/erro)

# Cálculo ROGm
# ROGm = (Receita - Custos) / Carteira
# Custos = Fixo + Comissao + PDD
df_final_metrics = df_final_metrics.withColumn("comissoes", F.col("receita_total") * COMISSAO_PCT) \
    .withColumn("custos_totais", F.lit(CUSTO_FIXO_MENSAL_PADRAO) + F.col("comissoes") + F.col("perda_esperada")) \
    .withColumn("resultado_operacional", F.col("receita_total") - F.col("custos_totais")) \
    .withColumn("rogm",
        F.when(F.col("carteira_ativa") > 0, F.col("resultado_operacional") / F.col("carteira_ativa"))
         .otherwise(0)
    )

# 6. Identificar Top Performers (Top 25% Acumulado últimos 12M)
print("Calculando Top Performers...")
mes_corte = date.today().replace(day=1) - timedelta(days=365)
df_perf_12m = df_final_metrics.filter(F.col("mes_ref") >= mes_corte) \
    .groupBy("id_gerente") \
    .agg(F.sum("resultado_operacional").alias("res_acum"))

# Quantil 0.75
try:
    corte_top = df_perf_12m.approxQuantile("res_acum", [0.75], 0.01)[0]
    top_performers = df_perf_12m.filter(F.col("res_acum") >= corte_top).select("id_gerente").rdd.flatMap(lambda x: x).collect()
except:
    top_performers = []

# Flag Top Performer
df_final_metrics = df_final_metrics.withColumn("is_top_performer", F.col("id_gerente").isin(top_performers))

# 7. Curvas de Referência (Benchmarks)
print("Gerando Curvas...")
# Média Geral por MOB
df_curve_avg = df_final_metrics.groupBy("mob").agg(F.avg("rogm").alias("rogm_medio_mercado"))

# Média Top Performers por MOB
df_curve_top = df_final_metrics.filter(F.col("is_top_performer") == True) \
    .groupBy("mob").agg(F.avg("rogm").alias("rogm_medio_top"))

# Join das Curvas de volta na base (para comparação linha a linha se quiser)
# Mas para o output final, queremos manter a granularidade Gerente/Mês e ter as colunas de ref.
df_with_bench = df_final_metrics.join(df_curve_avg, "mob", "left").join(df_curve_top, "mob", "left")

# 8. Projeção (Python/Pandas + Sklearn)
# Para cada gerente ativo (MOB < 24), projetar próximo mês
print("Calculando Projeções...")

# Converter para Pandas apenas o necessário (Gerentes recentes)
df_recentes = df_with_bench.filter(F.col("mob") <= 24).select("id_gerente", "mob", "rogm").toPandas()
projections = []

if not df_recentes.empty:
    for gerente, dados in df_recentes.groupby("id_gerente"):
        if len(dados) > 2: # Minimo 3 pontos para regressão
            X = dados["mob"].values.reshape(-1, 1)
            y = dados["rogm"].values
            # Peso maior para dados recentes? O modelo simples não faz, mas LinearRegression pega tendência.
            model = LinearRegression()
            model.fit(X, y)

            last_mob = dados["mob"].max()
            next_mob = last_mob + 1
            pred_rogm = model.predict([[next_mob]])[0]

            projections.append({
                "id_gerente": gerente,
                "mob_projecao": int(next_mob),
                "rogm_projetado": float(pred_rogm)
            })

# Transformar projeções em DataFrame Spark
if projections:
    df_proj = spark.createDataFrame(pd.DataFrame(projections))
    # Join de volta (Opcional, ou salvar em tabela separada)
    # Aqui vamos apenas exibir ou salvar tabela de projeções
    df_proj.write.mode("overwrite").saveAsTable("LH_Gold.analise_safra_projeccoes")
    print("Projeções salvas em LH_Gold.analise_safra_projeccoes")

# 9. Salvar Tabela Final
df_final_output = df_with_bench.select(
    "id_gerente", "mes_ref", "mob", "data_contratacao",
    "receita_total", "carteira_ativa", "perda_esperada", "custos_totais", "resultado_operacional",
    "rogm", "is_top_performer", "rogm_medio_mercado", "rogm_medio_top"
)

df_final_output.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)
print(f"Análise concluída. Tabela salva em: {TARGET_TABLE}")
