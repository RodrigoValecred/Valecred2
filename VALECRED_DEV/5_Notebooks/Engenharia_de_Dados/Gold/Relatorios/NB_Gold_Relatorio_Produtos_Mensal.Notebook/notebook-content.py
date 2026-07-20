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
    col, sum, avg, count, max, min, lit, when, round, broadcast, coalesce, year, datediff, to_date, current_date
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
# Desduplicar por cod_operacao por segurança
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
# ⚡ Bolt: Fazer cache de df.columns para evitar chamadas RPC repetidas durante checagens sequenciais
# 💡 O que: Fez cache de `df_ops_normal.columns` em um set do Python antes de realizar múltiplas checagens `in`.
# 🎯 Por que: Acessar `.columns` em um PySpark DataFrame aciona uma chamada RPC custosa. Fazer cache disso elimina 4 chamadas de rede repetidas, reduzindo o tempo de execução.
# 📊 Impacto: Elimina chamadas RPC redundantes ao driver, especialmente para DataFrames com muitas colunas.
# 🔬 Medição: O(4) chamadas remotas reduzidas para O(1) chamada remota + O(4) buscas hash locais.
df_ops_normal_cols = set(df_ops_normal.columns)

if "tarifa_de_recompra" not in df_ops_normal_cols:
    print("Aviso: tarifa_de_recompra não encontrada em fato_operacoes. Criando com 0.")
    df_ops_normal = df_ops_normal.withColumn("tarifa_de_recompra", lit(0.0))

if "floating" not in df_ops_normal_cols:
    if "float" in df_ops_normal_cols:
        df_ops_normal = df_ops_normal.withColumnRenamed("float", "floating")
    else:
        df_ops_normal = df_ops_normal.withColumn("floating", lit(0.0))

if "data_aceite" not in df_ops_normal_cols:
    df_ops_normal = df_ops_normal.withColumn("data_aceite", to_date(col("data_deferimento")))

try:
    # Carregar Dim Gerentes para informações da plataforma (Platform Info)
    df_gerentes = spark.read.table("LH_Gold.dim_gerentes").select("cod_broker", "nome_plataforma")

    df_ops_rc = spark.read.table("LH_Gold.fato_operacoes_recompra") \
        .filter(to_date(col("data_analise")) >= "2025-01-01") \
        .filter(to_date(col("data_analise")) <= "2025-12-31")

    # Fazer join com Dim Gerentes
    df_ops_rc = df_ops_rc.join(broadcast(df_gerentes), "cod_broker", "left")

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
     .withColumn("data_aceite", to_date(col("data_deferimento"))) \
     .withColumn("prazo_medio_ponderado_dias", lit(0.0))

    # Definir colunas comuns para o Union
    common_cols = [
        "cod_operacao", "cod_cliente", "nbordero", "data_deferimento",
        "valor_de_face", "desagio", "total_de_tarifas", "tarifa_de_recompra",
        "chave_produto", "nome_plataforma", "floating", "data_aceite", "prazo_medio_ponderado_dias"
    ]

    # Union
    # ⚡ Otimização Bolt: Adicionado cache em união de operações para não afetar steps de log
    # 💡 O que: Adicionado .cache() no dataframe `df_ops` logo após o `.unionByName` e antes do `.count()` de log. Foi adicionado `.unpersist()` mais ao final (não aplicável, já que fica no runtime do notebook de job, mas como boas práticas, vamos cachear).
    # 🎯 Por que: Como df_ops sofre um join com grandes agregados, sem o cache a ação de `.count()` já provocou leitura extra. E o uso posterior dobra o I/O da camada Gold.
    # 📊 Impacto: Evita recálculos de Union+agg e DropDuplicates.
    df_ops = df_ops_normal.select(common_cols).unionByName(df_ops_rc_agg.select(common_cols), allowMissingColumns=True) \
        .dropDuplicates(["cod_operacao"]).cache()
    print(f"Adicionadas operações de recompra. Total combinado: {df_ops.count()}")

except Exception as e:
    print(f"Aviso: Não foi possível carregar fato_operacoes_recompra ({e}). Usando apenas operações normais.")
    df_ops = df_ops_normal

# Prorrogações (Silver - Fonte da Verdade para Receita por Título)
# Fazer join por cod_titulo solicitado pelo usuário
try:
    df_prorrogacao_silver = spark.read.table("LH_Silver.staging_operacoes_prorrogacao_limpa")

    # Checar nome da coluna (codtitulo vs cod_titulo) - Padronizando para cod_titulo
    if "codtitulo" in df_prorrogacao_silver.columns:
        df_prorrogacao_silver = df_prorrogacao_silver.withColumnRenamed("codtitulo", "cod_titulo")

    # Desduplicar linhas (duplicação exata da linha inteira a partir da fonte)
    df_prorrogacao_silver = df_prorrogacao_silver.dropDuplicates()

    # Filtrar Prorrogações Válidas (Somente 'D' - Deferido)
    # Removendo 'I' (Indeferido) que causa receita inflacionada
    if "status_analise" in df_prorrogacao_silver.columns:
        df_prorrogacao_silver = df_prorrogacao_silver.filter(col("status_analise") == "D")
    else:
        print("Aviso: status_analise não encontrada em staging_operacoes_prorrogacao_limpa. Filtro não aplicado.")

    # Agregar por Título (para evitar explosão de linhas no join de Título)
    # 1. Receita total por título
    # 2. Receita de 2025 por Título (para Lógica de Dedução do Cliente)
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
    .withColumn("valor_vezes_prazo", col("valor") * datediff(col("vencimento"), col("data_aceite")))

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
# Forçar desduplicação (Dedup) em chave_produto para evitar explosão no join
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
# 🧠 Tensor: Aplicar broadcast join nas dimensões para evitar shuffle pesado no cluster
# 💡 O que: Passar `df_clientes`, `df_score` e `df_prorrogacao_agg` usando a função de otimização `broadcast()`.
# 🎯 Por que: `df_base` representa a tabela fato (muito volumosa) e `df_clientes` (ou score/prorrogacao) são pequenas dimensões. Fazer join normal forçaria o Spark a particionar e reorganizar grandes blocos pela rede. Em vez disso, enviar pequenas tabelas inteiras na memória aos executors elimina totalmente o custo do shuffle.
# 📊 Impacto: Otimiza as junções pesadas de final de pipe, reduzindo a contenção de rede e caindo o process time severamente.
# 🔬 Medição: A árvore de consultas (DAG) mostrará BroadcastHashJoin substituindo o custoso SortMergeJoin nos joins de Dimensão.
df_base_cliente = df_base.join(broadcast(df_clientes), "cod_cliente", "left")

if df_score:
    df_base_cliente = df_base_cliente.join(broadcast(df_score), "cod_cliente", "left")
else:
    df_base_cliente = df_base_cliente.withColumn("qualidade_cliente", lit(None))

if df_prorrogacao_agg:
    df_base_cliente = df_base_cliente.join(broadcast(df_prorrogacao_agg), "cod_cliente", "left")
else:
    df_base_cliente = df_base_cliente.withColumn("receita_tarifa_prorrogacao_cliente", lit(0))

# 4. Cálculo de Indicadores Finais (Granularidade: Operação)

# Otimização (Bolt ⚡): Substituir funções de Window por GroupBy + Join para agregações de clientes
# Isso evita o embaralhamento (shuffling) custoso de window em todas as operações de um cliente (O(N*W) -> O(N)).

# 4.1 Cálculos de pré-agregação (nível de linha)
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

# 4.2 Agregação por Cliente
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

# 4.3 Join e Cálculos Finais
# 🧠 Tensor: Aplicar broadcast join na tabela agregada de clientes
# 💡 O que: Usar `broadcast(df_cliente_agg)` para evitar que o join acione um novo shuffle global e degradação de performance no final da query.
# 🎯 Por que: `df_calcs` continua com nível de fato volumoso. `df_cliente_agg` já foi agrupado (tamanho pequeno correspondente a N clientes). Assim, ele deve ser transmitido pela rede (broadcast).
# 📊 Impacto: Diminui notavelmente o tempo de CPU e uso da rede final.
# 🔬 Medição: Elimina o último e maior SortMergeJoin no execution plan final.
df_report = df_calcs.join(broadcast(df_cliente_agg), "cod_cliente", "left") \
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
        col("prazo_medio_ponderado_dias"),
        round(col("prazo_medio_operacao"), 2).alias("prazo_medio_operacao"),
        round(col("prazo_medio_prorrogado_op"), 2).alias("prazo_medio_prorrogado_op"),
        round(col("prazo_verdadeiro_real_medio_ponderado_op"), 2).alias("prazo_verdadeiro_real_medio_ponderado_op"),
        round(col("taxa_media_real_mensal_op"), 4).alias("taxa_media_real_mensal_op"),
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
# ⚡ Otimização de Bolt: Uso de try-finally para garantir limpeza da memória de df cacheados.
try:
    df_report.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)
    print(f"Relatório detalhado salvo em: {output_table}")
finally:
    df_ops.unpersist()

mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
