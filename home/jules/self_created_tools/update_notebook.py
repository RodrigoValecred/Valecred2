
import sys

# Read the original file content
with open("VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py", "r") as f:
    content = f.read()

# --- Section 0.2 Modifications ---
# Add reading of escrow table
search_text_0_2 = 'df_estudo_operacoes = spark.read.table("LH_Silver.staging_estudo_operacoes")'
replace_text_0_2 = """df_estudo_operacoes = spark.read.table("LH_Silver.staging_estudo_operacoes")

# Escrow (Silver)
print("Carregando Escrow (Silver)...")
try:
    df_escrow = spark.read.table("LH_Silver.staging_operacoes_escrow")
except Exception as e:
    print(f"AVISO: Tabela LH_Silver.staging_operacoes_escrow não encontrada ({e}). Criando dataframe vazio.")
    df_escrow = spark.createDataFrame([], schema=StructType([StructField("cod_operacao", LongType(), True), StructField("ESCROW", StringType(), True)]))"""

content = content.replace(search_text_0_2, replace_text_0_2)


# --- Section 1.2 Modifications ---
# Find the start of Célula 1.2
start_marker_1_2 = "# Célula 1.2: Operações Enriquecidas"
# Find the end of Célula 1.2 (start of next metadata block)
end_marker_1_2 = "# print(\"DataFrames intermediários criados e cacheados.\")" # Actually, look for the print statement at the end of the cell
end_marker_code_1_2 = 'print("DataFrames intermediários criados e cacheados.")'

# Construct the new code block for Célula 1.2
new_code_1_2 = """# Célula 1.2: Operações Enriquecidas
# -----------------------------------------------------------
print("Criando DataFrame intermediário: Operações Enriquecidas...")
from pyspark.sql.functions import unix_timestamp, ceil, abs, hour, month, weekofyear, dayofmonth

# PRE-CALCULO: Data Primeira Operação por Cliente (para Meses de Idade)
df_first_op = df_operacoes_limpa.filter(col("status_aceite") == 'A') \\
    .groupBy("cod_cliente").agg(min("data_analise").alias("data_primeira_operacao_calc"))

# PRE-CALCULO: Taxa Cadastro do Cliente (do Contrato Ativo)
df_client_rate = df_contratos.filter(col("status") == 'A') \\
    .groupBy("cod_cliente").agg(max("fator").alias("taxa_cadastro_cliente"))

# PRE-CALCULO: Gerente Enriquecido (Nome e Comissão)
# df_gerentes tem cod_broker, cod_usuario, taxa_comissao (added in Silver Prep)
# df_usuarios tem cod_usuario, nome
df_gerentes_enrich = df_gerentes.join(df_usuarios, "cod_usuario", "left") \\
    .select(col("cod_broker"), col("taxa_comissao"), col("nome").alias("nome_gerente"))

# PASSO 1: Tratamento de Ambiguidade
# Renomeamos o cod_cliente da bridge para garantir unicidade no join
df_bridge_prep = df_bridge_gerente.withColumnRenamed("cod_cliente", "cod_cliente_bridge")

# Enriquecimento com Gerente (Broker)
df_operacoes_com_historico = df_operacoes_limpa.join(
    df_bridge_prep,
    (df_operacoes_limpa["cod_cliente"] == df_bridge_prep["cod_cliente_bridge"]) &
    (df_operacoes_limpa["data_analise"].cast("date") >= df_bridge_prep["data_inicio_vigencia"]) &
    (df_operacoes_limpa["data_analise"].cast("date") <= df_bridge_prep["data_fim_vigencia"]),
    "left"
)

df_operacoes_com_gerente = df_operacoes_com_historico.withColumn(
    "cod_broker",
    when((col("cod_broker").isNotNull()) & (col("cod_broker") != 0), col("cod_broker")).otherwise(col("cod_gerente"))
).drop("cod_cliente_bridge","cod_gerente", "data_inicio_vigencia", "data_fim_vigencia")

# Identificação de Operações Informais
df_chave_danfe = df_cad_geral_arquivos.filter(col("DESCRICAO") == 'CHAVEDANFE')
df_titulos_com_chave = df_titulos_limpa.join(df_chave_danfe, df_titulos_limpa.cod_titulo == df_chave_danfe.CODTITULO, how="inner")
df_operacoes_com_chave_base = df_operacoes_com_gerente.join(df_titulos_com_chave, on="cod_operacao", how="inner")

df_operacoes_com_chave_filtrado = df_operacoes_com_chave_base.filter(
    (df_operacoes_com_gerente["nota_servico"] == 'N') &
    (df_operacoes_com_gerente["status_analise"] == 'D') &
    (df_operacoes_com_gerente["cod_empresa"] == 14) &
    (df_operacoes_com_gerente["status_aceite"] == 'A') &
    (df_operacoes_com_gerente["tto"].isin(['NO','CM','FC']))
)

df_vcount = df_operacoes_com_chave_filtrado.groupBy(df_operacoes_com_gerente["cod_operacao"]).count()
df_com_vcount = df_operacoes_com_gerente.join(df_vcount, on="cod_operacao", how="left")

# Enriquecimento com Usuarios, Motivos e Estudo
# Definindo aliases para tabelas
df_ops = df_com_vcount.alias("ops")
df_u_inc = df_usuarios.alias("u_inc")
df_u_ana = df_usuarios.alias("u_ana")
df_u_trava = df_usuarios.alias("u_trava")
df_motivos = df_motivos_indeferimento.alias("motivos")
df_estudo = df_estudo_operacoes.alias("estudo")

df_ops_enrich_step1 = df_ops \\
    .join(df_u_inc, col("ops.usua_inclusao") == col("u_inc.cod_usuario"), "left") \\
    .join(df_u_ana, col("ops.usua_st_analise") == col("u_ana.cod_usuario"), "left") \\
    .join(df_u_trava, col("ops.usua_trava") == col("u_trava.cod_usuario"), "left") \\
    .join(df_motivos, col("ops.cod_indeferimento") == col("motivos.codindeferimento"), "left") \\
    .join(df_estudo, col("ops.cod_operacao") == col("estudo.CODOPERACAO"), "left") \\
    .join(df_gerentes_enrich, col("ops.cod_broker") == col("df_gerentes_enrich.cod_broker"), "left") \\
    .join(df_escrow, "cod_operacao", "left") \\
    .join(df_first_op, "cod_cliente", "left") \\
    .join(df_client_rate, "cod_cliente", "left") \\
    .select(
        col("ops.*"),
        col("u_inc.nome").alias("usuario_inclusao"),
        col("u_inc.nivel").alias("nivel_usuario_inclusao"),
        col("u_inc.funcao").alias("incluido_por"),
        col("u_ana.nome").alias("analista"),
        col("u_trava.nome").alias("analista_trava"),
        col("motivos.motivo_indeferimento"),
        col("motivos.grupo_motivo_indeferimento"),
        col("estudo.fator").alias("taxa_cadastro"),
        col("df_gerentes_enrich.taxa_comissao"),
        col("df_gerentes_enrich.nome_gerente").alias("gestor_da_operacao"),
        col("df_escrow.ESCROW").alias("flag_escrow"),
        col("df_first_op.data_primeira_operacao_calc"),
        col("df_client_rate.taxa_cadastro_cliente")
    )

df_operacoes_enriquecida = df_ops_enrich_step1.withColumn(
    "operacao_informal",
    when(
        ((col("count").isNull()) | (col("count") == 0)) & (col("cod_empresa") == 14) & (col("nota_servico") == 'N'),
        lit(True)
    ).otherwise(lit(False))
).withColumn("data_deferimento", to_date(col("data_analise"))) \\
 .withColumn("era", when(col("data_deferimento") > lit("2023-08-31"), "VALE S").otherwise("VALE N")) \\
 .withColumn("chave_base_cliente", concat(lit("40-"), col("cod_cliente"))) \\
 .withColumn("chave_base_operacao", concat(lit("40-"), col("cod_operacao"))) \\
 .withColumn("chave_base_empresa", concat(lit("40-"), col("cod_empresa"))) \\
 .withColumn("chave_ano_mes_base_empresa", concat(lit("40-"), col("cod_empresa"), lit("-"), year(col("data_deferimento")), lit("-"), month(col("data_deferimento")))) \\
 .withColumn("chave_meta", concat(col("chave_ano_mes_base_empresa"), lit("-"), col("gestor_da_operacao"))) \\
 .withColumn("ano_do_deferimento", year(col("data_deferimento"))) \\
 .withColumn("comissao_das_tarifas", col("taxa_comissao") * col("total_de_tarifas")) \\
 .withColumn("data_inicio_do_mes", to_date(date_add(last_day(date_add(col("data_deferimento"), -1)), 1))) \\
 .withColumn("dia_da_operacao", dayofmonth(col("data_deferimento"))) \\
 .withColumn("dia_da_semana_da_operacao", dayofweek(col("data_deferimento"))) \\
 .withColumn("dia_da_semana_da_operacao_por_extenso",
    when(col("dia_da_semana_da_operacao") == 2, "Segunda")
    .when(col("dia_da_semana_da_operacao") == 3, "Terça")
    .when(col("dia_da_semana_da_operacao") == 4, "Quarta")
    .when(col("dia_da_semana_da_operacao") == 5, "Quinta")
    .when(col("dia_da_semana_da_operacao") == 6, "Sexta")
    .otherwise(None)) \\
 .withColumn("faixa_de_tempo_de_analise_horas", abs(ceil((unix_timestamp(col("data_analise")) - unix_timestamp(col("data_inclusao")))/3600))) \\
 .withColumn("faixa_de_tempo_de_analise_minutos", abs(ceil((unix_timestamp(col("data_analise")) - unix_timestamp(col("data_inclusao")))/60))) \\
 .withColumn("tempo_de_analise_minutos", (unix_timestamp(col("data_analise")) - unix_timestamp(col("data_inclusao"))) / 60) \\
 .withColumn("hora_da_inclusao", hour(col("data_inclusao"))) \\
 .withColumn("meses_de_idade_do_cliente", floor(months_between(col("data_deferimento"), col("data_primeira_operacao_calc")))) \\
 .withColumn("semana_do_deferimento", weekofyear(col("data_deferimento"))) \\
 .withColumn("status_analisado_no_mesmo_dia", to_date(col("data_inclusao")) == to_date(col("data_analise"))) \\
 .withColumn("status_escrow", when(col("flag_escrow") == True, "sim").otherwise("não")) \\
 .withColumn("status_meta", lit("SIM")) \\
 .withColumn("status_taxa_majorada",
    when(col("taxa") > col("taxa_cadastro_cliente"), "MAJORADA")
    .when(col("taxa") < col("taxa_cadastro_cliente"), "REDUZIDA")
    .otherwise("MANTIDA")) \\
 .withColumn("tarifa_de_recompra", col("tarifa_recompra") * col("n_docs_recompra")) \\
 .withColumn("tarifa_de_titulos", col("n_docs") * col("tarifa")) \\
 .na.fill(0, subset=["tac", "valor_taxa_adm", "valor_advalorem", "total_de_tarifas", "n_docs_recompra"]) \\
 .drop("count").cache()

print("DataFrames intermediários criados e cacheados.")"""

# Replace the block
import re
pattern_1_2 = re.compile(r'# Célula 1\.2: Operações Enriquecidas.*?print\("DataFrames intermediários criados e cacheados\."\)', re.DOTALL)
content = pattern_1_2.sub(new_code_1_2, content)

# --- Section 2.1 Modifications ---
# Add new columns to selection
search_text_2_1 = """    col("valor_advalorem"),
    col("n_docs_recompra")
)"""

replace_text_2_1 = """    col("valor_advalorem"),
    col("n_docs_recompra"),
    col("chave_meta"),
    col("ano_do_deferimento"),
    col("comissao_das_tarifas"),
    col("data_inicio_do_mes"),
    col("dia_da_operacao"),
    col("dia_da_semana_da_operacao"),
    col("dia_da_semana_da_operacao_por_extenso"),
    col("faixa_de_tempo_de_analise_horas"),
    col("faixa_de_tempo_de_analise_minutos"),
    col("tempo_de_analise_minutos"),
    col("hora_da_inclusao"),
    col("meses_de_idade_do_cliente"),
    col("semana_do_deferimento"),
    col("status_analisado_no_mesmo_dia"),
    col("status_escrow"),
    col("status_meta"),
    col("status_taxa_majorada"),
    col("tarifa_de_recompra"),
    col("tarifa_de_titulos"),
    col("gestor_da_operacao")
)"""

content = content.replace(search_text_2_1, replace_text_2_1)

# Write back
with open("VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py", "w") as f:
    f.write(content)
