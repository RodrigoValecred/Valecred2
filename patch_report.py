file_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Relatorio_Produtos_Mensal.Notebook/notebook-content.py"

with open(file_path, "r") as f:
    content = f.read()

# Update process_operacoes_stream
new_process_ops = """def process_operacoes_stream(df_ops, df_titulos):
    print("Processando Operações...")

    # Preparar Títulos para cálculo de Prazo Ponderado da Operação
    # Agregamos por operação primeiro

    # Enriquecer Títulos com Data de Deferimento da Operação (para cálculo do Prazo Original)
    df_titulos_dates = df_titulos.join(df_ops.select("cod_operacao", "data_aceite"), "cod_operacao", "inner") \\
        .withColumn("prazo_original_dias", datediff(col("vencimento"), col("data_aceite"))) \\
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
    df_stream_ops = df_ops_enrich \\
        .withColumn("mes_ref", trunc(col("data_deferimento"), "MM")) \\
        .groupBy("mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento", "floating") \\
        .agg(
            sum("valor_de_face").alias("volume"),
            sum("soma_valor_prazo_op").alias("total_valor_prazo_mes"),
            sum("soma_valor_prazo_original_op").alias("total_valor_prazo_original_mes"),
            sum("receita_total_op").alias("receita"),
            count("cod_operacao").alias("qtd_eventos")
        ) \\
        .withColumn("tipo_produto", lit("OPERACOES")) \\
        .withColumn("prazo_medio",
                    when(col("volume") > 0, col("total_valor_prazo_mes") / col("volume")).otherwise(0)) \\
        .withColumn("prazo_medio_original",
                    when(col("volume") > 0, col("total_valor_prazo_original_mes") / col("volume")).otherwise(0)) \\
        .withColumn("taxa_media",
                    when(col("total_valor_prazo_mes") > 0,
                        (col("receita") / (col("total_valor_prazo_mes") / 30)) * 100
                    ).otherwise(0)) \\
        .withColumnRenamed("chave_produto", "sub_tipo_produto") \\
        .drop("total_valor_prazo_mes", "total_valor_prazo_original_mes")

    return df_stream_ops"""

# We also need to change df_map_ops in load_and_prepare_data so that floating is mapped to PR/Mora? No, they don't have floating.
# But `floating` is selected in groupBy, so df_union will fail if other streams don't have `floating`.
# Let's add `lit(None).cast("double").alias("floating")` to the other streams.

import re

# Replace process_operacoes_stream
content = re.sub(r'def process_operacoes_stream.*?return df_stream_ops', new_process_ops, content, flags=re.DOTALL)

# Update prorrogacoes groupBy
content = content.replace(
    '.groupBy("cod_cliente", "mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento")',
    '.groupBy("mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento")'
)

content = content.replace(
    '.withColumn("prazo_medio_original", lit(None))',
    '.withColumn("prazo_medio_original", lit(None).cast("double")) \\\n        .withColumn("floating", lit(None).cast("double"))'
)

# Replace consolidation
consolidation = """# Union dos Streams
df_union = df_stream_ops.unionByName(df_stream_prorrog).unionByName(df_stream_mora)

df_final = df_union \\
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
    ) \\
    .orderBy("mes_referencia", "tipo_produto")"""

content = re.sub(r'# Union dos Streams.*?\.orderBy\("mes_referencia", "nome_cliente", "tipo_produto"\)', consolidation, content, flags=re.DOTALL)

with open(file_path, "w") as f:
    f.write(content)

print("Patched report notebook.")
