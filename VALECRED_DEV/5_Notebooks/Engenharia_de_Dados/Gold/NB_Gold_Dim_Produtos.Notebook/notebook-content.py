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
# META         },
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

# MARKDOWN ********************

# # Notebook de Construção da Dimensão Produtos (Gold)
# **Objetivo:** Criar a tabela `LH_Gold.dim_produtos` unificando a lógica de produtos (Monolito Power BI) na camada Gold.
# **Origem:** `LH_Silver.staging_operacoes_limpa` (TTO/STTO) e tabelas de domínio do Bronze (`tab_tipooperacao`, `tab_subtipooperacao`).

# CELL ********************

# Configuração e Imports
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, lit, concat, when, regexp_replace, upper, row_number, broadcast, trim, coalesce
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Iniciando construção da dim_produtos...")

# 1. Leitura dos dados
# Utilizamos staging_operacoes_limpa para obter as combinações TTO/STTO existentes
df_operacoes = spark.read.table("LH_Silver.staging_operacoes_limpa")
df_tto = spark.read.table("LH_Bronze.tab_tipooperacao")
df_stto = spark.read.table("LH_Bronze.tab_subtipooperacao")

# 2. Obter combinações únicas de TTO e STTO
df_prod_base = df_operacoes.select(col("tto"), col("stto")).distinct()

# 3. Join com descrições (Tipo e Subtipo)
# Aliasing para evitar ambiguidade na coluna DESCRICAO
df_tto_alias = df_tto.select(trim(col("CODTTO")).alias("CODTTO"), col("DESCRICAO").alias("desc_tipo"))
df_stto_alias = df_stto.select(trim(col("CODSTTO")).alias("CODSTTO"), col("DESCRICAO").alias("desc_subtipo"))


# Join robusto com Trim nas chaves
df_desc = df_prod_base \
    .join(broadcast(df_tto_alias), trim(df_prod_base.tto) == df_tto_alias.CODTTO, "left") \
    .join(broadcast(df_stto_alias), trim(df_prod_base.stto) == df_stto_alias.CODSTTO, "left") \
    .select(
        col("tto"),
        col("stto"),
        when(trim(col("tto")) == "NC", lit("NOTA COMERCIAL"))
        .otherwise(col("desc_tipo")).alias("tipo_produto"),
        col("desc_subtipo").alias("subtipo_produto")
    )

# 4. Transformações (Lógica do Power BI Monolito)

# Chave Produto
# Coalesce: Garante que chave_produto não seja nulo quando stto é nulo (ex: TTO='NC')
df_calc = df_desc.withColumn("chave_produto", concat(col("tto"), coalesce(col("stto"), lit(""))))

# 4.1. Incorporar Descrições de Produtos Ausentes (Manual Upload)
try:
    # Tabela de suporte manual para produtos que não existem no sistema legado
    df_produtos_ausentes = spark.read.table("LH_Silver.sup_produtos_ausentes")

    # Selecionar apenas colunas relevantes e renomear para evitar conflitos
    # Assumimos que sup_produtos_ausentes tem: tto, descricao (mapeado para tipo_produto)
    df_ausentes_lookup = df_produtos_ausentes.select(
        trim(col("codtto")).alias("tto_ausente"),
        col("descricao").alias("desc_ausente")
    )

    # Join para preencher tipo_produto nulo ou incorreto
    df_calc = df_calc.join(broadcast(df_ausentes_lookup), trim(df_calc.tto) == df_ausentes_lookup.tto_ausente, "left_outer") \
        .withColumn("tipo_produto", coalesce(col("tipo_produto"), col("desc_ausente"))) \
        .drop("tto_ausente", "desc_ausente")

except Exception as e:
    print(f"Aviso: Não foi possível carregar ou utilizar LH_Silver.sup_produtos_ausentes: {e}. Prosseguindo sem enrichment manual.")

# Coluna 'Produto'
# Lógica: Se subtipo nulo, usa tipo. Senão "Subtipo - Tipo"
df_calc = df_calc.withColumn("Produto",
    when(col("subtipo_produto").isNull(), col("tipo_produto"))
    .otherwise(concat(col("subtipo_produto"), lit(" - "), col("tipo_produto")))
)

# Replacements Específicos para 'Produto' (Ordem Importa)
# 1. Remover " - COMISSÁRIA" (Ex: "ABC - COMISSÁRIA" vira "ABC")
df_calc = df_calc.withColumn("Produto", regexp_replace(col("Produto"), " - COMISSÁRIA", ""))
# 2. Substituir "COMISSÁRIA" por "COMISSARIA SIMPLES"
df_calc = df_calc.withColumn("Produto", regexp_replace(col("Produto"), "COMISSÁRIA", "COMISSARIA SIMPLES"))

# Coluna 'Produto - Informação de Mercado'
df_calc = df_calc.withColumn("Produto - Informação de Mercado", col("Produto"))

# Replacements para Informação de Mercado
# NORMAL -> Desconto
df_calc = df_calc.withColumn("Produto - Informação de Mercado", regexp_replace(col("Produto - Informação de Mercado"), "NORMAL", "Desconto"))
# CGP -> Giro Parcelado
df_calc = df_calc.withColumn("Produto - Informação de Mercado", regexp_replace(col("Produto - Informação de Mercado"), "CGP - FLUXO DE CAIXA SECURITIZADORA", "Giro Parcelado"))
# MATERIA PRIMA -> Fomento
df_calc = df_calc.withColumn("Produto - Informação de Mercado", regexp_replace(col("Produto - Informação de Mercado"), "MATERIA PRIMA - FLUXO DE CAIXA SECURITIZADORA", "Fomento"))

# Uppercase
df_calc = df_calc.withColumn("Produto - Informação de Mercado", upper(col("Produto - Informação de Mercado")))

# Final Replacements em Informação de Mercado (Exceções)
# "NOTA DE SERVIÇO - CTE - DESCONTO" -> "DESCONTO - NOTA DE SERVIÇO - CTE"
df_calc = df_calc.withColumn("Produto - Informação de Mercado", regexp_replace(col("Produto - Informação de Mercado"), "NOTA DE SERVIÇO - CTE - DESCONTO", "DESCONTO - NOTA DE SERVIÇO - CTE"))
# "NOTA DE SERVIÇO - DESCONTO" -> "DESCONTO - NOTA DE SERVIÇO"
df_calc = df_calc.withColumn("Produto - Informação de Mercado", regexp_replace(col("Produto - Informação de Mercado"), "NOTA DE SERVIÇO - DESCONTO", "DESCONTO - NOTA DE SERVIÇO"))

# 5. Seleção Final e Padronização (Snake Case)
df_final_prep = df_calc.select(
    col("chave_produto"),
    col("Produto").alias("produto"),
    col("Produto - Informação de Mercado").alias("produto_informacao_de_mercado")
)

# Deduplicação Final por chave_produto (Segurança)
window_dedup = Window.partitionBy("chave_produto").orderBy(col("produto").asc())
df_dedup = df_final_prep.withColumn("rn", row_number().over(window_dedup)).filter(col("rn") == 1).drop("rn")

# Adicionar Surrogate Key (SK) - Ordenado por chave_produto
window_sk = Window.orderBy("chave_produto")
df_final = df_dedup.sort("chave_produto").withColumn("sk_produto", row_number().over(window_sk))

# Reordenar colunas
df_final = df_final.select("sk_produto", "chave_produto", "produto", "produto_informacao_de_mercado")

# 6. Escrita
output_path = "LH_Gold.dim_produtos"
df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path)
print(f"Tabela '{output_path}' criada com sucesso.")

# Visualização de verificação (Top 10)
# df_final.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
