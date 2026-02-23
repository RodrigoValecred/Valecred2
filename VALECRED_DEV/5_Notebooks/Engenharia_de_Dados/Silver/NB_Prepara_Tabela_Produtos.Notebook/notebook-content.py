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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==============================================================================
# 1. LEITURA DAS TABELAS (Bronze/Silver)
# ==============================================================================
# Ajuste os caminhos conforme seu Lakehouse real
df_operacoes = spark.table("LH_Bronze.tab_operacoes") # Tabela de Fatos
df_tipos     = spark.table("LH_Bronze.tab_tipooperacao") # Tabela Auxiliar
df_subtipos  = spark.table("LH_Bronze.tab_subtipooperacao") # Tabela Auxiliar

# ==============================================================================
# 2. SELEÇÃO E DISTINCT (A base dos produtos únicos)
# ==============================================================================
# Equivalente ao Table.SelectColumns e Table.Distinct do M
# ORIGINAL (FILTRO IMPLÍCITO): df_base = df_operacoes.select("TTO", "STTO").distinct()

# NOVO (TODOS OS PRODUTOS CADASTRADOS):
# 1. Produtos usados (Histórico) - Garantindo String
df_base_ops = df_operacoes.select(F.col("TTO").cast("string"), F.col("STTO").cast("string"))

# 2. Todos os Tipos (Sem Subtipo)
# Cast explícito para string no None para garantir compatibilidade no Union
df_all_types = df_tipos.select(F.col("CODTTO").cast("string").alias("TTO"), F.lit(None).cast("string").alias("STTO"))

# 3. Todos os Subtipos (Se houver vínculo CODTTO na tabela de subtipos)
# Verificação dinâmica de coluna para evitar quebra se o schema mudar
has_codtto = "CODTTO" in [c.upper() for c in df_subtipos.columns]

if has_codtto:
    print("Link CODTTO encontrado em tab_subtipooperacao. Incluindo subtipos cadastrados...")
    df_all_subtypes = df_subtipos.select(F.col("CODTTO").cast("string").alias("TTO"), F.col("CODSTTO").cast("string").alias("STTO"))

    # Union All dos 3 Dataframes
    df_base = df_base_ops.unionByName(df_all_types).unionByName(df_all_subtypes).distinct()
else:
    print("Link CODTTO NÃO encontrado em tab_subtipooperacao. Incluindo apenas Tipos cadastrados...")

    # Union All dos 2 Dataframes
    df_base = df_base_ops.unionByName(df_all_types).distinct()

# df_base.show(5)
# ==============================================================================
# 3. JOINS (Enriquecimento com as descrições)
# ==============================================================================
# Join com Tipos (CODTTO)
df_join_1 = df_base.join(df_tipos, df_base.TTO == df_tipos.CODTTO, "left") \
    .select(df_base["*"], df_tipos["DESCRICAO"].alias("desc_tipo"))
# df_join_1.show(5)
# Join com Subtipos (CODSTTO)
df_join_2 = df_join_1.join(df_subtipos, df_join_1.STTO == df_subtipos.CODSTTO, "left") \
    .select(df_join_1["*"], df_subtipos["DESCRICAO"].alias("desc_subtipo"))
# df_join_2.show(5)
# ==============================================================================
# 4. CRIAÇÃO DA COLUNA "PRODUTO" (Lógica de Negócio)
# ==============================================================================
# Lógica: Se subtipo é nulo, usa tipo. Senão, concatena "Subtipo - Tipo"
df_trata_nome = df_join_2.withColumn(
    "Produto_Raw",
    F.when(
        F.col("desc_subtipo").isNull(), 
        F.col("desc_tipo")
    ).otherwise(
        F.concat(F.col("desc_subtipo"), F.lit(" - "), F.col("desc_tipo"))
    )
)

# Limpezas específicas (Table.ReplaceValue do M)
df_limpo = df_trata_nome \
    .withColumn("Produto", F.regexp_replace("Produto_Raw", " - COMISSÁRIA", "")) \
    .withColumn("Produto", F.regexp_replace("Produto", "COMISSÁRIA", "COMISSARIA SIMPLES"))

# ==============================================================================
# 5. INFORMAÇÃO DE MERCADO (Regras de Tradução)
# ==============================================================================
# Criando a coluna duplicada e aplicando as regras do seu script
df_mercado = df_limpo.withColumn("Produto_Mercado", F.col("Produto"))

# Aplicação sequencial das regras (ReplaceValue)
df_mercado = df_mercado \
    .withColumn("Produto_Mercado", F.regexp_replace("Produto_Mercado", "NORMAL", "Desconto")) \
    .withColumn("Produto_Mercado", F.regexp_replace("Produto_Mercado", "CGP - FLUXO DE CAIXA SECURITIZADORA", "Giro Parcelado")) \
    .withColumn("Produto_Mercado", F.regexp_replace("Produto_Mercado", "MATERIA PRIMA - FLUXO DE CAIXA SECURITIZADORA", "Fomento")) \
    .withColumn("Produto_Mercado", F.upper(F.col("Produto_Mercado"))) # Transformar em Maiúsculas

# Ajustes finais de texto (Nota de Serviço)
df_mercado = df_mercado \
    .withColumn("Produto_Mercado", F.regexp_replace("Produto_Mercado", "NOTA DE SERVIÇO - CTE - DESCONTO", "DESCONTO - NOTA DE SERVIÇO - CTE")) \
    .withColumn("Produto_Mercado", F.regexp_replace("Produto_Mercado", "NOTA DE SERVIÇO - DESCONTO", "DESCONTO - NOTA DE SERVIÇO"))

# ==============================================================================
# 6. CRIAÇÃO DAS CHAVES (O Pulo do Gato para a IA 🧠)
# ==============================================================================

# Chave de Texto (Legado Power BI)
df_final = df_mercado.withColumn(
    "chave_produto_txt", 
    F.concat(
        F.col("TTO"), 
        F.coalesce(F.col("STTO"), F.lit(""))
    )
)

# --- AQUI ESTÁ A MASTERIZAÇÃO ---
# Criamos um ID Numérico sequencial para cada produto único.
# Isso permite que a IA (Sklearn) entenda "Giro Parcelado" como o número 4, por exemplo.
janela = Window.orderBy("Produto_Mercado")

df_final = df_final.withColumn(
    "cod_produto_ia", 
    F.dense_rank().over(janela)
)

# Seleção final das colunas úteis
df_dimensao_final = df_final.select(
    "cod_produto_ia",     # <--- O ID que a V.A.I. vai amar
    "chave_produto_txt",  # Para joins legados se precisar
    "TTO",
    "STTO",
    "Produto",            # Nome Técnico
    "Produto_Mercado"     # Nome Comercial
).distinct()

# ==============================================================================
# 7. SALVAR NO LAKEHOUSE
# ==============================================================================
df_dimensao_final.write.mode("overwrite").format("delta").saveAsTable("LH_Silver.Dim_Produto")

print("✅ Tabela Dim_Produto criada e padronizada!")
display(df_dimensao_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
