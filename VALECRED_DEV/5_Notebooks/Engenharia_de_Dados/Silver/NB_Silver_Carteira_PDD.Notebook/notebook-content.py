# Fabric notebook source


# CELL ********************

# Fabric notebook source

# MARKDOWN ********************

# # Notebook de Preparação da Tabela Silver `carteira_pdd`
#  
# **Objetivo:** Este notebook traduz a lógica de um script Power Query M para PySpark, processando os dados brutos da controladoria (tabelas `ctrl_*`) e gerando a tabela final `carteira_pdd` na camada Silver.
#  
# **Fluxo:**
# 1.  **Carregamento de Dados:** Carrega todas as tabelas `ctrl_*` relevantes da camada Bronze, incluindo os arquivos de carteira, PDD e gestão.
# 2.  **União e Filtragem Inicial:** Une os múltiplos arquivos de carteira, filtra registros indesejados e prepara os tipos de dados.
# 3.  **Cálculo da Faixa de PDD:** Calcula o prazo atual e determina a faixa de PDD (A, B, C, etc.) para cada título.
# 4.  **Enriquecimento com Dados Externos:** Junta os dados com as tabelas de ajuste de PDD e de gestão para enriquecer as informações.
# 5.  **Cálculos Finais:** Calcula os valores finais de PDD e ajusta os nomes das colunas.
# 6.  **Gravação:** Salva o resultado final na tabela `LH_Silver.carteira_pdd`.

# MARKDOWN ********************

# ## Seção 1: Carregamento e Preparação dos Dados

# CELL ********************

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import *
from functools import reduce
import re

def safe_load_table(table_name, schema):
    """Carrega uma tabela Spark de forma segura, retornando um DataFrame vazio se a tabela não existir.

    Esta função tenta carregar uma tabela Delta usando `spark.read.table`. Se a operação
    falhar porque a tabela não foi encontrada, em vez de lançar uma exceção, ela cria
    e retorna um DataFrame Spark vazio com o schema fornecido. Para qualquer outro erro,
    a exceção original é relançada.

    Args:
        table_name (str): O nome completo da tabela a ser carregada (ex: 'LH_Bronze.minha_tabela').
        schema (StructType): O schema a ser usado para criar o DataFrame vazio
                             caso a tabela não exista.

    Returns:
        DataFrame: O DataFrame carregado da tabela ou um DataFrame vazio com o schema
                   especificado se a tabela não for encontrada.
    """
    try:
        return spark.read.table(table_name)
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print(f"AVISO: Tabela de dependência '{table_name}' não encontrada. Usando um DataFrame vazio.")
            return spark.createDataFrame([], schema)
        else:
            raise e

# Schemas para as tabelas de dependência
pdd_ajustes_schema = StructType([
    StructField("cpfcnpj", StringType(), True),
    StructField("ajuste_pdd", DoubleType(), True)
])
base_gestao_schema = StructType([
    StructField("cpfcnpj", StringType(), True),
    StructField("gestao", StringType(), True)
])
pdd_percent_schema = StructType([
    StructField("mes_ano", DateType(), True),
    StructField("faixa", StringType(), True),
    StructField("valor", DoubleType(), True)
])
base_bordero_schema = StructType([
    StructField("bordero", StringType(), True),
    StructField("gestao", StringType(), True)
])


# Nomes das tabelas de dependência
pdd_ajustes_table = "LH_Bronze.ctrl_pdd_ajustes"
base_gestao_table = "LH_Bronze.ctrl_lista_gestao"
pdd_percent_table = "LH_Bronze.ctrl_pdd_percentual"
base_bordero_table = "LH_Bronze.ctrl_base_bordero"

# Carregar tabelas de dependência de forma segura
df_pdd_ajustes = safe_load_table(pdd_ajustes_table, pdd_ajustes_schema)
df_base_gestao = safe_load_table(base_gestao_table, base_gestao_schema)
df_pdd_percent = safe_load_table(pdd_percent_table, pdd_percent_schema)
df_base_bordero = safe_load_table(base_bordero_table, base_bordero_schema)


# Carregar todas as tabelas de carteira
all_tables = spark.sql("SHOW TABLES IN LH_Bronze")
carteira_tables_to_load = [
    row.tableName for row in all_tables.collect() 
    if row.tableName.startswith("ctrl_carteira_carteira_em_aberto") or row.tableName.startswith("ctrl_carteira_carteira_liq_a_considerar_em_aberto")
]

if not carteira_tables_to_load:
    raise Exception("Nenhuma tabela de carteira (`ctrl_carteira_carteira_*`) foi encontrada na camada Bronze para processar.")

# Adicionar a coluna 'nome_da_origem' para extrair a data base
dfs = [
    spark.read.table(f"LH_Bronze.{table_name}")
         .withColumn("nome_da_origem", F.lit(re.sub(r'^ctrl_carteira_carteira_', '', table_name)))
    for table_name in carteira_tables_to_load
]
df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)

# ⚡ Bolt Optimization: Cache dataframe to prevent re-evaluation on downstream actions
# 💡 O que: Adicionado `df.cache()` ao DataFrame resultante de uniões múltiplas que é usado para log.
# 🎯 Por que: A ação `.count()` força a materialização do DataFrame. Sem o `.cache()`, todas as leituras e uniões seriam reexecutadas quando o DataFrame for usado nas agregações e filtragens seguintes.
# 📊 Impacto: Evita a re-leitura completa de N tabelas (onde N é o número de carteiras), resultando em tempo de execução drasticamente menor e I/O reduzido.
# 🔬 Medição: Elimina múltiplos estágios repetitivos no Spark UI, de O(N) operações de leitura para O(1).
df.cache()
print(f"Unidas {len(carteira_tables_to_load)} tabelas de carteira. Total de registros: {df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Transformações e Regras de Negócio

# CELL ********************

# 1. Filtragem inicial
df_filtered = df.filter(
    (F.col("plataforma") != "DIRETORIA") & 
    (F.col("plataforma") != "VALECRED 5.0") & 
    (F.col("nome_da_empresa") != "VALECRED DISTRESSED & SPECIAL SITS FIDC")
)

# 2. Remover colunas desnecessárias
cols_to_drop = [
    "plataforma", "codcliente", "cidade", "uf", "nome_do_gerente", "codtitulo", "tdoc", 
    "stto", "intercompany", "soma_de_prazo", "soma_de_desconto", "liquidacao", 
    "motivo", "nossonumero", "codbancocobr"
]
df_cleaned = df_filtered.drop(*cols_to_drop)

# 3. Criar a coluna 'Data Base' a partir do nome do arquivo
df_with_base_date = df_cleaned.withColumn(
    "data_base_str", 
    F.regexp_replace(F.col("nome_da_origem"), r"(em_aberto_|liq_a_considerar_em_aberto_)", "")
).withColumn(
    "data_base",
    F.last_day(F.to_date(F.col("data_base_str"), "yyyy_MM"))
).drop("data_base_str", "nome_da_origem")

# 4. Ajustar tipos de dados e extrair datas
df_typed = df_with_base_date.withColumn("dataaceite", F.to_date(F.col("dataaceite"))) \
                            .withColumn("vencimento", F.to_date(F.col("vencimento"))) \
                            .withColumn("vencprorrogado", F.to_date(F.col("vencprorrogado")))

# 5. Calcular 'Prazo Atual'
df_with_prazo = df_typed.withColumn(
    "prazo_atual", 
    F.datediff(F.col("vencprorrogado"), F.col("data_base"))
)

# 6. Adicionar 'Faixa PDD'
df_with_faixa = df_with_prazo.withColumn("faixa_pdd",
    F.when(F.col("prazo_atual") < -365, "WOP")
     .when(F.col("prazo_atual") < -120, "F")
     .when(F.col("prazo_atual") < -90, "E")
     .when(F.col("prazo_atual") < -60, "D")
     .when(F.col("prazo_atual") < -30, "C")
     .when(F.col("prazo_atual") < -5, "B")
     .otherwise("A")
)

# 7. Join com a tabela de percentuais de PDD
df_pdd_percent_renamed = df_pdd_percent.withColumnRenamed("mes_ano", "pdd_mes_ano") \
                                       .withColumnRenamed("faixa", "pdd_faixa") \
                                       .withColumnRenamed("valor", "pdd_percent_valor")

df_joined_pdd = df_with_faixa.join(
    df_pdd_percent_renamed,
    (F.date_trunc('month', F.col("data_base")) == F.date_trunc('month', F.col("pdd_mes_ano"))) & 
    (F.col("faixa_pdd") == F.col("pdd_faixa")),
    "left"
)

# 8. Calcular '% PDD' inicial
df_calc_pdd = df_joined_pdd.withColumn(
    "percent_pdd",
    F.when(F.col("tto") == "RN", 1.0).otherwise(F.col("pdd_percent_valor"))
).drop("pdd_mes_ano", "pdd_faixa", "pdd_percent_valor")

# 9. Join com a tabela de ajustes de PDD
df_joined_ajustes = df_calc_pdd.join(df_pdd_ajustes, "cpfcnpj", "left") \
                               .na.fill(0, ["ajuste_pdd"])

# 10. Calcular 'VALOR RN LIQUIDO'
df_with_valor_liquido = df_joined_ajustes.withColumn("valor_rn_liquido",
    F.when(F.col("tto") == "RN", F.col("soma_de_valordevido") - F.col("soma_de_desagio"))
     .when(F.col("soma_de_valordevido") > 0, F.col("soma_de_valordevido"))
     .otherwise(F.col("soma_de_valor"))
)

# 11. Calcular '% PDD' final e 'PDD'
df_with_pdd_final = df_with_valor_liquido.withColumn(
    "pdd_percent_final", F.col("percent_pdd") - F.col("ajuste_pdd")
).withColumn(
    "pdd", F.col("valor_rn_liquido") * F.col("pdd_percent_final")
).drop("percent_pdd", "ajuste_pdd")

# 12. Renomear empresas
df_renamed_empresas = df_with_pdd_final.withColumn("nome_da_empresa",
    F.when(F.col("nome_da_empresa") == "VALECRED FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS LP", "VALECRED FIDC")
     .when(F.col("nome_da_empresa") == "VALECRED SECURITIZADORA DE CREDITOS S/A", "VALECRED SEC")
     .when(F.col("nome_da_empresa") == "FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS TATUHY", "TATUHY FIDC")
     .otherwise(F.col("nome_da_empresa"))
)

# 13. Joins para obter a 'Gestão'
df_joined_gestao = df_renamed_empresas.join(
    df_base_gestao.withColumnRenamed("gestao", "gestao_cedente"), "cpfcnpj", "left"
)

df_joined_bordero = df_joined_gestao.join(
    df_base_bordero.withColumnRenamed("gestao", "gestao_bordero"),
    F.col("codoperacao") == F.col("bordero"),
    "left"
)

# 14. Calcular 'Gestão' final
df_with_gestao = df_joined_bordero.withColumn("gestao",
    F.when(F.col("gestao_bordero").isNotNull(), F.col("gestao_bordero"))
     .otherwise(F.col("gestao_cedente"))
).drop("gestao_cedente", "gestao_bordero", "bordero")

# 15. Filtrar registros onde DATAACEITE <= Data Base
df_final_filter = df_with_gestao.filter(F.col("dataaceite") <= F.col("data_base"))

# 16. Adicionar 'Situação' e renomear colunas finais
df_final = df_final_filter.withColumn("situacao",
    F.when(F.col("prazo_atual") < 0, "VENCIDO").otherwise("A VENCER")
).withColumnRenamed("soma_de_valor", "valor") \
 .withColumnRenamed("soma_de_valordevido", "valordevido") \
 .withColumnRenamed("soma_de_desagio", "desagio")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: Gravação na Camada Silver

# CELL ********************

# Salvar a tabela final na camada Silver
target_table = "LH_Silver.carteira_pdd"
df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

print(f"Tabela '{target_table}' salva com sucesso na camada Silver.")
df_final.display()

# Limpar memória
df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
