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

# MARKDOWN ********************

# 
# 
# # Notebook de Preparação Silver - Títulos e Cobrança
# **Objetivo:** Processamento da tabela `tab_titulos` e tabelas relacionadas (`baixas`, `protestos`, `abatimentos`, `boletos`, `danfe`).
# 
# **Estratégia:** Implementa carga incremental para tabelas volumosas (`titulos`, `baixas`) e carga full para tabelas menores.

# MARKDOWN ********************

# ## Seção 0: Configuração do ambiente
# **Descrição:** Importa bibliotecas e define configurações do Spark.

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, when, lit, concat, length, regexp_replace,
    collect_list, concat_ws, upper, greatest, substring, year,
    lead, date_add, lag, max, coalesce, date_sub, datediff, current_date
)
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from delta.tables import *
from notebookutils import mssparkutils
import datetime

source_lakehouse = "LH_Bronze"
target_lakehouse = "LH_Silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Limpeza de `tab_titulos` (Incremental)
# **Objetivo:** Desduplicar e atualizar a tabela de títulos na Silver.

# CELL ********************

source_table_titulos = "tab_titulos"
target_table_titulos = "staging_titulos_limpa"
output_path_titulos = f"{target_lakehouse}.{target_table_titulos}"

print(f"Iniciando processamento de {target_table_titulos}...")

# Lógica de seleção de colunas comum
def select_titulos(df):
    return df.select(
        col("CODTITULO").alias("cod_titulo"),
        col("CODOPERACAO").alias("cod_operacao"),
        col("NDOC").alias("n_doc"),
        col("TDOC").alias("t_doc"),
        col("VENCIMENTO").alias("vencimento"),
        col("VENCPRORROGADO").alias("venc_prorrogado"),
        col("PRAZO").alias("prazo"),
        col("CPFCNPJSACADO").alias("cpf_cnpj_sacado"),
        col("CPFCNPJCEDENTE").alias("cpf_cnpj_cedente"),
        col("VALOR").alias("valor"),
        col("DESAGIO").alias("desagio"),
        col("LIQUIDO").alias("liquido"),
        col("AMORTIZACOES").alias("amortizacoes"),
        col("VALORDEVIDO").alias("valor_devido"),
        col("LIQUIDACAO").alias("liquidacao"),
        col("ACEITO").alias("aceito"),
        col("CODBANCOCOBR").alias("cod_banco_cobr"),
        col("DATACONF").alias("data_conf"),
        col("USUACONF").alias("usua_conf"),
        col("DATAALTERACAO").alias("data_alteracao"), # Adicionado para controle incremental
        col("DATAINCLUSAO").alias("data_inclusao"),
        col("DOCCONFIRMADO").alias("doc_confirmado"),
        col("MOTIVO").alias("motivo"),
        col("PRACA").alias("praca"),
        col("CHAVEDANFE").alias("chave_danfe"),
        col("NOSSONUMERO").alias("nosso_numero"),
        col("CODFUNDO").alias("cod_fundo"),
        col("TTO").alias("tipo_cobranca"),
        col("FILIAL").alias("raiz_cnpj"),
        col("CODEMISSAO").alias("cod_emissao"),
        col("STATUSCONFIRMACAO").alias("status_confirmacao"),
        col("SEUNUMERO").alias("seu_numero_bancario"),
        col("CODREMESSA").alias("cod_remessa"),
        # 1. Definir qual vencimento vale (Prorrogado ganha do Original)
        # Se VENCPRORROGADO for nulo, usa VENCIMENTO.
        # Criamos essa coluna calculada para facilitar contas futuras.
        coalesce(col("VENCPRORROGADO"), col("VENCIMENTO")).alias("vencimento_efetivo"),
        
        # 2. Calcular o Atraso (Regra de Negócio FIDC)
        # Se LIQUIDACAO existe (Pago) -> Diff entre LIQUIDACAO e VENCIMENTO_EFETIVO
        # Se LIQUIDACAO nula (Aberto) -> Diff entre HOJE e VENCIMENTO_EFETIVO
        (when(
            col("LIQUIDACAO").isNotNull(),
            datediff(col("LIQUIDACAO"), coalesce(col("VENCPRORROGADO"), col("VENCIMENTO")))
        ).otherwise(
            datediff(current_date(), coalesce(col("VENCPRORROGADO"), col("VENCIMENTO")))
        )).alias("dias_atraso"),
        
        # 3. Flag simples de Status (Opcional, mas útil para BI)
        (when(col("LIQUIDACAO").isNotNull(), "LIQUIDADO")
         .when(datediff(current_date(), coalesce(col("VENCPRORROGADO"), col("VENCIMENTO"))) > 0, "EM ATRASO")
         .otherwise("EM DIA")
        ).alias("status_titulo")
    )

def deduplicate_titulos(df, key_columns):
    """
    Deduplicates the dataframe based on the latest activity date.
    Prioritizes DATAALTERACAO, DATAINCLUSAO, and LIQUIDACAO.
    """
    df_with_latest = df.withColumn(
        "DATA_MAIS_RECENTE",
        greatest(col("DATAALTERACAO"), col("DATAINCLUSAO"), col("LIQUIDACAO"))
    )
    windowSpec = Window.partitionBy([col(c) for c in key_columns]).orderBy(col("DATA_MAIS_RECENTE").desc())
    return df_with_latest.withColumn("row_num", row_number().over(windowSpec)) \
        .filter(col("row_num") == 1).drop("row_num", "DATA_MAIS_RECENTE")

key_columns_titulos = ["CODTITULO"]

# Verifica se a tabela existe e se tem a coluna necessária para incremental (Schema Evolution Check)
is_incremental_possible = False
if DeltaTable.isDeltaTable(spark, output_path_titulos):
    try:
        # Checar se há nova coluna snake_case e data_alteracao
        target_cols = spark.read.format("delta").load(output_path_titulos).columns
        if "cod_titulo" in target_cols and "data_alteracao" in target_cols:
            is_incremental_possible = True
        else:
            print("Schema mismatch (cod_titulo or data_alteracao missing). Forcing Full Load.")
            is_incremental_possible = False
    except AnalysisException:
        print("Error accessing target table. Forcing Full Load.")
        is_incremental_possible = False
else:
    print("Tabela destino não existe. Forçando Full Load.")
    is_incremental_possible = False

if is_incremental_possible:
    print("Modo Incremental: Detectando alterações...")
    delta_table = DeltaTable.forPath(spark, output_path_titulos)
    
    # 1. Obter Watermark (Maior Data de Alteração/Inclusão na Silver)
    # Proteção contra tabela vazia ou nulos
    watermark_row = spark.read.format("delta").load(output_path_titulos) \
        .select(greatest(max("data_inclusao"), max("data_alteracao")).alias("max_date")) \
        .collect()
        
    last_watermark = "1900-01-01"
    if watermark_row and watermark_row[0][0]:
        last_watermark = watermark_row[0][0]
    
    print(f"Watermark aplicado: {last_watermark}")
    
    # 2. Ler Bronze filtrado
    df_bronze_titulos = spark.read.table(f"{source_lakehouse}.{source_table_titulos}") \
        .filter((col("DATAINCLUSAO") >= last_watermark) | (col("DATAALTERACAO") >= last_watermark))
    
    # 🧠 Tensor Optimization: Substituir count() > 0 por not df.isEmpty() para evitar varredura completa dos dados
    if not df_bronze_titulos.isEmpty():
        # 3. Desduplicar o batch incremental
        df_dedup = deduplicate_titulos(df_bronze_titulos, key_columns_titulos)
            
        df_final_batch = select_titulos(df_dedup)
        
        # 4. Merge
        print("Executando Merge...")
        delta_table.alias("t").merge(
            df_final_batch.alias("s"),
            "t.cod_titulo = s.cod_titulo"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("Merge concluído.")
    else:
        print("Nenhum dado novo encontrado para processar.")
        
else:
    print("Modo Full Load: Carga Inicial ou Atualização de Schema.")
    df_bronze_titulos = spark.read.table(f"{source_lakehouse}.{source_table_titulos}")
    
    df_dedup = deduplicate_titulos(df_bronze_titulos, key_columns_titulos)
    
    df_titulos_final = select_titulos(df_dedup).orderBy(col("data_inclusao").desc())
    
    # OverwriteSchema garante que a nova coluna data_alteracao seja adicionada
    df_titulos_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_titulos)
    print("Carga Full concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Processamento Chave DANFE
# **Dependência:** `staging_titulos_limpa`


# CELL ********************

danfe_target_table = "staging_chave_danfe_detalhada"
print(f"Processando {danfe_target_table}...")

df_titulos_danfe = spark.table(output_path_titulos)

df_chave_filtrada = df_titulos_danfe \
    .select(col("chave_danfe").alias("CHAVEDANFE")) \
    .na.drop(subset=["CHAVEDANFE"]) \
    .filter((col("CHAVEDANFE") != "") & (length(col("CHAVEDANFE")) == 44)) \
    .filter(~col("CHAVEDANFE").contains("XML NF-E")) \
    .withColumn("CHAVEDANFE", regexp_replace(col("CHAVEDANFE"), " ", "0")) \
    .distinct()

df_detalhada = df_chave_filtrada \
    .withColumn("uf", substring(col("CHAVEDANFE"), 1, 2)) \
    .withColumn("aamm", substring(col("CHAVEDANFE"), 3, 4)) \
    .withColumn("cnpj", substring(col("CHAVEDANFE"), 7, 14)) \
    .withColumn("modelo", substring(col("CHAVEDANFE"), 21, 2)) \
    .withColumn("serie", substring(col("CHAVEDANFE"), 23, 3)) \
    .withColumn("numero_nf", substring(col("CHAVEDANFE"), 26, 9)) \
    .withColumn("codigo_nf", substring(col("CHAVEDANFE"), 35, 9)) \
    .withColumn("dv", substring(col("CHAVEDANFE"), 44, 1)) \
    .withColumnRenamed("CHAVEDANFE", "chave_danfe")

df_detalhada.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.{danfe_target_table}")
print("Tabela DANFE salva.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: Limpeza de `tab_titulos_baixas` (Incremental)

# CELL ********************

target_table_baixas = "staging_baixas_limpa"
output_path_baixas = f"{target_lakehouse}.{target_table_baixas}"
source_table_baixas = "tab_titulos_baixas"
print(f"Iniciando processamento de {target_table_baixas}...")

# Lógica de seleção de colunas para Baixas
def select_baixas(df):
    return df.select(
        col("CODTITULOBAIXAS").alias("cod_titulo_baixas"),
        col("CODTITULO").alias("cod_titulo"),
        col("DATAINCLUSAO").alias("data_inclusao"),
        col("DATAALTERACAO").alias("data_alteracao"),
        col("VLPAGO").alias("valor_pago"),
        col("DATABAIXA").alias("data_baixa"),
        col("DATABAIXASIST").alias("data_baixa_sist"),
        col("DESCONTO").alias("desconto"),
        col("JUROS").alias("juros"),
        col("TARIFARECOMPRA").alias("tarifa_recompra"),
        col("DATAVENCIMENTO").alias("data_vencimento"),
        col("PAGOPELO").alias("pago_pelo"),
        col("FORMA").alias("forma"),
        col("TIPOBAIXA").alias("tipo_baixa"),
        col("MOTIVO").alias("motivo"),
        col("CODOPERACAO").alias("cod_operacao")
    )

is_incremental_baixas = False
if DeltaTable.isDeltaTable(spark, output_path_baixas):
    # Checar se coluna snake_case existe
    if "cod_titulo_baixas" in spark.read.format("delta").load(output_path_baixas).columns:
        is_incremental_baixas = True
    else:
        print("Schema mismatch for Baixas. Forcing Full Load.")

if is_incremental_baixas:
    print("Modo Incremental: Baixas")
    delta_table_baixas = DeltaTable.forPath(spark, output_path_baixas)
    
    # Watermark baseada em DATAINCLUSAO (baixas geralmente são imutáveis/append)
    watermark_row_b = spark.read.format("delta").load(output_path_baixas) \
        .agg(max("data_inclusao").alias("max_date")).collect()
    
    last_watermark_b = "1900-01-01"
    if watermark_row_b and watermark_row_b[0][0]:
        last_watermark_b = watermark_row_b[0][0]
        
    print(f"Watermark Baixas: {last_watermark_b}")
    
    df_bronze_baixas = spark.read.table(f"{source_lakehouse}.{source_table_baixas}") \
        .filter(col("DATAINCLUSAO") >= last_watermark_b)
        
    # 🧠 Tensor Optimization: Substituir count() > 0 por not df.isEmpty() para evitar varredura completa dos dados
    if not df_bronze_baixas.isEmpty():
        key_cols_baixa = ["CODTITULOBAIXAS"]
        window_baixa = Window.partitionBy([col(c) for c in key_cols_baixa]).orderBy(col("DATAINCLUSAO").desc())
        df_baixas_dedup = df_bronze_baixas.withColumn("row_num", row_number().over(window_baixa)) \
            .filter(col("row_num") == 1).drop("row_num")

        # Aplicar renomeação explicita
        df_baixas_final = select_baixas(df_baixas_dedup)
            
        delta_table_baixas.alias("t").merge(
            df_baixas_final.alias("s"),
            "t.cod_titulo_baixas = s.cod_titulo_baixas"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("Merge Baixas concluído.")
    else:
        print("Sem novas baixas.")
else:
    print("Modo Full Load: Baixas")
    df_baixas = spark.read.table(f"{source_lakehouse}.{source_table_baixas}")
    key_cols_baixa = ["CODTITULOBAIXAS"]
    window_baixa = Window.partitionBy([col(c) for c in key_cols_baixa]).orderBy(col("DATAINCLUSAO").desc())
    df_baixas_desduplicada = df_baixas.withColumn("row_num", row_number().over(window_baixa)) \
                                        .filter(col("row_num") == 1).drop("row_num")

    # Aplicar renomeação explicita
    df_baixas_final = select_baixas(df_baixas_desduplicada)

    df_baixas_final.write.mode("overwrite").option("overwriteSchema","true").saveAsTable(output_path_baixas)
    print("Carga Full Baixas concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 4: Protestos, Abatimentos, Notificações, Boletos
# **Estratégia:** Carga Full (Tabelas menores ou lógica complexa de join)

# CELL ********************

# 4.1 Protestos
print("Processando Protestos...")
df_ocorrencias = spark.read.table("LH_Bronze.rlc_titulos_ocorrencias_cobranca")
df_titulos_cobranca = spark.read.table("LH_Bronze.tab_titulos_cobranca")

# (Lógica original de protestos mantida integralmente para garantir corretude de negócio)
df_titulos_para_protesto_cobranca = df_titulos_cobranca \
    .filter(col("CODOCORCOBRANCA") == 1015).select("CODTITULO").distinct().withColumn("flag_protesto_cobranca", lit(True))
df_subquery_ocorrencia = df_ocorrencias \
    .filter(col("CODOCORINTERNA").isin(8, 34) & col("CODOCORCOBRBANCO").isin(19, 23)).select("CODTITULO").distinct().withColumn("flag_subquery_ocorrencia", lit(True))
df_ocorrencias_filtradas = df_ocorrencias.filter(
    ((col("CODOCORINTERNA").isin(8, 17, 34, 2, 82)) & (col("CODOCORCOBRBANCO").isin(6, 19, 23, 10, 43)) & (col("TOCORRENCIA") == 2)) |
    ((col("CODOCORINTERNA") == 8) & (col("CODOCORCOBRBANCO") == 9) & (col("TOCORRENCIA") == 1))
)
window_spec_latest = Window.partitionBy("CODTITULO").orderBy(col("CODTITULOOCORCOB").desc())
df_latest_ocorrencia = df_ocorrencias_filtradas \
    .withColumn("row_num", row_number().over(window_spec_latest)).filter(col("row_num") == 1).drop("row_num") \
    .join(df_titulos_para_protesto_cobranca, "CODTITULO", "left") \
    .join(df_subquery_ocorrencia, "CODTITULO", "left") \
    .fillna(False, subset=["flag_protesto_cobranca", "flag_subquery_ocorrencia"])

cond_p1 = (substring(col("MOTIVOCODOCORCOBRBANCO"), 1, 2) == '14')
cond_p2 = (col("CODOCORINTERNA") == 2) & (col("flag_subquery_ocorrencia") == True)
cond_p3 = (col("CODOCORINTERNA") == 82)
cond_p4 = (col("flag_protesto_cobranca") == True)
cond_e = (col("CODOCORINTERNA") == 8) & (col("CODOCORCOBRBANCO") == 9)
cond_i = (col("CODOCORINTERNA") == 8)
cond_c = (col("CODOCORINTERNA") == 34)

df_com_status_code = df_latest_ocorrencia.withColumn("STATUSPROTESTO",
    when(cond_p1 | cond_p2 | cond_p3 | cond_p4, lit("P")).when(cond_e, lit("E")).when(cond_i, lit("I")).when(cond_c, lit("C")).otherwise(lit("N")))
# Seleção final com renomeação para snake_case
df_final_protestos = df_com_status_code.withColumn("status_protesto",
    when(col("STATUSPROTESTO") == 'P', lit("Protestado"))
    .when(col("STATUSPROTESTO") == 'E', lit("Instrução Protesto Enviada"))
    .when(col("STATUSPROTESTO") == 'I', lit("Instrução Protesto"))
    .when(col("STATUSPROTESTO") == 'C', lit("Em Cartório"))
    .otherwise(lit("N/A"))
).filter(col("status_protesto") != "N/A") \
 .select(
    col("CODTITULO").alias("cod_titulo"),
    col("status_protesto"),
    col("DATAINCLUSAO").alias("data_ocorrencia_protesto")
 )

df_final_protestos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_protestos")

# 4.2 Abatimentos
print("Processando Abatimentos...")
df_abatimentos = spark.read.table("LH_Bronze.tab_titulos_abatimento")
df_abatimentos.select(
    col("CODTITULOABAT").alias("cod_titulo_abat"), col("CODOPERACAO").alias("cod_operacao"), col("CODTITULO").alias("cod_titulo"), col("CODOPERACAOAB").alias("cod_operacao_ab"), col("VALORDEVIDO").alias("valor_devido"), col("ABATIMENTO").alias("abatimento"), col("DATAINCLUSAO").alias("data_inclusao"), col("CODBANCOCOBR").alias("cod_banco_cobr"), col("USUAINCLUSAO").alias("usua_inclusao")
).withColumn("data", col("data_inclusao").cast("date")).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_abatimentos")

# 4.3 Notificações
print("Processando Notificações...")
df_notificacoes = spark.read.table("LH_Bronze.tab_titulos_cobranca")
df_notificacoes.filter(col("CODOCORCOBRANCA") == 12).select(
    col("CODOPERACAO").alias("cod_operacao"), col("CODTITULO").alias("cod_titulo"), col("COBRADOAO").alias("cobrado_ao"), col("CODOCORCOBRANCA").alias("cod_ocor_cobranca"), regexp_replace(regexp_replace(col("OBSERVACAO").cast("string"), "&ccedil;", "ç"), "&atilde;", "ã").alias("observacao"), col("DATAINCLUSAO").alias("data_inclusao"), col("USUAINCLUSAO").alias("usua_inclusao")
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_notificacoes")

# 4.4 Boletos
print("Processando Boletos...")
df_titulos_limpa = spark.table(output_path_titulos)
df_titulos_limpa \
    .filter(col("t_doc") == "BL") \
    .filter(col("data_inclusao").cast("string") >= "2021-01-01") \
    .drop("venc_prorrogado", "prazo", "desagio", "data_conf", "usua_conf", "doc_confirmado", "chave_danfe") \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_boletos_titulos")

print("Limpeza Silver - Títulos finalizada.")
mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
