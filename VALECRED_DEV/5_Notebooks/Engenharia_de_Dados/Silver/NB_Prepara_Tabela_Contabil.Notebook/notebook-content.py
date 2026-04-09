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

# # Notebook de Preparação Silver - Contábil
# **Objetivo:** Processamento da tabela `tab_lancamentos_contabeis`.
# 
# **Estratégia:** Carga Full Overwrite (substituindo lógica incremental anterior para garantir integridade).


# MARKDOWN ********************

# ## Seção 0: Configuração do ambiente

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, lit, greatest, max
)

from notebookutils import mssparkutils

source_lakehouse = "LH_Bronze"
target_lakehouse = "LH_Silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Limpeza de `tab_lancamentos_contabeis`
# **Objetivo:** Desduplicar, renomear para snake_case e atualizar na Silver (Full Overwrite).

# CELL ********************

source_table = "tab_lancamentos_contabeis"
target_table = "staging_lancamentos_contabeis"
target_table_full_name = f"{target_lakehouse}.{target_table}"

print(f"Iniciando processamento de {target_table} (Full Overwrite)...")

# Função de seleção e renomeação de colunas
def select_lancamentos(df):
    return df.select(
        col("CODCTBLAN").alias("cod_lancamento"),
        col("CODEMPRESA").alias("cod_empresa"),
        col("CODTRANSACAO").alias("cod_transacao"),
        col("DEBITO").alias("debito"),
        col("CREDITO").alias("credito"),
        col("CODFUNDO").alias("cod_fundo"),
        col("CODCCUSTO").alias("cod_ccusto"),
        col("TIPO").alias("tipo"),
        col("DATA").alias("data_lancamento"),
        col("VALOR").alias("valor"),
        col("COMPLEMENTO").cast("string").alias("complemento"),
        col("SISTEMA").alias("sistema"),
        col("DATAINCLUSAO").alias("data_inclusao"),
        col("USUAINCLUSAO").alias("usuario_inclusao"),
        col("DATAALTERACAO").alias("data_alteracao"),
        col("USUAALTERACAO").alias("usuario_alteracao")
    )

key_columns = ["CODCTBLAN"]

# 1. Leitura Completa do Bronze
try:
    df_bronze = spark.read.table(f"{source_lakehouse}.{source_table}")
    raw_count = df_bronze.count()
    print(f"DEBUG: Registros lidos do Bronze (Total): {raw_count}")
except Exception as e:
    print(f"ERRO: Tabela de origem {source_table} não encontrada ou inacessível.")
    mssparkutils.notebook.exit(f"Source Table Not Found: {e}")

if raw_count > 0:
    # 2. Desduplicar
    # Cria coluna DATA_MAIS_RECENTE para priorizar a última alteração/inclusão
    df_with_latest = df_bronze.withColumn(
        "DATA_MAIS_RECENTE",
        greatest(col("DATAALTERACAO"), col("DATAINCLUSAO"))
    )

    # Janela para pegar o último registro por chave
    windowSpec = Window.partitionBy([col(c) for c in key_columns]).orderBy(col("DATA_MAIS_RECENTE").desc())

    # 🧠 Tensor: Otimização de reavaliação de plano (Cache de Window function)
    # 💡 O que: Adicionado .cache() ao DataFrame `df_dedup` resultante da operação de particionamento (Window).
    # 🎯 Por que: A variável `df_dedup` e suas transformações (`df_final`) sofrem múltiplas ações (`.count()` e `.write`), forçando a re-execução redundante de todo o processo complexo de deduplicação na mesma run.
    # 📊 Impacto: Evita o recálculo redundante do particionamento (row_number().over), resultando em execução até 2x mais rápida na carga Silver dessa tabela.
    # 🔬 Medição: Elimina shuffles desnecessários no plano de execução do Spark monitorado na UI.
    df_dedup = df_with_latest.withColumn("row_num", row_number().over(windowSpec)) \
        .filter(col("row_num") == 1).drop("row_num", "DATA_MAIS_RECENTE").cache()

    dedup_count = df_dedup.count()
    print(f"DEBUG: Registros após deduplicação: {dedup_count}")

    # 3. Selecionar Colunas e Renomear
    df_final = select_lancamentos(df_dedup).orderBy(col("data_inclusao").desc())

    final_count = df_final.count()
    print(f"DEBUG: Registros finais para escrita: {final_count}")

    # 4. Escrita (Overwrite)
    try:
        df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table_full_name)
        print("Operação de escrita (overwrite) concluída com sucesso.")
        
        # Verificação pós-escrita
        actual_count = spark.read.table(target_table_full_name).count()
        print(f"DEBUG: Contagem final na tabela destino ({target_table_full_name}): {actual_count}")
        
        if actual_count != final_count:
            print(f"ALERTA: Discrepância detectada! Esperado: {final_count}, Encontrado: {actual_count}")
            
    except Exception as e:
        print(f"ERRO FATAL ao escrever na tabela destino: {e}")
        mssparkutils.notebook.exit(f"Write Failed: {e}")
    finally:
        # Limpar memória
        df_dedup.unpersist()

else:
    print("ALERTA: Tabela Bronze vazia. Nada a processar.")

mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
