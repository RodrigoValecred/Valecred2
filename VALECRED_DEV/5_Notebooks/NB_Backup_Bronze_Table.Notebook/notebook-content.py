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
# META         },
# META         {
# META           "id": "ee40705b-0100-49bc-8f35-81d71839f042"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Célula 0: Configuração da Sessão Spark
# ------------------------------------

# Corrige o problema de LEITURA de datas antigas (formato LEGACY do parquet)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

# Corrige o problema de ESCRITA de datas antigas (formato LEGACY do parquet)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Notebook de Backup de Tabela Bronze
# **Objetivo:** Este notebook cria uma cópia de segurança de uma tabela específica na camada Bronze.

# CELL ********************

from pyspark.sql import SparkSession
from datetime import datetime

# --- CONFIGURAÇÃO ---
SOURCE_TABLE_NAME = "tab_titulos"
# Para este pedido específico, a data foi fixada em 2025-09-29
BACKUP_DATE_STR = "20250929"
BACKUP_TABLE_NAME = f"tab_titulos_bckp_{BACKUP_DATE_STR}"

print(f"Iniciando o processo de backup...")
print(f"Tabela de Origem: '{SOURCE_TABLE_NAME}'")
print(f"Tabela de Destino (Backup): '{BACKUP_TABLE_NAME}'")


# --- LEITURA DA TABELA DE ORIGEM ---
# O notebook já está configurado para usar o Lakehouse 'LH_Bronze' como padrão.
try:
    print(f"\nLendo a tabela de origem '{SOURCE_TABLE_NAME}' do Lakehouse...")
    df_source = spark.table(SOURCE_TABLE_NAME)

    # --- ESCRITA DA TABELA DE BACKUP ---
    print(f"Salvando o DataFrame na nova tabela de backup '{BACKUP_TABLE_NAME}'...")
    # Usamos 'overwrite' para garantir que, se o notebook for executado novamente no mesmo dia,
    # a tabela de backup seja substituída pela versão mais recente.
    df_source.write.mode("overwrite").format("delta").saveAsTable(BACKUP_TABLE_NAME)

    print("\n--- SUCESSO ---")
    print("O backup da tabela foi concluído com sucesso.")

    # --- VERIFICAÇÃO ---
    print("\nVerificando a criação da tabela de backup...")
    # Conta o número de registros na tabela de origem e de destino para garantir a consistência.
    count_source = df_source.count()
    count_backup = spark.table(BACKUP_TABLE_NAME).count()

    print(f"Registros na tabela de origem '{SOURCE_TABLE_NAME}': {count_source}")
    print(f"Registros na tabela de backup '{BACKUP_TABLE_NAME}': {count_backup}")

    if count_source == count_backup:
        print("A contagem de registros é consistente. Backup verificado com sucesso!")
    else:
        print(f"AVISO: A contagem de registros entre a origem ({count_source}) e o backup ({count_backup}) é diferente!")

except Exception as e:
    print("\n--- ERRO ---")
    print(f"Ocorreu um erro durante a execução do processo de backup: {e}")
    # Lança a exceção para que a célula do notebook seja marcada como falha.
    raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
