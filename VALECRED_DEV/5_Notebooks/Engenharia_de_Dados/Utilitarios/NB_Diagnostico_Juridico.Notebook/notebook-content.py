# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8f85c372-56ad-4f3f-acf9-3be2e9b99513",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         },
# META         {
# META           "id": "553c2931-573b-4db0-838d-a70a01306d32"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Diagnóstico Jurídico
# **Objetivo:** Investigar inconsistências ou falta de registros recentes na tabela de títulos enviados ao jurídico.

# CELL ********************

from pyspark.sql.functions import col, max, count, lit

def check_bronze_events(spark):
    print("\n1. Verificando Tabela Bronze (tab_titulos_cobranca)...")
    try:
        df_bronze = spark.read.table("LH_Bronze.tab_titulos_cobranca")
        df_juridico_events = df_bronze.filter(col("CODOCORCOBRANCA") == 26)

        bronze_stats = df_juridico_events.agg(
            max("DATAINCLUSAO").alias("max_data_inclusao"),
            count("*").alias("total_registros")
        ).collect()[0]

        print(f"Total de eventos 'Enviado ao Jurídico' (Cód 26) na Bronze: {bronze_stats['total_registros']}")
        print(f"Data mais recente de inclusão na Bronze: {bronze_stats['max_data_inclusao']}")
        return df_juridico_events

    except Exception as e:
        print(f"ERRO ao ler Bronze: {e}")
        return None

def check_silver_titulos(spark):
    print("\n2. Verificando Tabela Silver de Títulos (staging_titulos_limpa)...")
    try:
        df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa")
        silver_stats = df_titulos.agg(
            max("data_inclusao").alias("max_data_inclusao"),
            count("*").alias("total_titulos")
        ).collect()[0]

        print(f"Total de títulos na Silver: {silver_stats['total_titulos']}")
        print(f"Data mais recente de inclusão de título na Silver: {silver_stats['max_data_inclusao']}")
        return df_titulos

    except Exception as e:
        print(f"ERRO ao ler Silver Títulos: {e}")
        return None

def check_final_report(spark):
    print("\n3. Verificando Tabela Final (relatorio_titulos_juridico)...")
    try:
        df_final = spark.read.table("LH_Silver.relatorio_titulos_juridico")
        final_stats = df_final.agg(
            max("data_envio_juridico").alias("max_data_envio"),
            count("*").alias("total_final")
        ).collect()[0]

        print(f"Total de registros na tabela final: {final_stats['total_final']}")
        print(f"Data mais recente de envio na tabela final: {final_stats['max_data_envio']}")
        return df_final

    except Exception as e:
        print(f"ERRO ao ler Tabela Final: {e}")
        return None

def analyze_discrepancies(df_juridico_events, df_titulos):
    print("\n4. Analisando Discrepâncias (Eventos na Bronze sem Título na Silver)...")
    # Left Anti Join: Mostra o que tem na esquerda (eventos) mas não na direita (títulos)
    df_orfaos = df_juridico_events.join(df_titulos, df_juridico_events.CODTITULO == df_titulos.cod_titulo, "left_anti")
    count_orfaos = df_orfaos.count()
    
    print(f"Quantidade de eventos de jurídico SEM título correspondente na Silver: {count_orfaos}")
    
    if count_orfaos > 0:
        print("Amostra de eventos órfãos (Top 5):")
        df_orfaos.select("CODTITULO", "DATAINCLUSAO", "USUAINCLUSAO").show(5)
        print("DICA: Se houver muitos órfãos recentes, a tabela 'staging_titulos_limpa' pode estar desatualizada.")
    else:
        print("Todos os eventos de jurídico possuem título correspondente na Silver.")

print("=== INICIANDO DIAGNÓSTICO JURÍDICO ===")

df_juridico_events = check_bronze_events(spark)
df_titulos = check_silver_titulos(spark)
check_final_report(spark)

if df_juridico_events is not None and df_titulos is not None:
    analyze_discrepancies(df_juridico_events, df_titulos)
else:
    print("Não foi possível realizar a análise de discrepância devido a erros anteriores.")

print("\n=== FIM DO DIAGNÓSTICO ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
