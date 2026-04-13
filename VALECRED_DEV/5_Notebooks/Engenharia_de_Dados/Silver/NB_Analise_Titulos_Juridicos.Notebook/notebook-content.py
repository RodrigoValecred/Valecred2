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

# # Análise de Títulos no Jurídico
# **Objetivo:** Listar títulos com status "ENVIADO AO JURÍDICO" (Código 26).
# 
# **Contexto:**
# - **Tabela de Ligação:** `LH_Bronze.tab_titulos_cobranca` (Contém `CODTITULO` e `CODOCORCOBRANCA`).
# - **Código da Ocorrência:** `26` ("ENVIADO AO JURÍDICO").
# - **Tabela de Dados dos Títulos:** `LH_Silver.staging_titulos_limpa` (Camada Silver com dados tratados).

# CELL ********************

from pyspark.sql.functions import col, lit, current_timestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Iniciando busca por títulos no Jurídico (Código 26)...")

# 1. Carregar tabela de ligação (Bronze)
# Esta tabela registra eventos de cobrança associados aos títulos
df_cobranca = spark.read.table("LH_Bronze.tab_titulos_cobranca")

# 2. Filtrar pelo Código 26 (Enviado ao Jurídico)
df_juridico_events = df_cobranca.filter(col("CODOCORCOBRANCA") == 26) \
    .select(
        col("CODTITULO").alias("cod_titulo"),
        col("DATAINCLUSAO").alias("data_envio_juridico"),
        col("USUAINCLUSAO").alias("usuario_responsavel"),
        col("OBSERVACAO").alias("observacao_cobranca")
    )

# ⚡ Bolt: Cache DataFrame antes do .count() de log
# 💡 O que: Adicionado df_juridico_events.cache() antes da ação count() e .unpersist() no final.
# 🎯 Por que: A chamada .count() força a avaliação eager de todo o plano Catalyst. Como df_juridico_events é usado em um join depois, sem o cache a leitura da Bronze e os filtros seriam executados duas vezes.
# 📊 Impacto: Evita reavaliação redundante do DAG (full table scan e filtros), reduzindo significativamente o I/O e o tempo total de processamento.
# 🔬 Medição: Elimina do Spark UI o estágio duplicado para carregamento da tabela tab_titulos_cobranca.
# ⚡ Otimização de Bolt: Remove lazy count em log e substitui condicional por isEmpty
# 💡 O que: Trocou `df_juridico_events.cache(); count_events = df_juridico_events.count(); if count_events > 0:` por `if not df_juridico_events.isEmpty():` e chama cache apenas se for processar dados.
# 🎯 Por que: A chamada `.count()` aciona uma avaliação completa mesmo para partições vazias para retornar o total de registros. `.isEmpty()` processa apenas 1 partição e retorna imediato, não necessitando de cache para esse check rápido em runs incrementais sem evento. O log explícito .count() foi removido pois a tabela final imprime total no fim.
# 📊 Impacto: Otimiza a execução de pipelines incrementais quando não existem eventos novos sem gastar I/O.
if not df_juridico_events.isEmpty():
    df_juridico_events.cache()
    print(f"Registros de envio ao jurídico encontrados. Prosseguindo...")
    # 3. Carregar tabela de Títulos (Silver) para enriquecimento
    df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa")
    
    # 4. Realizar o Join
    # Usamos inner join para trazer apenas os títulos que existem na base ativa/histórica da Silver
    df_resultado = df_juridico_events.join(df_titulos, "cod_titulo", "inner") \
        .select(
            col("cod_titulo"),
            col("n_doc"),
            col("data_envio_juridico"),
            col("cpf_cnpj_sacado"),
            col("valor_devido"),
            col("vencimento_efetivo"),
            col("dias_atraso"),
            col("liquidacao"),
            col("usuario_responsavel"),
            col("observacao_cobranca")
        ).orderBy(col("data_envio_juridico").desc())
        
    print(f"Total de títulos únicos listados: {df_resultado.count()}")
    
    print("\n--- Amostra dos Títulos no Jurídico ---")
    display(df_resultado.limit(50))
    
    # Opcional: Salvar resultado temporário para análise posterior
    output_path = "LH_Silver.relatorio_titulos_juridico"
    df_resultado.write.mode("overwrite").saveAsTable(output_path)
    print(f"Relatório salvo em: {output_path}")

else:
    print("Nenhum registro encontrado com CODOCORCOBRANCA = 26.")

# Limpeza de cache para economizar memória do cluster
df_juridico_events.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
