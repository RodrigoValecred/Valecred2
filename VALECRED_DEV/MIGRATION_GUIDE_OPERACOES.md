# Guia de Migração: Tabela "Operações" (Legacy M) para Gold Layer

Este documento detalha como substituir a consulta Power Query "operacoes" utilizando as tabelas padronizadas da camada Gold no Lakehouse.

## Mapeamento

A lógica original do Power Query combina dados de operações com dados de gerentes. Na camada Gold, esses dados estão normalizados em duas tabelas principais:

| Origem Power Query (M) | Tabela Gold Equivalente | Notas |
|------------------------|-------------------------|-------|
| `stg_operacoes` | `LH_Gold.fato_operacoes` | Contém a lógica de resolução de `cod_broker` (Broker vs Gerente de Conta) e correções de TTO. |
| `gerentes` | `LH_Gold.dim_gerentes` | Contém `nome_gerente`, `nome_plataforma`, `gestor_da_plataforma`. |

## Lógica de Substituição

Para reproduzir exatamente a tabela "operacoes", você deve realizar um **JOIN** entre a fato e a dimensão, aplicando os filtros necessários.

### Código PySpark (Exemplo para Notebook/View)

```python
from pyspark.sql.functions import col, date_format, concat, lit

# 1. Carregar tabelas Gold
df_fato = spark.read.table("LH_Gold.fato_operacoes")
df_gerentes = spark.read.table("LH_Gold.dim_gerentes")

# 2. Aplicar Filtros (TTO)
# Filtro original: CM, FC, NO, RN, GR, CS, NC
tto_filter = ["CM", "FC", "NO", "RN", "GR", "CS", "NC"]
df_fato_filtered = df_fato.filter(col("tto").isin(tto_filter))

# 3. Realizar o Join (Left Outer)
# A chave de ligação é 'cod_broker'. Na fato_operacoes, esta coluna já passou pela lógica de "Personalizar" (contingência para cod_gerente).
df_joined = df_fato_filtered.join(df_gerentes, "cod_broker", "left")

# 4. Criar Colunas Calculadas (Relatório)
# chave_ano_mes: MM/yyyy
# chave_ano_mes_base_empresa: MM/yyyy-CODEMPRESA
df_final = df_joined.withColumn("chave_ano_mes", date_format(col("data_analise"), "MM/yyyy")) \
    .withColumn("chave_ano_mes_base_empresa", concat(col("chave_ano_mes"), lit("-"), col("cod_empresa"))) \
    .select(
        col("cod_operacao"),
        col("data_analise").alias("Data deferimento"),
        col("nome_gerente").alias("Nome do gerente"),
        col("nome_plataforma").alias("Plataforma da operação"),
        col("gestor_da_plataforma").alias("Gestor da operação"),
        col("chave_ano_mes"),
        col("chave_ano_mes_base_empresa"),
        col("tto"),
        col("stto"),
        col("cod_broker").alias("CODBROKER"),
        col("era"),
        # Adicione outras colunas conforme a necessidade da view original
        col("valor_de_face"),
        col("valor_desembolsado")
    )

# Exibir ou Salvar
# df_final.write.saveAsTable("LH_Gold.relatorio_operacoes_bi")
```

## Benefícios da Migração

1.  **Performance:** O processamento ocorre no Spark (Lakehouse), reduzindo a carga no Power BI.
2.  **Consistência:** Utiliza a lógica centralizada de `cod_broker` e `dim_gerentes` da camada Gold, garantindo que o relatório esteja sempre alinhado com os demais dashboards.
3.  **Manutenção:** Correções em nomes de gerentes ou regras de negócio são aplicadas automaticamente nas tabelas Gold.
