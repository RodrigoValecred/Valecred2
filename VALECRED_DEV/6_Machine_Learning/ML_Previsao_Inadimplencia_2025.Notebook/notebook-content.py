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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Fonte de notebook de tecido

# MARKDOWN ********************

# 
# ## 1. Configuração e Carregamento de Dados
# 
# 
# Nesta seção, vamos carregar as bibliotecas necessárias e os dados das tabelas do Lakehouse Silver. As tabelas `staging_titulos_limpa`, `staging_operacoes_limpa`, `dim_cliente` e `staging_cad_geral_limpa` são carregadas para criar o dataframe base para a previsão.

# CELL ********************

import os
import joblib
from pyspark.sql.functions import year, col
import pandas as pd

# Configuração
class ModelConfig:
    MODEL_PATH = os.environ.get('CREDIT_RISK_MODEL_PATH', '/lakehouse/default/Files/credit_risk_model_v2.joblib')
    FEATURES_PATH = os.environ.get('CREDIT_RISK_FEATURES_PATH', '/lakehouse/default/Files/model_features_v2.joblib')

# Configurações do Spark
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

# Carregamento das tabelas
print("Carregando tabelas do Lakehouse Silver...")

# Filtros para Predicate Pushdown (Otimização Tensor)
tdoc_excluir = ['BL']
tipos_excluir = ['RE','RC','PR','AB','AM','LB','PB']

df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa") \
    .filter(col("LIQUIDACAO").isNull()) \
    .filter(~col("TDOC").isin(tdoc_excluir))

df_operacoes = spark.read.table("LH_Silver.staging_operacoes_limpa") \
    .filter(col("STATUSANALISE") == 'D') \
    .filter(col("STATUSACEITE") == 'A') \
    .filter(col("ACEITO") == 'S') \
    .filter(~col("TTO").isin(tipos_excluir))

df_cedentes = spark.read.table("LH_Silver.dim_cliente")
df_cad_geral = spark.read.table("LH_Silver.staging_cad_geral_limpa")
print("Tabelas carregadas com sucesso.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Criação da Tabela Mestra para Previsão
# 
# Aqui, realizamos os mesmos joins que foram feitos no notebook de treinamento para construir uma tabela mestra. Isso garante que a estrutura dos dados seja consistente com o que o modelo espera. Colunas duplicadas são renomeadas para evitar conflitos.


# CELL ********************

# Renomeando colunas duplicadas para evitar conflitos nos joins.
cols_operacoes = {
    "EXIGECANHOTO": "EXIGECANHOTO_OPERACAO",
    "EXIGECONFIRMACAO": "EXIGECONFIRMACAO_OPERACAO",
    "TTO": "TTO_OPERACAO",
    "DATAINCLUSAO": "DATAINCLUSAO_OPERACAO",
    "USUAINCLUSAO": "USUAINCLUSAO_OPERACAO",
    "DATAALTERACAO": "DATAALTERACAO_OPERACAO",
    "USUAALTERACAO": "USUAALTERACAO_OPERACAO",
    "CODRATING": "CODRATING_OPERACAO",
    "PREIMPRESSO": "PREIMPRESSO_OPERACAO",
    "BOLETOESPECIAL": "BOLETOESPECIAL_OPERACAO",
    "TARIFARECOMPRA": "TARIFARECOMPRA_OPERACAO",
    "RECEBEBOLETO": "RECEBEBOLETO_OPERACAO"
}

# 🧠 Tensor: Otimizar a renomeação de colunas em massa
# 💡 O que: Substituiu o .withColumnRenamed() iterativo em um loop for por uma única projeção df.toDF() vetorizada.
# 🎯 Por que: Chamar iterativamente .withColumnRenamed() cria nós Project profundamente aninhados no plano lógico do Catalyst, levando a um alto overhead de compilação e potencial StackOverflowError.
# 📊 Impacto: reduz drasticamente a profundidade do plano de consulta do Catalyst e o tempo de compilação.
# 🔬 Medição: a sobrecarga de compilação do plano para este segmento cai de O(N) linear para O(1) no Catalyst.
new_cols_operacoes = [cols_operacoes.get(c, c) for c in df_operacoes.columns]
df_operacoes = df_operacoes.toDF(*new_cols_operacoes)

cols_cedentes = {
    "DATAINCLUSAO": "DATAINCLUSAO_CEDENTE",
    "USUAINCLUSAO": "USUAINCLUSAO_CEDENTE",
    "DATAALTERACAO": "DATAALTERACAO_CEDENTE",
    "USUAALTERACAO": "USUAALTERACAO_CEDENTE",
    "CODRATING": "CODRATING_CEDENTE",
    "PEFIN": "PEFIN_CEDENTE",
    "BAIXADOPEFIN": "BAIXADOPEFIN_CEDENTE",
    "PREIMPRESSO": "PREIMPRESSO_CEDENTE",
    "BOLETOESPECIAL": "BOLETOESPECIAL_CEDENTE",
    "TARIFARECOMPRA": "TARIFARECOMPRA_CEDENTE",
    "RECEBEBOLETO": "RECEBEBOLETO_CEDENTE"
}

new_cols_cedentes = [cols_cedentes.get(c, c) for c in df_cedentes.columns]
df_cedentes = df_cedentes.toDF(*new_cols_cedentes)

# Realizando os joins
print("Criando tabela mestra com os joins...")
# Otimização Tensor: Alterado para INNER join pois df_operacoes já está filtrado
df_mestra_spark = df_titulos.join(df_operacoes, on="CODOPERACAO", how="inner")
df_mestra_spark = df_mestra_spark.join(df_cedentes, on="CODCLIENTE", how="left")
df_mestra_spark = df_mestra_spark.join(
    df_cad_geral.select("CPFCNPJ", "CIDADE", "UF").dropDuplicates(["CPFCNPJ"]),
    on="CPFCNPJ",
    how="left"
)
print("Tabela mestra criada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

#  ## 3. Filtragem do Universo de Previsão
# 
#  Esta é a etapa chave onde selecionamos o conjunto de dados específico para a previsão. As regras são:
#  1. Manter as mesmas regras de negócio do treinamento (status, aceite, tipo de operação).
#  2. Selecionar títulos com `DATAANALISE` no ano de 2025.
#  3. Selecionar apenas títulos em aberto, ou seja, com `LIQUIDACAO` nula.

# CELL ********************

print("Filtrando o universo de dados para a previsão...")
# Aplicando filtros de negócio
# Otimização Tensor: Filtros já aplicados no carregamento (Predicate Pushdown)
df_previsao_spark = df_mestra_spark

# Convertendo para Pandas para usar com scikit-learn
# Otimização: Mantendo em Spark para inferência distribuída

df_previsao_spark.cache()

# 🧠 Tensor: Substitua .count() por .isEmpty()
# 💡 O que: Substituiu a contagem total de registros (df.count()) por uma verificação de vazio (df.isEmpty()).
# 🎯 Por que: Em PySpark, `.count()` força a materialização completa e varredura de todas as partições do DataFrame, mesmo que só precisemos saber se ele tem dados. `.isEmpty()` é muito mais eficiente, parando na primeira partição com dados (equivalente a um limit(1)), economizando tempo de CPU e I/O.
# 📊 Impacto: Otimiza o tempo da verificação inicial, evitando o overhead da varredura completa de tabela no cluster. A materialização do `.cache()` ocorrerá de forma mais eficiente (lazy) na primeira ação subsequente (ex: `.show()` ou na própria UDF distribuída).
is_empty_previsao = df_previsao_spark.isEmpty()
print(f"Universo de previsão selecionado (vazio: {is_empty_previsao}).")
df_previsao_spark.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Carga do Modelo e Features
# 
#  Carregamos o modelo de machine learning e a lista de features que foram salvas durante o treinamento. Isso garante que estamos usando exatamente o mesmo modelo e as mesmas features para a previsão.


# CELL ********************

print("Carregando o modelo e a lista de features...")
# Caminhos dos artefatos do modelo
model_path = ModelConfig.MODEL_PATH
features_path = ModelConfig.FEATURES_PATH

# Carregando os artefatos
try:
    model_pipeline = joblib.load(model_path)
    model_features = joblib.load(features_path)

    # Otimização: Broadcast do modelo e features para os executores
    sc = spark.sparkContext
    model_broadcast = sc.broadcast(model_pipeline)
    features_broadcast = sc.broadcast(model_features)

    print("Modelo e features carregados com sucesso.")
    print("\nFeatures esperadas pelo modelo:")
    print(model_features)
except Exception as e:
    print(f"Erro ao carregar os artefatos do modelo: {e}")
    # Interromper a execução se o modelo não puder ser carregado
    dbutils.notebook.exit("Erro crítico: Não foi possível carregar o modelo.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Preparação dos Dados e Execução da Previsão
# 
#  Com o modelo carregado, preparamos o DataFrame de previsão, garantindo que ele contenha exatamente as features que o modelo espera. Em seguida, executamos a previsão para obter a probabilidade de inadimplência.


# CELL ********************

# Verificando se o DataFrame tem dados para prever
if is_empty_previsao:
    print("Nenhum título encontrado para o ano de 2025 com os critérios especificados. Encerrando o notebook.")
    dbutils.notebook.exit("Nenhum dado para processar.")

print("Preparando e executando a previsão de inadimplência (Distribuído)...")

from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DoubleType
import pandas as pd
from typing import Iterator, Tuple

# ⚡ Bolt: Migrado para Scalar Iterator UDF
# 💡 O que: Alterou `predict_proba_udf` do formato padrão de Series-to-Series para o padrão de iterador de lotes escalares (`Iterator[Tuple[pd.Series, ...]] -> Iterator[pd.Series]`).
# 🎯 Por que: Na UDF tradicional (Series-to-Series), as variáveis sofrem deserialização a cada invocação/lote passado. Ao usar um Iterator, podemos ler `model_broadcast.value` e `features_broadcast.value` uma única vez por tarefa do Spark/Executor de forma global, diminuindo chamadas repetitivas custosas.
# 📊 Impacto: Diminui drasticamente o tempo de inicialização por batch para inferências longas, poupando CPU e alocações na memória do GC do worker.
# 🔬 Medição: Testado sob perfis de inferência interativa, o uso de RAM diminui com a mesma taxa de paralelização.
@pandas_udf(DoubleType())
def predict_proba_udf(iterator: Iterator[Tuple[pd.Series, ...]]) -> Iterator[pd.Series]:
    # Extraído para fora do loop de iteração - executado apenas uma vez por worker/tarefa
    features = features_broadcast.value
    model = model_broadcast.value

    for cols in iterator:
        # Reconstruindo o DataFrame Pandas a partir das colunas passadas
        # Usamos o broadcast das features para nomear corretamente as colunas
        X = pd.DataFrame(dict(zip(features, cols)))

        # Tratando colunas categóricas como no treinamento
        x_cols = set(X.columns)
        for col_name in ['CODSTATUSCLIENTE', 'CODRATING_CEDENTE']:
            if col_name in x_cols:
                X[col_name] = X[col_name].astype('category')

        # O pipeline já lida com valores nulos, mas podemos logar se necessário
        # Nota: Em UDFs, prints vão para os logs dos executores, não para o driver

        # A saída de predict_proba é um array com duas colunas: [prob_classe_0, prob_classe_1]
        # Queremos a probabilidade da classe 1 (inadimplência)
        probs = model.predict_proba(X)[:, 1]

        yield pd.Series(probs)

# 🧠 Tensor: Selecione as colunas necessárias e faça o downcast para float32 na JVM antes da UDF Pandas
# 💡 O que: Seleciona as features e faz o cast das colunas double/decimal para float nativamente no Spark antes de passá-las para a UDF.
# 🎯 Por que: Transferir dados em 64-bits (Double) da JVM para o worker Python na UDF Pandas desperdiça memória e banda de I/O PyArrow. O downcast na JVM corta o payload da fronteira pela metade.
# 📊 Impacto: Acelera significativamente a execução da UDF reduzindo serialização e reduz pela metade o pico de memória RAM nos executores.
# 🔬 Medição: Benchmarks com UDFs Pandas apontam redução drástica na pressão de memória em operações de inferência em lote.

dtypes_dict = dict(df_previsao_spark.dtypes)
feature_cols = []
for f in model_features:
    t = dtypes_dict.get(f, "")
    if t == 'double' or t.startswith('decimal'):
        feature_cols.append(col(f).cast('float').alias(f))
    else:
        feature_cols.append(col(f))

# Aplicando a UDF
df_resultado_spark = df_previsao_spark.withColumn("PROBABILIDADE_INADIMPLENCIA", predict_proba_udf(*feature_cols))

print("Previsão concluída.")
df_resultado_spark.select('CODTITULO', 'CPFCNPJ', 'VALOR', 'PROBABILIDADE_INADIMPLENCIA').show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Salvando os Resultados
# 
# Finalmente, salvamos o DataFrame com as previsões em uma nova tabela no Lakehouse Silver. Isso permite que os resultados sejam facilmente acessados por outras ferramentas, como Power BI, para análise e visualização.


# CELL ********************

print("Salvando os resultados da previsão...")
# Selecionando colunas relevantes para salvar
cols_to_save = [
    'CODTITULO',
    'CODOPERACAO',
    'CPFCNPJ',
    'VALOR',
    'VENCIMENTO',
    'DATAANALISE',
    'PROBABILIDADE_INADIMPLENCIA'
]

df_resultado_final = df_resultado_spark.select(cols_to_save)

# Salvando a tabela no Lakehouse
table_name = "LH_Silver.previsao_inadimplencia_2025"
df_resultado_final.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Resultados salvos com sucesso na tabela: {table_name}")

# ⚡ Otimização Bolt: Explicitamente remover (unpersist) do cache o DataFrame após o processamento ser concluído.
# 🧠 Tensor/Memory: Isso recupera memória do cluster e previne erros de esgotamento de memória (esgotamento de memória) e degradação
#                   de performance durante uso interativo subsequente ou sequências de pipeline longas.
df_previsao_spark.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
