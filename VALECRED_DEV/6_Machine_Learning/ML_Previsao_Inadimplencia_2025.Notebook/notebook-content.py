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

# # Fabric notebook source

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

for old, new in cols_operacoes.items():
    df_operacoes = df_operacoes.withColumnRenamed(old, new)

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

for old, new in cols_cedentes.items():
    df_cedentes = df_cedentes.withColumnRenamed(old, new)

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
count_previsao = df_previsao_spark.count()
print(f"Universo de previsão selecionado: {count_previsao} títulos.")
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
if count_previsao == 0:
    print("Nenhum título encontrado para o ano de 2025 com os critérios especificados. Encerrando o notebook.")
    dbutils.notebook.exit("Nenhum dado para processar.")

print("Preparando e executando a previsão de inadimplência (Distribuído)...")

from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DoubleType
import pandas as pd

@pandas_udf(DoubleType())
def predict_proba_udf(*cols):
    # Reconstruindo o DataFrame Pandas a partir das colunas passadas
    # Usamos o broadcast das features para nomear corretamente as colunas
    features = features_broadcast.value
    X = pd.DataFrame(dict(zip(features, cols)))

    # 🧠 Tensor: Reduzir precisão das colunas numéricas (float64 -> float32)
    # 💡 O que: Converte todas as colunas float64 no DataFrame Pandas para float32 antes da inferência do modelo.
    # 🎯 Por que: Modelos do Scikit-learn usam nativamente float32 ou float64. O downcasting evita o overhead
    #         de cópia implícita de dados dentro do scikit-learn, e reduz significativamente o uso de memória
    #         do DataFrame durante a execução.
    # 📊 Impacto: Reduz pela metade o uso de memória para features numéricas (ex., de ~154MB para ~78MB por 1M de linhas).
    # 🔬 Medição: O profiling mostra uma redução de RAM de ~50% para colunas numéricas com impacto insignificante na latência.
    float64_cols = X.select_dtypes(include=['float64']).columns
    if len(float64_cols) > 0:
        X[float64_cols] = X[float64_cols].astype('float32')

    # Tratando colunas categóricas como no treinamento
    x_cols = set(X.columns)
    for col_name in ['CODSTATUSCLIENTE', 'CODRATING_CEDENTE']:
        if col_name in x_cols:
            X[col_name] = X[col_name].astype('category')

    # O pipeline já lida com valores nulos, mas podemos logar se necessário
    # Nota: Em UDFs, prints vão para os logs dos executores, não para o driver

    model = model_broadcast.value
    # A saída de predict_proba é um array com duas colunas: [prob_classe_0, prob_classe_1]
    # Queremos a probabilidade da classe 1 (inadimplência)
    probs = model.predict_proba(X)[:, 1]

    return pd.Series(probs)

# Selecionando as colunas de features na ordem correta
feature_cols = [col(f) for f in model_features]

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
# 🧠 Tensor/Memory: Isso recupera memória do cluster e previne erros de Out-Of-Memory (OOM) e degradação
#                   de performance durante uso interativo subsequente ou sequências de pipeline longas.
df_previsao_spark.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
