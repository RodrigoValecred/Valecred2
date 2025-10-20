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

import joblib
from pyspark.sql.functions import year, col
import pandas as pd

# Configurações do Spark
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

# Carregamento das tabelas
print("Carregando tabelas do Lakehouse Silver...")
df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa")
df_operacoes = spark.read.table("LH_Silver.staging_operacoes_limpa")
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
df_operacoes = df_operacoes.withColumnRenamed("EXIGECANHOTO", "EXIGECANHOTO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("EXIGECONFIRMACAO", "EXIGECONFIRMACAO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("TTO", "TTO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("DATAINCLUSAO", "DATAINCLUSAO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("USUAINCLUSAO", "USUAINCLUSAO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("DATAALTERACAO", "DATAALTERACAO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("USUAALTERACAO", "USUAALTERACAO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("CODRATING", "CODRATING_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("PREIMPRESSO", "PREIMPRESSO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("BOLETOESPECIAL", "BOLETOESPECIAL_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("TARIFARECOMPRA", "TARIFARECOMPRA_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("RECEBEBOLETO", "RECEBEBOLETO_OPERACAO")

df_cedentes = df_cedentes.withColumnRenamed("DATAINCLUSAO", "DATAINCLUSAO_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("USUAINCLUSAO", "USUAINCLUSAO_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("DATAALTERACAO", "DATAALTERACAO_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("USUAALTERACAO", "USUAALTERACAO_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("CODRATING", "CODRATING_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("PEFIN", "PEFIN_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("BAIXADOPEFIN", "BAIXADOPEFIN_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("PREIMPRESSO", "PREIMPRESSO_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("BOLETOESPECIAL", "BOLETOESPECIAL_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("TARIFARECOMPRA", "TARIFARECOMPRA_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("RECEBEBOLETO", "RECEBEBOLETO_CEDENTE")

# Realizando os joins
print("Criando tabela mestra com os joins...")
df_mestra_spark = df_titulos.join(df_operacoes, on="CODOPERACAO", how="left")
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
df_filtrado_spark = df_mestra_spark.filter(
    (col('STATUSANALISE') == 'D') &
    (col('STATUSACEITE') == 'A') &
    (col('ACEITO') == 'S')
)

# Excluindo renegociações
tipos_excluir = ['RE','RC','PR','AB','AM','LB','PB']
df_filtrado_spark = df_filtrado_spark.filter(~col('TTO_OPERACAO').isin(tipos_excluir))

# Excluindo boletos
tdoc_excluir = ['BL']
df_filtrado_spark = df_filtrado_spark.filter(~df_titulos['TDOC'].isin(tdoc_excluir))


# Filtrando para o ano de 2025 e títulos em aberto
df_previsao_spark = df_filtrado_spark.filter(
    (df_titulos["LIQUIDACAO"].isNull())
)

# Convertendo para Pandas para usar com scikit-learn
print("Convertendo para DataFrame Pandas...")
df_previsao_pandas = df_previsao_spark.toPandas()

print(f"Universo de previsão selecionado: {len(df_previsao_pandas)} títulos.")
print(df_previsao_pandas.head())

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
model_path = '/lakehouse/default/Files/credit_risk_model_v2.joblib'
features_path = '/lakehouse/default/Files/model_features_v2.joblib'

# Carregando os artefatos
try:
    model_pipeline = joblib.load(model_path)
    model_features = joblib.load(features_path)
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
if df_previsao_pandas.empty:
    print("Nenhum título encontrado para o ano de 2025 com os critérios especificados. Encerrando o notebook.")
    dbutils.notebook.exit("Nenhum dado para processar.")

print("Preparando os dados para a previsão...")
# Selecionando apenas as features necessárias para o modelo
X_previsao = df_previsao_pandas[model_features].copy()

# Tratando colunas categóricas como no treinamento
for col_name in ['CODSTATUSCLIENTE', 'CODRATING_CEDENTE']:
    if col_name in X_previsao.columns:
        X_previsao[col_name] = X_previsao[col_name].astype('category')

# Lidando com valores nulos (se houver) antes da previsão
# O pipeline já lida com isso, mas é uma boa prática verificar
if X_previsao.isnull().sum().sum() > 0:
    print("Atenção: Valores nulos encontrados nas features. O pipeline de pré-processamento deve tratá-los.")
    # Exemplo de preenchimento, se necessário (o OneHotEncoder já lida com isso se handle_unknown='ignore')
    # X_previsao.fillna({'CIDADE': 'Desconhecida', 'UF': 'XX'}, inplace=True)

print("Executando a previsão de inadimplência...")
# Usando o pipeline para prever as probabilidades
# A saída de predict_proba é um array com duas colunas: [prob_classe_0, prob_classe_1]
# Queremos a probabilidade da classe 1 (inadimplência)
predicted_probabilities = model_pipeline.predict_proba(X_previsao)[:, 1]

# Adicionando as previsões de volta ao DataFrame original
df_previsao_pandas['PROBABILIDADE_INADIMPLENCIA'] = predicted_probabilities

print("Previsão concluída.")
print(df_previsao_pandas[['CODTITULO', 'CPFCNPJ', 'VALOR', 'PROBABILIDADE_INADIMPLENCIA']].head())

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
df_resultado = df_previsao_pandas[[
    'CODTITULO',
    'CODOPERACAO',
    'CPFCNPJ',
    'VALOR',
    'VENCIMENTO',
    'DATAANALISE',
    'PROBABILIDADE_INADIMPLENCIA'
]].copy()

# Convertendo o DataFrame Pandas de volta para um DataFrame Spark
spark_df_resultado = spark.createDataFrame(df_resultado)

# Salvando a tabela no Lakehouse
table_name = "LH_Silver.previsao_inadimplencia_2025"
spark_df_resultado.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Resultados salvos com sucesso na tabela: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
