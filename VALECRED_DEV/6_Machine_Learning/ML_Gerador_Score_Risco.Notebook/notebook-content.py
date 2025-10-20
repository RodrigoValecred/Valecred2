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

# # NB_Gerador_Score_Risco
# 
# **Objetivo:** Este notebook utiliza o modelo de machine learning treinado para gerar um score de risco de inadimplência para um cliente específico (ou um conjunto de títulos) e fornecer insights sobre os fatores que mais contribuem para esse risco.
# 
# **Como usar:**
# 1. Insira o CPF/CNPJ do cliente na célula de "Execução".
# 2. Execute o notebook.
# 3. O resultado mostrará o score de risco médio para os títulos em aberto do cliente e uma análise dos principais fatores de risco.

# MARKDOWN ********************

# ## 1. Configuração e Carregamento de Artefatos
# Carregando bibliotecas, o modelo treinado e a lista de features.

# CELL ********************

import joblib
import pandas as pd
from pyspark.sql.functions import col

# Caminhos dos artefatos do modelo
model_path = '/lakehouse/default/Files/credit_risk_model_v2.joblib'
features_path = '/lakehouse/default/Files/model_features_v2.joblib'

# Carregando os artefatos
try:
    model_pipeline = joblib.load(model_path)
    model_features = joblib.load(features_path)
    print("Modelo e features carregados com sucesso.")
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

# ## 2. Carregamento e Preparação dos Dados
# Carregando as tabelas do Lakehouse e construindo a tabela mestra, assim como no notebook de previsão.

# CELL ********************

# Configurações do Spark
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

# Carregamento das tabelas
print("Carregando tabelas do Lakehouse Silver...")
df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa")
df_operacoes = spark.read.table("LH_Silver.staging_operacoes_limpa")
df_cedentes = spark.read.table("LH_Silver.dim_cliente")
df_cad_geral = spark.read.table("LH_Silver.staging_cad_geral_limpa")

# Renomeando colunas para evitar conflitos
df_operacoes = df_operacoes.withColumnRenamed("TTO", "TTO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("CODRATING", "CODRATING_OPERACAO")
df_cedentes = df_cedentes.withColumnRenamed("CODRATING", "CODRATING_CEDENTE")

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

# ## 3. Função de Geração de Score e Alertas
#  Esta função isola um cliente, prepara seus dados, calcula o score de risco e gera alertas com base na comparação de seus dados com a média da carteira.

# CELL ********************

def gerar_score_e_alertas(cpf_cnpj, df_mestra_spark, model_pipeline, model_features):
    """Calcula o score de risco para os títulos em aberto de um cliente, exibe os resultados e gera alertas.

    A função filtra a tabela mestra para um CPF/CNPJ específico, seleciona apenas os títulos
    em aberto e aplica o modelo de machine learning treinado para prever a probabilidade de
    inadimplência (score de risco) de cada título. Ao final, exibe um resumo com o score
    médio, os principais fatores de risco identificados e uma lista detalhada dos títulos
    com seus respectivos scores.

    Args:
        cpf_cnpj (str): O CPF ou CNPJ do cliente a ser analisado.
        df_mestra_spark (pyspark.sql.DataFrame): O DataFrame Spark contendo os dados
                                                 consolidados de títulos, operações e clientes.
        model_pipeline (sklearn.pipeline.Pipeline): O pipeline do modelo treinado (joblib),
                                                    pronto para fazer previsões.
        model_features (list): Uma lista de strings com os nomes das features que o
                               modelo espera, na ordem correta.
    """
    print(f"\nIniciando análise para o cliente: {cpf_cnpj}")

    # 1. Filtrar o universo de dados para o cliente e títulos em aberto
    df_cliente_spark = df_mestra_spark.filter(
        (col('CPFCNPJ') == cpf_cnpj) &
        (col('LIQUIDACAO').isNull()) &
        (col('STATUSANALISE') == 'D') &
        (col('STATUSACEITE') == 'A') &
        (col('ACEITO') == 'S')
    )

    # Excluindo renegociações
    tipos_excluir = ['RE','RC','PR','AB','AM','LB','PB']
    df_cliente_spark = df_cliente_spark.filter(~col('TTO_OPERACAO').isin(tipos_excluir))

    df_cliente_pandas = df_cliente_spark.toPandas()

    if df_cliente_pandas.empty:
        print(f"Nenhum título em aberto encontrado para o cliente {cpf_cnpj} com os critérios aplicados.")
        return

    # 2. Preparar dados para o modelo
    X_cliente = df_cliente_pandas[model_features].copy()
    for col_name in ['CODSTATUSCLIENTE', 'CODRATING_CEDENTE']:
        if col_name in X_cliente.columns:
            X_cliente[col_name] = X_cliente[col_name].astype('category')

    # 3. Executar a previsão
    probabilidades = model_pipeline.predict_proba(X_cliente)[:, 1]
    df_cliente_pandas['SCORE_RISCO'] = probabilidades

    score_medio = df_cliente_pandas['SCORE_RISCO'].mean()

    print("\n--- RESULTADO DA ANÁLISE DE RISCO ---")
    print(f"Cliente: {cpf_cnpj}")
    print(f"Score de Risco Médio (0 a 1): {score_medio:.2f}")
    print(f"Total de Títulos em Aberto Analisados: {len(df_cliente_pandas)}")

    # 4. Gerar Alertas (Análise Simplificada de Contribuição)
    print("\n--- Principais Fatores de Risco ---")

    # Mapeamento de features para nomes amigáveis
    nomes_amigaveis = {
        'CODRATING_CEDENTE': 'Rating do Cliente',
        'PRAZO': 'Prazo do Título',
        'VALOR': 'Valor do Título',
        'DESAGIO': 'Deságio da Operação'
    }

    # Análise de features categóricas de alto risco
    if 'CODRATING_CEDENTE' in X_cliente.columns:
        rating = X_cliente['CODRATING_CEDENTE'].mode()[0]
        # Supondo que ratings piores são letras maiores (C, D, E...)
        if rating > 'B':
            print(f"- ALERTA: O rating predominante do cliente é '{rating}', considerado de alto risco.")

    # Análise de features numéricas
    # (Em um cenário real, compararíamos com a média/mediana da população de treino)
    if 'PRAZO' in X_cliente.columns and X_cliente['PRAZO'].mean() > 90:
        print(f"- ATENÇÃO: O prazo médio dos títulos ({X_cliente['PRAZO'].mean():.0f} dias) é elevado.")

    if 'VALOR' in X_cliente.columns and X_cliente['VALOR'].mean() > 10000:
        print(f"- INFORMATIVO: O valor médio dos títulos é alto (R$ {X_cliente['VALOR'].mean():,.2f}).")

    if score_medio < 0.15:
        print("\nConclusão: Risco Baixo. O cliente apresenta um perfil de crédito saudável.")
    elif score_medio < 0.40:
        print("\nConclusão: Risco Moderado. Recomenda-se monitoramento.")
    else:
        print("\nConclusão: Risco Alto. Ações preventivas são recomendadas.")

    print("\n--- Detalhes dos Títulos ---")
    display(df_cliente_pandas[['CODTITULO', 'VALOR', 'VENCIMENTO', 'SCORE_RISCO']])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Execução da Análise
# 
# Insira o CPF/CNPJ do cliente que deseja analisar e execute a célula abaixo.

# CELL ********************

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#    Insira o CPF/CNPJ do cliente para análise
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
CLIENTE_A_ANALISAR = "14630809000101"


if CLIENTE_A_ANALISAR == "INSIRA_O_CPFCNPJ_AQUI":
    print("Por favor, insira o CPF/CNPJ do cliente na variável CLIENTE_A_ANALISAR.")
else:
    gerar_score_e_alertas(
        cpf_cnpj=CLIENTE_A_ANALISAR,
        df_mestra_spark=df_mestra_spark,
        model_pipeline=model_pipeline,
        model_features=model_features
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
