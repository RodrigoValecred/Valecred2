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

import os
import joblib
import pandas as pd
from pyspark.sql.functions import col

# Configuração
class ModelConfig:
    MODEL_PATH = os.environ.get('CREDIT_RISK_MODEL_PATH', '/lakehouse/default/Files/credit_risk_model_v2.joblib')
    FEATURES_PATH = os.environ.get('CREDIT_RISK_FEATURES_PATH', '/lakehouse/default/Files/model_features_v2.joblib')

# Caminhos dos artefatos do modelo
model_path = ModelConfig.MODEL_PATH
features_path = ModelConfig.FEATURES_PATH

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

class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def draw_risk_meter(score, width=30):
    """Draws an ASCII risk meter."""
    # Handle NaN (Not a Number)
    if score != score:
        return f"{Colors.YELLOW}[DADOS INSUFICIENTES]{Colors.RESET}"

    score = max(0, min(1, score)) # Clamp between 0 and 1
    filled_len = int(width * score)

    # Determine color based on score
    if score < 0.15:
        color = Colors.GREEN
        icon = "✅"
    elif score < 0.40:
        color = Colors.YELLOW
        icon = "⚠️"
    else:
        color = Colors.RED
        icon = "🚨"

    bar = color + '█' * filled_len + Colors.RESET + '░' * (width - filled_len)
    return f"[{bar}] {color}{score:.2f}{Colors.RESET} {icon}"

def calcular_score_cliente(cpf_cnpj, df_mestra_spark, model_pipeline, model_features):
    """Calcula o score de risco para os títulos em aberto de um cliente.

    Args:
        cpf_cnpj (str): O CPF ou CNPJ do cliente a ser analisado.
        df_mestra_spark (pyspark.sql.DataFrame): O DataFrame Spark contendo os dados.
        model_pipeline (sklearn.pipeline.Pipeline): O pipeline do modelo treinado.
        model_features (list): Lista de features esperadas pelo modelo.

    Returns:
        pandas.DataFrame: DataFrame contendo os dados do cliente e a coluna 'SCORE_RISCO'.
                          Retorna None se nenhum título for encontrado.
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
        return None

    # 2. Preparar dados para o modelo
    X_cliente = df_cliente_pandas[model_features].copy()
    for col_name in ['CODSTATUSCLIENTE', 'CODRATING_CEDENTE']:
        if col_name in X_cliente.columns:
            X_cliente[col_name] = X_cliente[col_name].astype('category')

    # 3. Executar a previsão
    probabilidades = model_pipeline.predict_proba(X_cliente)[:, 1]
    df_cliente_pandas['SCORE_RISCO'] = probabilidades

    return df_cliente_pandas


def exibir_analise_risco(df_cliente_pandas, cpf_cnpj):
    """Exibe os resultados da análise de risco e gera alertas.

    Args:
        df_cliente_pandas (pandas.DataFrame): DataFrame com os dados e scores do cliente.
        cpf_cnpj (str): O CPF ou CNPJ do cliente.
    """
    score_medio = df_cliente_pandas['SCORE_RISCO'].mean()

    print(f"\n{Colors.BOLD}{Colors.CYAN}--- RESULTADO DA ANÁLISE DE RISCO ---{Colors.RESET}")
    print(f"{Colors.BOLD}Cliente:{Colors.RESET} {cpf_cnpj}")
    print(f"{Colors.BOLD}Score de Risco Médio:{Colors.RESET} {draw_risk_meter(score_medio)}")
    print(f"Total de Títulos em Aberto Analisados: {len(df_cliente_pandas)}")

    # 4. Gerar Alertas (Análise Simplificada de Contribuição)
    print(f"\n{Colors.BOLD}{Colors.CYAN}--- Principais Fatores de Risco ---{Colors.RESET}")

    # Análise de features categóricas de alto risco
    if 'CODRATING_CEDENTE' in df_cliente_pandas.columns:
        rating = df_cliente_pandas['CODRATING_CEDENTE'].mode()[0]
        # Supondo que ratings piores são letras maiores (C, D, E...)
        if rating > 'B':
            print(f"- {Colors.RED}ALERTA:{Colors.RESET} O rating predominante do cliente é '{rating}', considerado de alto risco.")

    # Análise de features numéricas
    if 'PRAZO' in df_cliente_pandas.columns and df_cliente_pandas['PRAZO'].mean() > 90:
        print(f"- {Colors.YELLOW}ATENÇÃO:{Colors.RESET} O prazo médio dos títulos ({df_cliente_pandas['PRAZO'].mean():.0f} dias) é elevado.")

    if 'VALOR' in df_cliente_pandas.columns and df_cliente_pandas['VALOR'].mean() > 10000:
        print(f"- INFORMATIVO: O valor médio dos títulos é alto (R$ {df_cliente_pandas['VALOR'].mean():,.2f}).")

    if score_medio < 0.15:
        print(f"\nConclusão: {Colors.GREEN}Risco Baixo.{Colors.RESET} O cliente apresenta um perfil de crédito saudável.")
    elif score_medio < 0.40:
        print(f"\nConclusão: {Colors.YELLOW}Risco Moderado.{Colors.RESET} Recomenda-se monitoramento.")
    else:
        print(f"\nConclusão: {Colors.RED}Risco Alto.{Colors.RESET} Ações preventivas são recomendadas.")

    print(f"\n{Colors.BOLD}{Colors.CYAN}--- Detalhes dos Títulos (Mapa de Calor) ---{Colors.RESET}")

    # Aplica gradiente de cor na coluna SCORE_RISCO
    # RdYlGn_r: Verde (Baixo/0) -> Amarelo -> Vermelho (Alto/1)
    styled_df = df_cliente_pandas[['CODTITULO', 'VALOR', 'VENCIMENTO', 'SCORE_RISCO']].style.background_gradient(
        subset=['SCORE_RISCO'],
        cmap='RdYlGn_r',
        vmin=0,
        vmax=1
    ).format({'SCORE_RISCO': "{:.2f}", 'VALOR': "R$ {:,.2f}"})

    display(styled_df)


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
    df_resultado = calcular_score_cliente(cpf_cnpj, df_mestra_spark, model_pipeline, model_features)

    if df_resultado is not None:
        exibir_analise_risco(df_resultado, cpf_cnpj)
    else:
        print(f"Nenhum título em aberto encontrado para o cliente {cpf_cnpj} com os critérios aplicados.")

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
