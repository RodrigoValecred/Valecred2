# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import pandas as pd
from datetime import datetime, timedelta

# Exemplo de dados simulando a leitura da Camada Bronze/Silver
# Em produção, isso viria de um spark.read.table("silver_operacoes")
data_hoje = datetime(2025, 12, 23).date()
data_semana_passada = data_hoje - timedelta(days=7)

# DataFrame de Operações (Risco Atual)
df_ops = pd.DataFrame({
    'grupo': ['Vale/Transvale', 'Vale/Transvale', 'Vale/Transvale', 'Outro Grupo'],
    'cedente': ['Vale Rio Novo', 'Transvale', 'Vale Rio Novo', 'Outra Empresa'],
    'produto': ['Comissária', 'Nota Comercial', 'Fomento', 'Fomento'],
    'valor_risco': [1785316.73, 9225550.48, 4788696.00, 500000.00],
    'data_vencimento': ['2026-01-14', '2026-04-24', '2026-01-14', '2025-12-30']
})

# DataFrame de Limites (Parametrizado para escalar para outros grupos)
df_limites = pd.DataFrame({
    'grupo': ['Vale/Transvale', 'Outro Grupo'],
    'limite_global': [30000000.00, 1000000.00],
    'validade_limite': ['2025-12-12', '2025-12-30'] # Note que Vale já venceu no exemplo do email
})

# 1. Agregação de Risco por Grupo
df_risco_total = df_ops.groupby('grupo')['valor_risco'].sum().reset_index()

# 2. Join com Limites
df_consolidado = pd.merge(df_risco_total, df_limites, on='grupo', how='left')

# 3. Cálculo de KPIs de Negócio
df_consolidado['excesso_valor'] = df_consolidado['valor_risco'] - df_consolidado['limite_global']
# Se negativo (dentro do limite), zeramos o excesso visual
df_consolidado['excesso_valor'] = df_consolidado['excesso_valor'].apply(lambda x: x if x > 0 else 0)

df_consolidado['utilizacao_pct'] = (df_consolidado['valor_risco'] / df_consolidado['limite_global']) * 100

# Exibição do resultado para o relatório
display(df_consolidado)

# Lógica para "Call to Action":
# Se utilizacao_pct > 100 ou validade_limite < hoje -> Gatilho de Alerta
alerts = df_consolidado[df_consolidado['utilizacao_pct'] > 100]
if not alerts.empty:
    print(f"ALERTA: {len(alerts)} Grupos com estouro de limite detectados.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
