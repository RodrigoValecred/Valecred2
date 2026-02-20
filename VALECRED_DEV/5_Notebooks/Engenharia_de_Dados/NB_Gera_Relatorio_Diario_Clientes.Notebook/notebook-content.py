# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# Este notebook visa criar um relatório diário do risco do cliente
# Usado pela mesa de operações que acompanham clientes específicos e encaminham para diretoria diariamente.
# O objetivo é automatizar essa coleta de dados para disponibilizar um relatório pronto diariamente.

# CELL ********************

import pandas as pd
import numpy as np
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

# ==============================================================================
# DASHBOARD RÁPIDO DE RISCO (UX)
# ==============================================================================

# Color constants for ANSI terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def format_currency_br(value):
    """Formats float to Brazilian currency string (R$ 1.234,56)."""
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def display_risk_dashboard(df):
    W = 60

    print("\n")
    print(Colors.BOLD + Colors.CYAN + "═"*W + Colors.RESET)
    print(Colors.BOLD + Colors.CYAN + f"{' 📊 PAINEL DE RISCO - RELATÓRIO DIÁRIO':^{W}}" + Colors.RESET)
    print(Colors.BOLD + Colors.CYAN + "═"*W + Colors.RESET)

    # Summary
    n_groups = len(df)
    n_alerts = len(df[df['utilizacao_pct'] > 100])
    summary_color = Colors.RED if n_alerts > 0 else Colors.GREEN
    print(f" Resumo: {n_groups} Grupos analisados. {summary_color}{n_alerts} Alertas.{Colors.RESET}")
    print(Colors.CYAN + "─"*W + Colors.RESET)

    total_rows = len(df)
    for i, (_, row) in enumerate(df.iterrows()):
        grupo = str(row['grupo'])
        if len(grupo) > 50:
             grupo = grupo[:47] + "..."

        utilizacao = row['utilizacao_pct']
        risco = row['valor_risco']
        limite = row['limite_global']
        excesso = row.get('excesso_valor', 0)

        # Progress Bar Logic (V3)
        bar_length = 25
        color = Colors.RESET

        if pd.isna(utilizacao) or np.isinf(utilizacao):
            bar = '░' * bar_length
            status_icon = "⚠️"
            util_str = "N/A"
            color = Colors.YELLOW
        else:
            pct_clamped = min(max(utilizacao, 0), 100)
            filled_length = int(bar_length * pct_clamped / 100)

            if utilizacao <= 80:
                status_icon = "✅"
                color = Colors.GREEN
            elif utilizacao <= 100:
                status_icon = "⚠️"
                color = Colors.YELLOW
            else:
                status_icon = "🚨"
                color = Colors.RED

            bar = color + '█' * filled_length + Colors.RESET + '░' * (bar_length - filled_length)
            util_str = f"{utilizacao:.1f}%"

        print(f" {Colors.BOLD}🏢 {grupo}{Colors.RESET}")

        # Metrics
        # Indent 4 spaces
        bar_display = f"[{bar}] {color}{util_str:>6}{Colors.RESET} {status_icon}"
        print(f"    Utilização: {bar_display}")
        print(f"    Risco:      {format_currency_br(risco):>15}")
        print(f"    Limite:     {format_currency_br(limite):>15}")

        if not (pd.isna(utilizacao) or np.isinf(utilizacao)) and utilizacao > 100:
             print(f"    {Colors.BOLD}{Colors.RED}🔥 EXCESSO: {format_currency_br(excesso):>15}{Colors.RESET}")

        # Separator
        if i < total_rows - 1:
            print(" " + Colors.CYAN + "─"*(W-2) + Colors.RESET)

    print(Colors.BOLD + Colors.CYAN + "═"*W + Colors.RESET)
    print("\n")

# Exibição do resultado para o relatório
display(df_consolidado)
display_risk_dashboard(df_consolidado)

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
