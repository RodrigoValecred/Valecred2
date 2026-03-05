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

try:
    from pyspark.sql.functions import col, sum as spark_sum, min as spark_min, max as spark_max, coalesce, lit
except ImportError:
    pass

def get_production_data(spark):
    """
    Reads production data from Gold layer (fato_titulos, dim_clientes).
    Returns (df_ops, df_limites) as Pandas DataFrames matching the mock schema.

    Note: We use LH_Gold tables (NB_Curadoria_Gold output) as they contain the
    curated risk and limit data, rather than raw Silver tables mentioned in
    older comments (silver_operacoes).
    """
    print("Reading production data from Gold Layer...")

    # 1. Read Tables
    df_titulos = spark.read.table("LH_Gold.fato_titulos")
    df_clientes = spark.read.table("LH_Gold.dim_clientes")

    # 2. Filter Active Risk (Accepted and Not Liquidated)
    df_risk_active = df_titulos.filter(
        (col("status_deferimento") == "Sim") &
        (col("liquidacao").isNull())
    )

    # 3. Join with Client Data to get Group info
    # We join on cod_cliente.
    df_joined = df_risk_active.join(df_clientes, "cod_cliente", "left")

    # 4. Prepare df_ops (Granular Operations/Titles)
    # Schema: grupo, cedente, produto, valor_risco, data_vencimento
    # Logic:
    # - grupo: nome_do_grupo (fallback to 'Sem Grupo')
    # - cedente: nome (fallback to cod_cliente)
    # - produto: chave_produto
    # - valor_risco: valor_devido
    # - data_vencimento: data_vencimento_util

    df_ops_spark = df_joined.select(
        coalesce(col("nome_do_grupo"), lit("Sem Grupo")).alias("grupo"),
        coalesce(col("nome"), col("cod_cliente").cast("string")).alias("cedente"),
        col("chave_produto").alias("produto"),
        col("valor_devido").alias("valor_risco"),
        col("data_vencimento_util").alias("data_vencimento")
    )

    # 5. Prepare df_limites (Aggregated by Group)
    # Schema: grupo, limite_global, validade_limite
    # We use MAX logic for limits assuming group limits are projected to clients in dim_clientes.
    df_limites_spark = df_clientes.groupBy(
        coalesce(col("nome_do_grupo"), lit("Sem Grupo")).alias("grupo")
    ).agg(
        spark_max("limite").alias("limite_global"),
        spark_min("vencimento_limite").alias("validade_limite")
    ).select(
        col("grupo"),
        col("limite_global"),
        col("validade_limite")
    )

    # Check if we should use spark.sql.execution.arrow.pyspark.enabled
    try:
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    except Exception as e:
        print(f"Aviso: Não foi possível habilitar o pyarrow: {e}")

    # 🧠 TENSOR OPTIMIZATION: Retornar Spark DataFrames para evitar overhead de driver e network.
    # Em vez de chamar .toPandas() em df_ops_spark (granular, com milhares de linhas),
    # delegamos a agregação e o join para o cluster Spark antes da conversão.
    return df_ops_spark, df_limites_spark

def get_mock_data():
    """
    Returns mock data for simulation/testing purposes.
    """
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

    return df_ops, df_limites

# Main Data Loading Logic
# In production (Synapse/Fabric), 'spark' is available globally.
if 'spark' in locals() or 'spark' in globals():
    print("Spark session detected. Loading production data from Gold Layer...")
    # Intentionally letting exceptions propagate here to fail the job if production data is missing/invalid.
    df_ops_spark, df_limites_spark = get_production_data(spark)

    # Use current date for production report
    data_hoje = datetime.now().date()
    print("Successfully loaded production data.")

    # 🧠 TENSOR OPTIMIZATION: Spark distributed aggregation and join before collecting to driver memory.
    df_risco_total_spark = df_ops_spark.groupBy('grupo').agg(
        spark_sum('valor_risco').alias('valor_risco')
    )
    df_consolidado_spark = df_risco_total_spark.join(df_limites_spark, on='grupo', how='left')

    # Finally, convert the much smaller, aggregated DataFrame to Pandas
    df_consolidado = df_consolidado_spark.toPandas()

else:
    print("Spark session not found. Using mock data for simulation/testing.")
    df_ops, df_limites = get_mock_data()

    # Use simulation date to match mock data scenario (e.g. limit expiry check)
    # Mock data has limit expiring in Dec 2025, so we simulate Dec 2025 context.
    data_hoje = datetime(2025, 12, 23).date()

    # 1. Agregação de Risco por Grupo
    df_risco_total = df_ops.groupby('grupo')['valor_risco'].sum().reset_index()

    # 2. Join com Limites
    df_consolidado = pd.merge(df_risco_total, df_limites, on='grupo', how='left')

data_semana_passada = data_hoje - timedelta(days=7)

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

def style_risk_dataframe(df):
    """
    Applies visual styling to the risk dataframe for better readability.
    - Formats currency columns
    - Color codes utilization percentage
    """
    styler = df.style

    # 1. Format Currency Columns
    currency_cols = ['valor_risco', 'limite_global', 'excesso_valor']
    existing_cols = [c for c in currency_cols if c in df.columns]

    format_dict = {c: format_currency_br for c in existing_cols}
    format_dict['utilizacao_pct'] = "{:.2f}%"

    styler = styler.format(format_dict)

    # 2. Color Code 'utilizacao_pct'
    def color_utilization(val):
        color = 'white'
        if pd.isna(val) or np.isinf(val):
             color = 'white'
        elif val > 100:
            color = '#ffcccc' # Light Red
        elif val > 80:
            color = '#fff4cc' # Light Yellow
        else:
            color = '#ccffcc' # Light Green
        return f'background-color: {color}; color: black'

    if 'utilizacao_pct' in df.columns:
        # Use map (Pandas 1.3+) or applymap (older)
        if hasattr(styler, 'map'):
            styler = styler.map(color_utilization, subset=['utilizacao_pct'])
        else:
            styler = styler.applymap(color_utilization, subset=['utilizacao_pct'])

    styler = styler.set_caption("Relatório Diário de Risco - Detalhado")
    return styler

def prepare_dashboard_data(df, ref_date):
    """
    Prepares the risk dashboard data for display.
    Returns a list of dictionaries containing formatted strings and display properties.
    """
    view_data = []

    for row in df.itertuples(index=False):
        item = {}

        # 1. Group Name Truncation
        grupo = str(row.grupo)
        if len(grupo) > 50:
             item['grupo_display'] = grupo[:47] + "..."
        else:
             item['grupo_display'] = grupo

        # 2. Extract Values
        utilizacao = row.utilizacao_pct
        risco = row.valor_risco
        limite = row.limite_global
        excesso = getattr(row, 'excesso_valor', 0)
        validade = getattr(row, 'validade_limite', 'N/A')

        item['risco_fmt'] = format_currency_br(risco)
        item['limite_fmt'] = format_currency_br(limite)
        item['excesso_fmt'] = format_currency_br(excesso)

        # 3. Validity Logic
        validade_display = str(validade)
        try:
             val_date = datetime.strptime(str(validade), '%Y-%m-%d').date()
             days_remaining = (val_date - ref_date).days

             # Format date as DD/MM/YYYY for better UX consistency
             val_date_str = val_date.strftime('%d/%m/%Y')

             if days_remaining < 0:
                 validade_display = f"{Colors.RED}{val_date_str} (VENCIDO) ⚠️{Colors.RESET}"
             elif days_remaining <= 30:
                 validade_display = f"{Colors.YELLOW}{val_date_str} ({days_remaining}d){Colors.RESET}"
             else:
                 validade_display = val_date_str
        except (ValueError, TypeError) as e:
             if str(validade) not in ('N/A', 'nan', 'None', 'NaT'):
                 print(f"Aviso: Não foi possível formatar a data de validade '{validade}': {e}")
        item['validade_display'] = validade_display

        # 4. Available Amount
        disponivel = max(0, limite - risco)
        item['disponivel_fmt'] = format_currency_br(disponivel)

        # 5. Progress Bar Logic
        bar_length = 25
        color = Colors.RESET
        status_icon = ""
        util_str = ""

        if pd.isna(utilizacao) or np.isinf(utilizacao):
            bar = '░' * bar_length
            status_icon = "⚠️"
            status_text = "Indisponível"
            util_str = "N/A"
            color = Colors.YELLOW
            item['is_valid_utilization'] = False
        else:
            item['is_valid_utilization'] = True
            pct_clamped = min(max(utilizacao, 0), 100)
            filled_length = int(bar_length * pct_clamped / 100)

            if utilizacao <= 80:
                status_icon = "✅"
                status_text = "Seguro"
                color = Colors.GREEN
            elif utilizacao <= 100:
                status_icon = "⚠️"
                status_text = "Atenção"
                color = Colors.YELLOW
            else:
                status_icon = "🚨"
                status_text = "Crítico"
                color = Colors.RED

            bar = color + '█' * filled_length + Colors.RESET + '░' * (bar_length - filled_length)
            util_str = f"{utilizacao:.1f}%"

        # Improved UX: Add status text for clarity
        item['bar_display'] = f"[{bar}] {color}{util_str:>6}{Colors.RESET} {status_icon} {color}({status_text}){Colors.RESET}"
        item['utilizacao_val'] = utilizacao
        item['is_excess'] = (not (pd.isna(utilizacao) or np.isinf(utilizacao))) and (utilizacao > 100)

        view_data.append(item)

    return view_data

def display_risk_dashboard(df, ref_date=None):
    W = 60

    # Handle optional ref_date with backward compatibility for global 'data_hoje'
    if ref_date is None:
        if 'data_hoje' in globals():
            ref_date = globals()['data_hoje']
        else:
            ref_date = datetime.now().date()

    view_data = prepare_dashboard_data(df, ref_date)

    print("\n")
    print(Colors.BOLD + Colors.CYAN + "═"*W + Colors.RESET)
    print(Colors.BOLD + Colors.CYAN + f"{' 📊 PAINEL DE RISCO - RELATÓRIO DIÁRIO':^{W}}" + Colors.RESET)

    # Improved UX: Show reference date clearly
    date_str = ref_date.strftime('%d/%m/%Y') if ref_date else "N/A"
    print(Colors.BOLD + Colors.CYAN + f"{f'📅 Data de Referência: {date_str}':^{W}}" + Colors.RESET)

    print(Colors.BOLD + Colors.CYAN + "═"*W + Colors.RESET)

    # Empty State UX
    if df.empty:
        print(f"{'⚠️ NENHUM GRUPO COM RISCO ATIVO ENCONTRADO':^{W}}")
        print(Colors.BOLD + Colors.CYAN + "═"*W + Colors.RESET)
        print("\n")
        return

    # Summary
    n_groups = len(df)
    n_alerts = len(df[df['utilizacao_pct'] > 100])
    summary_color = Colors.RED if n_alerts > 0 else Colors.GREEN
    print(f" Resumo: {n_groups} Grupos analisados. {summary_color}{n_alerts} Alertas.{Colors.RESET}")
    print(Colors.CYAN + "─"*W + Colors.RESET)

    total_rows = len(view_data)
    for i, item in enumerate(view_data):
        print(f" {Colors.BOLD}🏢 {item['grupo_display']}{Colors.RESET}")

        # Metrics
        print(f"    Utilização: {item['bar_display']}")
        print(f"    Risco:      {item['risco_fmt']:>15}")
        print(f"    Limite:     {item['limite_fmt']:>15}")
        print(f"    Validade:   {item['validade_display']:>26}")

        if item.get('is_valid_utilization', False):
            if item.get('is_excess', False):
                print(f"    {Colors.BOLD}{Colors.RED}🔥 EXCESSO: {item['excesso_fmt']:>15}{Colors.RESET}")
            else:
                print(f"    {Colors.GREEN}Disponível: {item['disponivel_fmt']:>15}{Colors.RESET}")

        # Separator
        if i < total_rows - 1:
            print(" " + Colors.CYAN + "─"*(W-2) + Colors.RESET)

    print(Colors.BOLD + Colors.CYAN + "═"*W + Colors.RESET)

    # Legend for UX
    print(Colors.CYAN + f"{'Legenda: ✅ Seguro (<=80%) | ⚠️ Atenção (80-100%) | 🚨 Crítico (>100%)':^{W}}" + Colors.RESET)

    print("\n")

# Exibição do resultado para o relatório
display(style_risk_dataframe(df_consolidado))
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
