import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime, date

import sys
sys.path.append('.')


FILE_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Relatorio_Titulos_Vencidos_Gerente.Notebook/notebook-content.py"

@pytest.fixture
def mock_spark_session():
    mock_spark = MagicMock()
    mock_spark.conf.set = MagicMock()

    # Mock para os dataframes
    mock_df_titulos = MagicMock()
    mock_df_clientes = MagicMock()
    mock_df_gerentes = MagicMock()

    def mock_read_table(table_name):
        if table_name == "LH_Gold.fato_titulos":
            return mock_df_titulos
        elif table_name == "LH_Gold.dim_clientes":
            return mock_df_clientes
        elif table_name == "LH_Gold.dim_gerentes":
            return mock_df_gerentes
        else:
            raise ValueError(f"Tabela desconhecida: {table_name}")

    mock_spark.read.table = MagicMock(side_effect=mock_read_table)

    # Mock das junções e agg
    mock_filtered = MagicMock()
    mock_df_titulos.filter.return_value = mock_filtered

    mock_clientes_selected = MagicMock()
    mock_df_clientes.select.return_value = mock_clientes_selected

    mock_joined_1 = MagicMock()
    mock_filtered.join.return_value = mock_joined_1

    mock_gerentes_selected = MagicMock()
    mock_df_gerentes.select.return_value = mock_gerentes_selected

    mock_joined_2 = MagicMock()
    mock_joined_1.join.return_value = mock_joined_2

    mock_grouped = MagicMock()
    mock_joined_2.groupBy.return_value = mock_grouped

    mock_agg = MagicMock()
    mock_grouped.agg.return_value = mock_agg

    # Mock do mssparkutils
    mock_mssparkutils = MagicMock()

    return mock_spark, mock_df_titulos, mock_df_clientes, mock_df_gerentes, mock_mssparkutils, mock_agg

class ColMock:
    def __init__(self, name):
        self.name = name
    def __eq__(self, other):
        return MagicMock()
    def __lt__(self, other):
        return MagicMock()
    def __gt__(self, other):
        return MagicMock()
    def isNull(self):
        return MagicMock()
    def __and__(self, other):
        return MagicMock()

@patch('pyspark.sql.functions.col', lambda x: ColMock(x))
@patch('pyspark.sql.functions.sum', MagicMock())
@patch('pyspark.sql.functions.count', MagicMock())
@patch('pyspark.sql.functions.when', MagicMock())
@patch('pyspark.sql.functions.current_date', MagicMock())
@patch('pyspark.sql.functions.broadcast', MagicMock(side_effect=lambda x: x))
def test_notebook_execution(mock_spark_session):
    mock_spark, mock_df_titulos, mock_df_clientes, mock_df_gerentes, mock_mssparkutils, mock_agg = mock_spark_session

    import pyspark.sql.functions as F

    # Injetar mocks
    exec_globals = {
        'spark': mock_spark,
        'mssparkutils': mock_mssparkutils,
        'F': F,
        'col': lambda x: ColMock(x),
        '_sum': F.sum,
        'count': F.count,
        'when': F.when,
        'current_date': F.current_date,
        'broadcast': F.broadcast
    }

    # Executar o código do notebook
    with open(FILE_PATH, 'r') as f:
        code = f.read()

    # Como o arquivo usa 'mssparkutils.notebook.exit', precisamos dar um mock no exception dele ou dar bypass
    mock_mssparkutils.notebook.exit.side_effect = Exception("EXIT")

    try:
        exec(code, exec_globals)
    except Exception as e:
        if str(e) != "EXIT":
            raise

    # Verificações
    mock_spark.read.table.assert_any_call("LH_Gold.fato_titulos")
    mock_spark.read.table.assert_any_call("LH_Gold.dim_clientes")
    mock_spark.read.table.assert_any_call("LH_Gold.dim_gerentes")

    # Verifica se a gravação foi chamada
    mock_agg.write.mode.assert_called_with("overwrite")
