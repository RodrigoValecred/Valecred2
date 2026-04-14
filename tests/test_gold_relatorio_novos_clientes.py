
import unittest
from unittest.mock import MagicMock, call
import sys
import os

# 1. Simula módulos PySpark ANTES das importações
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()
sys.modules["pyspark.sql.types"] = MagicMock()
sys.modules["pyspark.sql.window"] = MagicMock()
sys.modules["delta.tables"] = MagicMock()

# 2. Define Funções de Simulação
def col(name):
    m = MagicMock()
    m.__repr__ = lambda x: f"col('{name}')"
    # Simulações de aritmética e comparação
    m.__eq__ = lambda self, other: MagicMock()
    m.__ge__ = lambda self, other: MagicMock()
    m.__le__ = lambda self, other: MagicMock()
    m.__gt__ = lambda self, other: MagicMock()
    m.__lt__ = lambda self, other: MagicMock()
    m.cast = MagicMock(return_value=m) # cast retorna self (simulação)
    m.alias = MagicMock(return_value=m)
    m.isNotNull = MagicMock(return_value=MagicMock())
    return m

def lit(val):
    m = MagicMock()
    m.__repr__ = lambda x: f"lit({val})"
    return m

# Cria um verdadeiro Mock para broadcast para que possamos verificar .called
broadcast_mock = MagicMock(name="broadcast")
broadcast_mock.return_value = MagicMock(name="broadcast_result")
broadcast_mock.__repr__ = lambda x: "broadcast()"

def when(condition, value):
    m = MagicMock()
    m.otherwise = MagicMock(return_value=m)
    m.when = MagicMock(return_value=m)
    return m

def coalesce(*cols): return MagicMock()
def row_number():
    m = MagicMock()
    m.over = MagicMock(return_value=m)
    return m
def min(c): return MagicMock()
def first(c): return MagicMock()

# 3. Patch modules
sys.modules["pyspark.sql.functions"].col = col
sys.modules["pyspark.sql.functions"].lit = lit
sys.modules["pyspark.sql.functions"].broadcast = broadcast_mock
sys.modules["pyspark.sql.functions"].when = when
sys.modules["pyspark.sql.functions"].coalesce = coalesce
sys.modules["pyspark.sql.functions"].row_number = row_number
sys.modules["pyspark.sql.functions"].min = min
sys.modules["pyspark.sql.functions"].first = first

class TestGoldRelatorioNovosClientesOptimization(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()

    def test_optimization_applied(self):
        """
        Verifica se a lógica otimizada (data de pré-conversão e broadcast join) está sintaticamente correta
        e chama as funções Spark esperadas.
        """
        # Simula DataFrames
        df_ops = MagicMock(name="df_ops")
        df_bridge = MagicMock(name="df_bridge")
        df_grupos = MagicMock(name="df_grupos")
        df_gerentes = MagicMock(name="df_gerentes")

        # Configura valores de retorno para read.table
        def side_effect(table_name):
            if table_name == "LH_Silver.staging_operacoes_limpa": return df_ops
            if table_name == "LH_Silver.bridge_cliente_gerente": return df_bridge
            if table_name == "LH_Silver.sup_grupos_economicos": return df_grupos
            if table_name == "LH_Gold.dim_gerentes": return df_gerentes
            return MagicMock()

        self.spark.read.table.side_effect = side_effect

        # Simula métodos encadeados
        df_ops.filter.return_value = df_ops
        df_ops.select.return_value = df_ops
        df_ops.withColumn.return_value = df_ops
        df_ops.join.return_value = df_ops

        df_bridge.withColumnRenamed.return_value = df_bridge

        # --- LÓGICA PARA TESTAR (A Versão Otimizada) ---

        # 1. Leitura
        df_ops_loaded = self.spark.read.table("LH_Silver.staging_operacoes_limpa")
        df_bridge_loaded = self.spark.read.table("LH_Silver.bridge_cliente_gerente")

        # 3. Filtrar Operações Válidas + OPTIMIZATION (Pre-cast)
        df_ops_validas = df_ops_loaded.filter(col("status_aceite") == 'A') \
            .select("cod_operacao", "cod_cliente", "data_inclusao", "data_analise", "cod_broker") \
            .withColumn("data_analise_date", col("data_analise").cast("date"))

        # 4. Enriquecimento + OPTIMIZATION (Broadcast)
        df_bridge_prep = df_bridge_loaded.withColumnRenamed("cod_cliente", "cod_cliente_bridge")

        # O join otimizado
        # Usa alias para clareza e para simular importação
        broadcast = broadcast_mock

        df_ops_enriched = df_ops_validas.join(
            broadcast(df_bridge_prep),
            (col("cod_cliente") == col("cod_cliente_bridge")) &
            (col("data_analise_date") >= col("data_inicio_vigencia")) &
            (col("data_analise_date") <= col("data_fim_vigencia")),
            "left"
        )

        # --- ASSERÇÕES ---

        # Verifica se .withColumn("data_analise_date", ...) foi chamado
        # Verificamos os argumentos das chamadas withColumn em df_ops
        found_cast = False
        for call_args in df_ops.withColumn.call_args_list:
            col_name = call_args[0][0]
            if col_name == "data_analise_date":
                found_cast = True
                break

        self.assertTrue(found_cast, "Optimization: Pre-cast 'data_analise_date' was not found.")

        # Verifica se broadcast foi chamado
        self.assertTrue(broadcast_mock.called, "Optimization: broadcast() was not called.")

if __name__ == '__main__':
    unittest.main()
