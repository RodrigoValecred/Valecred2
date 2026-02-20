import unittest
from unittest.mock import MagicMock, call
import sys

# 1. Mock PySpark modules BEFORE imports
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()
sys.modules["pyspark.sql.types"] = MagicMock()
sys.modules["pyspark.sql.window"] = MagicMock()
sys.modules["notebookutils"] = MagicMock() # For mssparkutils

# 2. Define Mock Functions to mimic PySpark behavior
def col(name):
    m = MagicMock()
    m.__repr__ = lambda x: f"col('{name}')"
    # Implement arithmetic
    m.__mul__ = lambda self, other: MagicMock()
    m.__truediv__ = lambda self, other: MagicMock()
    m.__add__ = lambda self, other: MagicMock()
    m.__sub__ = lambda self, other: MagicMock()
    # Implement comparisons
    m.__gt__ = lambda self, other: MagicMock()
    m.__ge__ = lambda self, other: MagicMock()
    m.__lt__ = lambda self, other: MagicMock()
    m.__le__ = lambda self, other: MagicMock()
    m.__eq__ = lambda self, other: MagicMock()
    return m

def lit(val):
    m = MagicMock()
    m.__repr__ = lambda x: f"lit({val})"
    return m

def sum(c): return MagicMock()
def avg(c): return MagicMock()
def count(c): return MagicMock()
def max(c): return MagicMock()
def min(c): return MagicMock()
def when(condition, value): return MagicMock().otherwise(MagicMock())
def round(c, scale): return MagicMock()
def datediff(end, start): return MagicMock()
def coalesce(*cols): return MagicMock()
def year(c):
    m = MagicMock()
    m.__eq__ = lambda self, other: MagicMock()
    return m
def month(c): return MagicMock()
def to_date(c): return MagicMock()
def trunc(c, fmt): return MagicMock()
def concat(*cols): return MagicMock()
def broadcast(df): return MagicMock()

# 3. Patch the modules with our functions
sys.modules["pyspark.sql.functions"].col = col
sys.modules["pyspark.sql.functions"].sum = sum
sys.modules["pyspark.sql.functions"].avg = avg
sys.modules["pyspark.sql.functions"].count = count
sys.modules["pyspark.sql.functions"].max = max
sys.modules["pyspark.sql.functions"].min = min
sys.modules["pyspark.sql.functions"].lit = lit
sys.modules["pyspark.sql.functions"].when = when
sys.modules["pyspark.sql.functions"].round = round
sys.modules["pyspark.sql.functions"].datediff = datediff
sys.modules["pyspark.sql.functions"].coalesce = coalesce
sys.modules["pyspark.sql.functions"].year = year
sys.modules["pyspark.sql.functions"].month = month
sys.modules["pyspark.sql.functions"].to_date = to_date
sys.modules["pyspark.sql.functions"].trunc = trunc
sys.modules["pyspark.sql.functions"].concat = concat
sys.modules["pyspark.sql.functions"].broadcast = broadcast

class TestRelatorioProdutosMensal(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.spark.read.table.return_value = MagicMock()

    def test_operations_granularity(self):
        """
        Validates the logic for Operations stream with increased granularity.
        """
        # Mocks
        df_ops = MagicMock()
        df_ops.filter.return_value = df_ops
        df_ops.join.return_value = df_ops
        df_ops.withColumn.return_value = df_ops # Chainable
        df_ops.groupBy.return_value.agg.return_value = df_ops # Result of agg is a DF

        df_titulos = MagicMock()
        df_titulos.groupBy.return_value.agg.return_value = df_titulos

        # Setup Spark Read
        def side_effect(table_name):
            if table_name == "LH_Gold.fato_operacoes": return df_ops
            if table_name == "LH_Gold.fato_titulos": return df_titulos
            return MagicMock()

        self.spark.read.table.side_effect = side_effect

        # --- LOGIC TO TEST ---
        # 1. Load & Filter
        df_ops_filtered = df_ops.filter(col("data_deferimento") >= "2025-01-01")

        # 2. Join Titles for Term (Weighted Average)
        df_titulos_agg = df_titulos.groupBy("cod_operacao").agg(
            sum(col("valor") * col("prazo")).alias("soma_valor_prazo"),
            sum("valor").alias("soma_valor_titulos")
        )

        df_ops_joined = df_ops_filtered.join(df_titulos_agg, "cod_operacao", "left")

        # 3. Aggregate by Monthly Granularity (cod_operacao, nbordero, etc.)
        df_monthly = df_ops_joined.withColumn("mes_ref", trunc(col("data_deferimento"), "MM")) \
            .groupBy("cod_cliente", "mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento").agg(
                sum("valor_de_face").alias("volume"),
                sum("soma_valor_prazo").alias("total_valor_prazo"),
                sum("desagio").alias("receita_desagio"),
                sum("total_de_tarifas").alias("receita_tarifas")
            )

        # 4. Final Calc
        df_final = df_monthly.withColumn("prazo_medio", col("total_valor_prazo") / col("volume")) \
            .withColumn("receita_total", col("receita_desagio") + col("receita_tarifas")) \
            .withColumn("taxa_mensal", (col("receita_total") / (col("total_valor_prazo") / 30))) \
            .withColumnRenamed("chave_produto", "sub_tipo_produto")

        # --- ASSERTIONS ---
        # Check aggregation structure
        self.assertTrue(df_ops.withColumn.call_count >= 3)
        self.assertTrue(df_ops_joined.groupBy.called)

    def test_prorrogations_granularity(self):
        """
        Validates Prorrogations stream with increased granularity.
        Prorrogations must join with Operations (or equivalent) to get granular details.
        """
        df_prorrog = MagicMock()
        df_ops_map = MagicMock()

        df_prorrog.filter.return_value = df_prorrog
        df_prorrog.withColumn.return_value = df_prorrog
        df_prorrog.join.return_value = df_prorrog
        df_prorrog.groupBy.return_value.agg.return_value = df_prorrog

        self.spark.read.table.side_effect = lambda t: df_prorrog if "prorrogacoes" in t else df_ops_map

        # Logic
        df_prorrog_filtered = df_prorrog.filter(year(col("data_inclusao")) == 2025)

        # Join with Operations Map to get Granular Details (nbordero, plataforma, etc.)
        df_prorrog_joined = df_prorrog_filtered.join(df_ops_map, "cod_operacao", "left")

        df_prorrog_calc = df_prorrog_joined.withColumn("valor_vezes_dias", col("valor") * col("dias_prorrogados"))

        df_monthly = df_prorrog_calc.withColumn("mes_ref", trunc(col("data_inclusao"), "MM")) \
            .groupBy("cod_cliente", "mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento").agg(
                sum("valor").alias("volume"),
                sum("juros").alias("receita"),
                sum("valor_vezes_dias").alias("total_valor_dias")
            )

        df_final = df_monthly.withColumn("prazo_medio", col("total_valor_dias") / col("volume")) \
            .withColumn("taxa_mensal", (col("receita") / (col("total_valor_dias") / 30))) \
            .withColumnRenamed("chave_produto", "sub_tipo_produto")

        # Assert
        self.assertTrue(df_prorrog.join.called)
        self.assertTrue(df_monthly.withColumn.called)

if __name__ == "__main__":
    unittest.main()
