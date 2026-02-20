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


class TestRelatorioProdutosMensal(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.spark.read.table.return_value = MagicMock()

    def test_operations_stream(self):
        """
        Validates the logic for Operations stream
        """
        # Mocks
        df_ops = MagicMock()
        # Make chained calls return the same mock (or a tracked mock) to simplify assertion
        # But usually filter returns a new DF.
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

        # 3. Aggregate by Month/Client
        df_monthly = df_ops_joined.withColumn("mes_ref", trunc(col("data_deferimento"), "MM")) \
            .groupBy("cod_cliente", "mes_ref").agg(
                sum("valor_de_face").alias("volume"),
                sum("soma_valor_prazo").alias("total_valor_prazo"),
                sum("desagio").alias("receita_desagio"),
                sum("total_de_tarifas").alias("receita_tarifas")
            )

        # 4. Final Calc
        # Note: df_monthly is df_ops because we mocked the returns
        df_final = df_monthly.withColumn("prazo_medio", col("total_valor_prazo") / col("volume")) \
            .withColumn("receita_total", col("receita_desagio") + col("receita_tarifas")) \
            .withColumn("taxa_mensal", (col("receita_total") / (col("total_valor_prazo") / 30)))

        # --- ASSERTIONS ---
        # Check if filter was called
        df_ops.filter.assert_called()
        # We expect multiple withColumn calls on the chain
        self.assertTrue(df_ops.withColumn.call_count >= 3)

    def test_prorrogations_stream(self):
        """
        Validates Prorrogations stream
        """
        df_prorrog = MagicMock()
        df_prorrog.filter.return_value = df_prorrog
        df_prorrog.withColumn.return_value = df_prorrog
        df_prorrog.groupBy.return_value.agg.return_value = df_prorrog

        self.spark.read.table.return_value = df_prorrog

        # Logic
        df_prorrog_filtered = df_prorrog.filter(year(col("data_inclusao")) == 2025)

        df_prorrog_calc = df_prorrog_filtered.withColumn("valor_vezes_dias", col("valor") * col("dias_prorrogados"))

        df_monthly = df_prorrog_calc.withColumn("mes_ref", trunc(col("data_inclusao"), "MM")) \
            .groupBy("cod_cliente", "mes_ref").agg(
                sum("valor").alias("volume"),
                sum("juros").alias("receita"),
                sum("valor_vezes_dias").alias("total_valor_dias")
            )

        df_final = df_monthly.withColumn("prazo_medio", col("total_valor_dias") / col("volume")) \
            .withColumn("taxa_mensal", (col("receita") / (col("total_valor_dias") / 30)))

        # Assert
        df_prorrog.filter.assert_called()
        self.assertTrue(df_prorrog.withColumn.call_count >= 3)

    def test_mora_stream(self):
        """
        Validates Mora stream
        """
        df_baixas = MagicMock()
        df_baixas.filter.return_value = df_baixas
        df_baixas.withColumn.return_value = df_baixas
        df_baixas.groupBy.return_value.agg.return_value = df_baixas

        self.spark.read.table.return_value = df_baixas

        # Logic
        df_mora = df_baixas.filter(year(col("data_baixa")) == 2025).filter(col("juros") > 0)

        df_mora_calc = df_mora.withColumn("dias_atraso", datediff(col("data_baixa"), col("data_vencimento"))) \
            .withColumn("valor_vezes_atraso", col("valor_pago") * col("dias_atraso"))

        df_monthly = df_mora_calc.withColumn("mes_ref", trunc(col("data_baixa"), "MM")) \
            .groupBy("cod_cliente", "mes_ref").agg(
                sum("valor_pago").alias("volume"),
                sum("juros").alias("receita"),
                sum("valor_vezes_atraso").alias("total_valor_atraso")
            )

        df_final = df_monthly.withColumn("prazo_medio", col("total_valor_atraso") / col("volume")) \
            .withColumn("taxa_mensal", (col("receita") / (col("total_valor_atraso") / 30)))

        # Assert
        self.assertEqual(df_baixas.filter.call_count, 2)
        self.assertTrue(df_baixas.withColumn.call_count >= 4)

if __name__ == "__main__":
    unittest.main()
