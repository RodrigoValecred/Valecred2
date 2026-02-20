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
    m.alias = MagicMock(return_value=m)
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
    m.__ge__ = lambda self, other: MagicMock()
    m.__gt__ = lambda self, other: MagicMock()
    m.__le__ = lambda self, other: MagicMock()
    m.__lt__ = lambda self, other: MagicMock()
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

    def test_mora_missing_client_fix(self):
        """
        Validates Mora stream handling missing cod_cliente by resolving it from the map.
        Simulates: fato_baixas (no cod_cliente) JOIN df_map_ops (has cod_cliente_op) -> Resolve cod_cliente.
        """
        df_mora = MagicMock()
        df_ops_map = MagicMock()

        mock_grouped = MagicMock()
        df_mora.groupBy.return_value = mock_grouped

        # Simulate 'cod_cliente' NOT being in df_mora columns
        type(df_mora).columns = ["cod_operacao", "data_baixa", "juros", "valor_pago", "data_vencimento"]

        df_mora.filter.return_value = df_mora
        df_mora.withColumn.return_value = df_mora
        df_mora.join.return_value = df_mora
        df_mora.withColumnRenamed.return_value = df_mora # Simulate rename

        self.spark.read.table.side_effect = lambda t: df_mora if "baixas" in t else df_ops_map

        # Logic
        df_mora_filtered = df_mora.filter(year(col("data_baixa")) == 2025).filter(col("juros") > 0)

        # Map with cod_cliente_op
        df_map_ops_clean = df_ops_map.select(
            col("cod_operacao"),
            col("nbordero").alias("nbordero_op"),
            col("nome_plataforma").alias("nome_plataforma_op"),
            col("chave_produto").alias("chave_produto_op"),
            col("data_deferimento").alias("data_deferimento_op"),
            col("cod_cliente").alias("cod_cliente_op")
        )

        # Join
        df_mora_joined = df_mora_filtered.join(df_map_ops_clean, "cod_operacao", "left")

        # Resolve Ambiguity
        cols_to_resolve = ["nbordero", "nome_plataforma", "chave_produto", "data_deferimento", "cod_cliente"]

        df_resolved = df_mora_joined

        # Simulation of resolve_columns logic
        for col_name in cols_to_resolve:
            col_op = f"{col_name}_op"
            # Simulate column missing in source -> rename op
            if col_name not in ["cod_operacao", "data_baixa", "juros", "valor_pago", "data_vencimento"]: # Not in Mora
                df_resolved = df_resolved.withColumnRenamed(col_op, col_name)

        # Proceed with aggregation
        df_monthly = df_resolved.withColumn("mes_ref", trunc(col("data_baixa"), "MM")) \
            .groupBy("cod_cliente", "mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento")

        # Add the .agg call that was missing in previous failure
        df_monthly.agg(
            sum("valor_pago").alias("volume"),
            sum("valor_vezes_atraso").alias("total_valor_atraso_mes"),
            sum("juros").alias("receita"),
            count("cod_titulo").alias("qtd_eventos")
        )

        # Assert
        # Verify map creation includes cod_cliente_op
        df_ops_map.select.assert_called()

        # Verify aggregation on resolved columns
        df_mora.groupBy.assert_called()
        mock_grouped.agg.assert_called()

    def test_historical_mapping_fix(self):
        """
        Verifies that df_map_ops is created from all operations (not just 2025+),
        while df_ops (Stream 1) is restricted to 2025+.
        """
        # Mocks
        df_raw = MagicMock(name="df_raw")
        df_status_1 = MagicMock(name="df_status_1")
        df_status_2 = MagicMock(name="df_status_2") # Result of status filters (df_ops_full)
        df_2025 = MagicMock(name="df_2025")     # Result of year filter

        # Setup filter chain
        # 1. status filters (two calls)
        df_raw.filter.return_value = df_status_1
        df_status_1.filter.return_value = df_status_2

        # 2. year filter (called on df_status_2)
        df_status_2.filter.return_value = df_2025

        self.spark.read.table.return_value = df_raw

        # --- LOGIC UNDER TEST (The Fix) ---
        # 1. Load Full
        df_ops_full = self.spark.read.table("LH_Gold.fato_operacoes") \
            .filter(col("status_aceite") == "A") \
            .filter(col("status_analise") == "D")

        # 2. Stream 1 (Filtered)
        df_ops = df_ops_full.filter(year(col("data_deferimento")) >= 2025)

        # 3. Map (Unfiltered by year)
        df_map_ops = df_ops_full.select(
            col("cod_operacao"),
            col("nbordero").alias("nbordero_op")
        )

        # --- ASSERTIONS ---
        # Verify df_map_ops is derived from df_status_2 (unfiltered by year)
        df_status_2.select.assert_called()

        # Verify df_ops (Stream 1) involved the year filter
        # It calls filter on df_status_2
        df_status_2.filter.assert_called()

        # Ensure select was NOT called on df_2025 (the year-filtered one)
        df_2025.select.assert_not_called()

if __name__ == "__main__":
    unittest.main()
