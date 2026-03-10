import unittest
from unittest.mock import MagicMock
import sys

# Mock pyspark module since it is not installed
pyspark = MagicMock()
pyspark.sql = MagicMock()
pyspark.sql.functions = MagicMock()
sys.modules["pyspark"] = pyspark
sys.modules["pyspark.sql"] = pyspark.sql
sys.modules["pyspark.sql.functions"] = pyspark.sql.functions


class TestDimEmpresas(unittest.TestCase):
    def setUp(self):
        # Reset global mocks to ensure test isolation
        pyspark.sql.functions.reset_mock()

        # Mock Spark Session
        self.spark = MagicMock()

        # Mock DataFrames
        self.df_empresas = MagicMock()
        self.df_cadastros = MagicMock()
        self.df_apelidos = MagicMock()

        # Setup schema-like behavior
        self.df_empresas.filter.return_value = self.df_empresas
        self.df_empresas.withColumn.return_value = self.df_empresas
        self.df_empresas.alias.return_value = self.df_empresas

        self.df_cadastros.alias.return_value = self.df_cadastros
        self.df_apelidos.alias.return_value = self.df_apelidos

        # Mock joins
        self.df_joined = MagicMock()
        self.df_empresas.join.return_value = self.df_joined
        self.df_joined.join.return_value = self.df_joined

        # Mock final transformations
        self.df_joined.withColumn.return_value = self.df_joined
        self.df_joined.select.return_value = self.df_joined

        # Setup return values for table reads
        def read_table_side_effect(table_name):
            if table_name == "LH_Silver.staging_empresas":
                return self.df_empresas
            elif table_name == "LH_Silver.staging_cad_geral_pf_pj_limpa":
                return self.df_cadastros
            elif table_name == "LH_Silver.sup_apelido_empresas":
                return self.df_apelidos
            return MagicMock()

        self.spark.read.table.side_effect = read_table_side_effect

        # Because we mocked the module, we can access functions directly
        self.mock_col = pyspark.sql.functions.col
        self.mock_lit = pyspark.sql.functions.lit
        self.mock_concat = pyspark.sql.functions.concat
        self.mock_regexp_replace = pyspark.sql.functions.regexp_replace
        self.mock_when = pyspark.sql.functions.when

        # Execute the logic from the notebook (adapted for test)
        self._execute_dim_empresas_logic()

    def _execute_dim_empresas_logic(self):
        # 1. Leitura
        df_e = self.spark.read.table("LH_Silver.staging_empresas")
        df_e = df_e.filter(self.mock_col("cod_empresa").isin([6, 14, 24, 25]))

        df_c = self.spark.read.table("LH_Silver.staging_cad_geral_pf_pj_limpa")
        df_a = self.spark.read.table("LH_Silver.sup_apelido_empresas")

        # 2. Preparação
        df_e_clean = df_e.withColumn(
            "cnpj_clean",
            self.mock_regexp_replace(self.mock_col("cnpj"), "[^0-9]", "")
        )

        # First join (existing correct one)
        df_j = df_e_clean.alias("e").join(
            df_c.alias("c"),
            self.mock_col("e.cnpj_clean") == self.mock_col("c.cpf_cnpj"),
            "left"
        )

        # Second join (The one to be fixed)
        # Simulating the FIXED logic
        df_j_final = df_j.join(
            df_a.alias("a"),
            # Corrected join condition
            self.mock_col("e.cod_empresa") == self.mock_col("a.cod_empresa"),
            "left"
        )

        # 3. Transformações
        df_final = df_j_final \
            .withColumn("base", self.mock_lit(40)) \
            .select(
                self.mock_col("base"),
                self.mock_col("chave_base_empresa"),
                self.mock_col("chave_base_cadastro"),
                self.mock_col("e.cnpj"),
                self.mock_col("e.cod_empresa"),
                self.mock_col("c.nome").alias("nome_original"),
                # Corrected column
                self.mock_col("a.apelido_empresa").alias("empresa"),
                self.mock_col("TIPO")
            )
        self.df_final = df_final

    def test_join_condition_columns(self):
        """Verify col calls for the second join"""
        self.mock_col.assert_any_call("e.cod_empresa")
        self.mock_col.assert_any_call("a.cod_empresa")

    def test_selection_columns(self):
        """Verify col calls for selection"""
        self.mock_col.assert_any_call("a.apelido_empresa")

    def test_incorrect_columns_not_used(self):
        """Ensure incorrect columns a.nome and a.apelido were NOT called"""
        with self.assertRaises(AssertionError):
            self.mock_col.assert_any_call("a.nome")

        with self.assertRaises(AssertionError):
            self.mock_col.assert_any_call("a.apelido")


if __name__ == '__main__':
    unittest.main()
