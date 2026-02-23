
import unittest
from unittest.mock import MagicMock, patch
import sys

# Mock pyspark module since it is not installed
pyspark = MagicMock()
pyspark.sql = MagicMock()
pyspark.sql.functions = MagicMock()
sys.modules["pyspark"] = pyspark
sys.modules["pyspark.sql"] = pyspark.sql
sys.modules["pyspark.sql.functions"] = pyspark.sql.functions

class TestDimEmpresas(unittest.TestCase):
    def test_dim_empresas_logic(self):
        # Mock Spark Session
        spark = MagicMock()

        # Mock DataFrames
        df_empresas = MagicMock()
        df_cadastros = MagicMock()
        df_apelidos = MagicMock()

        # Setup schema-like behavior
        df_empresas.filter.return_value = df_empresas
        df_empresas.withColumn.return_value = df_empresas
        df_empresas.alias.return_value = df_empresas

        df_cadastros.alias.return_value = df_cadastros
        df_apelidos.alias.return_value = df_apelidos

        # Mock joins
        df_joined = MagicMock()
        df_empresas.join.return_value = df_joined
        df_joined.join.return_value = df_joined

        # Mock final transformations
        df_joined.withColumn.return_value = df_joined
        df_joined.select.return_value = df_joined

        # Setup return values for table reads
        def read_table_side_effect(table_name):
            if table_name == "LH_Silver.staging_empresas":
                return df_empresas
            elif table_name == "LH_Silver.staging_cad_geral_pf_pj_limpa":
                return df_cadastros
            elif table_name == "LH_Silver.sup_apelido_empresas":
                return df_apelidos
            return MagicMock()

        spark.read.table.side_effect = read_table_side_effect

        # Because we mocked the module, we can access the functions directly from the mock
        mock_col = pyspark.sql.functions.col
        mock_lit = pyspark.sql.functions.lit
        mock_concat = pyspark.sql.functions.concat
        mock_regexp_replace = pyspark.sql.functions.regexp_replace
        mock_when = pyspark.sql.functions.when

        # Execute the logic from the notebook (adapted for test)
        # 1. Leitura
        df_e = spark.read.table("LH_Silver.staging_empresas")
        df_e = df_e.filter(mock_col("cod_empresa").isin([6, 14, 24, 25]))

        df_c = spark.read.table("LH_Silver.staging_cad_geral_pf_pj_limpa")
        df_a = spark.read.table("LH_Silver.sup_apelido_empresas")

        # 2. Preparação
        df_e_clean = df_e.withColumn("cnpj_clean", mock_regexp_replace(mock_col("cnpj"), "[^0-9]", ""))

        # First join (existing correct one)
        df_j = df_e_clean.alias("e").join(
            df_c.alias("c"),
            mock_col("e.cnpj_clean") == mock_col("c.cpf_cnpj"),
            "left"
        )

        # Second join (The one to be fixed)
        # Simulating the FIXED logic
        df_j_final = df_j.join(
            df_a.alias("a"),
            mock_col("e.cod_empresa") == mock_col("a.cod_empresa"), # Corrected join condition
            "left"
        )

        # 3. Transformações
        df_final = df_j_final \
            .withColumn("base", mock_lit(40)) \
            .select(
                mock_col("base"),
                mock_col("chave_base_empresa"),
                mock_col("chave_base_cadastro"),
                mock_col("e.cnpj"),
                mock_col("e.cod_empresa"),
                mock_col("c.nome").alias("nome_original"),
                mock_col("a.apelido_empresa").alias("empresa"), # Corrected column
                mock_col("TIPO")
            )

        # Verification

        # Verify col calls for the second join
        mock_col.assert_any_call("e.cod_empresa")
        mock_col.assert_any_call("a.cod_empresa")

        # Verify col calls for selection
        mock_col.assert_any_call("a.apelido_empresa")

        # Ensure a.nome and a.apelido were NOT called
        try:
            mock_col.assert_any_call("a.nome")
            self.fail("FAIL: 'a.nome' was accessed!")
        except AssertionError:
            pass # Good

        try:
            mock_col.assert_any_call("a.apelido")
            self.fail("FAIL: 'a.apelido' was accessed!")
        except AssertionError:
            pass # Good

if __name__ == '__main__':
    unittest.main()
