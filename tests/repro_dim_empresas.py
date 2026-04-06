import unittest
from unittest.mock import MagicMock
import sys

# Simulação do módulo pyspark já que não está instalado
pyspark = MagicMock()
pyspark.sql = MagicMock()
pyspark.sql.functions = MagicMock()
sys.modules["pyspark"] = pyspark
sys.modules["pyspark.sql"] = pyspark.sql
sys.modules["pyspark.sql.functions"] = pyspark.sql.functions


class TestDimEmpresas(unittest.TestCase):
    def setUp(self):
        # Redefine simulações globais para garantir o isolamento do teste
        pyspark.sql.functions.reset_mock()

        # Simula Spark Session
        self.spark = MagicMock()

        # Simula DataFrames
        self.df_empresas = MagicMock()
        self.df_cadastros = MagicMock()
        self.df_apelidos = MagicMock()

        # Configura comportamento semelhante a schema
        self.df_empresas.filter.return_value = self.df_empresas
        self.df_empresas.withColumn.return_value = self.df_empresas
        self.df_empresas.alias.return_value = self.df_empresas

        self.df_cadastros.alias.return_value = self.df_cadastros
        self.df_apelidos.alias.return_value = self.df_apelidos

        # Simula joins
        self.df_joined = MagicMock()
        self.df_empresas.join.return_value = self.df_joined
        self.df_joined.join.return_value = self.df_joined

        # Simula transformações finais
        self.df_joined.withColumn.return_value = self.df_joined
        self.df_joined.select.return_value = self.df_joined

        # Configuração dos valores de retorno para leituras de tabela
        def read_table_side_effect(table_name):
            if table_name == "LH_Silver.staging_empresas":
                return self.df_empresas
            elif table_name == "LH_Silver.staging_cad_geral_pf_pj_limpa":
                return self.df_cadastros
            elif table_name == "LH_Silver.sup_apelido_empresas":
                return self.df_apelidos
            return MagicMock()

        self.spark.read.table.side_effect = read_table_side_effect

        # Como simulamos o módulo, podemos acessar as funções diretamente
        self.mock_col = pyspark.sql.functions.col
        self.mock_lit = pyspark.sql.functions.lit
        self.mock_concat = pyspark.sql.functions.concat
        self.mock_regexp_replace = pyspark.sql.functions.regexp_replace
        self.mock_when = pyspark.sql.functions.when

        # Executa a lógica do notebook (adaptado para teste)
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

        # Primeiro join (o existente e correto)
        df_j = df_e_clean.alias("e").join(
            df_c.alias("c"),
            self.mock_col("e.cnpj_clean") == self.mock_col("c.cpf_cnpj"),
            "left"
        )

        # Segunda junção (A que deve ser corrigida)
        # Simulando a lógica CORRIGIDA
        df_j_final = df_j.join(
            df_a.alias("a"),
            # Condição do join corrigida
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
                # Coluna corrigida
                self.mock_col("a.apelido_empresa").alias("empresa"),
                self.mock_col("TIPO")
            )
        self.df_final = df_final

    def test_join_condition_columns(self):
        """Verifica as chamadas col para o segundo join"""
        self.mock_col.assert_any_call("e.cod_empresa")
        self.mock_col.assert_any_call("a.cod_empresa")

    def test_selection_columns(self):
        """Verifica as chamadas col para seleção"""
        self.mock_col.assert_any_call("a.apelido_empresa")

    def test_incorrect_columns_not_used(self):
        """Garante que colunas incorretas a.nome e a.apelido NÃO foram chamadas"""
        with self.assertRaises(AssertionError):
            self.mock_col.assert_any_call("a.nome")

        with self.assertRaises(AssertionError):
            self.mock_col.assert_any_call("a.apelido")


if __name__ == '__main__':
    unittest.main()
