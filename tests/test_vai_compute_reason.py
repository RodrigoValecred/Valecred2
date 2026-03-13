import pytest
from tests.notebook_utils import extract_function_from_file

NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py"

class TestVaiComputeReasonUdf:
    def setup_method(self):
        # Extract the pure logic function from the notebook
        self.compute_reason_logic_source = extract_function_from_file(NOTEBOOK_PATH, "compute_reason_logic")

        if not self.compute_reason_logic_source:
            pytest.fail(f"Could not extract compute_reason_logic from {NOTEBOOK_PATH}")

        # Execute the function definition in a local namespace
        self.local_scope = {}
        exec(self.compute_reason_logic_source, self.local_scope, self.local_scope)
        self.compute_reason_logic = self.local_scope['compute_reason_logic']

        # Also extract the UDF wrapper to test the N/A condition if desired
        self.compute_reason_udf_source = extract_function_from_file(NOTEBOOK_PATH, "compute_reason_udf")
        if self.compute_reason_udf_source:
            scope = {
                'udf': lambda returnType=None: lambda f: f,
                'StringType': lambda: None,
                'compute_reason_logic': self.compute_reason_logic
            }
            exec(self.compute_reason_udf_source, scope, scope)
            self.compute_reason_udf = scope['compute_reason_udf']
        else:
            self.compute_reason_udf = None

    def test_historico_de_inadimplencia(self):
        """Test that inadimplencia > 0 returns 'Histórico de Inadimplência' regardless of score."""
        assert self.compute_reason_logic(1, 400) == "Histórico de Inadimplência"
        assert self.compute_reason_logic(1, 600) == "Histórico de Inadimplência"
        assert self.compute_reason_logic(2, None) == "Histórico de Inadimplência"

    def test_score_de_credito_baixo(self):
        """Test that inadimplencia <= 0 and score < 500 returns 'Score de Crédito Baixo'."""
        assert self.compute_reason_logic(0, 499) == "Score de Crédito Baixo"
        assert self.compute_reason_logic(-1, 0) == "Score de Crédito Baixo"

    def test_aprovado(self):
        """Test that inadimplencia <= 0 and score >= 500 returns 'Aprovado'."""
        assert self.compute_reason_logic(0, 500) == "Aprovado"
        assert self.compute_reason_logic(0, 800) == "Aprovado"

    def test_none_score(self):
        """Test that inadimplencia <= 0 and score is None returns 'Aprovado'."""
        assert self.compute_reason_logic(0, None) == "Aprovado"

    def test_insufficient_columns_udf(self):
        """Test that passing less than 2 columns to the UDF returns N/A."""
        if self.compute_reason_udf:
            assert self.compute_reason_udf(1) == "N/A"
            assert self.compute_reason_udf() == "N/A"
            assert self.compute_reason_udf(0, 500) == "Aprovado"
        else:
            pytest.fail("compute_reason_udf could not be extracted")
