import unittest
from unittest.mock import MagicMock

class TestSerproOptimization(unittest.TestCase):
    def test_bidding_optimization(self):
        """
        Verifies that the proposed optimization for Bidding numbers uses join instead of collect+isin.
        """
        # Mock Spark objects
        mock_df = MagicMock()
        mock_biddings_df = MagicMock()
        mock_ids_df = MagicMock()

        # Setup returns
        mock_biddings_df.select.return_value.distinct.return_value = mock_ids_df

        # --- The logic to be implemented in the notebook ---
        serpro_biddings_df = mock_biddings_df

        # Original: serpro_bidding_numbers = [row['Número Licitação'] for row in serpro_biddings_df.select('Número Licitação').distinct().collect()]
        # New:
        serpro_bidding_numbers_df = serpro_biddings_df.select('Número Licitação').distinct()

        df = mock_df
        # Simulate loop
        # Original: filtered_df = df.filter(col("Número Licitação").isin(serpro_bidding_numbers))
        # New:
        filtered_df = df.join(serpro_bidding_numbers_df, on="Número Licitação", how="left_semi")
        # ---------------------------------------------------------

        # Assertions
        mock_biddings_df.select.assert_called_with('Número Licitação')
        mock_biddings_df.select.return_value.distinct.assert_called()

        # Verify join called
        df.join.assert_called_with(mock_ids_df, on="Número Licitação", how="left_semi")

        # Verify collect was NOT called
        mock_biddings_df.collect.assert_not_called()
        mock_biddings_df.select.return_value.distinct.return_value.collect.assert_not_called()

    def test_contract_optimization(self):
        """
        Verifies that the proposed optimization for Contract numbers uses join instead of collect+isin.
        """
        # Mock Spark objects
        mock_df = MagicMock()
        mock_contracts_df = MagicMock()
        mock_ids_df = MagicMock()

        # Setup returns
        mock_contracts_df.select.return_value.distinct.return_value = mock_ids_df

        # --- The logic to be implemented in the notebook ---
        serpro_contracts_df = mock_contracts_df

        # Original: serpro_contract_numbers = [row['Número Contrato'] for row in serpro_contracts_df.select('Número Contrato').distinct().collect()]
        # New:
        serpro_contract_numbers_df = serpro_contracts_df.select('Número Contrato').distinct()

        df = mock_df
        # Simulate loop
        # Original: filtered_df = df.filter(col("Número Contrato").isin(serpro_contract_numbers))
        # New:
        filtered_df = df.join(serpro_contract_numbers_df, on="Número Contrato", how="left_semi")
        # ---------------------------------------------------------

        # Assertions
        mock_contracts_df.select.assert_called_with('Número Contrato')
        mock_contracts_df.select.return_value.distinct.assert_called()

        # Verify join called
        df.join.assert_called_with(mock_ids_df, on="Número Contrato", how="left_semi")

        # Verify collect was NOT called
        mock_contracts_df.collect.assert_not_called()
        mock_contracts_df.select.return_value.distinct.return_value.collect.assert_not_called()

if __name__ == '__main__':
    unittest.main()
