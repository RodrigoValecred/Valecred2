import unittest
import sys
import os
from unittest.mock import MagicMock, call

# Ensure the tests directory is in the path to import notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Operacoes.Notebook/notebook-content.py"

class TestProcessTacM(unittest.TestCase):

    def setUp(self):
        # Extract the function
        func_source = extract_function_from_file(NOTEBOOK_PATH, "transform_tac_m")
        if not func_source:
             self.fail("Function transform_tac_m not found in notebook.")

        # Prepare execution context with mocks
        self.mock_col = MagicMock(name="col")
        self.mock_trim = MagicMock(name="trim")
        self.mock_upper = MagicMock(name="upper")
        self.mock_lit = MagicMock(name="lit")

        # Execution context
        self.exec_globals = {
            'col': self.mock_col,
            'trim': self.mock_trim,
            'upper': self.mock_upper,
            'lit': self.mock_lit,
        }

        local_scope = {}
        exec(func_source, self.exec_globals, local_scope)
        self.transform_tac_m = local_scope['transform_tac_m']

    def test_transform_calls(self):
        # Mock DataFrame
        mock_df = MagicMock(name="df")

        # Chainable return values
        # df.withColumn returns df
        mock_df.withColumn.return_value = mock_df
        # df.filter returns df
        mock_df.filter.return_value = mock_df
        # df.orderBy returns df
        mock_df.orderBy.return_value = mock_df

        # Mock Column expressions
        # col("descricao") -> mock_col_desc
        mock_col_desc = MagicMock(name="col_descricao")

        # col("data_inclusao") -> mock_col_date
        mock_col_date = MagicMock(name="col_data_inclusao")

        def col_side_effect(arg):
            if arg == "descricao": return mock_col_desc
            if arg == "data_inclusao": return mock_col_date
            return MagicMock(name=f"col_{arg}")
        self.mock_col.side_effect = col_side_effect

        # upper(col) -> mock_upper_obj
        mock_upper_obj = MagicMock(name="upper_obj")
        self.mock_upper.return_value = mock_upper_obj

        # trim(upper) -> mock_trim_obj
        mock_trim_obj = MagicMock(name="trim_obj")
        self.mock_trim.return_value = mock_trim_obj

        # col.isin(...) -> mock_isin_expr
        mock_isin_expr = MagicMock(name="isin_expr")
        mock_col_desc.isin.return_value = mock_isin_expr

        # Variations list
        tac_variations = ["A", "B"]

        # Execute
        result = self.transform_tac_m(mock_df, tac_variations)

        # Verify Assertions

        # 1. Verify col("descricao") was called
        self.mock_col.assert_any_call("descricao")

        # 2. Verify upper(col("descricao"))
        # Since col returns different objects if we didn't use the same mock instance...
        # But we use side_effect returning mock_col_desc for "descricao" every time.
        self.mock_upper.assert_called_with(mock_col_desc)

        # 3. Verify trim(upper(...))
        self.mock_trim.assert_called_with(mock_upper_obj)

        # 4. Verify withColumn("descricao", trim(...))
        mock_df.withColumn.assert_any_call("descricao", mock_trim_obj)

        # 5. Verify filter(isin)
        # Check isin called with variations + ["TAC M"]
        mock_col_desc.isin.assert_called_with(tac_variations + ["TAC M"])
        # Check filter called with result of isin
        mock_df.filter.assert_called_with(mock_isin_expr)

        # 6. Verify withColumn("descricao", lit("TAC M"))
        self.mock_lit.assert_called_with("TAC M")
        mock_df.withColumn.assert_any_call("descricao", self.mock_lit.return_value)

        # 7. Verify orderBy
        self.mock_col.assert_any_call("data_inclusao")
        mock_col_date.desc.assert_called()
        mock_df.orderBy.assert_called()

if __name__ == '__main__':
    unittest.main()
