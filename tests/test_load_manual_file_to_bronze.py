import unittest
from unittest.mock import patch, MagicMock
import os
import pandas as pd
import re
import unicodedata

from tests.notebook_utils import extract_function_from_file

NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Load_Silver_From_Manual_Uploads.Notebook/notebook-content.py"

class TestLoadManualFileToBronze(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        func_source = extract_function_from_file(NOTEBOOK_PATH, "load_manual_file_to_bronze")
        if func_source:
            local_scope = {}
            cls.global_scope = {
                "re": re,
                "unicodedata": unicodedata,
                "os": os,
                "pd": pd
            }
            exec(func_source, cls.global_scope, local_scope)
            cls.load_manual_file_to_bronze = staticmethod(local_scope["load_manual_file_to_bronze"])
        else:
            cls.load_manual_file_to_bronze = None

    def setUp(self):
        if not self.load_manual_file_to_bronze:
            self.skipTest("Function not found")
        self.mock_spark = MagicMock()
        self.global_scope['spark'] = self.mock_spark

    @patch('os.path.exists')
    def test_directory_not_found(self, mock_exists):
        mock_exists.return_value = False
        self.load_manual_file_to_bronze("test.xlsx", "table")
        mock_exists.assert_called_once_with("/lakehouse/default/Files/manual_uploads")

    @patch('os.listdir')
    @patch('os.path.exists')
    def test_file_not_found(self, mock_exists, mock_listdir):
        mock_exists.return_value = True
        mock_listdir.return_value = ["other_file.xlsx"]
        self.load_manual_file_to_bronze("test.xlsx", "table")
        mock_listdir.assert_called_once_with("/lakehouse/default/Files/manual_uploads")

    @patch('pandas.read_excel')
    @patch('os.listdir')
    @patch('os.path.exists')
    def test_load_excel_success(self, mock_exists, mock_listdir, mock_read_excel):
        mock_exists.return_value = True
        mock_listdir.return_value = ["TEST.xlsx"]

        # Mock pandas dataframe using real pd.DataFrame since columns property is replaced in load_manual_file_to_bronze
        mock_df = pd.DataFrame({"Column One": [1], "ColumnTwo": [2]})
        mock_read_excel.return_value = mock_df

        # Mock spark dataframe
        mock_spark_df = MagicMock()
        self.mock_spark.createDataFrame.return_value = mock_spark_df

        self.load_manual_file_to_bronze("test.xlsx", "LH_Silver.test")

        mock_read_excel.assert_called_once_with("/lakehouse/default/Files/manual_uploads/TEST.xlsx")

        args, kwargs = self.mock_spark.createDataFrame.call_args
        df_passed = args[0]
        self.assertIsInstance(df_passed, pd.DataFrame)
        self.assertEqual(list(df_passed.columns), ["column_one", "column_two"])

        mock_spark_df.write.mode.assert_called_once_with("overwrite")
        mock_spark_df.write.mode().option.assert_called_once_with("overwriteSchema", "true")
        mock_spark_df.write.mode().option().saveAsTable.assert_called_once_with("LH_Silver.test")

    @patch('os.listdir')
    @patch('os.path.exists')
    def test_load_csv_success(self, mock_exists, mock_listdir):
        mock_exists.return_value = True
        mock_listdir.return_value = ["test.csv"]

        # Mock spark dataframe
        mock_spark_df = MagicMock()
        mock_spark_df.columns = ["Column One", "ColumnTwo"]

        mock_spark_df_renamed = MagicMock()
        mock_spark_df.toDF.return_value = mock_spark_df_renamed

        # Setup spark reader mock chain
        self.mock_spark.read.format().option().option().option().option().load.return_value = mock_spark_df

        self.load_manual_file_to_bronze("test.csv", "LH_Silver.test_csv")

        # Assert format, options, and load were called appropriately
        self.mock_spark.read.format.assert_called_with("csv")
        self.mock_spark.read.format().option.assert_called_with("header", "true")
        self.mock_spark.read.format().option().option().option().option().load.assert_called_with("/lakehouse/default/Files/manual_uploads/test.csv")

        mock_spark_df.toDF.assert_called_once_with("column_one", "column_two")

        mock_spark_df_renamed.write.mode.assert_called_once_with("overwrite")
        mock_spark_df_renamed.write.mode().option.assert_called_once_with("overwriteSchema", "true")
        mock_spark_df_renamed.write.mode().option().saveAsTable.assert_called_once_with("LH_Silver.test_csv")

    @patch('os.listdir')
    @patch('os.path.exists')
    def test_unsupported_format(self, mock_exists, mock_listdir):
        mock_exists.return_value = True
        mock_listdir.return_value = ["test.txt"]

        self.load_manual_file_to_bronze("test.txt", "LH_Silver.test")

        self.mock_spark.read.format.assert_not_called()
        self.mock_spark.createDataFrame.assert_not_called()

    @patch('pandas.read_excel')
    @patch('os.listdir')
    @patch('os.path.exists')
    def test_exception_handling(self, mock_exists, mock_listdir, mock_read_excel):
        mock_exists.return_value = True
        mock_listdir.return_value = ["test.xlsx"]
        mock_read_excel.side_effect = Exception("Read failed")

        # Should handle exception and not crash
        self.load_manual_file_to_bronze("test.xlsx", "LH_Silver.test")

if __name__ == '__main__':
    unittest.main()
