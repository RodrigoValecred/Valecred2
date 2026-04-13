import unittest
import sys
import os
import pandas as pd
import numpy as np

class TestFormataCep(unittest.TestCase):
    def test_formata_cep_vectorized(self):
        # Setup similar DataFrame structure (all arrays must be of length 7)
        df_pandas = pd.DataFrame({
            "cep_inicial": [123456, 12345678, 123456.0, "123456", pd.NA, float('nan'), "invalid"],
            "cep_final":   [123456, 12345678, 123456.0, "123456", pd.NA, float('nan'), "invalid"],
            "latitude":    ["-23,55052", "-23.55052", pd.NA, float('nan'), "invalid", "10,5", "-10"],
            "longitude":   ["-46,6333", "-46.6333", pd.NA, float('nan'), "invalid", "10,5", "-10"]
        })

        # Apply the vectorized operations from the notebook
        if "latitude" in df_pandas.columns:
            df_pandas["latitude"] = pd.to_numeric(df_pandas["latitude"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
        if "longitude" in df_pandas.columns:
            df_pandas["longitude"] = pd.to_numeric(df_pandas["longitude"].astype(str).str.replace(",", ".", regex=False), errors="coerce")

        if "cep_inicial" in df_pandas.columns:
            s = pd.to_numeric(df_pandas["cep_inicial"], errors="coerce").astype("Int64").astype(str)
            df_pandas["cep_inicial"] = s.where(s != "<NA>", np.nan).str.zfill(8)
        if "cep_final" in df_pandas.columns:
            s = pd.to_numeric(df_pandas["cep_final"], errors="coerce").astype("Int64").astype(str)
            df_pandas["cep_final"] = s.where(s != "<NA>", np.nan).str.zfill(8)

        # Assertions for cep_inicial
        self.assertEqual(df_pandas["cep_inicial"][0], "00123456")
        self.assertEqual(df_pandas["cep_inicial"][1], "12345678")
        self.assertEqual(df_pandas["cep_inicial"][2], "00123456")
        self.assertEqual(df_pandas["cep_inicial"][3], "00123456")
        self.assertTrue(pd.isna(df_pandas["cep_inicial"][4]))
        self.assertTrue(pd.isna(df_pandas["cep_inicial"][5]))
        self.assertTrue(pd.isna(df_pandas["cep_inicial"][6]))

        # Assertions for latitude
        self.assertEqual(df_pandas["latitude"][0], -23.55052)
        self.assertEqual(df_pandas["latitude"][1], -23.55052)
        self.assertTrue(pd.isna(df_pandas["latitude"][2]))
        self.assertTrue(pd.isna(df_pandas["latitude"][3]))
        self.assertTrue(pd.isna(df_pandas["latitude"][4]))
        self.assertEqual(df_pandas["latitude"][5], 10.5)
        self.assertEqual(df_pandas["latitude"][6], -10.0)

if __name__ == "__main__":
    unittest.main()
