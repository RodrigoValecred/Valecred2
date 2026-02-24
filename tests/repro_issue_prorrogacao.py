
import unittest
from unittest.mock import MagicMock, call

# Mock PySpark classes and functions since pyspark is not installed
class MockColumn:
    def __init__(self, name):
        self.name = name

    def alias(self, alias):
        return MockColumn(alias)

    def __eq__(self, other):
        return MockColumn(f"{self.name} == {other}")

class MockDataFrame:
    def __init__(self, name, columns, data):
        self.name = name
        self.columns = columns
        self.data = data # List of dicts

    def select(self, *cols):
        # Very basic select simulation
        selected_cols = []
        for c in cols:
            if isinstance(c, str):
                selected_cols.append(c)
            elif isinstance(c, MockColumn):
                selected_cols.append(c.name)
        return MockDataFrame(f"selected_{self.name}", selected_cols, self.data)

    def filter(self, condition):
        # Simulate filtering by creating a new dataframe with potentially fewer rows
        # In a real mock, we would evaluate the condition.
        # Here we just return a filtered version based on the condition string if we can parse it,
        # or just return a mock that represents the filtered state.
        return MockDataFrame(f"filtered_{self.name}", self.columns, [d for d in self.data if self._eval(condition, d)])

    def _eval(self, condition, row):
        # Basic evaluation for "col == val"
        if isinstance(condition, MockColumn):
            parts = condition.name.split(" == ")
            if len(parts) == 2:
                col_name = parts[0]
                val = parts[1].strip("'").strip('"')
                return str(row.get(col_name)) == val
        return True

    def join(self, other, on, how='inner'):
        # Basic join simulation
        joined_data = []
        for row in self.data:
            match = next((r for r in other.data if str(r.get(on)) == str(row.get(on))), None)
            if match:
                new_row = {**row, **match} # Merge dicts
                joined_data.append(new_row)
            elif how == 'left':
                new_row = {**row}
                # Add None for other columns
                for col in other.columns:
                    if col != on:
                        new_row[col] = None
                joined_data.append(new_row)

        all_cols = list(set(self.columns + other.columns))
        return MockDataFrame(f"joined_{self.name}_{other.name}", all_cols, joined_data)

    def withColumn(self, name, col_expr):
        # Simulate adding a column. Since we can't evaluate complex expressions easily,
        # we'll just add the column name to the schema.
        new_cols = self.columns + [name]
        return MockDataFrame(f"with_col_{self.name}", new_cols, self.data)

    def withColumnRenamed(self, existing, new):
        new_cols = [new if c == existing else c for c in self.columns]
        return MockDataFrame(f"renamed_{self.name}", new_cols, self.data)

    def dropDuplicates(self, subset=None):
        return self

    def orderBy(self, *cols):
        return self

def col(name):
    return MockColumn(name)

def lit(val):
    return val

def year(col):
    return MockColumn(f"year({col.name})")

def coalesce(*cols):
    return MockColumn(f"coalesce({cols})")

def trim(col):
    return MockColumn(f"trim({col.name})")

def when(condition, value):
    return MockColumn(f"when({condition}, {value})")

# Test Class
class TestRelatorioProdutos(unittest.TestCase):

    def test_repro_issue_platform_missing(self):
        # Mock Data
        # Operation 1: Accepted (A) and Deferido (D) -> Should be in map
        # Operation 2: Rejected (R) -> Should NOT be in filtered map, but might have Prorrogacao

        ops_data = [
            {"cod_operacao": "1", "status_aceite": "A", "status_analise": "D", "nome_plataforma": "Platform A", "nbordero": "100", "chave_produto": "NO", "data_deferimento": "2025-01-01"},
            {"cod_operacao": "2", "status_aceite": "R", "status_analise": "D", "nome_plataforma": "Platform B", "nbordero": "101", "chave_produto": "NO", "data_deferimento": "2025-01-02"}
        ]

        df_ops_raw = MockDataFrame("fato_operacoes", ["cod_operacao", "status_aceite", "status_analise", "nome_plataforma", "nbordero", "chave_produto", "data_deferimento"], ops_data)

        # Reproduce current logic: Filtering
        df_ops_filtered = df_ops_raw.filter(col("status_aceite") == "A").filter(col("status_analise") == "D")

        # Verify filtering
        self.assertEqual(len(df_ops_filtered.data), 1)
        self.assertEqual(df_ops_filtered.data[0]["cod_operacao"], "1")

        # Create Map
        df_map_ops = df_ops_filtered.select("cod_operacao", "nome_plataforma")
        # In mock select, we just keep columns. Real code aliases them.
        # Let's simulate alias manually for the test check
        map_data = [{"cod_operacao": d["cod_operacao"], "nome_plataforma_op": d["nome_plataforma"]} for d in df_ops_filtered.data]
        df_map_ops_mock = MockDataFrame("map_ops", ["cod_operacao", "nome_plataforma_op"], map_data)

        # Mock Prorrogacao (Fact Table)
        # Prorrogacao for Op 2 (The rejected one)
        prorrog_data = [
            {"cod_operacao": "2", "valor": 1000, "data_inclusao": "2025-02-01"}
        ]
        df_prorrog = MockDataFrame("fato_prorrogacoes", ["cod_operacao", "valor", "data_inclusao"], prorrog_data)

        # Join
        df_joined = df_prorrog.join(df_map_ops_mock, "cod_operacao", "left")

        # Assertions
        print("Joined Data (Current Logic):", df_joined.data)
        # Op 2 should have None for nome_plataforma_op because it was filtered out from map
        row_op2 = next(r for r in df_joined.data if r["cod_operacao"] == "2")
        self.assertIsNone(row_op2.get("nome_plataforma_op"))
        print("Confirmed: Platform is missing for Op 2 with current logic.")

    def test_fix_issue_platform_present(self):
        # Mock Data (Same as above)
        ops_data = [
            {"cod_operacao": "1", "status_aceite": "A", "status_analise": "D", "nome_plataforma": "Platform A", "nbordero": "100", "chave_produto": "NO", "data_deferimento": "2025-01-01"},
            {"cod_operacao": "2", "status_aceite": "R", "status_analise": "D", "nome_plataforma": "Platform B", "nbordero": "101", "chave_produto": "NO", "data_deferimento": "2025-01-02"}
        ]
        df_ops_raw = MockDataFrame("fato_operacoes", ["cod_operacao", "status_aceite", "status_analise", "nome_plataforma", "nbordero", "chave_produto", "data_deferimento"], ops_data)

        # Proposed Fix: No Filtering for Map
        df_ops_unfiltered = df_ops_raw # No filter

        # Create Map
        map_data = [{"cod_operacao": d["cod_operacao"], "nome_plataforma_op": d["nome_plataforma"]} for d in df_ops_unfiltered.data]
        df_map_ops_mock = MockDataFrame("map_ops", ["cod_operacao", "nome_plataforma_op"], map_data)

        # Mock Prorrogacao
        prorrog_data = [
            {"cod_operacao": "2", "valor": 1000, "data_inclusao": "2025-02-01"}
        ]
        df_prorrog = MockDataFrame("fato_prorrogacoes", ["cod_operacao", "valor", "data_inclusao"], prorrog_data)

        # Join
        df_joined = df_prorrog.join(df_map_ops_mock, "cod_operacao", "left")

        # Assertions
        print("Joined Data (Fixed Logic):", df_joined.data)
        # Op 2 should have "Platform B"
        row_op2 = next(r for r in df_joined.data if r["cod_operacao"] == "2")
        self.assertEqual(row_op2.get("nome_plataforma_op"), "Platform B")
        print("Confirmed: Platform is present for Op 2 with fixed logic.")

if __name__ == '__main__':
    unittest.main()
