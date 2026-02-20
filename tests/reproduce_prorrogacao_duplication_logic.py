import unittest

class TestProrrogacaoDuplication(unittest.TestCase):
    def test_prorrogacao_duplication_logic(self):
        # Simulate Silver Prorogation Data
        # We simulate a DataFrame with duplicate rows for the same title
        # Simulate columns: cod_titulo, juros, data_inclusao
        # Title 123 has 2 identical rows of 100.0 juros each.
        data = [
            {"cod_titulo": 123, "juros": 100.0, "data_inclusao": "2025-01-01"},
            {"cod_titulo": 123, "juros": 100.0, "data_inclusao": "2025-01-01"}
        ]

        # Logic WITHOUT deduplication (groupBy().sum("juros"))
        sum_without_dedup = sum([row["juros"] for row in data])
        print(f"Sum without dedup: {sum_without_dedup}")
        self.assertEqual(sum_without_dedup, 200.0, "Without dedup, sum should be double")

        # Logic WITH deduplication (dropDuplicates())
        # Convert list of dicts to set of tuples to dedup
        # Simulate: df.dropDuplicates()
        unique_data_tuples = {tuple(sorted(row.items())) for row in data}
        deduped_data = [dict(t) for t in unique_data_tuples]

        sum_with_dedup = sum([row["juros"] for row in deduped_data])
        print(f"Sum with dedup: {sum_with_dedup}")
        self.assertEqual(sum_with_dedup, 100.0, "With dedup, sum should be correct")

if __name__ == '__main__':
    unittest.main()
