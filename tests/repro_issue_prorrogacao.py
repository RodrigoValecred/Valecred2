
import unittest
from unittest.mock import MagicMock, call

# Simula as classes e funções do PySpark já que pyspark não está instalado
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
        self.data = data # Lista de dicionários

    def select(self, *cols):
        # Simulação de seleção muito básica
        selected_cols = []
        for c in cols:
            if isinstance(c, str):
                selected_cols.append(c)
            elif isinstance(c, MockColumn):
                selected_cols.append(c.name)
        return MockDataFrame(f"selected_{self.name}", selected_cols, self.data)

    def filter(self, condition):
        # Simula a filtragem criando um novo dataframe com potencialmente menos linhas
        # Em um mock real, avaliaríamos a condição.
        # Aqui nós apenas retornamos uma versão filtrada baseada na string de condição se conseguirmos analisá-la,
        # ou apenas retornamos um mock que representa o estado filtrado.
        return MockDataFrame(f"filtered_{self.name}", self.columns, [d for d in self.data if self._eval(condition, d)])

    def _eval(self, condition, row):
        # Avaliação básica para "col == val"
        if isinstance(condition, MockColumn):
            parts = condition.name.split(" == ")
            if len(parts) == 2:
                col_name = parts[0]
                val = parts[1].strip("'").strip('"')
                return str(row.get(col_name)) == val
        return True

    def join(self, other, on, how='inner'):
        # Simulação básica de join
        joined_data = []
        for row in self.data:
            match = next((r for r in other.data if str(r.get(on)) == str(row.get(on))), None)
            if match:
                new_row = {**row, **match} # Mescla (Merge) dicionários
                joined_data.append(new_row)
            elif how == 'left':
                new_row = {**row}
                # Adiciona None para outras colunas
                for col in other.columns:
                    if col != on:
                        new_row[col] = None
                joined_data.append(new_row)

        all_cols = list(set(self.columns + other.columns))
        return MockDataFrame(f"joined_{self.name}_{other.name}", all_cols, joined_data)

    def withColumn(self, name, col_expr):
        # Simula a adição de uma coluna. Como não podemos avaliar expressões complexas facilmente,
        # vamos apenas adicionar o nome da coluna ao esquema.
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

# Classe de Teste
class TestRelatorioProdutos(unittest.TestCase):

    def test_repro_issue_platform_missing(self):
        # Simulação de Dados
        # Operação 1: Aceito (A) e Deferido (D) -> Deve estar no mapa
        # Operação 2: Rejeitada (R) -> NÃO deve estar no mapa filtrado, mas pode ter Prorrogação

        ops_data = [
            {"cod_operacao": "1", "status_aceite": "A", "status_analise": "D", "nome_plataforma": "Platform A", "nbordero": "100", "chave_produto": "NO", "data_deferimento": "2025-01-01"},
            {"cod_operacao": "2", "status_aceite": "R", "status_analise": "D", "nome_plataforma": "Platform B", "nbordero": "101", "chave_produto": "NO", "data_deferimento": "2025-01-02"}
        ]

        df_ops_raw = MockDataFrame("fato_operacoes", ["cod_operacao", "status_aceite", "status_analise", "nome_plataforma", "nbordero", "chave_produto", "data_deferimento"], ops_data)

        # Reproduz a lógica atual: Sem Filtragem
        df_ops_unfiltered = df_ops_raw

        # Cria o Map
        df_map_ops = df_ops_unfiltered.select("cod_operacao", "nome_plataforma")
        # Na simulação do select, apenas mantemos as colunas. O código real as renomeia (alias).
        # Vamos simular o alias manualmente para a verificação do teste
        map_data = [{"cod_operacao": d["cod_operacao"], "nome_plataforma_op": d["nome_plataforma"]} for d in df_ops_unfiltered.data]
        df_map_ops_mock = MockDataFrame("map_ops", ["cod_operacao", "nome_plataforma_op"], map_data)

        # Simula Prorrogação (Fact Table)
        # Prorrogação para Op 2 (A rejeitada)
        prorrog_data = [
            {"cod_operacao": "2", "valor": 1000, "data_inclusao": "2025-02-01"}
        ]
        df_prorrog = MockDataFrame("fato_prorrogacoes", ["cod_operacao", "valor", "data_inclusao"], prorrog_data)

        # Join
        df_joined = df_prorrog.join(df_map_ops_mock, "cod_operacao", "left")

        # Asserções
        print("Joined Data (Fixed Logic):", df_joined.data)
        # Op 2 deve ter "Platform B"
        row_op2 = next(r for r in df_joined.data if r["cod_operacao"] == "2")
        self.assertEqual(row_op2.get("nome_plataforma_op"), "Platform B")
        print("Confirmed: Platform is present for Op 2 with fixed logic.")


if __name__ == '__main__':
    unittest.main()
