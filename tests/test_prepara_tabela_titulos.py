import unittest
import sys
import os
from unittest.mock import MagicMock, call

# Ensure the tests directory is in the path to import notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define the path to the notebook file relative to the repository root
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Titulos.Notebook/notebook-content.py"
)

class MockColumn:
    def __init__(self, expr):
        self.expr = expr
        self._alias = None

    def alias(self, name):
        self._alias = name
        return self

    def isNotNull(self):
        return MockColumn(f"({self.expr} IS NOT NULL)")

    def isin(self, *args):
        if len(args) == 1 and isinstance(args[0], list):
            items = args[0]
        else:
            items = list(args)
        return MockColumn(f"({self.expr} IS IN {items})")

    def desc(self):
        return MockColumn(f"{self.expr} DESC")

    def over(self, window):
        return MockColumn(f"{self.expr} OVER ({window})")

    def __gt__(self, other):
        return MockColumn(f"({self.expr} > {other})")

    def __eq__(self, other):
        # Always return a MockColumn to support Spark DSL building
        return MockColumn(f"({self.expr} == {other})")

    def __repr__(self):
        return self.expr

    def __str__(self):
        return self.expr

    def __bool__(self):
        # This is a hack to prevent MagicMock from thinking all MockColumns are equal
        # But wait, MagicMock uses '==' not 'is' or 'bool()'.
        # Actually, MagicMock uses equality.
        # If we want to avoid silent failures in MagicMock.assert_called_with,
        # we should probably NOT use assert_called_with for MockColumns.
        return True

def mock_col(name):
    return MockColumn(f"col('{name}')")

def mock_lit(val):
    return MockColumn(str(val))

def mock_when(condition, value):
    class WhenChain(MockColumn):
        def __init__(self, expr):
            super().__init__(expr)
        def when(self, cond, val):
            return WhenChain(f"{self.expr}.when({cond}, {val})")
        def otherwise(self, val):
            return MockColumn(f"{self.expr}.otherwise({val})")

    return WhenChain(f"when({condition}, {value})")

def mock_coalesce(*cols):
    return MockColumn(f"coalesce({', '.join(str(c) for c in cols)})")

def mock_datediff(end, start):
    return MockColumn(f"datediff({end}, {start})")

def mock_current_date():
    return MockColumn("current_date()")

def mock_greatest(*cols):
    return MockColumn(f"greatest({', '.join(str(c) for c in cols)})")

def mock_row_number():
    return MockColumn("row_number()")

class MockWindow:
    @staticmethod
    def partitionBy(*cols):
        flat_cols = []
        for c in cols:
            if isinstance(c, list):
                flat_cols.extend(c)
            else:
                flat_cols.append(c)
        return MockWindow(f"partitionBy({', '.join(str(c) for c in flat_cols)})")

    def __init__(self, expr):
        self.expr = expr

    def orderBy(self, *cols):
        flat_cols = []
        for c in cols:
            if isinstance(c, list):
                flat_cols.extend(c)
            else:
                flat_cols.append(c)
        return MockWindow(f"{self.expr}.orderBy({', '.join(str(c) for c in flat_cols)})")

    def __repr__(self):
        return self.expr

class TestPreparaTabelaTitulos(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting functions from {NOTEBOOK_PATH}")
        cls.select_titulos_source = extract_function_from_file(NOTEBOOK_PATH, "select_titulos")
        cls.select_baixas_source = extract_function_from_file(NOTEBOOK_PATH, "select_baixas")
        cls.deduplicate_titulos_source = extract_function_from_file(NOTEBOOK_PATH, "deduplicate_titulos")

        missing = []
        if not cls.select_titulos_source: missing.append("select_titulos")
        if not cls.select_baixas_source: missing.append("select_baixas")
        if not cls.deduplicate_titulos_source: missing.append("deduplicate_titulos")

        if missing:
             raise ValueError(f"Functions not found in notebook: {missing}")

    def setUp(self):
        self.exec_globals = {
            'col': mock_col,
            'lit': mock_lit,
            'when': mock_when,
            'datediff': mock_datediff,
            'current_date': mock_current_date,
            'coalesce': mock_coalesce,
            'greatest': mock_greatest,
            'row_number': mock_row_number,
            'Window': MockWindow,
        }

    def test_select_titulos_logic(self):
        local_scope = {}
        exec(self.select_titulos_source, self.exec_globals, local_scope)
        select_titulos = local_scope['select_titulos']

        mock_df = MagicMock()
        select_titulos(mock_df)

        args, _ = mock_df.select.call_args
        self.assertEqual(len(args), 36)

        # Map aliases to expressions
        aliases = {arg._alias: str(arg) for arg in args if hasattr(arg, '_alias')}

        expected_mappings = {
            "cod_titulo": "col('CODTITULO')",
            "cod_operacao": "col('CODOPERACAO')",
            "n_doc": "col('NDOC')",
            "t_doc": "col('TDOC')",
            "vencimento": "col('VENCIMENTO')",
            "venc_prorrogado": "col('VENCPRORROGADO')",
            "prazo": "col('PRAZO')",
            "cpf_cnpj_sacado": "col('CPFCNPJSACADO')",
            "cpf_cnpj_cedente": "col('CPFCNPJCEDENTE')",
            "valor": "col('VALOR')",
            "desagio": "col('DESAGIO')",
            "liquido": "col('LIQUIDO')",
            "amortizacoes": "col('AMORTIZACOES')",
            "valor_devido": "col('VALORDEVIDO')",
            "liquidacao": "col('LIQUIDACAO')",
            "aceito": "col('ACEITO')",
            "cod_banco_cobr": "col('CODBANCOCOBR')",
            "data_conf": "col('DATACONF')",
            "usua_conf": "col('USUACONF')",
            "data_alteracao": "col('DATAALTERACAO')",
            "data_inclusao": "col('DATAINCLUSAO')",
            "doc_confirmado": "col('DOCCONFIRMADO')",
            "motivo": "col('MOTIVO')",
            "praca": "col('PRACA')",
            "chave_danfe": "col('CHAVEDANFE')",
            "nosso_numero": "col('NOSSONUMERO')",
            "cod_fundo": "col('CODFUNDO')",
            "tipo_cobranca": "col('TTO')",
            "raiz_cnpj": "col('FILIAL')",
            "cod_emissao": "col('CODEMISSAO')",
            "status_confirmacao": "col('STATUSCONFIRMACAO')",
            "seu_numero_bancario": "col('SEUNUMERO')",
            "cod_remessa": "col('CODREMESSA')",
            "vencimento_efetivo": "coalesce(col('VENCPRORROGADO'), col('VENCIMENTO'))"
        }

        for alias, expected_expr in expected_mappings.items():
            self.assertEqual(aliases.get(alias), expected_expr, f"Column {alias} has wrong expression: {aliases.get(alias)}")

        expected_dias_atraso = (
            "when((col('LIQUIDACAO') IS NOT NULL), datediff(col('LIQUIDACAO'), coalesce(col('VENCPRORROGADO'), col('VENCIMENTO'))))"
            ".otherwise(datediff(current_date(), coalesce(col('VENCPRORROGADO'), col('VENCIMENTO'))))"
        )
        self.assertEqual(aliases.get("dias_atraso"), expected_dias_atraso)

        expected_status_titulo = (
            "when((col('LIQUIDACAO') IS NOT NULL), LIQUIDADO)"
            ".when((datediff(current_date(), coalesce(col('VENCPRORROGADO'), col('VENCIMENTO'))) > 0), EM ATRASO)"
            ".otherwise(EM DIA)"
        )
        self.assertEqual(aliases.get("status_titulo"), expected_status_titulo)

    def test_select_baixas_logic(self):
        local_scope = {}
        exec(self.select_baixas_source, self.exec_globals, local_scope)
        select_baixas = local_scope['select_baixas']

        mock_df = MagicMock()
        select_baixas(mock_df)

        args, _ = mock_df.select.call_args
        self.assertEqual(len(args), 16)

        aliases = {arg._alias: str(arg) for arg in args if hasattr(arg, '_alias')}

        expected_mappings = {
            "cod_titulo_baixas": "col('CODTITULOBAIXAS')",
            "cod_titulo": "col('CODTITULO')",
            "data_inclusao": "col('DATAINCLUSAO')",
            "data_alteracao": "col('DATAALTERACAO')",
            "valor_pago": "col('VLPAGO')",
            "data_baixa": "col('DATABAIXA')",
            "data_baixa_sist": "col('DATABAIXASIST')",
            "desconto": "col('DESCONTO')",
            "juros": "col('JUROS')",
            "tarifa_recompra": "col('TARIFARECOMPRA')",
            "data_vencimento": "col('DATAVENCIMENTO')",
            "pago_pelo": "col('PAGOPELO')",
            "forma": "col('FORMA')",
            "tipo_baixa": "col('TIPOBAIXA')",
            "motivo": "col('MOTIVO')",
            "cod_operacao": "col('CODOPERACAO')"
        }

        for alias, expected_expr in expected_mappings.items():
            self.assertEqual(aliases.get(alias), expected_expr, f"Baixas column {alias} has wrong expression: {aliases.get(alias)}")

    def test_deduplicate_titulos_logic(self):
        local_scope = {}
        exec(self.deduplicate_titulos_source, self.exec_globals, local_scope)
        deduplicate_titulos = local_scope['deduplicate_titulos']

        mock_df = MagicMock()
        mock_df_with_latest = MagicMock()
        mock_df.withColumn.return_value = mock_df_with_latest

        mock_df_with_rownum = MagicMock()
        mock_df_with_latest.withColumn.return_value = mock_df_with_rownum

        mock_df_filtered = MagicMock()
        mock_df_with_rownum.filter.return_value = mock_df_filtered

        mock_df_dropped = MagicMock()
        mock_df_filtered.drop.return_value = mock_df_dropped

        key_cols = ["CODTITULO"]
        deduplicate_titulos(mock_df, key_cols)

        # 1. Verify greatest column creation
        call_args_greatest = mock_df.withColumn.call_args
        self.assertEqual(call_args_greatest[0][0], "DATA_MAIS_RECENTE")
        self.assertEqual(str(call_args_greatest[0][1]), "greatest(col('DATAALTERACAO'), col('DATAINCLUSAO'), col('LIQUIDACAO'))")

        # 2. Verify row_number() over window
        rownum_call_args = mock_df_with_latest.withColumn.call_args
        self.assertEqual(rownum_call_args[0][0], "row_num")
        rownum_expr = str(rownum_call_args[0][1])
        self.assertEqual(rownum_expr, "row_number() OVER (partitionBy(col('CODTITULO')).orderBy(col('DATA_MAIS_RECENTE') DESC))")

        # 3. Verify filter row_num == 1
        filter_call_args = mock_df_with_rownum.filter.call_args
        self.assertEqual(str(filter_call_args[0][0]), "(col('row_num') == 1)")

        # 4. Verify drop columns
        mock_df_filtered.drop.assert_called_with("row_num", "DATA_MAIS_RECENTE")

if __name__ == '__main__':
    unittest.main()
