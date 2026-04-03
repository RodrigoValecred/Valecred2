
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tests.notebook_utils import extract_function_from_file

NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/NB_Gera_Relatorio_Diario_Clientes.Notebook/notebook-content.py"

class TestRelatorioDiarioUX(unittest.TestCase):
    def setUp(self):
        # 1. Prepara Escopo com Dependências
        self.scope = {'pd': pd, 'np': np, 'datetime': datetime}

        # Inject data_hoje
        self.data_hoje = datetime(2025, 12, 23).date()
        self.scope['data_hoje'] = self.data_hoje

        # Simula a classe Colors pois não podemos extrair classes facilmente com a util atual
        class MockColors:
            HEADER = ''
            BLUE = ''
            CYAN = ''
            GREEN = ''
            YELLOW = ''
            RED = ''
            RESET = ''
            BOLD = ''
        self.scope['Colors'] = MockColors

        # Extract helper function format_currency_br
        format_source = extract_function_from_file(NOTEBOOK_PATH, "format_currency_br")
        if format_source:
            exec(format_source, self.scope, self.scope)
        else:
            # Simulação de contingência (fallback) se não encontrado (embora devesse estar lá)
            self.scope['format_currency_br'] = lambda x: f"R$ {x:.2f}"

        # Extract prepare_dashboard_data
        prepare_source = extract_function_from_file(NOTEBOOK_PATH, "prepare_dashboard_data")
        if not prepare_source:
             self.fail("Function prepare_dashboard_data not found in notebook")
        try:
            exec(prepare_source, self.scope, self.scope)
            self.prepare_dashboard_data = self.scope['prepare_dashboard_data']
        except Exception as e:
             self.fail(f"Failed to execute extracted function prepare_dashboard_data: {e}")

        # Extract display_risk_dashboard
        source = extract_function_from_file(NOTEBOOK_PATH, "display_risk_dashboard")
        if not source:
            self.fail("Function display_risk_dashboard not found in notebook")

        try:
            # Usa self.scope tanto como globals quanto locals para garantir que closures (como Colors) funcionem
            exec(source, self.scope, self.scope)
            self.display_risk_dashboard = self.scope['display_risk_dashboard']
        except Exception as e:
            self.fail(f"Failed to execute extracted function source: {e}")

    def test_prepare_dashboard_data_logic(self):
        """Testa a lógica de negócio isoladamente sem simulação de impressão"""
        df = pd.DataFrame({
            'grupo': ['Safe Group', 'Risky Group'],
            'valor_risco': [50.0, 150.0],
            'limite_global': [100.0, 100.0],
            'utilizacao_pct': [50.0, 150.0],
            'excesso_valor': [0.0, 50.0],
            'validade_limite': ['2026-01-01', '2026-01-01']
        })

        view_data = self.prepare_dashboard_data(df, self.data_hoje)

        self.assertEqual(len(view_data), 2)

        # Safe Group
        item0 = view_data[0]
        self.assertEqual(item0['grupo_display'], 'Safe Group')
        self.assertTrue(item0['is_valid_utilization'])
        self.assertFalse(item0['is_excess'])
        self.assertIn("✅", item0['bar_display']) # Verifica o ícone (Colors simuladas são strings vazias mas o ícone é literal)
        self.assertIn("Seguro", item0['bar_display']) # Verifica o texto de status

        # Risky Group
        item1 = view_data[1]
        self.assertTrue(item1['is_excess'])
        self.assertIn("🚨", item1['bar_display'])
        self.assertIn("Crítico", item1['bar_display']) # Verifica o texto de status
        self.assertEqual(item1['excesso_fmt'], "R$ 50,00")

    def test_prepare_dashboard_data_validity_errors(self):
        # Caso de Teste: Formatos de data inválidos para acionar ValueError e TypeError
        df = pd.DataFrame({
            'grupo': ['Invalid String', 'Invalid Type None', 'Invalid Type Float'],
            'valor_risco': [10.0, 10.0, 10.0],
            'limite_global': [100.0, 100.0, 100.0],
            'utilizacao_pct': [10.0, 10.0, 10.0],
            'excesso_valor': [0, 0, 0],
            'validade_limite': [
                'not-a-date', # Deve acionar ValueError
                None,         # Deve acionar TypeError (ou ValueError dependendo do comportamento de strptime)
                123.45        # Tipo inválido
            ]
        })

        # Redireciona stdout temporariamente se não quisermos que as impressões poluam a saída do teste
        import io
        import sys
        captured_output = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured_output

        try:
            view_data = self.prepare_dashboard_data(df, self.data_hoje)
        finally:
            sys.stdout = old_stdout

        # Verifica contingência para a string original
        self.assertEqual(view_data[0]['validade_display'], 'not-a-date')
        self.assertEqual(view_data[1]['validade_display'], 'None')
        self.assertEqual(view_data[2]['validade_display'], '123.45')

    @patch('builtins.print')
    def test_display_risk_dashboard_output_structure(self, mock_print):
        # Configura dados simulados
        df = pd.DataFrame({
            'grupo': ['Test Group A', 'Test Group B'],
            'valor_risco': [100.0, 500.0],
            'limite_global': [200.0, 400.0],
            'utilizacao_pct': [50.0, 125.0],
            'excesso_valor': [0.0, 100.0]
        })

        # Executa a função
        self.display_risk_dashboard(df)

        # Collect all print outputs
        calls = [args[0] for args, _ in mock_print.call_args_list if args]
        full_output = "\n".join(calls)

        # Asserções
        self.assertIn("PAINEL DE RISCO", full_output)
        self.assertIn("Data de Referência", full_output)
        self.assertIn("Test Group A", full_output)
        self.assertIn("50.0%", full_output)
        # Nota: Colors são strings vazias na simulação, então não veremos códigos ANSI, mas a estrutura de texto permanece
        # Podemos verificar ícones se forem strings fixas, que é o que são no notebook
        self.assertIn("✅", full_output)
        self.assertIn("🚨", full_output)

        # Dashboard Summary UX Checks
        self.assertIn("✅ Seguro: 1    ", full_output)
        self.assertIn("🚨 Crítico: 1   ", full_output)

        # Verificação de Melhoria UX: "Disponível" deve ser mostrado para grupos seguros
        self.assertIn("Disponível:", full_output)
        # "EXCESSO" deve ser mostrado para grupos inseguros (já implícito na lógica, mas bom verificar)
        self.assertIn("EXCESSO:", full_output)

    @patch('builtins.print')
    def test_display_risk_dashboard_empty(self, mock_print):
        # DataFrame Vazio
        df = pd.DataFrame(columns=['grupo', 'valor_risco', 'limite_global', 'utilizacao_pct', 'excesso_valor'])
        self.display_risk_dashboard(df)

        calls = [args[0] for args, _ in mock_print.call_args_list if args]
        full_output = "\n".join(calls)

        self.assertIn("NENHUM GRUPO COM RISCO ATIVO ENCONTRADO", full_output)
        self.assertNotIn("Grupos analisados", full_output)

    @patch('builtins.print')
    def test_display_risk_dashboard_long_name(self, mock_print):
        long_name = "A Very Long Group Name That Should Be Truncated Because It Exceeds The Limit Of The Layout"
        df = pd.DataFrame({
            'grupo': [long_name],
            'valor_risco': [0],
            'limite_global': [0],
            'utilizacao_pct': [0],
            'excesso_valor': [0]
        })

        self.display_risk_dashboard(df)

        calls = [args[0] for args, _ in mock_print.call_args_list if args]
        full_output = "\n".join(calls)

        self.assertNotIn(long_name, full_output)
        truncated_part = long_name[:47]
        self.assertIn(truncated_part, full_output)
        self.assertIn("...", full_output)

    @patch('builtins.print')
    def test_display_risk_dashboard_validity(self, mock_print):
        # Caso de Teste: Expirado, Quase Expirado, Seguro
        df = pd.DataFrame({
            'grupo': ['Expired', 'Near', 'Safe'],
            'valor_risco': [10.0, 10.0, 10.0],
            'limite_global': [100.0, 100.0, 100.0],
            'utilizacao_pct': [10.0, 10.0, 10.0],
            'excesso_valor': [0, 0, 0],
            'validade_limite': [
                '2025-12-01', # Expirado (Assumindo que self.data_hoje é 2025-12-23)
                '2025-12-30', # Near (7 days)
                '2026-06-01'  # Safe
            ]
        })

        self.display_risk_dashboard(df)

        calls = [args[0] for args, _ in mock_print.call_args_list if args]
        full_output = "\n".join(calls)

        self.assertIn("VENCIDO", full_output)
        self.assertIn("(7d)", full_output)
        self.assertIn("01/06/2026", full_output)

    def test_style_risk_dataframe(self):
        # Esta função é o que estamos adicionando.
        # Nós a extraímos para verificar a lógica.
        style_source = extract_function_from_file(NOTEBOOK_PATH, "style_risk_dataframe")

        if not style_source:
             self.fail("Function style_risk_dataframe not found in notebook. Implement it!")

        # Executa
        exec(style_source, self.scope, self.scope)
        style_func = self.scope['style_risk_dataframe']

        # Dados de Teste
        df = pd.DataFrame({
            'grupo': ['A', 'B'],
            'valor_risco': [1000.0, 2000.0],
            'limite_global': [5000.0, 1000.0],
            'utilizacao_pct': [20.0, 200.0],
            'excesso_valor': [0.0, 1000.0]
        })

        styler = style_func(df)
        html = styler.to_html()

        # Verifica a Lógica CSS
        # 20% -> Green (#ccffcc)
        self.assertIn("#ccffcc", html)
        # 200% -> Red (#ffcccc)
        self.assertIn("#ffcccc", html)

        # Verifica a Lógica de Moeda
        # Apenas verifica se R$ parece aproximadamente correto.
        # HTML output creates <td>R$ 1.000,00</td> etc.
        self.assertIn("R$", html)

    def test_format_currency_br(self):
        """Testa a função utilitária format_currency_br."""
        # Verifica se a função foi devidamente extraída e injetada
        self.assertIn('format_currency_br', self.scope)
        format_func = self.scope['format_currency_br']

        # Testa caminhos felizes (happy paths)
        self.assertEqual(format_func(1234.56), "R$ 1.234,56")
        self.assertEqual(format_func(100), "R$ 100,00")
        self.assertEqual(format_func(0), "R$ 0,00")

        # Testa números grandes com múltiplos separadores de milhares
        self.assertEqual(format_func(1234567.89), "R$ 1.234.567,89")
        self.assertEqual(format_func(1000000000.00), "R$ 1.000.000.000,00")

        # Testa números negativos
        self.assertEqual(format_func(-500.25), "R$ -500,25")
        self.assertEqual(format_func(-1234.56), "R$ -1.234,56")

        # Testa valores ausentes
        # A função subjacente usa pd.isna() que gerencia corretamente np.nan, None e pd.NA.
        # Garante que nosso contexto de simulação pd os resolva corretamente.
        self.assertEqual(format_func(pd.NA), "-")
        self.assertEqual(format_func(np.nan), "-")
        self.assertEqual(format_func(None), "-")

if __name__ == '__main__':
    unittest.main()
