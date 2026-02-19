import unittest
import pandas as pd
import numpy as np

class TestRentabilidadeProrrogacao(unittest.TestCase):
    def test_prorrogacao_logic(self):
        # Scenario:
        # Client A has:
        # - Operation 1 (2025): Desagio=100.
        # - Prorogation P1 (2025, Value=10, Dates Diff).
        # - Prorogation P2 (2026, Value=20, Dates Diff).
        # - Prorogation P3 (2025, Value=5, Dates Equal - Should be Filtered).

        # 1. df_prorrogacao (All prorogations)
        df_prorrogacao = pd.DataFrame([
            {'cod_operacao': 1, 'cod_cliente': 'A', 'juros': 10, 'year': 2025, 'diff_dates': True},
            {'cod_operacao': 1, 'cod_cliente': 'A', 'juros': 20, 'year': 2026, 'diff_dates': True},
            {'cod_operacao': 1, 'cod_cliente': 'A', 'juros': 5,  'year': 2025, 'diff_dates': False} # Should be ignored
        ])

        # Filter Logic (Simulating Spark Filter)
        df_prorrogacao_clean = df_prorrogacao[
            (df_prorrogacao['diff_dates'] == True)
        ]

        # 2. Aggregations
        # Client Level (Calendar 2025) - existing logic
        df_prorrogacao_agg_cliente = df_prorrogacao_clean[df_prorrogacao_clean['year'] == 2025] \
            .groupby('cod_cliente')['juros'].sum().reset_index() \
            .rename(columns={'juros': 'receita_tarifa_prorrogacao_cliente'})

        # Operation Level (Lifetime) - New Logic 1
        df_prorrogacao_agg_op_lifetime = df_prorrogacao_clean \
            .groupby('cod_operacao')['juros'].sum().reset_index() \
            .rename(columns={'juros': 'receita_prorrogacao_op'})

        # Operation Level (Calendar 2025) - New Logic 2 (for deduplication)
        df_prorrogacao_agg_op_2025 = df_prorrogacao_clean[df_prorrogacao_clean['year'] == 2025] \
            .groupby('cod_operacao')['juros'].sum().reset_index() \
            .rename(columns={'juros': 'receita_prorrogacao_op_2025'})

        # 3. df_ops (Operations from 2025)
        df_ops = pd.DataFrame([
            {'cod_operacao': 1, 'cod_cliente': 'A', 'desagio': 100}
        ])

        # 4. Join Operations with Prorogation Op
        df_base = df_ops.merge(df_prorrogacao_agg_op_lifetime, on='cod_operacao', how='left')
        df_base = df_base.merge(df_prorrogacao_agg_op_2025, on='cod_operacao', how='left')
        df_base['receita_prorrogacao_op'] = df_base['receita_prorrogacao_op'].fillna(0)
        df_base['receita_prorrogacao_op_2025'] = df_base['receita_prorrogacao_op_2025'].fillna(0)

        # 5. Calculate Revenue Op (Lifetime)
        df_base['receita_total_op'] = df_base['desagio'] + df_base['receita_prorrogacao_op']

        # Verify Op Level
        row_op1 = df_base[df_base['cod_operacao'] == 1].iloc[0]
        # P1 (10) + P2 (20) included. P3 (5) excluded. Total = 100 + 30 = 130.
        self.assertEqual(row_op1['receita_total_op'], 130, "Op 1 Revenue should include all valid prorogations (100 + 10 + 20)")

        # 6. Aggregation by Client (df_cliente_agg)
        df_cliente_agg = df_base.groupby('cod_cliente').agg({
            'receita_total_op': 'sum',
            'receita_prorrogacao_op': 'sum',
            'receita_prorrogacao_op_2025': 'sum'
        }).rename(columns={
            'receita_total_op': 'soma_receita_total_op',
            'receita_prorrogacao_op': 'soma_prorrogacao_op_cliente',
            'receita_prorrogacao_op_2025': 'soma_prorrogacao_op_2025_cliente'
        }).reset_index()

        # 7. Join Client Agg with Prorogation Client Agg
        df_report = df_cliente_agg.merge(df_prorrogacao_agg_cliente, on='cod_cliente', how='left')
        df_report['receita_tarifa_prorrogacao_cliente'] = df_report['receita_tarifa_prorrogacao_cliente'].fillna(0)

        # 8. Calculate Final Client Revenue
        # Formula: soma_receita_total_op + (receita_tarifa_prorrogacao_cliente - soma_prorrogacao_op_2025_cliente)
        # We use 2025 deduplication because receita_tarifa_prorrogacao_cliente is filtered for 2025.
        df_report['receita_total_cliente'] = df_report['soma_receita_total_op'] + \
            (df_report['receita_tarifa_prorrogacao_cliente'] - df_report['soma_prorrogacao_op_2025_cliente'])

        # Verify Client Level
        row_clientA = df_report[df_report['cod_cliente'] == 'A'].iloc[0]

        # Expected:
        # soma_receita_total_op = 130 (Lifetime Op 1)
        # receita_tarifa_prorrogacao_cliente = 10 (Calendar 2025 - only P1)
        # soma_prorrogacao_op_2025_cliente = 10 (Calendar 2025 Op 1 - only P1)
        # Total = 130 + (10 - 10) = 130.

        # Wait, does Client Revenue include Non-Op 2026 revenue?
        # The metric `receita_tarifa_prorrogacao_cliente` is strictly 2025.
        # So any revenue in 2026 is NOT captured by `receita_tarifa_prorrogacao_cliente`.
        # However, `soma_receita_total_op` includes 2026 revenue (Lifetime).
        # So we are summing Lifetime Op Revenue + (Client 2025 - Op 2025).
        # This effectively adds "Extra" 2025 revenue from other sources.
        # It ignores "Extra" 2026 revenue from other sources (which is fine, report is Safra 2025).
        # But it INCLUDES "Op" 2026 revenue (which is fine, report is Safra 2025 Lifetime).

        self.assertEqual(row_clientA['soma_receita_total_op'], 130)
        self.assertEqual(row_clientA['receita_tarifa_prorrogacao_cliente'], 10)
        self.assertEqual(row_clientA['soma_prorrogacao_op_2025_cliente'], 10)
        self.assertEqual(row_clientA['receita_total_cliente'], 130, "Client Revenue should cover Lifetime Op + Non-Op 2025")

if __name__ == '__main__':
    unittest.main()
