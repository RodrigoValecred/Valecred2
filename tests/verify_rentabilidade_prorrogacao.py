import unittest
import pandas as pd
import numpy as np

class TestRentabilidadeProrrogacao(unittest.TestCase):
    def test_prorrogacao_logic(self):
        # Scenario:
        # Client A has:
        # - Operation 1 (2025): Desagio=100.
        # - Prorogation P1 (2025, Value=10, Dates Diff).
        # - Prorogation P2 (2025, Value=20, Dates Diff).
        # - Prorogation P3 (2025, Value=5, Dates Equal - Should be Filtered).

        # 1. df_prorrogacao (All prorogations in 2025)
        df_prorrogacao = pd.DataFrame([
            {'cod_operacao': 1, 'cod_cliente': 'A', 'juros': 10, 'year': 2025, 'diff_dates': True},
            {'cod_operacao': 2, 'cod_cliente': 'A', 'juros': 20, 'year': 2025, 'diff_dates': True},
            {'cod_operacao': 1, 'cod_cliente': 'A', 'juros': 5,  'year': 2025, 'diff_dates': False} # Should be ignored
        ])

        # Filter Logic (Simulating Spark Filter)
        df_prorrogacao_filtered = df_prorrogacao[
            (df_prorrogacao['year'] == 2025) &
            (df_prorrogacao['diff_dates'] == True)
        ]

        # 2. Aggregations
        # Client Level (Existing Logic)
        df_prorrogacao_agg_cliente = df_prorrogacao_filtered \
            .groupby('cod_cliente')['juros'].sum().reset_index() \
            .rename(columns={'juros': 'receita_tarifa_prorrogacao_cliente'})

        # Operation Level (New Logic)
        df_prorrogacao_agg_op = df_prorrogacao_filtered \
            .groupby('cod_operacao')['juros'].sum().reset_index() \
            .rename(columns={'juros': 'receita_prorrogacao_op'})

        # 3. df_ops (Operations from 2025)
        df_ops = pd.DataFrame([
            {'cod_operacao': 1, 'cod_cliente': 'A', 'desagio': 100}
        ])

        # 4. Join Operations with Prorogation Op
        df_base = df_ops.merge(df_prorrogacao_agg_op, on='cod_operacao', how='left')
        df_base['receita_prorrogacao_op'] = df_base['receita_prorrogacao_op'].fillna(0)

        # 5. Calculate Revenue Op
        df_base['receita_total_op'] = df_base['desagio'] + df_base['receita_prorrogacao_op']

        # Verify Op Level
        row_op1 = df_base[df_base['cod_operacao'] == 1].iloc[0]
        # P1 (10) included. P3 (5) excluded.
        self.assertEqual(row_op1['receita_total_op'], 110, "Op 1 Revenue should include only valid prorogations (100 + 10)")

        # 6. Aggregation by Client (df_cliente_agg)
        df_cliente_agg = df_base.groupby('cod_cliente').agg({
            'receita_total_op': 'sum',
            'receita_prorrogacao_op': 'sum'
        }).rename(columns={
            'receita_total_op': 'soma_receita_total_op',
            'receita_prorrogacao_op': 'soma_prorrogacao_op_cliente'
        }).reset_index()

        # 7. Join Client Agg with Prorogation Client Agg
        df_report = df_cliente_agg.merge(df_prorrogacao_agg_cliente, on='cod_cliente', how='left')
        df_report['receita_tarifa_prorrogacao_cliente'] = df_report['receita_tarifa_prorrogacao_cliente'].fillna(0)

        # 8. Calculate Final Client Revenue
        # Formula: soma_receita_total_op + (receita_tarifa_prorrogacao_cliente - soma_prorrogacao_op_cliente)
        df_report['receita_total_cliente'] = df_report['soma_receita_total_op'] + \
            (df_report['receita_tarifa_prorrogacao_cliente'] - df_report['soma_prorrogacao_op_cliente'])

        # Verify Client Level
        row_clientA = df_report[df_report['cod_cliente'] == 'A'].iloc[0]

        # Expected:
        # soma_receita_total_op = 110 (Op 1)
        # receita_tarifa_prorrogacao_cliente = 30 (P1 + P2)
        # soma_prorrogacao_op_cliente = 10 (P1)
        # Total = 110 + (30 - 10) = 130.

        self.assertEqual(row_clientA['soma_receita_total_op'], 110)
        self.assertEqual(row_clientA['receita_tarifa_prorrogacao_cliente'], 30)
        self.assertEqual(row_clientA['soma_prorrogacao_op_cliente'], 10)
        self.assertEqual(row_clientA['receita_total_cliente'], 130, "Client Revenue should be 130")

if __name__ == '__main__':
    unittest.main()
