import sys

def create_sequential_invoices_tool():
    code = """
def check_sequential_invoices(df, col_emission_date="data_emissao", col_entry_date="data_entrada", col_volume="vlr_total_sacado", threshold_volume=100000.0):
    '''
    Verifica se existem notas sequenciais: emitidas e descontadas no mesmo dia em volumes altos.
    (Comportamento de quem está com pressa para fugir com o dinheiro).

    Retorna o DataFrame com uma nova coluna boolean `alerta_notas_sequenciais`.
    '''
    return df.withColumn(
        "alerta_notas_sequenciais",
        F.when(
            (F.to_date(F.col(col_emission_date)) == F.to_date(F.col(col_entry_date))) &
            (F.col(col_volume) >= threshold_volume),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
"""
    return code

print(create_sequential_invoices_tool())
