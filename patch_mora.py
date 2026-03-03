file_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Relatorio_Produtos_Mensal.Notebook/notebook-content.py"

with open(file_path, "r") as f:
    content = f.read()

# Replace any lingering cod_cliente in process_mora_stream and process_prorrogacoes_stream groupBy
content = content.replace(
    '.groupBy("cod_cliente", "mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento")',
    '.groupBy("mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento")'
)

# Replace df_final join
content = content.replace(
    'df_final = df_union.join(df_clientes, "cod_cliente", "left") \\',
    'df_final = df_union \\'
)

with open(file_path, "w") as f:
    f.write(content)
print("done")
