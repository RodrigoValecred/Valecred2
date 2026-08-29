import re
import sys

def check_broadcasthashjoin_logic():
    with open("VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Silver_Carteira_PDD.Notebook/notebook-content.py", "r") as f:
        content = f.read()

    # Small dimension tables in NB_Silver_Carteira_PDD:
    # df_pdd_percent_renamed, df_pdd_ajustes, df_base_gestao, df_base_bordero

    issues = []

    if "broadcast(" not in content:
        issues.append("Missing broadcast() entirely in NB_Silver_Carteira_PDD.")

    print(issues)

check_broadcasthashjoin_logic()
