
import sys
import re

filepath = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"
with open(filepath, "r") as f:
    content = f.read()

# --- 1. Alias DataFrames before Loop ---
# Finding where df_gerentes_enrich is defined
search_def = 'df_gerentes_enrich = df_gerentes.join(df_usuarios, "cod_usuario", "left")'
replace_def = """df_gerentes_enrich = df_gerentes.join(df_usuarios, "cod_usuario", "left") \\
    .select(col("cod_broker"), col("taxa_comissao"), col("nome").alias("nome_gerente")).alias("gerentes")

# Aliasing other tables for join safety
df_escrow = df_escrow.alias("escrow")
df_first_op = df_first_op.alias("first_op")
df_client_rate = df_client_rate.alias("client_rate")"""

# Note: The original definition didn't have alias("gerentes").
# I need to match the block carefully.
# Original block:
# df_gerentes_enrich = df_gerentes.join(df_usuarios, "cod_usuario", "left") \
#     .select(col("cod_broker"), col("taxa_comissao"), col("nome").alias("nome_gerente"))

pattern_def = r'df_gerentes_enrich = df_gerentes\.join\(df_usuarios, "cod_usuario", "left"\) \\\s+\.select\(col\("cod_broker"\), col\("taxa_comissao"\), col\("nome"\)\.alias\("nome_gerente"\)\)'
replace_def_val = r"""df_gerentes_enrich = df_gerentes.join(df_usuarios, "cod_usuario", "left") \
    .select(col("cod_broker"), col("taxa_comissao"), col("nome").alias("nome_gerente")).alias("gerentes")

# Aliasing other tables for join safety
df_escrow = df_escrow.alias("escrow")
df_first_op = df_first_op.alias("first_op")
df_client_rate = df_client_rate.alias("client_rate")"""

content = re.sub(pattern_def, replace_def_val, content)

# --- 2. Update Joins and Selects ---
# We need to replace the entire df_ops_enrich_step1 block to be safe and clean.

start_join_block = 'df_ops_enrich_step1 = df_ops'
end_join_block = 'col("df_client_rate.taxa_cadastro_cliente")\n    )'

# New Join Block
new_join_block = """df_ops_enrich_step1 = df_ops \\
    .join(df_u_inc, col("ops.usua_inclusao") == col("u_inc.cod_usuario"), "left") \\
    .join(df_u_ana, col("ops.usua_st_analise") == col("u_ana.cod_usuario"), "left") \\
    .join(df_u_trava, col("ops.usua_trava") == col("u_trava.cod_usuario"), "left") \\
    .join(df_motivos, col("ops.cod_indeferimento") == col("motivos.codindeferimento"), "left") \\
    .join(df_estudo, col("ops.cod_operacao") == col("estudo.CODOPERACAO"), "left") \\
    .join(df_gerentes_enrich, col("ops.cod_broker") == col("gerentes.cod_broker"), "left") \\
    .join(df_escrow, col("ops.cod_operacao") == col("escrow.cod_operacao"), "left") \\
    .join(df_first_op, col("ops.cod_cliente") == col("first_op.cod_cliente"), "left") \\
    .join(df_client_rate, col("ops.cod_cliente") == col("client_rate.cod_cliente"), "left") \\
    .select(
        col("ops.*"),
        col("u_inc.nome").alias("usuario_inclusao"),
        col("u_inc.nivel").alias("nivel_usuario_inclusao"),
        col("u_inc.funcao").alias("incluido_por"),
        col("u_ana.nome").alias("analista"),
        col("u_trava.nome").alias("analista_trava"),
        col("motivos.motivo_indeferimento"),
        col("motivos.grupo_motivo_indeferimento"),
        col("estudo.fator").alias("taxa_cadastro"),
        col("gerentes.taxa_comissao"),
        col("gerentes.nome_gerente").alias("gestor_da_operacao"),
        col("escrow.ESCROW").alias("flag_escrow"),
        col("first_op.data_primeira_operacao_calc"),
        col("client_rate.taxa_cadastro_cliente")
    )"""

# Regex replacement for the join block
# It's better to construct a regex that matches from start_join_block to the end of the select parenthesis
pattern_join = r'df_ops_enrich_step1 = df_ops.*?col\("df_client_rate\.taxa_cadastro_cliente"\)\s+\)'
content = re.sub(pattern_join, new_join_block, content, flags=re.DOTALL)

# Write back
with open(filepath, "w") as f:
    f.write(content)
