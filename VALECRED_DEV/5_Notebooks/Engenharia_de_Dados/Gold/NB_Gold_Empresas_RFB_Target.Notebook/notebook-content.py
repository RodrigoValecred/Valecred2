# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "385c2931-573b-4db0-838d-a70a01306d32",
# META       "default_lakehouse_name": "LH_Gold",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "385c2931-573b-4db0-838d-a70a01306d32"
# META         },
# META         {
# META           "id": "553c2931-573b-4db0-838d-a70a01306d32"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook de Geração de Empresas Alvo (Gold)
# **Objetivo:** Filtrar e enriquecer os dados da Receita Federal para identificar empresas alvo para oferta de FIDC.
# **Regras de Negócio:**
# 1.  Situação Cadastral Ativa (02).
# 2.  Localização: SP ou MG.
# 3.  Tempo de Atividade: > 2 anos.
# 4.  Setor: Indústria ou Serviço (baseado no CNAE Principal).

# CELL ********************

from pyspark.sql.functions import col, current_date, expr, when, lit, months_between
from pyspark.sql.types import IntegerType

# --- Configurações e Constantes ---
STATUS_ATIVA = '02'
UFS_ALVO = ['SP', 'MG']
MIN_YEARS_ACTIVITY = 2

# Tabelas Origem
TABLE_EMPRESAS = "LH_Bronze.rfb_empresas_full"
TABLE_ESTABELECIMENTOS = "LH_Bronze.rfb_estabelecimentos_full"
# Tabela Destino
TABLE_TARGET = "LH_Gold.dim_empresas_rfb_target"

# --- Carregamento ---
print("Carregando tabelas Bronze...")
df_empresas = spark.read.table(TABLE_EMPRESAS)
df_estab = spark.read.table(TABLE_ESTABELECIMENTOS)

# CELL ********************

# --- Aplicação das Regras de Negócio (Filtragem de Estabelecimentos) ---

# 1. Filtro de Situação Cadastral e UF
df_filtered = df_estab.filter(
    (col("situacao_cadastral") == STATUS_ATIVA) &
    (col("uf").isin(UFS_ALVO))
)

# 2. Filtro de Idade (> 2 anos)
# Calcula a diferença em meses entre hoje e data de início
df_filtered = df_filtered.withColumn("meses_atividade", months_between(current_date(), col("data_inicio_atividade")))
df_filtered = df_filtered.filter(col("meses_atividade") >= (MIN_YEARS_ACTIVITY * 12))

# 3. Classificação de Setor (CNAE)
# CNAE é uma string, geralmente de 7 dígitos. Os dois primeiros indicam a Divisão.
# Extraímos os 2 primeiros dígitos para categorizar.
# Indústria: 05 a 33
# Serviço: 35 a 99 (Excluindo Comércio 45-47)
# Comércio: 45, 46, 47

df_filtered = df_filtered.withColumn("cnae_divisao", col("cnae_fiscal_principal").substr(1, 2).cast(IntegerType()))

df_filtered = df_filtered.withColumn("setor",
    when((col("cnae_divisao") >= 5) & (col("cnae_divisao") <= 33), "Industria")
    .when((col("cnae_divisao") >= 35) & (col("cnae_divisao") <= 99) & (~col("cnae_divisao").isin([45, 46, 47])), "Servico")
    .otherwise(None)
)

# Filtra apenas os setores de interesse (Industria e Servico)
df_filtered = df_filtered.filter(col("setor").isNotNull())

print(f"Estabelecimentos filtrados (Ativos, SP/MG, >2 anos, Ind/Serv): {df_filtered.count()}")

# CELL ********************

# --- Join com Empresas (Enriquecimento) ---

# Join para pegar Razão Social, Capital Social, Natureza Jurídica
# Chave: cnpj_basico

df_joined = df_filtered.join(df_empresas, "cnpj_basico", "inner")

# Criar coluna CNPJ Completo formatado (apenas números para join futuro ou formatado se preferir)
# Vamos manter apenas números: basico + ordem + dv
df_final = df_joined.withColumn("cnpj_completo", expr("concat(cnpj_basico, cnpj_ordem, cnpj_dv)"))

# Seleção de Colunas Finais
cols_to_select = [
    "cnpj_completo",
    "razao_social",
    "nome_fantasia",
    "uf",
    "municipio",
    "bairro",
    "logradouro",
    "numero",
    "cep",
    "cnae_fiscal_principal",
    "setor",
    "data_inicio_atividade",
    "capital_social",
    "natureza_juridica",
    "porte_empresa"
]

df_target = df_final.select(cols_to_select)

# CELL ********************

# --- Salvamento ---

print(f"Salvando {df_target.count()} registros na tabela Gold: {TABLE_TARGET}")

df_target.write.format("delta").mode("overwrite").saveAsTable(TABLE_TARGET)

print("Processo concluído com sucesso.")

# MARKDOWN ********************

# ## Exemplo de Uso: Cruzamento com Dados CVM
#
# O código abaixo demonstra como cruzar a tabela gerada (`LH_Gold.dim_empresas_rfb_target`) com os dados de FIDCs da CVM (`LH_Bronze.cvm_fidc_informe_mensal`) para encontrar clientes com alta concentração.
#
# > **Nota:** Este bloco é apenas demonstrativo e depende da existência da tabela da CVM carregada.

# CELL ********************

# # Exemplo de Query
#
# # 1. Carregar dados de FIDC (Exemplo: Informe Mensal)
# # Supondo que a tabela CVM tenha uma coluna de CNPJ do Devedor/Cedente ou similar.
# # Muitas vezes, nos informes mensais, a granularidade pode ser por Fundo, e não por Devedor individual (depende da tabela específica TAB_VI, TAB_IV, etc).
# # Se tivermos uma tabela de "Operações" ou "Carteira" detalhada com CNPJ do sacado:
#
# try:
#     df_cvm = spark.read.table("LH_Bronze.cvm_fidc_informe_mensal") # Exemplo
#     df_target_rfb = spark.read.table("LH_Gold.dim_empresas_rfb_target")
#
#     # Supondo coluna 'CNPJ_Devedor' na CVM
#     # df_opportunities = df_cvm.join(df_target_rfb, df_cvm.CNPJ_Devedor == df_target_rfb.cnpj_completo, "inner")
#
#     # Filtrar por concentração (Exemplo hipotético de coluna)
#     # df_high_concentration = df_opportunities.filter(col("percentual_concentracao") > 0.10)
#
#     # display(df_high_concentration)
#     print("Exemplo de código de join pronto para uso (comentado).")
#
# except Exception as e:
#     print(f"Tabelas para exemplo não disponíveis no momento: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
