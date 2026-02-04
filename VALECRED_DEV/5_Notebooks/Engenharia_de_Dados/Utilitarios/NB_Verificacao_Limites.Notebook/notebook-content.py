# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "553c2931-573b-4db0-838d-a70a01306d32",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "553c2931-573b-4db0-838d-a70a01306d32"
# META         },
# META         {
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         },
# META         {
# META           "id": "ee40705b-0100-49bc-8f35-81d71839f042"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook de Verificação de Limites (Extra Plus)
#
# **Objetivo:** Verificar a origem e o cálculo dos limites "Extra" e "Plus" para um determinado cliente ou grupo econômico.
#
# **Contexto:**
# O valor final do limite Extra/Plus na tabela `LH_Gold.dim_clientes` é calculado da seguinte forma:
# 1. Leitura do arquivo manual `limites_extra_plus.xlsx` (carregado em `LH_Silver.sup_limites_extra_plus`).
# 2. Associação com `LH_Silver.staging_clientes` via CNPJ para obter o `cod_cliente`.
# 3. Associação com `LH_Silver.sup_grupos_economicos` para identificar o Grupo Econômico.
# 4. **Agregação por Grupo:** O sistema seleciona o **MAIOR** valor de limite encontrado dentre todos os CNPJs que pertencem ao mesmo grupo econômico.
#
# **Como usar:**
# 1. Preencha o CNPJ ou Nome (parcial) na variável `search_term` abaixo.
# 2. Execute o notebook.
# 3. Analise os resultados para entender qual CNPJ está "puxando" o valor do limite para cima (lógica de MAX).

# CELL ********************

# --- CONFIGURAÇÃO ---
# Digite parte do CNPJ (apenas números) ou parte do Nome do cliente
search_term = "DIGITE_AQUI"

print(f"Buscando por: {search_term}")

from pyspark.sql.functions import col, regexp_replace, max, lit, coalesce

# --- 1. CARGA DOS DADOS ---
print("Carregando tabelas...")
try:
    df_limites_manual = spark.read.table("LH_Silver.sup_limites_extra_plus")
    df_grupos = spark.read.table("LH_Silver.sup_grupos_economicos")
    df_clientes = spark.read.table("LH_Silver.staging_clientes_limpa")
except Exception as e:
    print(f"Erro ao carregar tabelas: {e}")
    raise e

# --- 2. PADRONIZAÇÃO ---

# Limpeza do CNPJ na tabela de limites manuais para garantir o join
df_limites_manual = df_limites_manual.withColumn("cnpj_clean", regexp_replace(col("cnpj"), "[^0-9]", ""))

# Padronização de nomes de colunas na tabela de grupos (para evitar erros se nomes mudarem)
if "nomegrupo" in df_grupos.columns:
    df_grupos = df_grupos.withColumnRenamed("nomegrupo", "grupo_economico")
if "codcliente" in df_grupos.columns:
    df_grupos = df_grupos.withColumnRenamed("codcliente", "cod_cliente")

# --- 3. JOIN E RASTREABILIDADE ---
# Fluxo: Limites (Excel) -> Clientes (CNPJ->Cod) -> Grupos (Cod->Nome Grupo)

print("Realizando cruzamentos...")
df_trace = df_limites_manual.join(
    df_clientes.select(col("cpf_cnpj").alias("cnpj_clean"), "cod_cliente", "nome"),
    "cnpj_clean",
    "left"
).join(
    df_grupos,
    "cod_cliente",
    "left"
).select(
    col("cnpj"),
    col("cnpj_clean"),
    col("cod_cliente"),
    col("nome"),
    col("grupo_economico"),
    col("limite"),
    col("limite_extra"),
    col("limite_plus")
)

# --- 4. FILTRAGEM ---
df_specific = df_trace.filter(
    (col("cnpj_clean").contains(search_term)) |
    (col("nome").contains(search_term.upper()))
)

count = df_specific.count()
if count == 0:
    print(f"Nenhum registro encontrado em 'sup_limites_extra_plus' para o termo '{search_term}'.")
    print("Verifique se o cliente consta no arquivo 'limites_extra_plus.xlsx' e se o CNPJ está correto.")
else:
    print(f"Encontrados {count} registro(s) correspondente(s) na tabela manual:")
    display(df_specific)

    # --- 5. ANÁLISE DE GRUPO (A Lógica Gold) ---
    # Para cada grupo econômico encontrado, vamos mostrar TODOS os membros e o cálculo do MAX
    rows = df_specific.select("grupo_economico").distinct().collect()
    grupos_encontrados = [r["grupo_economico"] for r in rows if r["grupo_economico"]]

    if not grupos_encontrados:
        print("\nAVISO: O cliente foi encontrado na tabela manual, mas NÃO possui vínculo na tabela 'sup_grupos_economicos'.")
        print("Neste caso, o limite manual é considerado apenas para ele mesmo (ou ignorado se a lógica exigir grupo).")
    else:
        for grupo in grupos_encontrados:
            print(f"\n--- ANÁLISE DO GRUPO: {grupo} ---")
            print("Abaixo, todos os CNPJs deste grupo que constam no arquivo manual (sup_limites_extra_plus):")

            # Mostrar todos os membros do grupo que têm limite manual atribuído
            df_group_members = df_trace.filter(col("grupo_economico") == grupo)
            display(df_group_members)

            print("CÁLCULO FINAL (Lógica da Camada Gold):")
            print("O sistema pega o MAIOR valor (MAX) de cada coluna dentro do grupo.")

            df_gold_calc = df_group_members.groupBy("grupo_economico").agg(
                max("limite").alias("limite_grupo_manual_GOLD"),
                max("limite_extra").alias("limite_extra_grupo_GOLD"),
                max("limite_plus").alias("limite_plus_grupo_GOLD")
            )
            display(df_gold_calc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
