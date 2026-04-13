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

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

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

# ⚡ Otimização Bolt: Caching antes de ação de log para evitar reavaliação de plano físico
# 💡 O que: Adicionado `df_filtered.cache()` antes de seu `.count()` de log. Foi adicionado `.unpersist()` ao final do uso.
# 🎯 Por que: O comando `df_filtered.count()` para log aciona uma avaliação lazy. Como `df_filtered` é usado logo em seguida no `.join()` com `df_empresas`, o Spark reexecutará todo o scan inicial de "estabelecimentos" e filtros pesados de idade/CNAE se os dados não estiverem cacheados, dobrando a carga de leitura da camada Bronze.
# 📊 Impacto: Previne o re-processamento das regras iniciais e scans, poupando I/O redundante.
# 🔬 Medição: O Spark UI demonstrará `InMemoryTableScan` ao executar a query de Join subsequente.
df_filtered.cache()
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
    "situacao_cadastral",
    "ddd_1",
    "telefone_1",
    "correio_eletronico",
    "porte_empresa"
]

df_target = df_final.select(cols_to_select)

# CELL ********************

# --- Salvamento ---

print(f"Salvando {df_target.count()} registros na tabela Gold: {TABLE_TARGET}")

df_target.write.format("delta").mode("overwrite").saveAsTable(TABLE_TARGET)
df_filtered.unpersist()

print("Processo concluído com sucesso.")

# MARKDOWN ********************

# ## Cruzamento com Dados CVM (Concentração)
#
# Cruza a tabela gerada (`LH_Gold.dim_empresas_rfb_target`) com os dados de FIDCs da CVM (`LH_Bronze.cvm_fidc_informe_mensal`) para encontrar clientes com alta concentração.

# CELL ********************

from pyspark.sql.functions import array, struct, explode, col, lit, regexp_replace

try:
    print("Iniciando cruzamento com dados da CVM...")
    df_cvm = spark.read.table("LH_Bronze.cvm_fidc_informe_mensal")
    df_target_rfb = spark.read.table("LH_Gold.dim_empresas_rfb_target")

    # Os dados da CVM na TAB_I possuem colunas para os 9 maiores cedentes.
    # TAB_I2A12_CPF_CNPJ_CEDENTE_1 até 9
    # TAB_I2A12_PR_CEDENTE_1 até 9

    # Criamos um array de structs para desaninhar (unpivot) os cedentes
    # Otimização: Capturamos as colunas uma única vez para evitar múltiplas chamadas ao driver/JVM
    cols_existentes = set(df_cvm.columns)

    cedentes_cols = [
        struct(
            col(f"TAB_I2A12_CPF_CNPJ_CEDENTE_{i}").alias("cnpj_cedente"),
            col(f"TAB_I2A12_PR_CEDENTE_{i}").cast("double").alias("percentual_concentracao")
        )
        for i in range(1, 10)
        if f"TAB_I2A12_CPF_CNPJ_CEDENTE_{i}" in cols_existentes
        and f"TAB_I2A12_PR_CEDENTE_{i}" in cols_existentes
    ]

    if cedentes_cols:
        df_cvm_unpivoted = df_cvm.withColumn("cedentes", array(*cedentes_cols)) \
                                 .select("CNPJ_FUNDO_CLASSE", "DENOM_SOCIAL", "DT_COMPTC", explode("cedentes").alias("cedente")) \
                                 .select(
                                     col("CNPJ_FUNDO_CLASSE"),
                                     col("DENOM_SOCIAL").alias("fundo_denom_social"),
                                     col("DT_COMPTC"),
                                     col("cedente.cnpj_cedente").alias("cnpj_cedente_raw"),
                                     col("cedente.percentual_concentracao")
                                 )

        # Limpar o CNPJ da CVM (remover pontuações)
        df_cvm_unpivoted = df_cvm_unpivoted.withColumn("cnpj_cedente_limpo", regexp_replace(col("cnpj_cedente_raw"), "[^0-9]", ""))

        # Filtrar por concentração > 10% e CNPJ não nulo
        df_high_concentration = df_cvm_unpivoted.filter((col("percentual_concentracao") > 10.0) & (col("cnpj_cedente_limpo") != ""))

        # Realizar o join com as empresas target
        df_opportunities = df_high_concentration.join(
            df_target_rfb,
            df_high_concentration.cnpj_cedente_limpo == df_target_rfb.cnpj_completo,
            "inner"
        )

        # Selecionar e organizar as colunas finais
        cols_final = [
            "CNPJ_FUNDO_CLASSE",
            "fundo_denom_social",
            "DT_COMPTC",
            "percentual_concentracao",
            "cnpj_completo",
            "razao_social",
            "nome_fantasia",
            "setor",
            "cnae_fiscal_principal",
            "uf",
            "municipio",
            "situacao_cadastral",
            "data_inicio_atividade",
            "ddd_1",
            "telefone_1",
            "correio_eletronico"
        ]

        # ⚡ Otimização Bolt: Adicionado cache em dataframe cruzado com log
        # 💡 O que: Inserido .cache() no dataframe `df_resultado_final` e .unpersist() no final
        # 🎯 Por que: Uma ação count() forçava a leitura da CVM (Bronze) cruzada com a Gold. Ao cachear, economizamos I/O no .saveAsTable() subsequente.
        df_resultado_final = df_opportunities.select(cols_final).cache()

        table_cvm_target = "LH_Gold.fato_empresas_target_cvm_concentracao"
        print(f"Salvando {df_resultado_final.count()} oportunidades cruzadas na tabela Gold: {table_cvm_target}")

        df_resultado_final.write.format("delta").mode("overwrite").saveAsTable(table_cvm_target)
        df_resultado_final.unpersist()
        print("Cruzamento com CVM concluído com sucesso.")

    else:
        print("Colunas de cedente não encontradas no schema da CVM.")

except Exception as e:
    print(f"Erro ao processar o cruzamento com a CVM: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
