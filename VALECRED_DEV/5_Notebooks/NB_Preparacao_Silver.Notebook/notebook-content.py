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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook de Preparação da Camada Silver (Staging)
# **Objetivo:** Este notebook é responsável por ler os dados brutos da camada **Bronze**, aplicar uma série de transformações de limpeza e regras de negócio, e salvar os dados resultantes na camada **Silver**. As tabelas geradas aqui são tabelas de "staging" (intermediárias), que servirão de base para a construção do modelo dimensional na camada Gold.
# **Processos realizados:**
# 1.  **Configuração do Ambiente:** Define configurações do Spark e importa as bibliotecas necessárias.
# 2.  **Limpeza de `tab_titulos`:** Remove duplicatas para garantir que cada título seja único.
# 3.  **Limpeza de `cad_clientes`:** Desduplica a tabela base para a dimensão de clientes.
# 4.  **Limpeza e Enriquecimento de `cad_geral_pf_pj`:** Remove duplicatas do cadastro de clientes (Pessoa Física e Jurídica) e enriquece com informações de contato.
# 5.  **Pré-processamento de Eventos de Protesto:** Isola e limpa os eventos de protesto de títulos.
# 6.  **Limpeza de `tab_operacoes`:** Remove duplicatas e enriquece a tabela de operações.
# 7.  **Processamento da Chave DANFE:** Extrai informações detalhadas da chave da nota fiscal.
# 8.  **Limpeza e Enriquecimento de `tab_titulos_baixas`:** Limpa os dados de baixas de títulos e os enriquece com dimensões, criando a `fato_baixas`.
# 9.  **Processamento Incremental de Pareceres:** Processa novos pareceres para construir a `esteira_de_propostas`.
# 10. **Limpeza de Cache:** Libera os DataFrames da memória.


# MARKDOWN ********************

# ## Seção 0: Configuração do ambiente Python
# **Descrição:** Esta célula prepara a sessão Spark e importa todas as funções e bibliotecas que serão utilizadas ao longo do notebook.
# - `spark.conf.set`: Estas configurações são importantes para garantir a compatibilidade com datas em formatos mais antigos que podem existir nos dados legados, evitando erros de leitura ou escrita no formato Parquet.
# - `Window`, `row_number`, `col`, `when`, `lit`: Funções essenciais do PySpark para manipulação de dados, especialmente para a lógica de desduplicação.

# CELL ********************

# Célula 0: Configuração da Sessão Spark
# ------------------------------------

# Corrige o problema de LEITURA de datas antigas (formato LEGACY do parquet)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

# Corrige o problema de ESCRITA de datas antigas (formato LEGACY do parquet)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# Importando as funções necessárias do PySpark
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, when, lit, concat, length, regexp_replace,
    collect_list, concat_ws, upper, greatest, substring, year,
    lead, date_add, lag, max, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from functools import reduce
from delta.tables import *
import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 11: Teste de Verificação da Correção do Watermark
# **Objetivo:** Esta seção contém um teste automatizado para verificar a correção do bug no cálculo do watermark. O teste simula o cenário problemático e valida que a nova lógica funciona como esperado.
# **Cenário de Teste:**
# 1.  **Carga Inicial:** Simula uma primeira carga com um registro datado de `2024-01-01`. O watermark inicial é `1900-01-01`.
# 2.  **Verificação da Carga Inicial:** O teste verifica se o watermark avança corretamente para `2024-01-01`.
# 3.  **Carga Incremental com Atualização:** Simula uma segunda carga onde o mesmo registro é *atualizado* em `2024-02-01` (`DATAALTERACAO`), mas sua data de criação (`DATAINCLUSAO`) permanece `2024-01-01`.
# 4.  **Verificação da Correção:**
#     - **Com a lógica antiga (bug):** O watermark não avançaria, permanecendo em `2024-01-01`, pois `max(DATAINCLUSAO)` não mudou.
#     - **Com a lógica nova (corrigida):** O teste afirma que o watermark deve avançar para a `DATAALTERACAO` (`2024-02-01`), provando que o bug foi corrigido.


# CELL ********************

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import datetime

# Função de processamento incremental refatorada e corrigida
def process_incremental_pareceres_fixed(spark, df_pareceres_raw, last_watermark):
    """
    Processa um DataFrame de pareceres de forma incremental, aplicando a lógica de
    cálculo de watermark corrigida.
    """
    df_pareceres_incremental = df_pareceres_raw.filter(
        (col("DATAINCLUSAO") > last_watermark) | (col("DATAALTERACAO") > last_watermark)
    )

    record_count = df_pareceres_incremental.count()

    if record_count > 0:
        new_watermark_df = df_pareceres_incremental.withColumn(
            "latest_date",
            greatest(
                coalesce(col("DATAINCLUSAO"), lit(datetime.datetime(1900, 1, 1))),
                coalesce(col("DATAALTERACAO"), lit(datetime.datetime(1900, 1, 1)))
            )
        ).agg(max("latest_date").alias("NewWatermark"))
        new_watermark = new_watermark_df.collect()[0]["NewWatermark"]
    else:
        new_watermark = last_watermark

    return new_watermark, record_count


class TestWatermarkBugFix(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("WatermarkTest").getOrCreate()

    def test_watermark_advances_on_updated_records(self):
        """
        Valida que o watermark avança corretamente quando um registro antigo é atualizado.
        """
        # --- Estrutura dos dados de teste ---
        schema = StructType([
            StructField("CODPARECER", IntegerType(), True),
            StructField("DATAINCLUSAO", TimestampType(), True),
            StructField("DATAALTERACAO", TimestampType(), True)
        ])

        # --- Cenário 1: Carga Inicial ---
        initial_load_data = [(1, datetime.datetime(2024, 1, 1, 10, 0, 0), datetime.datetime(2024, 1, 1, 10, 0, 0))]
        df_initial = self.spark.createDataFrame(initial_load_data, schema)

        # Watermark inicial muito antigo
        watermark_run1 = datetime.datetime(1900, 1, 1)

        # Executa o processamento
        new_watermark_run1, count_run1 = process_incremental_pareceres_fixed(self.spark, df_initial, watermark_run1)

        print(f"Execução 1: Watermark Inicial = {watermark_run1}, Registros Processados = {count_run1}, Novo Watermark = {new_watermark_run1}")
        self.assertEqual(count_run1, 1)
        self.assertEqual(new_watermark_run1, datetime.datetime(2024, 1, 1, 10, 0, 0))

        # --- Cenário 2: Carga Incremental com Registro Atualizado ---
        # O mesmo registro agora tem uma DATAALTERACAO mais recente
        updated_load_data = [(1, datetime.datetime(2024, 1, 1, 10, 0, 0), datetime.datetime(2024, 2, 1, 12, 0, 0))]
        df_updated = self.spark.createDataFrame(updated_load_data, schema)

        # O watermark agora é o da primeira execução
        watermark_run2 = new_watermark_run1

        # Executa o processamento com a lógica corrigida
        new_watermark_run2, count_run2 = process_incremental_pareceres_fixed(self.spark, df_updated, watermark_run2)

        print(f"Execução 2: Watermark Inicial = {watermark_run2}, Registros Processados = {count_run2}, Novo Watermark = {new_watermark_run2}")

        # Verificações
        self.assertEqual(count_run2, 1, "O registro atualizado deveria ter sido processado.")

        # Esta é a asserção chave que falharia com a lógica antiga e passa com a nova.
        # A lógica antiga retornaria `2024-01-01` como novo watermark, pois `max(DATAINCLUSAO)` não mudou.
        self.assertEqual(new_watermark_run2, datetime.datetime(2024, 2, 1, 12, 0, 0), "O watermark deveria ter avançado para a DATAALTERACAO.")
        print("\nTeste concluído com sucesso. A lógica de watermark corrigida funciona como esperado.")


# Função de processamento da esteira refatorada para ser testável
def process_esteira_transitions(spark, df_pareceres_completa, use_lag_logic=False):
    """
    Executa a lógica de transição de status para construir a esteira de propostas.
    Inclui um parâmetro para alternar entre a lógica original (lead) e a corrigida (lag).
    """
    # Lógica de janela: lead (incorreta) vs. lag (correta)
    if use_lag_logic:
        window_spec = Window.partitionBy("CODCLIENTE").orderBy("DATALOG")
        df_com_anterior = df_pareceres_completa \
            .withColumn("STATUS_DO_CLIENTE_ANTERIOR", lag("STATUS_DO_CLIENTE").over(window_spec)) \
            .withColumn("DATALOG_ANTERIOR", lag("DATALOG").over(window_spec)) \
            .withColumn("MACROPROCESSO_ANTERIOR", lag("MACROPROCESSO").over(window_spec)) \
            .withColumn("FASE_ANTERIOR", lag("FASE").over(window_spec))
    else: # Lógica original com bug
        window_lead = Window.partitionBy("CODCLIENTE").orderBy("DATALOG")
        df_com_anterior = df_pareceres_completa \
            .withColumn("STATUS_DO_CLIENTE_ANTERIOR", lead("STATUS_DO_CLIENTE").over(window_lead)) \
            .withColumn("DATALOG_ANTERIOR", lead("DATALOG").over(window_lead)) \
            .withColumn("MACROPROCESSO_ANTERIOR", lead("MACROPROCESSO").over(window_lead)) \
            .withColumn("FASE_ANTERIOR", lead("FASE").over(window_lead))

    # Filtra apenas as transições de status válidas
    df_transicoes = df_com_anterior.filter(col("STATUS_DO_CLIENTE") != col("STATUS_DO_CLIENTE_ANTERIOR")).na.drop(subset=["STATUS_DO_CLIENTE_ANTERIOR"])

    # Lógica de flags (corrigida para usar o estado atual vs. o anterior)
    if use_lag_logic:
        df_esteira_final = df_transicoes \
            .withColumn("DEVOLUCAO", when((col("MACROPROCESSO_ANTERIOR") == "CREDITO") & (col("MACROPROCESSO") == "COMERCIAL"), True).otherwise(False)) \
            .withColumn("RECEBIDA", when((col("MACROPROCESSO_ANTERIOR") == "COMERCIAL") & (col("MACROPROCESSO") == "CREDITO"), True).otherwise(False))
    else: # Lógica original com bug
         df_esteira_final = df_transicoes \
            .withColumn("DEVOLUCAO", when((col("MACROPROCESSO") == "CREDITO") & (col("MACROPROCESSO_ANTERIOR") == "COMERCIAL"), True).otherwise(False)) \
            .withColumn("RECEBIDA", when((col("MACROPROCESSO") == "COMERCIAL") & (col("MACROPROCESSO_ANTERIOR") == "CREDITO"), True).otherwise(False))

    return df_esteira_final.select(
        "CODCLIENTE", "DATALOG", "STATUS_DO_CLIENTE",
        "DATALOG_ANTERIOR", "STATUS_DO_CLIENTE_ANTERIOR",
        "DEVOLUCAO", "RECEBIDA"
    )

class TestEsteiraLogicBugFix(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("EsteiraLogicTest").getOrCreate()

    def test_state_transition_logic(self):
        """
        Valida que a transição de status na esteira de propostas usa a lógica correta (lag).
        """
        schema = StructType([
            StructField("CODCLIENTE", IntegerType(), True),
            StructField("DATALOG", TimestampType(), True),
            StructField("STATUS_DO_CLIENTE", StringType(), True),
            StructField("MACROPROCESSO", StringType(), True),
            StructField("FASE", StringType(), True),
        ])

        # Dados de teste simulando a jornada de um cliente
        # 1. Entrada -> 2. Comercial -> 3. Crédito (Recebida) -> 4. Comercial (Devolução)
        test_data = [
            (101, datetime.datetime(2024, 1, 1, 9, 0, 0), "AGUARDANDO DOCUMENTAÇÃO", "COMERCIAL", "FASE 1"),
            (101, datetime.datetime(2024, 1, 2, 10, 0, 0), "ANÁLISE DE CRÉDITO", "CREDITO", "FASE 2"),
            (101, datetime.datetime(2024, 1, 3, 11, 0, 0), "PENDÊNCIA COMERCIAL", "COMERCIAL", "FASE 3"),
            (101, datetime.datetime(2024, 1, 4, 12, 0, 0), "APROVADO", "CREDITO", "FASE 4"),
        ]
        df_pareceres_teste = self.spark.createDataFrame(test_data, schema)

        # --- Verificação da Lógica com Bug (usando lead) ---
        print("\nExecutando teste com a lógica original (lead)...")
        df_result_bug = process_esteira_transitions(self.spark, df_pareceres_teste, use_lag_logic=False)

        # Esta transição está errada. O `_ANTERIOR` é na verdade o estado *seguinte*.
        # DEVOLUCAO é True, mas deveria ser RECEBIDA.
        devolucao_bug_row = df_result_bug.filter(col("STATUS_DO_CLIENTE") == "AGUARDANDO DOCUMENTAÇÃO").collect()[0]
        self.assertTrue(devolucao_bug_row["DEVOLUCAO"], "A lógica de 'DEVOLUCAO' com lead está incorreta.")
        self.assertEqual(devolucao_bug_row["STATUS_DO_CLIENTE_ANTERIOR"], "ANÁLISE DE CRÉDITO")
        print("A lógica com bug (lead) se comportou como esperado (incorretamente).")

        # --- Verificação da Lógica Corrigida (usando lag) ---
        print("\nExecutando teste com a lógica corrigida (lag)...")
        df_result_fixed = process_esteira_transitions(self.spark, df_pareceres_teste, use_lag_logic=True)
        df_result_fixed.show(truncate=False)

        # Transição 1: COMERCIAL -> CREDITO (Deve ser RECEBIDA)
        recebida_row = df_result_fixed.filter(col("STATUS_DO_CLIENTE") == "ANÁLISE DE CRÉDITO").collect()[0]
        self.assertTrue(recebida_row["RECEBIDA"])
        self.assertFalse(recebida_row["DEVOLUCAO"])
        self.assertEqual(recebida_row["STATUS_DO_CLIENTE_ANTERIOR"], "AGUARDANDO DOCUMENTAÇÃO")

        # Transição 2: CREDITO -> COMERCIAL (Deve ser DEVOLUCAO)
        devolucao_row = df_result_fixed.filter(col("STATUS_DO_CLIENTE") == "PENDÊNCIA COMERCIAL").collect()[0]
        self.assertTrue(devolucao_row["DEVOLUCAO"])
        self.assertFalse(devolucao_row["RECEBIDA"])
        self.assertEqual(devolucao_row["STATUS_DO_CLIENTE_ANTERIOR"], "ANÁLISE DE CRÉDITO")

        print("\nTeste concluído com sucesso. A lógica da esteira corrigida (lag) funciona como esperado.")


# Executar os testes
suite = unittest.TestSuite()
suite.addTest(unittest.makeSuite(TestWatermarkBugFix))
suite.addTest(unittest.makeSuite(TestEsteiraLogicBugFix))
runner = unittest.TextTestRunner()
runner.run(suite)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Limpeza da Tabela tab_titulos
# **Objetivo:** A tabela `tab_titulos` na camada Bronze pode conter múltiplos registros para o mesmo título. Esta seção isola apenas o registro mais recente e válido para cada título e o armazena em cache para ser reutilizado por outras seções.

# CELL ********************

# Célula 1.1: Parâmetros e Leitura
# ------------------------------------------------
source_lakehouse = "LH_Bronze"
target_lakehouse = "LH_Silver"
source_table_titulos = "tab_titulos"
target_table_titulos = "staging_titulos_limpa"

print(f"Iniciando a limpeza da tabela: {source_lakehouse}.{source_table_titulos}")
df_bronze_titulos = spark.read.table(f"{source_lakehouse}.{source_table_titulos}")

# Célula 1.2: Lógica de Desduplicação
# ----------------------------------------------------
key_columns_titulos = ["CODTITULO"]
df_with_latest_date = df_bronze_titulos.withColumn(
    "DATA_MAIS_RECENTE",
    greatest(col("DATAALTERACAO"), col("DATAINCLUSAO"), col("LIQUIDACAO"))
)
windowSpec_titulos = Window.partitionBy([col(c) for c in key_columns_titulos]).orderBy(col("DATA_MAIS_RECENTE").desc())
df_ranked_titulos = df_with_latest_date.withColumn("row_num", row_number().over(windowSpec_titulos))
df_deduplicated_titulos = df_ranked_titulos.filter(col("row_num") == 1).drop("row_num", "DATA_MAIS_RECENTE")

# Célula 1.4: Salvar e Armazenar em Cache
# ------------------------------------------------------
output_path_titulos = f"{target_lakehouse}.{target_table_titulos}"
df_deduplicated_titulos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_titulos)

# Armazena o resultado em cache para uso futuro nas seções 4, 5 e 7
spark.table(output_path_titulos).cache()
print(f"Tabela limpa salva e em cache: {output_path_titulos}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Limpeza da Tabela cad_clientes
# **Objetivo:** Desduplicar a tabela `cad_clientes`, que serve como base para a `dim_cliente`. O resultado é colocado em cache para ser usado no processamento incremental de pareceres (Seção 8).

# CELL ********************

# Célula 2.1: Parâmetros e Leitura
# ------------------------------------------------
source_table_clientes = "cad_clientes"
target_table_clientes = "staging_clientes_limpa"

print(f"\nIniciando a limpeza da tabela: {source_lakehouse}.{source_table_clientes}")
df_bronze_clientes = spark.read.table(f"{source_lakehouse}.{source_table_clientes}")

# Célula 2.2: Lógica de Desduplicação
# ----------------------------------------------------
key_columns_clientes = ["CODCLIENTE"]
order_by_column_clientes = "DATAALTERACAO"
windowSpec_clientes = Window.partitionBy([col(c) for c in key_columns_clientes]).orderBy(col(order_by_column_clientes).desc())
df_ranked_clientes = df_bronze_clientes.withColumn("row_num", row_number().over(windowSpec_clientes))
df_deduplicated_clientes = df_ranked_clientes.filter(col("row_num") == 1).drop("row_num")

# Célula 2.3: Salvar e Armazenar em Cache
# ------------------------------------------------------
output_path_clientes = f"{target_lakehouse}.{target_table_clientes}"
df_deduplicated_clientes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_clientes)

# Armazena o resultado em cache para uso futuro na Seção 8
spark.table(output_path_clientes).cache()
print(f"Tabela limpa salva e em cache: {output_path_clientes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: Limpeza e Enriquecimento da Tabela cad_geral_pf_pj
# **Objetivo:** Esta seção limpa a tabela de cadastro geral, desduplicando os registros, e a enriquece com informações de contato (endereço, email, telefone) em um único processo otimizado.

# CELL ********************

# Célula 3.1: Processamento de Telefones
# --------------------------------------------------------------------------------
print("\nIniciando o tratamento de telefones em memória...")
df_telefones_bronze = spark.read.table("LH_Bronze.cad_telefones")
df_telefones_agg = df_telefones_bronze \
    .filter((col("FONE").isNotNull() & (col("FONE") != "")) & (col("DDD").isNotNull() & (col("DDD") != ""))) \
    .withColumn("FONE_limpo", regexp_replace(col("FONE"), "-", "")) \
    .withColumn("FONE_COMPLETO", regexp_replace(concat(col("DDD"), col("FONE_limpo")), " ", "")) \
    .filter((length(col("FONE_COMPLETO")) >= 10) & (length(col("FONE_COMPLETO")) <= 11)) \
    .select(col("CPFCNPJ"), col("FONE_COMPLETO").alias("FONE"), col("CONTATO")).distinct() \
    .groupBy("CPFCNPJ").agg(concat_ws("; ", collect_list("FONE")).alias("Telefones"))
print("Telefones agregados em memória.")

# Célula 3.2: Processamento de Emails
# --------------------------------------------------------------------------------
print("\nIniciando o tratamento de emails em memória...")
df_emails_bronze = spark.read.table("LH_Bronze.cad_email")
df_emails_agg = df_emails_bronze \
    .filter(col("EMAIL").isNotNull() & (col("EMAIL") != "")) \
    .select("CPFCNPJ", "EMAIL").distinct() \
    .groupBy("CPFCNPJ").agg(concat_ws("; ", collect_list("EMAIL")).alias("Emails"))
print("Emails agregados em memória.")

# Célula 3.3: Processamento de Endereços
# --------------------------------------------------------------------------------
print("\nIniciando o tratamento de endereços em memória...")
schema_regioes = StructType([
    StructField("Sigla", StringType(), True), StructField("Estado", StringType(), True),
    StructField("Capital", StringType(), True), StructField("Regiao", StringType(), True)
])
data_regioes = [("AC", "Acre", "Rio Branco", "Norte"),("AL", "Alagoas", "Maceió", "Nordeste"),("AP", "Amapá", "Macapá", "Norte"),("AM", "Amazonas", "Manaus", "Norte"),("BA", "Bahia", "Salvador", "Nordeste"),("CE", "Ceará", "Fortaleza", "Nordeste"),("DF", "Distrito Federal", "Brasília", "Centro-Oeste"),("ES", "Espírito Santo", "Vitória", "Sudeste"),("GO", "Goiás", "Goiânia", "Centro-Oeste"),("MA", "Maranhão", "São Luís", "Nordeste"),("MT", "Mato Grosso", "Cuiabá", "Centro-Oeste"),("MS", "Mato Grosso do Sul", "Campo Grande", "Centro-Oeste"),("MG", "Minas Gerais", "Belo Horizonte", "Sudeste"),("PA", "Pará", "Belém", "Norte"),("PB", "Paraíba", "João Pessoa", "Nordeste"),("PR", "Paraná", "Curitiba", "Sul"),("PE", "Pernambuco", "Recife", "Nordeste"),("PI", "Piauí", "Teresina", "Nordeste"),("RJ", "Rio de Janeiro", "Rio de Janeiro", "Sudeste"),("RN", "Rio Grande do Norte", "Natal", "Nordeste"),("RS", "Rio Grande do Sul", "Porto Alegre", "Sul"),("RO", "Rondônia", "Porto Velho", "Norte"),("RR", "Roraima", "Boa Vista", "Norte"),("SC", "Santa Catarina", "Florianópolis", "Sul"),("SP", "São Paulo", "São Paulo", "Sudeste"),("SE", "Sergipe", "Aracaju", "Nordeste"),("TO", "Tocantins", "Palmas", "Norte")]
df_regioes = spark.createDataFrame(data=data_regioes, schema=schema_regioes).cache()

df_enderecos_bronze = spark.read.table("LH_Bronze.cad_enderecos")
colunas_para_remover = ["PAIS", "FONE", "FAX", "TIPO", "DATAINCLUSAO", "USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODMUNICIPIO", "CODENDERECO"]
df_enderecos_filtrado = df_enderecos_bronze.drop(*colunas_para_remover) \
    .filter(~col("ENDERECO").isin(["END PENDENTE", "Rua Teste", "S/ EDEREÇO", "S/ ENDERECO", "S/ ENDEREÇO", "test2", "teste"]) & col("CIDADE").isNotNull() & (col("CIDADE") != "") & (col("CIDADE") != "0"))
cols_to_upper = ["ENDERECO", "COMPLEMENTO", "BAIRRO", "CIDADE", "UF"]
df_upper = reduce(lambda df, c: df.withColumn(c, upper(col(c))), cols_to_upper, df_enderecos_filtrado)
replacements_uf = {"RP": "PR", "SÃ": "SP", " C": "CE", "G": "GO", "11": "SP", "DP": "SP", "SA": "SP", "S": "SP", "MF": "MG", "ED": "ES", "31": None, "0": None, "A": None, "1": None, "3": None, "..": None, "7": None, "-": None, "+": None, ".": None, "SS": None, "9": None, "2": None, "GU": None, "": None, "68": None}
df_enderecos_final = df_upper \
    .filter(col("CEP").isNotNull() & ~col("CEP").isin(["00      ", "0000000", "00000000", "0"])).distinct() \
    .replace(replacements_uf, subset=['UF']).filter(col("UF").isNotNull()) \
    .dropDuplicates(["CPFCNPJ"]) \
    .filter(col("CPFCNPJ") != "00000000000000") \
    .withColumn("CIDADE_limpa", regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(col("CIDADE"), "[ÃÂÁ]", "A"), "[É]", "E"), "[Í]", "I"), "[Ó]", "O"), "[Ú]", "U"), "-", " ")) \
    .drop("CIDADE").withColumnRenamed("CIDADE_limpa", "CIDADE") \
    .join(df_regioes, col("UF") == df_regioes.Sigla, "left") \
    .withColumn("CPFCNPJ_valido", col("CPFCNPJ").cast("long")).filter(col("CPFCNPJ_valido").isNotNull()) \
    .select("CPFCNPJ", "ENDERECO", "NUMERO", "COMPLEMENTO", "BAIRRO", "CIDADE", "UF", "CEP", "Estado", "Capital", "Regiao")
df_regioes.unpersist()
print("Endereços processados em memória.")

# Célula 3.4: Limpeza e Enriquecimento do Cadastro Geral
# --------------------------------------------------------------------------------
print("\nIniciando a limpeza e enriquecimento da tabela de cadastro geral.")
df_geral_bronze = spark.read.table("LH_Bronze.cad_geral_pf_pj")
key_cols_geral = ["CPFCNPJ"]
order_by_col_geral = "DATAALTERACAO"
window_geral = Window.partitionBy([col(c) for c in key_cols_geral]).orderBy(col(order_by_col_geral).desc())
df_geral_deduplicated = df_geral_bronze.withColumn("row_num", row_number().over(window_geral)) \
                                     .filter(col("row_num") == 1) \
                                     .drop("row_num")
df_enriquecido = df_geral_deduplicated \
    .join(df_enderecos_final.select("CPFCNPJ", "CIDADE", "UF", "CEP"), on="CPFCNPJ", how="left") \
    .join(df_emails_agg, on="CPFCNPJ", how="left") \
    .join(df_telefones_agg, on="CPFCNPJ", how="left")
output_path_geral = "LH_Silver.staging_cad_geral_limpa"
df_enriquecido.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_geral)
print(f"Tabela de cadastro geral limpa e enriquecida salva com sucesso em: {output_path_geral}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 4: Processamento de Status de Protesto
# **Objetivo:** Calcular o status de protesto mais recente e preciso para cada título, com base em uma lógica complexa de ocorrências de cobrança. Os status possíveis são 'Protestado', 'Em Cartório', 'Instrução Protesto', e 'Instrução Protesto Enviada'. A tabela resultante, `staging_protestos`, é utilizada para enriquecer a `dim_titulo`.

# CELL ********************

print("\nIniciando o processamento de status de protesto de títulos...")

# 1. Leitura das tabelas de origem
df_ocorrencias_bronze = spark.read.table("LH_Bronze.rlc_titulos_ocorrencias_cobranca")
df_titulos_cobranca_bronze = spark.read.table("LH_Bronze.tab_titulos_cobranca")
print("Tabelas de ocorrências e cobrança lidas da camada Bronze.")

# 2. Pré-cálculo para a subquery de tab_titulos_cobranca
# SQL: WHEN A.CODTITULO IN (SELECT CODTITULO FROM tab_titulos_cobranca WHERE ... AND CODOCORCOBRANCA = 1015)
df_titulos_para_protesto_cobranca = df_titulos_cobranca_bronze \
    .filter(col("CODOCORCOBRANCA") == 1015) \
    .select("CODTITULO") \
    .distinct() \
    .withColumn("flag_protesto_cobranca", lit(True))

# 3. Pré-cálculo para a subquery correlacionada em rlc_titulos_ocorrencias_cobranca
# SQL: A.CODTITULO IN (SELECT CODTITULO FROM rlc_titulos_ocorrencias_cobranca WHERE ... AND A.CODOCORINTERNA = 2)
df_subquery_ocorrencia = df_ocorrencias_bronze \
    .filter(col("CODOCORINTERNA").isin(8, 34) & col("CODOCORCOBRBANCO").isin(19, 23)) \
    .select("CODTITULO") \
    .distinct() \
    .withColumn("flag_subquery_ocorrencia", lit(True))

# 4. Filtragem principal da tabela de ocorrências
# SQL: WHERE ((A.CODOCORINTERNA IN (...) AND ... ) OR (A.CODOCORINTERNA IN (...) AND ...))
df_ocorrencias_filtradas = df_ocorrencias_bronze.filter(
    (
        (col("CODOCORINTERNA").isin(8, 17, 34, 2, 82)) &
        (col("CODOCORCOBRBANCO").isin(6, 19, 23, 10, 43)) &
        (col("TOCORRENCIA") == 2)
    ) |
    (
        (col("CODOCORINTERNA") == 8) &
        (col("CODOCORCOBRBANCO") == 9) &
        (col("TOCORRENCIA") == 1)
    )
)
print("Filtro inicial de ocorrências de cobrança aplicado.")

# 5. Isolar a ocorrência mais recente e juntar as flags
# SQL: ORDER BY CODTITULOOCORCOB DESC LIMIT 0,1
window_spec_latest = Window.partitionBy("CODTITULO").orderBy(col("CODTITULOOCORCOB").desc())

df_latest_ocorrencia = df_ocorrencias_filtradas \
    .withColumn("row_num", row_number().over(window_spec_latest)) \
    .filter(col("row_num") == 1) \
    .drop("row_num") \
    .join(df_titulos_para_protesto_cobranca, "CODTITULO", "left") \
    .join(df_subquery_ocorrencia, "CODTITULO", "left") \
    .fillna(False, subset=["flag_protesto_cobranca", "flag_subquery_ocorrencia"])

print("Ocorrência mais recente por título isolada e flags de condição adicionadas.")
df_latest_ocorrencia.cache()

# 6. Aplicar a lógica CASE para determinar o código de status (WPROTESTADO)
cond_p1 = (substring(col("MOTIVOCODOCORCOBRBANCO"), 1, 2) == '14')
cond_p2 = (col("CODOCORINTERNA") == 2) & (col("flag_subquery_ocorrencia") == True)
cond_p3 = (col("CODOCORINTERNA") == 82)
cond_p4 = (col("flag_protesto_cobranca") == True)
cond_e = (col("CODOCORINTERNA") == 8) & (col("CODOCORCOBRBANCO") == 9)
cond_i = (col("CODOCORINTERNA") == 8)
cond_c = (col("CODOCORINTERNA") == 34)

df_com_status_code = df_latest_ocorrencia.withColumn("STATUSPROTESTO",
    when(cond_p1 | cond_p2 | cond_p3 | cond_p4, lit("P"))
    .when(cond_e, lit("E"))
    .when(cond_i, lit("I"))
    .when(cond_c, lit("C"))
    .otherwise(lit("N"))
)
print("Código de status de protesto (P, E, I, C, N) calculado.")

# 7. Mapear o código de status para a descrição final e filtrar status não relevantes
df_com_status_desc = df_com_status_code.withColumn("STATUS_PROTESTO",
    when(col("STATUSPROTESTO") == 'P', lit("Protestado"))
    .when(col("STATUSPROTESTO") == 'E', lit("Instrução Protesto Enviada"))
    .when(col("STATUSPROTESTO") == 'I', lit("Instrução Protesto"))
    .when(col("STATUSPROTESTO") == 'C', lit("Em Cartório"))
    .otherwise(lit("N/A"))
).filter(col("STATUS_PROTESTO") != "N/A")
print("Descrição final do status de protesto mapeada.")

# 8. Selecionar colunas finais e salvar o resultado
df_final_protestos = df_com_status_desc.select(
    "CODTITULO",
    "STATUS_PROTESTO",
    col("DATAINCLUSAO").alias("DATA_OCORRENCIA_PROTESTO")
)

output_path_protestos = "LH_Silver.staging_protestos"
df_final_protestos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_protestos)
print(f"Tabela de staging para protestos salva com sucesso em: {output_path_protestos}")

df_latest_ocorrencia.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 5: Limpeza da Tabela tab_operacoes
# **Objetivo:** Limpar, desduplicar e enriquecer a tabela `tab_operacoes`, que contém informações sobre as operações de crédito.

# CELL ********************

# Célula 5.1: Parâmetros e Leitura
# ------------------------------------------------
source_table_operacoes = "tab_operacoes"
target_table_operacoes = "staging_operacoes_limpa"
print(f"\nIniciando a limpeza da tabela: {source_lakehouse}.{source_table_operacoes}")
df_bronze_operacoes = spark.read.table(f"{source_lakehouse}.{source_table_operacoes}")

# Célula 5.2: Lógica de Correção e Desduplicação
# ----------------------------------------------------
df_corrigido = df_bronze_operacoes.withColumn("TTO_corrigido", when(col("CODOPERACAO") == 3042074, lit("CS")).otherwise(col("TTO"))).drop("TTO").withColumnRenamed("TTO_corrigido", "TTO")
key_columns_operacoes = ["CODOPERACAO"]
order_by_column_operacoes = "DATAALTERACAO"
windowSpec_operacoes = Window.partitionBy([col(c) for c in key_columns_operacoes]).orderBy(col(order_by_column_operacoes).desc())
df_ranked_operacoes = df_corrigido.withColumn("row_num", row_number().over(windowSpec_operacoes))
df_deduplicated_operacoes = df_ranked_operacoes.filter(col("row_num") == 1).drop("row_num")
print("Desduplicação concluída.")

# Célula 5.3: Enriquecimento com o Gerente Correto (Broker)
# ---------------------------------------------------------
print("Iniciando o enriquecimento com o gerente histórico correto...")
# Carregar a tabela bridge criada pelo notebook NB_Build_Bridge_Cliente_Gerente
df_bridge_gerente = spark.read.table("LH_Silver.bridge_cliente_gerente")

# Juntar as operações com a bridge de histórico do gerente
# O join é feito por ClienteID e a DATAANALISE da operação deve estar dentro do período de vigência da bridge
df_operacoes_com_historico = df_deduplicated_operacoes.join(
    df_bridge_gerente,
    (df_deduplicated_operacoes["CODCLIENTE"] == df_bridge_gerente["ClienteID"]) &
    (df_deduplicated_operacoes["DATAANALISE"].cast("date") >= df_bridge_gerente["DataInicioVigencia"]) &
    (df_deduplicated_operacoes["DATAANALISE"].cast("date") <= df_bridge_gerente["DataFimVigencia"]),
    "left"
)

# Criar a coluna final 'CODBROKER', priorizando o valor que já existe na operação.
# Se for nulo ou 0, usa o GerenteID que veio da bridge.
df_operacoes_com_gerente_final = df_operacoes_com_historico.withColumn(
    "CODBROKER",
    when(
        (col("CODBROKER").isNotNull()) & (col("CODBROKER") != 0),
        col("CODBROKER")
    ).otherwise(col("GerenteID"))
).drop("ClienteID", "GerenteID", "DataInicioVigencia", "DataFimVigencia") # Limpa colunas da bridge
print("Enriquecimento com o gerente histórico concluído.")


# Célula 5.4: Enriquecimento com a coluna `operacao_informal`
# -----------------------------------------------------------
print("Iniciando a lógica para adicionar a coluna 'operacao_informal'.")
df_titulos_limpa_cached = spark.table("LH_Silver.staging_titulos_limpa") # Usando a tabela em cache
df_cad_geral_arquivos = spark.read.table("LH_Bronze.cad_geral_arquivos")
df_chave_danfe = df_cad_geral_arquivos.filter(col("DESCRICAO") == 'CHAVEDANFE')
df_titulos_com_chave = df_titulos_limpa_cached.join(df_chave_danfe, on="CODTITULO", how="inner")
df_operacoes_com_chave_base = df_operacoes_com_gerente_final.join(df_titulos_com_chave, on="CODOPERACAO", how="inner")
df_operacoes_com_chave_filtrado = df_operacoes_com_chave_base.filter(
    (df_operacoes_com_gerente_final["NOTASERVICO"] == 'N') &
    (df_operacoes_com_gerente_final["STATUSANALISE"] == 'D') &
    (df_operacoes_com_gerente_final["CODEMPRESA"] == 14) &
    (df_operacoes_com_gerente_final["STATUSACEITE"] == 'A') &
    (df_operacoes_com_gerente_final["TTO"].isin(['NO','CM','FC']))
)
df_vcount = df_operacoes_com_chave_filtrado.groupBy(df_operacoes_com_gerente_final["CODOPERACAO"]).count()
df_com_vcount = df_operacoes_com_gerente_final.join(df_vcount, on="CODOPERACAO", how="left")
df_final_com_informal = df_com_vcount.withColumn(
    "operacao_informal",
    when(
        ((col("count").isNull()) | (col("count") == 0)) &
        (col("CODEMPRESA") == 14) &
        (col("NOTASERVICO") == 'N'),
        lit(True)
    ).otherwise(lit(False))
).drop("count")
print("Coluna 'operacao_informal' adicionada com sucesso.")

# Célula 5.4: Salvar o Resultado Limpo e Enriquecido
# ------------------------------------------------------
output_path_operacoes = f"{target_lakehouse}.{target_table_operacoes}"
df_final_com_informal.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_operacoes)
print(f"Tabela limpa e enriquecida salva com sucesso em: {output_path_operacoes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 6: Processamento e Detalhamento da Chave da DANFE
# **Objetivo:** Extrair e decodificar as informações contidas na `CHAVEDANFE` dos títulos, como UF, CNPJ, e Número da Nota Fiscal.

# CELL ********************

# Célula 6.1: Parâmetros e Leitura da Tabela em Cache
# ------------------------------------------------
danfe_source_table = "staging_titulos_limpa"
danfe_target_table = "staging_chave_danfe_detalhada"
print(f"\nIniciando o processamento da CHAVEDANFE da tabela: {target_lakehouse}.{danfe_source_table}")
df_titulos_danfe = spark.table(f"{target_lakehouse}.{danfe_source_table}") # Usando a tabela em cache

# Célula 6.2: Lógica de Transformação da CHAVEDANFE
# ------------------------------------------------
df_chave_filtrada = df_titulos_danfe \
    .select("CHAVEDANFE") \
    .na.drop(subset=["CHAVEDANFE"]) \
    .filter((col("CHAVEDANFE") != "") & (length(col("CHAVEDANFE")) == 44)) \
    .filter(~col("CHAVEDANFE").contains("XML NF-E 495 MOMENTUM OP. 149717.XML de NF-E")) \
    .withColumn("CHAVEDANFE_limpa", regexp_replace(col("CHAVEDANFE"), " ", "0")) \
    .select("CHAVEDANFE_limpa") \
    .withColumnRenamed("CHAVEDANFE_limpa", "CHAVEDANFE") \
    .distinct()
print("Limpeza e filtragem da CHAVEDANFE concluídas.")

df_detalhada = df_chave_filtrada \
    .withColumn("UF", substring(col("CHAVEDANFE"), 1, 2)) \
    .withColumn("AAMM", substring(col("CHAVEDANFE"), 3, 4)) \
    .withColumn("CNPJ", substring(col("CHAVEDANFE"), 7, 14)) \
    .withColumn("Modelo", substring(col("CHAVEDANFE"), 21, 2)) \
    .withColumn("Serie", substring(col("CHAVEDANFE"), 23, 3)) \
    .withColumn("NumeroNF", substring(col("CHAVEDANFE"), 26, 9)) \
    .withColumn("CodigoNF", substring(col("CHAVEDANFE"), 35, 9)) \
    .withColumn("DV", substring(col("CHAVEDANFE"), 44, 1))
print("Extração dos campos da CHAVEDANFE concluída.")

# Célula 6.3: Salvar o Resultado
# ------------------------------------------------------
output_path_danfe = f"{target_lakehouse}.{danfe_target_table}"
df_detalhada.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_danfe)
print(f"Tabela detalhada da CHAVEDANFE salva com sucesso em: {output_path_danfe}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 7: Limpeza e Enriquecimento da `tab_titulos_baixas`
# **Objetivo:** Processar as "baixas" de títulos, corrigir dados inconsistentes e enriquecer as informações com tabelas de dimensão, resultando na tabela de fatos `fato_baixas`.

# CELL ********************

# Célula 7.1: Carregar Dimensões e Limpar Baixas
# ------------------------------------------------
print("\nIniciando o processamento da tab_titulos_baixas")
df_dim_pago_por = spark.read.table("LH_Silver.sup_pago_pelo")
df_dim_forma_pagamento = spark.read.table("LH_Silver.sup_forma_de_pagamento")
df_dim_tipo_taxa = spark.read.table("LH_Silver.sup_tipo_de_baixa")
df_dim_motivo_baixa = spark.read.table("LH_Silver.sup_motivo_baixa")
print("Dimensões de decodificação carregadas.")

df_baixas = spark.read.table("LH_Bronze.tab_titulos_baixas")
key_cols_baixa = ["CODTITULOBAIXAS"]
order_by_col_baixa = "DATAINCLUSAO"
window_baixa = Window.partitionBy([col(c) for c in key_cols_baixa]).orderBy(col(order_by_col_baixa).desc())
df_baixas_desduplicada = df_baixas.withColumn("row_num", row_number().over(window_baixa)) \
                                    .filter(col("row_num") == 1).drop("row_num")
output_path_baixas_staging = "LH_Silver.staging_baixas_limpa"
df_baixas_desduplicada.write.mode("overwrite").option("overwriteSchema","true").saveAsTable(output_path_baixas_staging)
print(f"Tabela de baixas limpa e salva em: {output_path_baixas_staging}")

# Célula 7.2: Construção da fato_baixas
# ------------------------------------------------
print("\nIniciando a construção da fato_baixas...")
df_baixas_staging = spark.read.table(output_path_baixas_staging)
df_titulos_staging_cached = spark.read.table("LH_Silver.staging_titulos_limpa") # Usando tabela em cache
df_baixas_corrigido = df_baixas_staging.withColumn("JUROS",
    when(col("JUROS") == -858005.8, 3912.5)
    .when(col("JUROS") == -4948525.71, -56747.24)
    .when(col("JUROS") == -4140.75, 0)
    .when(col("JUROS") == -1447.5, 52.5)
    .when(col("JUROS") == -1825.72, 66.28)
    .when(col("JUROS") == -965, 35)
    .when(col("JUROS") == -26000, 0)
    .otherwise(col("JUROS")))
df_enriquecido_baixas = df_baixas_corrigido \
    .join(df_titulos_staging_cached, on="CODTITULO", how="left") \
    .join(df_dim_pago_por, df_baixas_corrigido.PAGOPELO == df_dim_pago_por.id, how="left") \
    .join(df_dim_forma_pagamento, df_baixas_corrigido.FORMA == df_dim_forma_pagamento.id, how="left") \
    .join(df_dim_tipo_taxa, df_baixas_corrigido.TIPOBAIXA == df_dim_tipo_taxa.id, how="left") \
    .join(df_dim_motivo_baixa, df_baixas_corrigido.MOTIVO == df_dim_motivo_baixa.id, how="left")
df_fato_baixas = df_enriquecido_baixas.select(
    df_baixas_corrigido["CODTITULOBAIXAS"], df_baixas_corrigido["CODTITULO"],
    df_baixas_corrigido["DATABAIXA"], df_baixas_corrigido["DATABAIXASIST"],
    df_baixas_corrigido["VLPAGO"], df_baixas_corrigido["DESCONTO"],
    df_baixas_corrigido["JUROS"], df_baixas_corrigido["TARIFARECOMPRA"],
    df_baixas_corrigido["DATAVENCIMENTO"], df_baixas_corrigido["CODOPERACAO"],
    df_dim_pago_por["descricao"].alias("PagoPor"),df_dim_forma_pagamento["descricao"].alias("Forma"),
    df_dim_tipo_taxa["descricao"].alias("TipoBaixa"), df_dim_motivo_baixa["descricao"].alias("Motivo"))
output_path_fato_baixas = "LH_Silver.fato_baixas"
df_fato_baixas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_fato_baixas)
print(f"Tabela 'fato_baixas' construída e salva com sucesso em: {output_path_fato_baixas}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  ## Seção 8: Processamento Incremental de Pareceres
# 
#  **Objetivo:** Processar a tabela `cad_geral_pareceres` de forma incremental para evitar timeouts e problemas de performance. A tabela `esteira_de_propostas` é reconstruída a cada execução a partir dos dados atualizados.


# CELL ********************

# Célula 8.1: Imports e Configurações
# ----------------------------------------------------------------
print("\nIniciando o processamento incremental de pareceres.")
source_table_name_pareceres = "LH_Bronze.cad_geral_pareceres"
target_pareceres_status_table_name = "LH_Silver.pareceres_de_alteracao_de_status"
target_esteira_table_name = "LH_Silver.esteira_de_propostas"
watermark_table_name = "LH_Silver.etl_watermark_control"
notebook_name = "NB_Prepare_Silver_Staging_Pareceres"

# Célula 8.2: Leitura do Watermark
# ---------------------------------
print(f"Lendo o watermark da tabela: {watermark_table_name}")
try:
    df_watermark = spark.read.table(watermark_table_name)
    last_watermark_str = df_watermark.filter(col("TableName") == notebook_name).select("LastWatermarkValue").collect()[0][0]
    # Tenta fazer o parse com microsegundos, se falhar, tenta sem.
    # Isso torna a leitura robusta a formatos de data salvos anteriormente.
    try:
        last_watermark = datetime.datetime.strptime(last_watermark_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        last_watermark = datetime.datetime.strptime(last_watermark_str, "%Y-%m-%d %H:%M:%S")
    print(f"Watermark encontrado: {last_watermark}")
except Exception as e:
    last_watermark = datetime.datetime(1900, 1, 1)
    print(f"Nenhum watermark encontrado ou erro na leitura. Usando valor padrão: {last_watermark}. Erro: {e}")

# Célula 8.3: Leitura e Padronização Incremental dos Dados
# -------------------------------------------------------
print(f"Lendo dados incrementais de {source_table_name_pareceres} a partir de {last_watermark}")
df_pareceres_raw = spark.read.table(source_table_name_pareceres)
df_clientes_cached = spark.read.table("LH_Silver.staging_clientes_limpa") # Usando tabela em cache
df_usuarios_raw = spark.read.table("LH_Bronze.cad_usuarios")

# A tabela de status agora é pré-processada e garantida pelo notebook NB_Load_Bronze_From_Manual_Uploads_Status_Clientes
print("Lendo a tabela de status de clientes pré-processada da camada Silver...")
df_status_clientes_esteira = spark.read.table("LH_Silver.sup_status_de_clientes_da_esteira")
print("Leitura da tabela de status concluída.")

df_pareceres_incremental = df_pareceres_raw.filter(
    (col("DATAINCLUSAO") > last_watermark) | (col("DATAALTERACAO") > last_watermark)
).cache() # Cache para evitar recomputação com o .count()

record_count = df_pareceres_incremental.count()

if record_count > 0:
        # CORREÇÃO: Usa a função `greatest` para considerar a maior data entre
        # `DATAINCLUSAO` e `DATAALTERACAO` para o cálculo do novo watermark.
        # Isso corrige o bug que impedia o avanço do watermark quando apenas
        # registros antigos eram atualizados.
        new_watermark_df = df_pareceres_incremental.withColumn(
            "latest_date",
            greatest(
                coalesce(col("DATAINCLUSAO"), lit(datetime.datetime(1900, 1, 1))),
                coalesce(col("DATAALTERACAO"), lit(datetime.datetime(1900, 1, 1)))
            )
        ).agg(max("latest_date").alias("NewWatermark"))

        new_watermark = new_watermark_df.collect()[0]["NewWatermark"]
        print(f"Leitura incremental concluída. {record_count} registros a serem processados.")
        print(f"Novo watermark a ser gravado: {new_watermark}")
else:
    new_watermark = last_watermark
    print("Nenhum dado novo encontrado. O processo continuará, pois as etapas subsequentes são projetadas para lidar com um dataframe vazio.")

# Célula 8.4: Transformação da Lógica de Pareceres (Aplicada ao Delta)
# --------------------------------------------------------------------
print("Aplicando a lógica de transformação ao delta de pareceres...")
df_replica_pareceres_delta = df_pareceres_incremental \
    .filter(year(col("DATAINCLUSAO")) >= 2024) \
    .drop("ENCAMINHAR", "ALERTA", "CODPASTA", "CODTAREFA", "USUAALTERACAO", "DATAALTERACAO") \
    .withColumn("OBS", col("OBS").substr(1, 255)) \
    .withColumn("codTipoParecer", col("CODTIPOPARECER").cast(LongType())) \
    .filter(col("codTipoParecer") == 1) \
    .filter((col("CPFCNPJ").isNotNull() & (col("CPFCNPJ") != "")) & (col("OBS").isNotNull() & (col("OBS") != "")) & (col("USUAINCLUSAO").isNotNull()) & (col("DATAINCLUSAO").isNotNull())) \
    .filter(col("OBS").startswith("STATUS ALTERADO PARA ")) \
    .withColumn("STATUS_DO_CLIENTE", substring(col("OBS"), 22, 100)) \
    .withColumn("BASE", lit(40).cast(LongType())) \
    .select("CODPARECER", "CPFCNPJ", "CODOPERACAO", "DATAINCLUSAO", "USUAINCLUSAO", "STATUS_DO_CLIENTE", "BASE")

window_cliente_data_delta = Window.partitionBy("CODCLIENTE").orderBy(col("DATAINCLUSAO").asc())
df_pareceres_enriquecidos_delta = df_replica_pareceres_delta \
    .join(df_clientes_cached.select("CPFCNPJ", "CODCLIENTE"), ["CPFCNPJ"], "left") \
    .withColumn("chave_base_cliente", concat(col("BASE"), lit("-"), col("CODCLIENTE"))) \
    .join(df_usuarios_raw.select("CODUSUARIO", "NOME"), col("USUAINCLUSAO") == col("CODUSUARIO"), "left") \
    .withColumnRenamed("NOME", "USUARIO") \
    .join(df_status_clientes_esteira, "STATUS_DO_CLIENTE", "left") \
    .filter(col("CODCLIENTE").isNotNull() & (col("CODCLIENTE") != "")) \
    .withColumn("INDICE", row_number().over(window_cliente_data_delta)) \
    .withColumn("chave_original", (col("INDICE") * 1000000000 + col("CODCLIENTE")).cast(LongType())) \
    .withColumnRenamed("DATAINCLUSAO", "DATALOG") \
    .select("CODPARECER", "CODCLIENTE", "STATUS_DO_CLIENTE", "DATALOG", "BASE", "USUARIO", "chave_base_cliente", "INDICE", "chave_original", "MACROPROCESSO", "FASE")
print("Transformação do delta concluída.")
df_pareceres_incremental.unpersist() # Libera o cache do delta

# Célula 8.5: MERGE dos Dados na Tabela Silver
# --------------------------------------------
print(f"Iniciando o MERGE para a tabela {target_pareceres_status_table_name}...")
if spark.catalog.tableExists(target_pareceres_status_table_name):
    delta_table = DeltaTable.forName(spark, target_pareceres_status_table_name)
    delta_table.alias("t").merge(df_pareceres_enriquecidos_delta.alias("s"), "t.CODPARECER = s.CODPARECER").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("MERGE concluído com sucesso.")
else:
    df_pareceres_enriquecidos_delta.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_pareceres_status_table_name)
    print(f"Tabela {target_pareceres_status_table_name} criada pela primeira vez.")

# Célula 8.6: Reconstrução Completa da `esteira_de_propostas`
# ---------------------------------------------------------
print("Iniciando a reconstrução completa da tabela `esteira_de_propostas`...")
df_pareceres_completa = spark.read.table(target_pareceres_status_table_name)

# CORREÇÃO: A lógica foi alterada de `lead` para `lag`.
# `lead` olhava para o evento *futuro*, mas a coluna era nomeada `_ANTERIOR`,
# causando uma grande confusão de lógica. `lag` olha para o evento
# *passado*, que é o comportamento esperado.
window_lag = Window.partitionBy("CODCLIENTE").orderBy("DATALOG")
df_com_lag = df_pareceres_completa \
    .withColumn("STATUS_DO_CLIENTE_ANTERIOR", lag("STATUS_DO_CLIENTE").over(window_lag)) \
    .withColumn("DATALOG_ANTERIOR", lag("DATALOG").over(window_lag)) \
    .withColumn("MACROPROCESSO_ANTERIOR", lag("MACROPROCESSO").over(window_lag)) \
    .withColumn("FASE_ANTERIOR", lag("FASE").over(window_lag))

df_transicoes = df_com_lag.filter(col("STATUS_DO_CLIENTE") != col("STATUS_DO_CLIENTE_ANTERIOR")).na.drop(subset=["STATUS_DO_CLIENTE_ANTERIOR"])

# CORREÇÃO: A lógica das flags foi ajustada para a comparação correta
# entre o estado ANTERIOR e o ATUAL.
# DEVOLUÇÃO: O macroprocesso ANTERIOR era 'CREDITO' e o ATUAL é 'COMERCIAL'.
# RECEBIDA: O macroprocesso ANTERIOR era 'COMERCIAL' e o ATUAL é 'CREDITO'.
df_esteira_final = df_transicoes \
    .withColumn("DEVOLUCAO", when((col("MACROPROCESSO_ANTERIOR") == "CREDITO") & (col("MACROPROCESSO") == "COMERCIAL"), True).otherwise(False)) \
    .withColumn("RECEBIDA", when((col("MACROPROCESSO_ANTERIOR") == "COMERCIAL") & (col("MACROPROCESSO") == "CREDITO"), True).otherwise(False)) \
    .select(
        "INDICE",
        "CODCLIENTE",
        "BASE",
        "DATALOG_ANTERIOR",
        "DATALOG",
        "chave_base_cliente",
        "STATUS_DO_CLIENTE_ANTERIOR",
        "STATUS_DO_CLIENTE",
        "MACROPROCESSO_ANTERIOR",
        "MACROPROCESSO",
        "FASE_ANTERIOR",
        "FASE",
        "USUARIO",
        "DEVOLUCAO",
        "RECEBIDA"
    )
df_esteira_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_esteira_table_name)
print(f"Tabela final '{target_esteira_table_name}' reconstruída e salva com sucesso.")

# Célula 8.7: Atualização do Watermark
# -----------------------------------
print(f"Atualizando o watermark para {new_watermark}...")
# Garante que o formato de data sempre incluirá os microsegundos
new_watermark_str = new_watermark.strftime("%Y-%m-%d %H:%M:%S.%f")
new_watermark_data = [(notebook_name, new_watermark_str)]
df_new_watermark = spark.createDataFrame(new_watermark_data, ["TableName", "LastWatermarkValue"])
if spark.catalog.tableExists(watermark_table_name):
    delta_watermark_table = DeltaTable.forName(spark, watermark_table_name)
    delta_watermark_table.alias("t").merge(df_new_watermark.alias("s"), "t.TableName = s.TableName") \
        .whenMatchedUpdate(set={"LastWatermarkValue": "s.LastWatermarkValue"}) \
        .whenNotMatchedInsert(values={"TableName": "s.TableName", "LastWatermarkValue": "s.LastWatermarkValue"}) \
        .execute()
else:
    df_new_watermark.write.mode("overwrite").saveAsTable(watermark_table_name)
print("Watermark atualizado com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## Seção 9: Processamento de Contratos de Clientes
# **Objetivo:** Limpar e transformar os dados da tabela `cad_contratos_clientes` para criar uma tabela de staging que será usada para enriquecer a `dim_cliente`.

# CELL ********************

# Célula 9.1: Parâmetros e Leitura
# ------------------------------------------------
source_table_contratos = "cad_contratos_clientes"
target_table_contratos = "staging_contratos_clientes_limpa"
print(f"\nIniciando o processamento da tabela: {source_lakehouse}.{source_table_contratos}")
df_bronze_contratos = spark.read.table(f"{source_lakehouse}.{source_table_contratos}")

# Célula 9.2: Lógica de Transformação
# ----------------------------------------------------
df_transformed_contratos = df_bronze_contratos \
    .withColumn("PERCCONFIRMACAO", col("PERCCONFIRMACAO") / 100) \
    .withColumn("STATUSDIRETORIA", when(col("OBSERVACOES").like("%#STATUSDIRETORIA%"), 1).otherwise(0)) \
    .select(
        "CODCONTRATO",
        "CODCLIENTE",
        "DTINICONTRATO",
        "VALIDADELIMITE",
        "FATOR",
        "LIMITEFOMENTO",
        "LIMITECOMISSARIA",
        "STATUS",
        "PERCCONFIRMACAO",
        "TRANCHE",
        "STATUSDIRETORIA"
    ) \
    .orderBy(col("CODCONTRATO").desc())

# Célula 9.3: Salvar o Resultado
# ------------------------------------------------------
output_path_contratos = f"{target_lakehouse}.{target_table_contratos}"
df_transformed_contratos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_contratos)
print(f"Tabela de staging de contratos salva com sucesso em: {output_path_contratos}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 10: Limpeza do Cache
# **Objetivo:** Liberar os DataFrames que foram armazenados em cache da memória do Spark para otimizar o uso de recursos.

# CELL ********************

print("\nLimpando os DataFrames do cache...")

# Libera o cache da tabela de títulos
spark.catalog.uncacheTable("LH_Silver.staging_titulos_limpa")
print("Cache de 'staging_titulos_limpa' liberado.")

# Libera o cache da tabela de clientes
spark.catalog.uncacheTable("LH_Silver.staging_clientes_limpa")
print("Cache de 'staging_clientes_limpa' liberado.")

print("Limpeza do cache concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
