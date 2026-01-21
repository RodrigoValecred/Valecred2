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

# # Análise de Títulos no Jurídico e Rastreamento Reverso
# **Objetivo:** Identificar a coluna e o código correspondente ao status "Envio ao Jurídico", validando com o exemplo 'RN 03/89' fornecido pelo usuário.
#
# **Solicitação:** "preciso identificar uma coluna, é um status de cobrança ... pode tentar rastrear essa coluna? preciso descobrir os títulos com esse status"
# **Exemplo:** "o titulo com ndoc (numero_documento) = RN 03/89 está com esse registro"

# CELL ********************

from pyspark.sql.functions import col, upper, lit

print("Iniciando análise com rastreamento reverso...")

# ==============================================================================
# PARTE 1: Investigação do Exemplo 'RN 03/89'
# ==============================================================================
print("\n=== PARTE 1: Investigando Título Exemplo 'RN 03/89' ===")

# 1. Localizar o Título e seu ID (cod_titulo)
# Buscando em LH_Silver.staging_titulos_limpa
try:
    df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa")
    # Tenta encontrar exato ou parcial
    target_doc = "RN 03/89"
    df_example = df_titulos.filter(col("n_doc") == target_doc)

    if df_example.count() == 0:
        print(f"AVISO: Documento '{target_doc}' não encontrado na Silver. Tentando Bronze...")
        df_titulos_bronze = spark.read.table("LH_Bronze.tab_titulos")
        df_example = df_titulos_bronze.filter(col("NDOC") == target_doc).select(col("CODTITULO").alias("cod_titulo"), col("NDOC").alias("n_doc"))

    count_ex = df_example.count()
    print(f"Documento '{target_doc}' encontrado: {count_ex} registro(s).")

    if count_ex > 0:
        display(df_example)
        # Pegar o primeiro ID encontrado para rastrear
        cod_titulo_alvo = df_example.first()["cod_titulo"]
        print(f"ID do Título Alvo para rastreio: {cod_titulo_alvo}")

        # 2. Buscar TODAS as ocorrências de cobrança para este título
        print(f"Buscando histórico de ocorrências para cod_titulo = {cod_titulo_alvo}...")
        df_rlc = spark.read.table("LH_Bronze.rlc_titulos_ocorrencias_cobranca")
        df_historico_alvo = df_rlc.filter(col("CODTITULO") == cod_titulo_alvo)

        # 3. Cruzar com descrições para ler o que aconteceu
        try:
            df_tipo = spark.read.table("LH_Silver.dim_tipo_cobranca")
            # dim_tipo_cobranca usa chave_ocorrencia composta, vamos tentar cruzar pelas partes se possível
            # Se não, vamos de Bronze direto para facilitar a leitura humana agora
        except:
            pass

        df_tipo_bronze = spark.read.table("LH_Bronze.cad_conta_cobranca_ocorrencia") \
            .withColumnRenamed("DESCRICAO", "descricao_ocorrencia") \
            .select("TOCORRENCIA", "CODINSTRUCAO", "CODBANCO", "descricao_ocorrencia") # Chaves potenciais

        # O Join correto depende das colunas de chave.
        # Supondo TOCORRENCIA como principal indicador de 'O que aconteceu'
        # E talvez CODINSTRUCAO/CODBANCO como qualificadores.

        # Fazendo join apenas por TOCORRENCIA para ver as descrições gerais
        # Isso pode gerar duplicidade se o código repetir para bancos diferentes, mas serve para leitura.
        df_analise_exemplo = df_historico_alvo.join(df_tipo_bronze, "TOCORRENCIA", "left") \
            .select(
                "CODTITULO", "DATAINCLUSAO", "TOCORRENCIA", "CODINSTRUCAO", "CODBANCO", "descricao_ocorrencia", "MOTIVOCODOCORCOBRBANCO"
            ).orderBy("DATAINCLUSAO")

        print("Histórico de Ocorrências do Título Exemplo:")
        display(df_analise_exemplo)

        # Tentar identificar automaticamente qual linha fala de "JURIDICO"
        df_juridico_exemplo = df_analise_exemplo.filter(upper(col("descricao_ocorrencia")).contains("JURIDICO"))
        if df_juridico_exemplo.count() > 0:
            print(">>> SUCESSO: Status de Jurídico identificado no exemplo!")
            row_juridico = df_juridico_exemplo.first()
            codigo_identificado = row_juridico["TOCORRENCIA"]
            descricao_identificada = row_juridico["descricao_ocorrencia"]
            print(f"Código Identificado: {codigo_identificado}")
            print(f"Descrição Identificada: {descricao_identificada}")
        else:
            print(">>> AVISO: Nenhuma descrição contendo 'JURIDICO' explícito encontrada no histórico deste título.")
            print("Por favor, analise a tabela acima para ver se há outro termo (ex: 'CONTENCIOSO', 'JUDICIAL', 'ADVOGADO').")
            codigo_identificado = None

    else:
        print("Impossível prosseguir com análise do exemplo (Título não localizado).")
        codigo_identificado = None

except Exception as e:
    print(f"Erro na análise do exemplo: {e}")
    codigo_identificado = None


# ==============================================================================
# PARTE 2: Varredura Geral (Baseada na descoberta acima ou busca textual)
# ==============================================================================
print("\n=== PARTE 2: Varredura Geral ===")

if codigo_identificado:
    print(f"Usando o código confirmado ({codigo_identificado}) para buscar todos os títulos...")
    # Busca por código
    df_rlc = spark.read.table("LH_Bronze.rlc_titulos_ocorrencias_cobranca")
    df_todos_juridico = df_rlc.filter(col("TOCORRENCIA") == codigo_identificado)
else:
    print("Código não confirmado pelo exemplo. Fazendo busca textual ampla por 'JURIDICO'...")
    # Carrega descrições
    df_tipo_bronze = spark.read.table("LH_Bronze.cad_conta_cobranca_ocorrencia")
    codigos_juridico = [row['TOCORRENCIA'] for row in df_tipo_bronze.filter(upper(col("DESCRICAO")).contains("JURIDICO")).select("TOCORRENCIA").distinct().collect()]

    print(f"Códigos potenciais encontrados por texto: {codigos_juridico}")
    df_rlc = spark.read.table("LH_Bronze.rlc_titulos_ocorrencias_cobranca")
    df_todos_juridico = df_rlc.filter(col("TOCORRENCIA").isin(codigos_juridico))

# Final: Cruzar com dados do título para entregar relatório
if df_todos_juridico.count() > 0:
    print("Gerando relatório final de títulos...")

    # Pegar apenas a ocorrência mais recente de jurídico por título (se houver duplicatas)
    # ou simplesmente lista de distintos IDs
    df_ids_unicos = df_todos_juridico.select("CODTITULO").distinct().withColumnRenamed("CODTITULO", "cod_titulo")

    df_titulos_limpa = spark.read.table("LH_Silver.staging_titulos_limpa")

    df_relatorio = df_ids_unicos.join(df_titulos_limpa, "cod_titulo", "inner") \
        .select(
            "cod_titulo",
            "n_doc",
            "cpf_cnpj_sacado",
            "valor_devido",
            "vencimento_efetivo",
            "dias_atraso",
            "liquidacao"
        )

    print(f"Total de títulos encontrados: {df_relatorio.count()}")
    display(df_relatorio.limit(50))
else:
    print("Nenhum título encontrado com os critérios atuais.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
