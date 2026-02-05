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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Carregar dados da CVM
# 
# Este notebook baixa os dados mensais de FIDC (Fundos de Investimento em Direitos Creditórios) do portal de dados abertos da CVM e os carrega na camada Bronze.
# 
# **Fonte:** https://dados.cvm.gov.br/dataset/fidc-doc-inf_mensal
# 
# ### Como parametrizar este notebook no Microsoft Fabric:
# 1. Adicione uma nova célula no topo.
# 2. Adicione as variáveis que você quer parametrizar, por exemplo:
#    ```python
#    ano = "2025"
#    mes = "09"
#    lista_periodos = ["202501", "202502"] # Opcional: Para processar múltiplos meses explicitamente
#    buscar_novos_meses = True # Opcional: Se True, busca automaticamente meses subsequentes ao último carregado
#    ```
# 3. Na barra de ferramentas da célula, clique em reticências (...) e selecione "Ativar/desativar célula de parâmetro".
# 4. Ao executar este notebook a partir de um pipeline do Fabric, você poderá passar valores para `ano`, `mes`, `lista_periodos` ou `buscar_novos_meses`.

# CELL ********************

import requests
import zipfile
import os
import shutil
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max, concat
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parâmetros do Notebook
# Estes valores podem ser substituídos por parâmetros de pipeline do Fabric.
# Veja a célula de markdown acima para instruções.
ano = ""
mes = ""
lista_periodos = ["202507", "202508","202509", "202510", "202511", "202512", "202601", "202602"] # Ex: ["202401", "202402"] - Se preenchido e buscar_novos_meses=False, processa estes
buscar_novos_meses = False # Se True, tenta descobrir e processar novos meses após o último existente

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Processamento dos Dados (Download, Leitura e Salvamento)
# 
# O bloco abaixo determina quais períodos processar (seja via lista explícita ou descoberta automática) e executa o carregamento.

# CELL ********************

# Habilitar o modo de substituição de partição dinâmica para garantir idempotência
# Isso previne duplicidade: se rodarmos um mês que já existe, ele sobrescreve apenas aquela partição.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
bronze_table_name = "LH_Bronze.cvm_fidc_informe_mensal"

# --- Lógica de Determinação de Períodos ---
periodos_para_processar = []

if buscar_novos_meses:
    print("Modo de Busca Automática: ATIVADO.")
    start_ano = int(ano)
    start_mes = int(mes)

    # Tentar descobrir o último mês carregado
    try:
        df_existing = spark.read.table(bronze_table_name)
        # Encontra o maior ANO_REF e MES_REF
        max_row = df_existing.selectExpr("max(concat(ANO_REF, MES_REF)) as max_period").collect()[0][0]

        if max_row:
            print(f"Último período encontrado na base: {max_row}")
            last_ano_db = int(max_row[:4])
            last_mes_db = int(max_row[4:])

            # Recuar 2 meses a partir do último mês encontrado para garantir reprocessamento dos últimos 3 meses
            # (Ex: Se último é Março, queremos processar Jan, Fev, Mar e procurar Abr)
            # Lógica matemática para subtrair meses
            calc_mes = last_mes_db - 2
            calc_ano = last_ano_db

            if calc_mes < 1:
                calc_mes += 12
                calc_ano -= 1

            start_ano = calc_ano
            start_mes = calc_mes
            print(f"Data de início ajustada para reprocessar últimos meses (retroativo): {start_ano}-{start_mes:02d}")

        else:
            print("Tabela encontrada mas vazia. Usando parâmetros iniciais.")

    except AnalysisException:
        print(f"Tabela {bronze_table_name} não encontrada. Usando parâmetros iniciais como ponto de partida.")

    # Loop de "Caça" aos arquivos
    current_date = datetime(start_ano, start_mes, 1)
    # Limite de segurança: não buscar mais que 3 meses à frente da data atual do sistema
    limit_date = datetime.now() + timedelta(days=90)

    print(f"Iniciando busca a partir de: {current_date.strftime('%Y-%m')}")

    while current_date < limit_date:
        p_ano = str(current_date.year)
        p_mes = f"{current_date.month:02d}"

        url_check = f"https://dados.cvm.gov.br/dados/FIDC/DOC/INF_MENSAL/DADOS/inf_mensal_fidc_{p_ano}{p_mes}.zip"

        try:
            # Head request para checar existência sem baixar
            response = requests.head(url_check)
            if response.status_code == 200:
                print(f"ENCONTRADO: {p_ano}{p_mes}")
                periodos_para_processar.append(f"{p_ano}{p_mes}")
                # Avançar para o próximo mês
                current_date = (current_date + timedelta(days=32)).replace(day=1)
            else:
                print(f"NÃO ENCONTRADO: {p_ano}{p_mes} (Status {response.status_code}). Encerrando busca.")
                break
        except Exception as e:
            print(f"Erro ao verificar URL {url_check}: {e}")
            break

elif lista_periodos:
    periodos_para_processar = lista_periodos
    print(f"Usando lista explícita de períodos: {periodos_para_processar}")
else:
    periodos_para_processar = [f"{ano}{mes}"]
    print(f"Usando período único dos parâmetros: {periodos_para_processar}")

# --- Loop de Processamento ---

if not periodos_para_processar:
    print("Nenhum período novo para processar.")
else:
    print(f"Iniciando processamento de {len(periodos_para_processar)} períodos...")

    for periodo in periodos_para_processar:
        current_ano = periodo[:4]
        current_mes = periodo[4:]
        print(f"\n=== Processando: Ano {current_ano}, Mês {current_mes} ===")
    
        temp_base_path = f"Files/temp/cvm_fidc_{current_ano}{current_mes}"

        try:
            # --- 1. Download ---
            url = f"https://dados.cvm.gov.br/dados/FIDC/DOC/INF_MENSAL/DADOS/inf_mensal_fidc_{current_ano}{current_mes}.zip"
            download_path = f"{temp_base_path}/inf_mensal_fidc.zip"
            unzip_path = f"{temp_base_path}/unzipped/"

            # Limpa o diretório temporário de execuções anteriores
            if os.path.exists(temp_base_path):
                shutil.rmtree(temp_base_path)
            os.makedirs(unzip_path, exist_ok=True)

            print(f"Baixando arquivo de {url}...")
            response = requests.get(url)

            if response.status_code != 200:
                print(f"AVISO: Falha ao baixar o arquivo para {periodo}. Status code: {response.status_code}. Pulando...")
                continue

            with open(download_path, "wb") as f:
                f.write(response.content)
            print(f"Arquivo salvo em {download_path}")

            # --- 2. Descompactação ---
            print(f"Descompactando arquivo em {unzip_path}...")
            with zipfile.ZipFile(download_path, "r") as zip_ref:
                zip_ref.extractall(unzip_path)

            # --- 3. Leitura ---
            csv_files = [f for f in os.listdir(unzip_path) if f.endswith('.csv')]
            target_file_prefix = "inf_mensal_fidc_tab_I_"
            target_file = next((f for f in csv_files if f.startswith(target_file_prefix)), None)

            if not target_file:
                print(f"AVISO: Nenhum arquivo com o prefixo '{target_file_prefix}' foi encontrado no ZIP para {periodo}. Pulando...")
                continue

            local_csv_path = os.path.join(unzip_path, target_file)
            print(f"Lendo o arquivo com Pandas: {local_csv_path}")

            # Ler com Pandas
            pandas_df = pd.read_csv(local_csv_path, sep=';', encoding='ISO-8859-1', dtype=str)

            # Criar DataFrame Spark
            df = spark.createDataFrame(pandas_df)

            # Forçar tipo String
            for column in df.columns:
                df = df.withColumn(column, col(column).cast(StringType()))

            # Adicionar colunas de partição
            df = df.withColumn("ANO_REF", lit(current_ano)).withColumn("MES_REF", lit(current_mes))

            # --- 4. Salvamento ---
            print(f"Salvando dados de {periodo} na tabela {bronze_table_name}...")
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("ANO_REF", "MES_REF")
                .option("mergeSchema", "true")
                .saveAsTable(bronze_table_name)
            )
            print(f"SUCESSO: Dados de {periodo} salvos!")

        except Exception as e:
            print(f"ERRO CRÍTICO ao processar {periodo}: {e}")
            # Opcional: raise e se quiser parar tudo, mas num loop geralmente queremos tentar o próximo

        finally:
            # --- 5. Limpeza ---
            print(f"Limpando arquivos temporários para {periodo}...")
            if os.path.exists(temp_base_path):
                shutil.rmtree(temp_base_path)

    print("\nProcessamento de todos os períodos concluído.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
