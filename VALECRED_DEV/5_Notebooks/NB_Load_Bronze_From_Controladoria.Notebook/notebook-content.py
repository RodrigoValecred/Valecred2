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

# CELL ********************

# Fabric notebook source

# MARKDOWN ********************

# # Notebook de Carga para Arquivos da Controladoria
#  
# **Objetivo:** Este notebook é responsável por ler **recursivamente** todos os arquivos (Excel e CSV) carregados no diretório `Files/controladoria` e seus subdiretórios, e carregá-los como tabelas Delta na camada Bronze.
#  
# **Fluxo:**
# 1.  **Instalação de Dependências:** Instala bibliotecas necessárias, como `openpyxl`.
# 2.  **Definição da Função de Carga:** Uma função genérica `load_controladoria_file_to_bronze` encapsula a lógica de leitura, padronização de schema e gravação.
# 3.  **Descoberta e Processamento:** O notebook usa `os.walk` para encontrar todos os arquivos nos subdiretórios, gera dinamicamente um nome de tabela único baseado no caminho relativo do arquivo (ex: `ctrl_carteira_em_aberto_2025_01`) e executa a função de carga para cada um.

# MARKDOWN ********************

# ## Seção 1: Instalação de Dependências
# **Descrição:** Instala as bibliotecas necessárias para o processamento dos arquivos.

# CELL ********************

%pip install openpyxl

# MARKDOWN ********************

# ## Seção 2: Definição da Função de Carga
# **Descrição:** Esta função lê um arquivo (CSV ou Excel), o converte para um DataFrame Spark e o salva como uma tabela na camada Bronze. O nome da tabela é derivado do caminho do arquivo para garantir unicidade.

# CELL ********************

import pandas as pd
import os
import re
import unicodedata

def sanitize_for_table_name(path_name):
    """
    Padroniza um caminho de arquivo para um nome de tabela compatível com Delta.

    Args:
        path_name (str): O caminho do arquivo a ser convertido.

    Returns:
        str: Um nome de tabela limpo e seguro para uso no Delta Lake.
    """
    # Remove a extensão do arquivo
    name_without_ext = os.path.splitext(path_name)[0]
    # Normaliza para remover acentos (ex: 'ç' -> 'c')
    nfkd_form = unicodedata.normalize('NFKD', str(name_without_ext))
    sanitized = u"".join([c for c in nfkd_form if not unicodedata.combining(c)])
    # Converte para minúsculas
    sanitized = sanitized.lower()
    # Substitui separadores de diretório, espaços e caracteres não-alfanuméricos por underscore
    sanitized = re.sub(r'[\\/]+', '_', sanitized)
    sanitized = re.sub(r'[^a-z0-9_]+', '_', sanitized)
    # Remove múltiplos underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Remove underscores no início ou fim
    sanitized = sanitized.strip('_')
    return sanitized

def load_controladoria_file_to_bronze(file_path, base_path):
    """Lê um arquivo da pasta da controladoria, converte para um DataFrame Spark e salva na camada Bronze.

    A função lê um arquivo (Excel ou CSV) do caminho especificado, padroniza os nomes das colunas
    para um formato compatível com Delta Lake, converte o conteúdo para um DataFrame Spark e o
    salva como uma tabela Delta na camada Bronze. O nome da tabela de destino é gerado
    dinamicamente a partir do caminho relativo do arquivo para garantir unicidade.

    Args:
        file_path (str): O caminho completo do arquivo a ser processado.
        base_path (str): O caminho base do diretório da controladoria, usado para
                         calcular o nome relativo da tabela.
    """
    source_filename = os.path.basename(file_path)
    relative_path = os.path.relpath(file_path, base_path)
    
    # Gera o nome da tabela de destino a partir do caminho relativo do arquivo para garantir unicidade
    target_table_name = f"LH_Bronze.ctrl_{sanitize_for_table_name(relative_path)}"
    
    print(f"--- Iniciando processamento para: {relative_path} ---")
    
    try:
        # Ler o arquivo com pandas baseado na extensão
        if source_filename.lower().endswith('.xlsx'):
            pandas_df = pd.read_excel(file_path, engine='openpyxl')
        elif source_filename.lower().endswith('.csv'):
            # Tenta com separador ';' primeiro (comum no Brasil), depois com ','
            try:
                pandas_df = pd.read_csv(file_path, sep=';', encoding='latin-1')
            except Exception:
                pandas_df = pd.read_csv(file_path, sep=',')
        else:
            print(f"AVISO: Formato de arquivo não suportado para '{source_filename}'. Pulando...")
            return

        print(f"Arquivo '{source_filename}' lido com sucesso usando pandas.")
        
        # Padroniza os nomes das colunas
        def sanitize_column_name(col_name):
            nfkd_form = unicodedata.normalize('NFKD', str(col_name))
            sanitized = u"".join([c for c in nfkd_form if not unicodedata.combining(c)])
            sanitized = sanitized.lower()
            sanitized = re.sub(r'[^a-z0-9_]+', '_', sanitized)
            sanitized = re.sub(r'_+', '_', sanitized)
            sanitized = sanitized.strip('_')
            return sanitized

        original_columns = pandas_df.columns.tolist()
        pandas_df.columns = [sanitize_column_name(col) for col in original_columns]
        new_columns = pandas_df.columns.tolist()

        if original_columns != new_columns:
            print("Nomes de colunas foram padronizados:")
            for original, new in zip(original_columns, new_columns):
                if original != new:
                    print(f"  '{original}' -> '{new}'")

        # Converter para DataFrame Spark
        df_spark = spark.createDataFrame(pandas_df)
        print("DataFrame convertido para Spark com sucesso.")

        # Salvar na camada Bronze
        print(f"Salvando dados na tabela de destino: {target_table_name}")
        df_spark.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table_name)

        print(f"--- Processamento de {source_filename} concluído com sucesso. Tabela '{target_table_name}' criada/atualizada. ---")

    except Exception as e:
        print(f"ERRO ao processar o arquivo '{source_filename}' ({relative_path}): {e}")
        pass


# MARKDOWN ********************

# ## Seção 3: Execução do Processamento
# **Descrição:** Lista todos os arquivos no diretório `Files/controladoria` e seus subdiretórios, e executa a função de carga para cada um.

# CELL ********************

controladoria_path = "/lakehouse/default/Files/controladoria/Carteira/CARTEIRA"

# Verifica se o diretório existe
if not os.path.exists(controladoria_path) or not os.path.isdir(controladoria_path):
    print(f"AVISO: O diretório '{controladoria_path}' não foi encontrado. Nenhum arquivo será processado.")
    dbutils.notebook.exit("Diretório de origem não encontrado.")

# Usa os.walk para encontrar todos os arquivos recursivamente
files_to_process = []
for root, dirs, files in os.walk(controladoria_path):
    for filename in files:
        if not filename.startswith('.'): # Ignora arquivos ocultos
            files_to_process.append(os.path.join(root, filename))

if not files_to_process:
    print(f"Nenhum arquivo encontrado no diretório '{controladoria_path}' para processar.")
else:
    print(f"Iniciando o processo de carga para {len(files_to_process)} arquivo(s) encontrados em '{controladoria_path}' e seus subdiretórios.")
    for file_path in files_to_process:
        load_controladoria_file_to_bronze(file_path, controladoria_path)

print("\nProcesso de carga de arquivos da controladoria finalizado.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
