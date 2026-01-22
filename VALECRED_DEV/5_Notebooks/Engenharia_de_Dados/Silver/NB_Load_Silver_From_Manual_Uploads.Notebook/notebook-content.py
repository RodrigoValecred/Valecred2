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

# # Notebook de Carga Genérico para Arquivos Manuais
#  
# **Objetivo:** Este notebook é responsável por ler diversos arquivos de suporte (Excel e CSV) carregados manualmente no diretório `Files/manual_uploads`, e promovê-los para a camada Silver como tabelas Delta.
#  
# **Fluxo:**
# 1.  **Instalação de Dependências:** Instala bibliotecas necessárias para a leitura de arquivos, como `openpyxl`.
# 2.  **Definição da Função de Carga:** Uma função genérica `load_manual_file_to_silver` é definida para encapsular a lógica de leitura, conversão e gravação.
# 3.  **Configuração:** Uma lista de dicionários (`files_to_process`) define quais arquivos devem ser processados e quais os nomes das tabelas de destino.
# 4.  **Processamento em Loop:** O notebook itera sobre a lista de configuração e executa a função de carga para cada arquivo, registrando o sucesso ou a falha de cada um.

# MARKDOWN ********************

# ## Seção 1: Instalação de Dependências
# **Descrição:** Instala as bibliotecas necessárias para o processamento dos arquivos.

# CELL ********************

%pip install openpyxl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Definição da Função de Carga Genérica
# **Descrição:** Esta função lê um arquivo (CSV ou Excel) da pasta `manual_uploads`, o converte para um DataFrame Spark e o salva como uma tabela na camada Silver.

# CELL ********************

import pandas as pd
import os
import re
import unicodedata

def load_manual_file_to_bronze(source_filename, target_table_name):
    """Lê um arquivo da pasta de uploads manuais, converte para um DataFrame Spark e salva na camada Silver.

    A função localiza um arquivo (Excel ou CSV) no diretório `Files/manual_uploads`,
    o carrega com pandas, padroniza os nomes de suas colunas para o formato `snake_case`
    compatível com Delta Lake, o converte para um DataFrame Spark e o salva como uma
    tabela Delta na camada Silver, sobrescrevendo qualquer versão anterior.

    Args:
        source_filename (str): O nome do arquivo a ser lido (ex: 'sup_regiao.xlsx').
        target_table_name (str): O nome completo da tabela de destino no formato
                                 `lakehouse.tabela` (ex: 'LH_Silver.sup_regiao').
    """
    base_path = "/lakehouse/default/Files/manual_uploads"
    
    print(f"--- Iniciando processamento para: {source_filename} ---")
    
    try:
        # Verificar se o diretório existe
        if not os.path.exists(base_path):
            print(f"ERRO: Diretório base '{base_path}' não encontrado.")
            return

        # Busca case-insensitive pelo arquivo
        available_files = os.listdir(base_path)
        actual_filename = None
        for f in available_files:
            if f.lower() == source_filename.lower():
                actual_filename = f
                break

        if not actual_filename:
            print(f"ERRO: Arquivo '{source_filename}' não encontrado em '{base_path}'.")
            print(f"Arquivos disponíveis no diretório: {available_files}")
            return

        file_path = f"{base_path}/{actual_filename}"
        print(f"Arquivo encontrado: {actual_filename}")

        # Ler o arquivo com pandas baseado na extensão (do arquivo real)
        if actual_filename.lower().endswith('.xlsx'):
            pandas_df = pd.read_excel(file_path)
        elif actual_filename.lower().endswith('.csv'):
            # Assume separador por vírgula e encoding UTF-8. Ajuste se necessário.
            pandas_df = pd.read_csv(file_path)
        else:
            print(f"AVISO: Formato de arquivo não suportado para '{actual_filename}'. Pulando...")
            return

        print(f"Arquivo '{actual_filename}' lido com sucesso usando pandas.")
        # Padroniza os nomes das colunas para serem compatíveis com o formato Delta
        def sanitize_column_name(col_name):
            """Padroniza um nome de coluna para o formato snake_case.

            Args:
                col_name (str): O nome da coluna original.

            Returns:
                str: O nome da coluna padronizado.
            """
            # Normaliza para remover acentos (ex: 'ç' -> 'c')
            nfkd_form = unicodedata.normalize('NFKD', str(col_name))
            sanitized = u"".join([c for c in nfkd_form if not unicodedata.combining(c)])
            # Converte para minúsculas
            sanitized = sanitized.lower()
            # Substitui espaços e caracteres não-alfanuméricos por underscore
            sanitized = re.sub(r'[^a-z0-9_]+', '_', sanitized)
            # Remove múltiplos underscores
            sanitized = re.sub(r'_+', '_', sanitized)
            # Remove underscores no início ou fim
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

        # Salvar na camada Silver
        print(f"Salvando dados na tabela de destino: {target_table_name}")
        df_spark.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table_name)

        print(f"--- Processamento de {source_filename} concluído com sucesso. ---")

    except Exception as e:
        print(f"ERRO ao processar o arquivo '{source_filename}': {e}")
        # Continue para o próximo arquivo em vez de parar o notebook inteiro
        pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: Configuração e Execução do Processamento
# **Descrição:** Define a lista de arquivos a serem processados e executa a função de carga para cada um.

# CELL ********************

# Mapeamento de arquivos de origem para tabelas de destino.
# Adicione novos arquivos a esta lista para que sejam processados automaticamente.
files_to_process = [
    {
        "source_filename": "apelido_empresas.xlsx",
        "target_table_name": "LH_Silver.sup_apelido_empresas"
    },
    {
        "source_filename": "clientes_desconsiderados_do_pdd.xlsx",
        "target_table_name": "LH_Silver.sup_clientes_desconsiderados_do_pdd"
    },
    {
        "source_filename": "clientes_diretoria_info_mercado.xlsx",
        "target_table_name": "LH_Silver.sup_clientes_diretoria_info_mercado"
    },
    {
        "source_filename": "clientes_em_perdas.xlsx",
        "target_table_name": "LH_Silver.sup_clientes_em_perdas"
    },
    {
        "source_filename": "cor_clientes_inadimplencia.xlsx",
        "target_table_name": "LH_Silver.sup_cor_clientes_inadimplencia"
    },
    {
        "source_filename": "forma_de_pagamento.xlsx",
        "target_table_name": "LH_Silver.sup_forma_de_pagamento"
    },
    {
        "source_filename": "gerentes_ativos.xlsx",
        "target_table_name": "LH_Silver.sup_gerentes_ativos"
    },
    {
        "source_filename": "gestor_de_plataforma.xlsx",
        "target_table_name": "LH_Silver.sup_gestor_de_plataforma"
    },
    {
        "source_filename": "grupos_economicos.xlsx",
        "target_table_name": "LH_Silver.sup_grupos_economicos"
    },
    {
        "source_filename": "metas.xlsx",
        "target_table_name": "LH_Silver.sup_metas"
    },
    {
        "source_filename": "motivo_baixa.xlsx",
        "target_table_name": "LH_Silver.sup_motivo_baixa"
    },
    {
        "source_filename": "motivos_de_indeferimento.xlsx",
        "target_table_name": "LH_Silver.sup_motivos_de_indeferimento"
    },
    {
        "source_filename": "municipios_com_regioes.csv",
        "target_table_name": "LH_Silver.sup_municipios_com_regioes"
    },
    {
        "source_filename": "NivelMaturidade.csv",
        "target_table_name": "LH_Silver.sup_nivel_maturidade"
    },
    {
        "source_filename": "nivel_usuario.xlsx",
        "target_table_name": "LH_Silver.sup_nivel_usuario"
    },
    {
        "source_filename": "pago_pelo.xlsx",
        "target_table_name": "LH_Silver.sup_pago_pelo"
    },
    {
        "source_filename": "produtos_ausentes.xlsx",
        "target_table_name": "LH_Silver.sup_produtos_ausentes"
    },
    {
        "source_filename": "regiao.xlsx",
        "target_table_name": "LH_Silver.sup_regiao"
    },
    {
        "source_filename": "status_de_clientes_da_esteira.xlsx",
        "target_table_name": "LH_Silver.sup_status_de_clientes_da_esteira"
    },
    {
        "source_filename": "tipo_baixa.xlsx",
        "target_table_name": "LH_Silver.sup_tipo_de_baixa"
    },
    {
        "source_filename": "uf.xlsx",
        "target_table_name": "LH_Silver.sup_uf"
    }
]

print(f"Iniciando o processo de carga para {len(files_to_process)} arquivo(s).")

for file_info in files_to_process:
    load_manual_file_to_bronze(
        source_filename=file_info["source_filename"],
        target_table_name=file_info["target_table_name"]
    )

print("\nProcesso de carga de arquivos manuais finalizado.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
