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

from pyspark.sql.functions import lit, current_timestamp, col, to_json, struct
from notebookutils import mssparkutils

# 1. MAPEAMENTO DE PASTAS - Use caminhos relativos ao Lakehouse conectado
diretorio_novos = "Files/Ingestao/Vadu/Novos/"
diretorio_processados = "Files/Ingestao/Vadu/Processados/"

print(f"Verificando pasta: {diretorio_novos}")

try:
    # Tenta listar os arquivos para ver se o caminho está correto
    arquivos = mssparkutils.fs.ls(diretorio_novos)
    print(f"Arquivos encontrados: {len(arquivos)}")
except Exception as e:
    print(f"Erro ao acessar diretório: {e}. Verifique se a pasta 'Novos' existe exatamente neste caminho.")
    arquivos = []

for arquivo in arquivos:
    # O ls retorna objetos.arquivo.name, verificamos se é o seu txt ou json
    if arquivo.name.endswith(".txt") or arquivo.name.endswith(".json"):
        caminho_origem = arquivo.path
        caminho_destino = diretorio_processados + arquivo.name
        
        print(f"--- Iniciando processamento de: {arquivo.name} ---")

        try:
            # 3. LEITURA NO SPARK
            df_raw = spark.read.option("multiLine", "true").json(caminho_origem)
            
            # 4. TRATAMENTO (Validando nomes de colunas do seu arquivo)
            # O cast para Long garante compatibilidade com a tabela BIGINT que criamos
            df_final = df_raw.select(
                col("borana_id").cast("long").alias("Bordero_ID"),
                col("sacCNPJCPF").alias("CNPJ_Sacado"),
                to_json(struct("*")).alias("JSON_Bruto")
            ).withColumn("Data_Hora_Ingestao", current_timestamp())

            # 5. ESCRITA NA TABELA
            print(f"Gravando {arquivo.name} na tabela Delta...")
            df_final.write.format("delta").mode("append").saveAsTable("tbl_vadu_bronze")

            # 6. MOVIMENTAÇÃO DO ARQUIVO
            print(f"Movendo arquivo para: {caminho_destino}")
            mssparkutils.fs.mv(caminho_origem, caminho_destino)
            print(f"SUCESSO: {arquivo.name} processado e movido.")

        except Exception as error:
            print(f"FALHA no arquivo {arquivo.name}: {error}")
    else:
        print(f"Arquivo ignorado (não é JSON/TXT): {arquivo.name}")

print("--- Fim da execução de Debug ---")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
