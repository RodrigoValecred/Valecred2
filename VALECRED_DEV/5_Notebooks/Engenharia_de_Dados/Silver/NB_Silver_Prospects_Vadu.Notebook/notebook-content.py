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

# # **Objetivo:** Filtrar a base de consultas Vadu em JSON (Serasa) para extrair os CNPJs prospectáveis.
#
# A regra determina que um "Prospect Ideal" (Com Visão Cedente) possui o bloco `"assignor":{...}` preenchido com dados de histórico (`paymentHistory`), e "Não Prospect" (Sem Visão Cedente) retorna como `"assignor":{}` no JSON bruto.
# A tabela criada `LH_Silver.staging_prospects_vadu` lista os CNPJs limpos.

# CELL ********************

from pyspark.sql.functions import col, lit

def extract_prospects():
    source_path = "Files/Ingestao/Vadu/Novos/Todas_as_consultas_em_json.csv"
    target_table = "LH_Silver.staging_prospects_vadu"

    print(f"Lendo base bruta de: {source_path}")
    # Assume que a leitura em CSV da camada bronze.
    # O arquivo não tem cabeçalho, então usamos _c1 para a segunda coluna (CNPJ)
    # e assume que Retorno está em alguma coluna. Se o arquivo tem cabeçalho,
    # header=True leria os nomes corretamente. Vamos tentar com header=True primeiro.

    # O user specification says: "utilizando a segunda coluna do arquivo para extrair o CNPJ da empresa e aplicando uma regra de filtro nos dados da coluna Retorno".

    df_raw = spark.read.csv(source_path, header=True, sep=";")

    # Obter os nomes das colunas
    cols = df_raw.columns
    if len(cols) >= 2:
        cnpj_col_name = cols[1]

        # Filtra onde a coluna 'Retorno' NÃO contem '"assignor":{}'
        # ou seja, contém dados.
        df_filtered = df_raw.filter(~col("Retorno").contains('"assignor":{}'))

        # Seleciona apenas a coluna CNPJ
        df_prospects = df_filtered.select(col(cnpj_col_name).alias("cnpj"))

        print(f"Salvando dados limpos em: {target_table}")
        df_prospects.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
        print("Carga concluída.")
    else:
        print("Erro: O arquivo CSV não possui colunas suficientes.")

if __name__ == "__main__":
    extract_prospects()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
