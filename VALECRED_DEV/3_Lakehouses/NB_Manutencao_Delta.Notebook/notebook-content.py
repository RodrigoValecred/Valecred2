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

# CELL ********************

# 1. Compatibilidade com Datas Legadas (Sistemas de Origem Antigos)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# 2. Captura dinâmica do parâmetro enviado pela Pipeline
if 'nome_lakehouse' not in locals():
    nome_lakehouse = "LH_Bronze"

tabelas = spark.catalog.listTables(nome_lakehouse)
print(f"Iniciando manutenção em {len(tabelas)} tabelas no Lakehouse: {nome_lakehouse}...\n")

for tabela in tabelas:
    if tabela.tableType == "MANAGED": 
        # Proteção com backticks (crases) para tabelas com espaços no nome
        nome_protegido = f"`{nome_lakehouse}`.`{tabela.name}`"
        
        try:
            # Validação do protocolo Delta
            is_delta = spark.sql(f"DESCRIBE DETAIL {nome_protegido}").select("format").collect()[0][0] == 'delta'
            
            if is_delta:
                print(f"⏳ Processando Delta: {nome_protegido}")
                spark.sql(f"OPTIMIZE {nome_protegido}")
                spark.sql(f"VACUUM {nome_protegido}")
                print(f"✅ Concluída!")
            else:
                print(f"⚠️ Ignorada: {nome_protegido} não é Delta.")
                
        except Exception as e:
            print(f"❌ Erro na tabela {tabela.name}: {str(e)[:200]}...")

print(f"🏁 Rotina de manutenção finalizada no ambiente: {nome_lakehouse}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
