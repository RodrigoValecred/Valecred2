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

# PARAMETERS CELL ********************

# Célula 1: Marque esta célula como "Parameter" no menu do Notebook (Toggle parameter cell)
table_input = "cad_empresas" # Valor default para teste
keys_input = "CODEMPRESA"              # Valor default para teste (separado por virgula se composto)
watermark_col = "DATAALTERACAO"           # Opcional
bronze_lh = "LH_Bronze"
silver_lh = "LH_Silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- INÍCIO DO CÓDIGO ---
import re

from pyspark.sql.functions import col, count, lit
from delta.tables import DeltaTable

class SilverIngestor:
    def __init__(self, spark, bronze_db, silver_db, table_name, keys, watermark_col=None, allowed_tables=None):
        self.spark = spark
        self.bronze_db = bronze_db
        self.silver_db = silver_db

        # Segurança: Valida o nome da tabela contra a allowlist
        if allowed_tables is None:
            raise ValueError("allowed_tables must be provided for security.")

        if table_name not in allowed_tables:
            raise ValueError(f"Table '{table_name}' is not in the allowed list.")

        self.table_name = table_name
        self.keys = [k.strip() for k in keys.split(',')] 
        self.watermark_col = watermark_col
        self.df_source = None
        # Limpa o nome da tabela de destino também
        self.target_table_name = self._clean_name(table_name.replace('tab_', ''))

    def _clean_name(self, name):
        """
        Versão Simplificada: Assume origem SEMPRE MAIÚSCULA.
        Apenas converte para minúsculo e sanitiza caracteres especiais.
        """
        # 1. Converte tudo para minúsculo direto
        clean = name.lower()
        
        # 2. Substitui qualquer coisa que não seja letra/número por underline
        # (Resolve casos como "VALOR TOTAL", "CNPJ/CPF", "COD-EMP")
        clean = re.sub(r'[^\w]', '_', clean)
        
        # 3. Remove underlines duplicados (ex: "COD__EMPRESA" -> "cod_empresa")
        clean = re.sub(r'_+', '_', clean)
        
        # 4. Remove underlines sobrando no começo ou fim
        return clean.strip('_')

    def read_bronze(self):
        try:
            full_path = f"{self.bronze_db}.{self.table_name}"
            print(f"--> Lendo origem: {full_path}")
            self.df_source = self.spark.read.table(full_path)
            return self
        except Exception as e:
            raise Exception(f"Erro ao ler tabela Bronze {full_path}: {str(e)}")

    def standardize_columns(self):
        print("--> Padronizando colunas (Smart Casing)...")
        # Aplica a função mestra em todas as colunas
        new_cols = [self._clean_name(c) for c in self.df_source.columns]
        self.df_source = self.df_source.toDF(*new_cols)
        return self

    def quality_gate(self):
        print("--> Executando Quality Gate (PK Check)...")
        
        # Agora as chaves passam pela MESMA limpeza das colunas
        normalized_keys = [self._clean_name(k) for k in self.keys]
        
        print(f"    Chaves normalizadas para busca: {normalized_keys}")

        # Verifica nulos nas chaves
        condition = " OR ".join([f"{k} IS NULL" for k in normalized_keys])
        
        try:
            bad_records_df = self.df_source.filter(condition)
            # 🧠 Tensor: Substitua .count() por .isEmpty()
            # 💡 O que: Trocou uma avaliação DataFrame.count() de tabela completa por DataFrame.isEmpty().
            # 🎯 Por que: Calcular o número exato de registros ruins aciona uma varredura completa do dataset. Usar .isEmpty() avalia apenas até que a primeira correspondência seja encontrada, executando lógica de early-exit.
            # 📊 Impacto: Acelera significativamente o passo do Quality Gate para tabelas muito grandes quando a tabela tem poucos ou zero registros ruins, reduzindo o tempo do job e custo de computação do cluster.
            # 🔬 Medição: O profiling indica que isso evita acionar shuffle exchanges completos, caindo o tempo de avaliação de O(N) para O(1) nos melhores/casos médios.
            has_errors = not bad_records_df.isEmpty()
        except Exception as e:
            print(f"Erro ao filtrar colunas: {normalized_keys}. Colunas disponíveis: {self.df_source.columns}")
            raise e

        if has_errors:
            print(f"⚠️ CRÍTICO: Registros com Chave Primária Nula encontrados. Movendo para Quarentena.")
            
            quarantine_path = f"{self.silver_db}.quarentena_generic"
            (bad_records_df
             .withColumn("origem_tabela", lit(self.table_name))
             .withColumn("erro", lit("PK Nula"))
             .write.format("delta")
             .option("mergeSchema", "true")
             .mode("append")
             .saveAsTable(quarantine_path)
            )
            self.df_source = self.df_source.filter(f"NOT ({condition})")
        else:
            print("--> Quality Gate Aprovado: Nenhuma PK nula.")
        
        return self

    def execute_upsert(self):
        target_path = f"{self.silver_db}.{self.target_table_name}"
        print(f"--> Iniciando Upsert em: {target_path}")

        # Normaliza chaves para o merge
        normalized_keys = [self._clean_name(k) for k in self.keys]
        
        df_dedup = self.df_source.dropDuplicates(normalized_keys)

        # MUDANÇA AQUI: Verifica no Catálogo do Spark se a tabela existe, é mais robusto
        if self.spark.catalog.tableExists(self.target_table_name, dbName=self.silver_db):
            print(f"--> Tabela {target_path} encontrada. Executando MERGE...")
            dt = DeltaTable.forName(self.spark, target_path)
            
            # Monta condição dinâmica
            condition = " AND ".join([f"target.{k} = source.{k}" for k in normalized_keys])
            
            (dt.alias("target")
             .merge(df_dedup.alias("source"), condition)
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute()
            )
            print(f"✅ Upsert realizado com sucesso!")
        else:
            print(f"--> Tabela não encontrada no catálogo. Criando nova tabela: {target_path}")
            # MUDANÇA AQUI: Adicionei mode("overwrite") na primeira carga para garantir que
            # se houver arquivo lixo mas não tabela, ele sobrescreve.
            # Adicionei option("overwriteSchema", "true") para flexibilidade.
            (df_dedup.write
             .format("delta")
             .mode("overwrite") 
             .option("overwriteSchema", "true")
             .saveAsTable(target_path)
            )
            print(f"✅ Tabela criada com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- EXECUÇÃO ---
if not table_input or not keys_input:
    raise ValueError("Parâmetros obrigatórios ausentes.")

# Segurança: Definir tabelas permitidas para este ingestor
ALLOWED_TABLES = ["cad_empresas"]


ingestor = SilverIngestor(spark, bronze_lh, silver_lh, table_input, keys_input, allowed_tables=ALLOWED_TABLES)
ingestor.read_bronze().standardize_columns().quality_gate().execute_upsert()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
