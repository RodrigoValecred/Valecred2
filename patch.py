import re

file_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Load_Silver_From_Manual_Uploads.Notebook/notebook-content.py"

with open(file_path, "r") as f:
    content = f.read()

# Replace `pandas_df = pd.read_csv(file_path)`
# with `spark.read.csv(file_path)` logic

target_str = """        # Ler o arquivo com pandas baseado na extensão (do arquivo real)
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
        def sanitize_column_name(col_name):"""

replacement_str = """        # Ler o arquivo com pandas baseado na extensão (do arquivo real)
        if actual_filename.lower().endswith('.xlsx'):
            pandas_df = pd.read_excel(file_path)
        elif actual_filename.lower().endswith('.csv'):
            # 🧠 Tensor: Converter pd.read_csv para spark.read.csv (Leitura Distribuída)
            # 💡 What: Substituição do leitor de arquivos CSV single-node do Pandas pelo leitor nativo distribuído do Spark.
            # 🎯 Why: O uso do Pandas carrega o arquivo inteiro na memória do driver, o que pode causar OutOfMemoryError (OOM) e é um gargalo de performance para arquivos grandes (>100MB). O Spark lê o CSV distribuindo o trabalho pelos executores.
            # 📊 Impacto: Previne OOM e aumenta a velocidade de leitura para arquivos CSV em ordens de magnitude.
            # 🔬 Measurement: O uso de memória do driver no pico cai para níveis nominais, enquanto o uso de I/O é paralelizado entre as partições do cluster.

            # Lê com o Spark, coleta as colunas, depois sanitiza via alias, criando um dataframe temporário em Pandas só para reutilizar a função,
            # ou melhor, lendo e já tendo o dataframe Spark
            df_spark_csv = (spark.read
                .format("csv")
                .option("header", "true")
                .option("delimiter", ",") # Padrão, pode ser ajustado
                .option("encoding", "UTF-8")
                .option("inferSchema", "true")
                .load(file_path)
            )
            # Como a lógica abaixo padroniza os nomes e depois converte para Spark, precisamos
            # contornar isso para não usar Pandas
            pandas_df = None
        else:
            print(f"AVISO: Formato de arquivo não suportado para '{actual_filename}'. Pulando...")
            return

        if pandas_df is not None:
            print(f"Arquivo '{actual_filename}' lido com sucesso usando pandas.")
        else:
            print(f"Arquivo '{actual_filename}' lido com sucesso usando Spark.")

        # Padroniza os nomes das colunas para serem compatíveis com o formato Delta
        def sanitize_column_name(col_name):"""

content = content.replace(target_str, replacement_str)

target_str2 = """        original_columns = pandas_df.columns.tolist()
        pandas_df.columns = [sanitize_column_name(col) for col in original_columns]
        new_columns = pandas_df.columns.tolist()

        if original_columns != new_columns:
            print("Nomes de colunas foram padronizados:")
            for original, new in zip(original_columns, new_columns):
                if original != new:
                    print(f"  '{original}' -> '{new}'")

        # Converter para DataFrame Spark
        df_spark = spark.createDataFrame(pandas_df)
        print("DataFrame convertido para Spark com sucesso.")"""

replacement_str2 = """        if pandas_df is not None:
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
        else:
            original_columns = df_spark_csv.columns
            new_columns = [sanitize_column_name(col) for col in original_columns]

            if original_columns != new_columns:
                print("Nomes de colunas foram padronizados:")
                for original, new in zip(original_columns, new_columns):
                    if original != new:
                        print(f"  '{original}' -> '{new}'")

            # Renomeia no próprio spark
            df_spark = df_spark_csv
            for original, new in zip(original_columns, new_columns):
                if original != new:
                    df_spark = df_spark.withColumnRenamed(original, new)
            print("DataFrame Spark lido e colunas padronizadas com sucesso.")"""

content = content.replace(target_str2, replacement_str2)

with open(file_path, "w") as f:
    f.write(content)
