from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc

# Inicializa a sessão Spark (necessário se rodar como script standalone)
spark = SparkSession.builder.appName("CalculateSectorProfit").getOrCreate()

# O notebook 'NB_Gold_Relatorio_Produtos_Mensal' gera a tabela 'LH_Gold.relatorio_produtos_mensal'.
# Portanto, lemos essa tabela para responder à pergunta.
table_name = "LH_Gold.relatorio_produtos_mensal"
try:
    df = spark.read.table(table_name)
except Exception as e:
    print(f"Erro ao ler a tabela {table_name}: {e}")
    # Fallback para teste ou explicação
    df = None

if df:
    # Agrupa por Plataforma (que representa o Setor/Unidade de Negócio neste contexto) e soma a receita.
    # A coluna 'nome_plataforma' (ex: 'PLATAFORMA BROKER', 'PLATAFORMA VALECRED') é a melhor aproximação de 'Setor'.
    df_setor_lucro = df.groupBy("nome_plataforma") \
        .agg(sum("receita").alias("total_receita")) \
        .orderBy(desc("total_receita"))

    # Exibe o setor com maior lucro
    top_setor = df_setor_lucro.first()
    if top_setor:
        print(f"Setor (Plataforma) com maior lucro: {top_setor['nome_plataforma']}")
        print(f"Receita Total: {top_setor['total_receita']}")
    else:
        print("Nenhum dado encontrado.")

    # Alternativa: Se o usuário considerar 'sub_tipo_produto' como setor
    print("\n--- Alternativa por Tipo de Produto ---")
    df_produto_lucro = df.groupBy("sub_tipo_produto") \
        .agg(sum("receita").alias("total_receita")) \
        .orderBy(desc("total_receita"))

    top_produto = df_produto_lucro.first()
    if top_produto:
        print(f"Tipo de Produto com maior lucro: {top_produto['sub_tipo_produto']}")
        print(f"Receita Total: {top_produto['total_receita']}")
else:
    print("Não foi possível carregar o DataFrame.")
