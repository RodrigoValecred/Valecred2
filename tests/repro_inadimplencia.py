from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode, sequence, to_date, last_day, when, sum as _sum, months_between, expr, broadcast
import time
from datetime import date

def run_test():
    spark = SparkSession.builder \
        .appName("Benchmark Inadimplencia") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # 1. Simula df_calendario
    df_calendario = spark.sql("""
        SELECT explode(
            sequence(to_date('2024-01-01'), current_date(), interval 1 month)
        ) as inicio_mes
    """).select(last_day("inicio_mes").alias("DATA_CORTE"))

    # 2. Simula df_titulos_enrich
    # Gera alguns dados
    # Cria um DataFrame com 1000 títulos
    data = []
    for i in range(1000):
        data.append((
            i,
            date(2024, 1, 1), # data_analise
            None if i % 2 == 0 else date(2024, 6, 1), # liquidacao
            date(2024, 2, 1) # venc_prorrogado
        ))

    schema = ["cod_operacao", "data_analise", "liquidacao", "venc_prorrogado"]
    df_titulos_enrich = spark.createDataFrame(data, schema)

    print("Running Original Logic (CrossJoin + Filter)...")
    start_time = time.time()

    # LÓGICA ORIGINAL
    # 4. Cross Join com Calendário (Multiplica Títulos x Meses)
    df_historico_base = df_titulos_enrich.crossJoin(df_calendario)

    # 5. O Filtro de "Existência" (Agora usando DATAACEITE)
    df_calculo_status_orig = df_historico_base.filter(
        # O título só "existe" no gráfico se a operação já tinha sido aceita naquela data
        col("data_analise") <= col("DATA_CORTE")
    ).withColumn(
        # Verifica se estava ABERTO na data do corte
        # Regra: Não foi liquidado OU foi liquidado DEPOIS da data de corte
        "IS_ABERTO_NA_DATA",
        when(
            (col("liquidacao").isNull()) | (col("liquidacao") > col("DATA_CORTE")),
            lit(1)
        ).otherwise(lit(0))
    ).filter(col("IS_ABERTO_NA_DATA") == 1) # Mantém apenas a carteira ativa da época

    count_orig = df_calculo_status_orig.count()
    end_time = time.time()
    time_orig = end_time - start_time
    print(f"Original Time: {time_orig:.4f}s, Count: {count_orig}")


    print("Running Optimized Logic (Broadcast Join)...")
    start_time = time.time()

    # LÓGICA OTIMIZADA
    df_calculo_status_opt = df_titulos_enrich.join(
        broadcast(df_calendario),
        (col("data_analise") <= col("DATA_CORTE")) &
        ((col("liquidacao").isNull()) | (col("liquidacao") > col("DATA_CORTE"))),
        "inner"
    ).withColumn(
        "IS_ABERTO_NA_DATA", lit(1) # Mantendo a coluna para compatibilidade se necessário, embora não seja estritamente necessário para a correção lógica
    )

    count_opt = df_calculo_status_opt.count()
    end_time = time.time()
    time_opt = end_time - start_time
    print(f"Optimized Time: {time_opt:.4f}s, Count: {count_opt}")

    # Verifica results
    # Classifica e compara ou apenas conta
    assert count_orig == count_opt, f"Counts differ: Original={count_orig}, Optimized={count_opt}"

    # Verifica se o conteúdo corresponde a uma amostra
    # Podemos juntá-los e verificar se há incompatibilidades, mas a contagem é um bom primeiro passo.
    # Para ser minucioso, vamos verificar IDs distintos.

    ids_orig = df_calculo_status_orig.select("cod_operacao", "DATA_CORTE").orderBy("cod_operacao", "DATA_CORTE").collect()
    ids_opt = df_calculo_status_opt.select("cod_operacao", "DATA_CORTE").orderBy("cod_operacao", "DATA_CORTE").collect()

    assert ids_orig == ids_opt, "Content differs!"

    print("SUCCESS: Logic is equivalent.")
    spark.stop()

if __name__ == "__main__":
    run_test()
