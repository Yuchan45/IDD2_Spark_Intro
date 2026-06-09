from time import perf_counter

from pyspark.sql import SparkSession

from app_spark.dataset import DATASET_PATH, SAMPLE_FRACTION


def medir_tiempo_consulta(nombre: str, dataset) -> float:
    """Ejecuta una consulta y devuelve su tiempo en segundos."""
    inicio = perf_counter()
    filas = dataset.limit(10000).collect()
    duracion = perf_counter() - inicio

    print(f"{nombre}: {len(filas)} filas procesadas en {duracion:.2f} segundos.")
    return duracion


def resolver_parte_7(spark: SparkSession) -> None:
    """Compara consultas con todas las columnas y con seleccion parcial."""
    print("\n=== PARTE 7: Optimizacion y Big Data ===")

    # Se crean lecturas nuevas para observar el column pruning de Parquet.
    dataset_completo = (
        spark.read.parquet(str(DATASET_PATH))
        .sample(False, SAMPLE_FRACTION, seed=42)
    )
    dataset_parcial = (
        spark.read.parquet(str(DATASET_PATH))
        .select("product_category", "star_rating")
        .sample(False, SAMPLE_FRACTION, seed=42)
    )

    # 7.a y 7.b: ejecutar ambas consultas y comparar sus tiempos.
    print("\nComparacion de tiempos para 10.000 filas:")
    tiempo_completo = medir_tiempo_consulta(
        "Consulta con todas las columnas",
        dataset_completo,
    )
    tiempo_parcial = medir_tiempo_consulta(
        "Consulta con seleccion parcial",
        dataset_parcial,
    )

    print("\nComparacion:")
    print(f"- Columnas leidas en consulta completa: {len(dataset_completo.columns)}")
    print(f"- Columnas leidas en consulta parcial: {len(dataset_parcial.columns)}")
    print(f"- Tiempo consulta completa: {tiempo_completo:.2f} segundos")
    print(f"- Tiempo consulta parcial: {tiempo_parcial:.2f} segundos")

    # El plan muestra el ReadSchema utilizado por Spark para cada lectura.
    print("\nPlan fisico con todas las columnas:")
    dataset_completo.limit(10000).explain(mode="formatted")

    print("\nPlan fisico con seleccion parcial:")
    dataset_parcial.limit(10000).explain(mode="formatted")

    # 7.c: explicacion conceptual.
    # ¿Cómo beneficia el formato Parquet a estas consultas?
    # Parquet organiza los datos por columnas (columnar). 
    # El beneficion de esto es que por ejemplo muchas de las consultas que se hicieron son sobre determinadas columnas, como product_category o star_rating. 
    # Al leer solamente las columnas necesarias, Spark puede evitar leer datos innecesarios del disco, lo que reduce el tiempo de lectura y el uso de memoria.
    #
    # ¿Qué es “column pruning”?
    # "Spark detecta automáticamente qué columnas necesita una consulta y evita leer las demás."
    # Column pruning es la optimizacion que elimina del plan las columnas que
    # una consulta no utiliza.
    #
    # Digamos que de:
    # review_id, customer_id, product_id, product_title, product_category, star_rating
    # Voos ejecutas:
    # dataset.select("star_rating").show()
    # Spark detecta que solo se necesita star_rating y no lee las otras columnas.

