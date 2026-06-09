from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def resolver_parte_2(dataset: DataFrame) -> None:
    """Resuelve la exploracion general del dataset."""
    print("\n=== PARTE 2: Exploracion general ===")

    # 2.a: calcular cantidades generales del subconjunto.
    cantidades = dataset.agg(
        F.count("*").alias("total_registros"),
        F.countDistinct("product_id").alias("productos_distintos"),
        F.countDistinct("product_category").alias("categorias_distintas"),
    )

    print("\nCantidades generales:")
    cantidades.show(truncate=False)

    # 2.b.i: identificar las 10 categorias mas frecuentes.
    print("\nTop 10 categorias mas frecuentes:")
    (
        dataset.groupBy("product_category")
        .count()
        .orderBy(F.desc("count"))
        .show(10, truncate=False)
    )

    # 2.b.ii: contar cuantas resenas existen para cada calificacion.
    print("\nDistribucion de calificaciones:")
    (
        dataset.groupBy("star_rating")
        .count()
        .orderBy("star_rating")
        .show(truncate=False)
    )
