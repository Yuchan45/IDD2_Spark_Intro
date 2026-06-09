from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def resolver_parte_3(dataset: DataFrame) -> None:
    """Resuelve el analisis de calificaciones."""
    print("\n=== PARTE 3: Analisis de calificaciones ===")

    # 3.a.i: calcular el promedio de calificacion de todas las resenas.
    print("\nPromedio de calificacion global:")
    dataset.agg(
        F.round(F.avg("star_rating"), 2).alias("promedio_global")
    ).show()

    # 3.a.ii: mostrar el promedio de calificacion de cada categoria.
    # Esta agregacion se reutiliza para mostrar promedios y extremos.
    promedio_por_categoria = (
        dataset.filter(F.col("product_category").isNotNull())
        .groupBy("product_category")
        .agg(
            F.round(F.avg("star_rating"), 2).alias("promedio_rating"),
            F.count("*").alias("total_resenas"),
        )
    )

    print("\nCalificacion promedio por categoria:")
    promedio_por_categoria.orderBy("product_category").show(
        50,
        truncate=False
    )

    # 3.a.iii: contar resenas para cada valor de rating.
    print("\nCantidad de resenas por rating:")
    (
        dataset.groupBy("star_rating")
        .count()
        .orderBy("star_rating")
        .show(truncate=False)
    )

    # 3.b.i: categorias con los promedios mas altos.
    print("\nTop 10 categorias con mejor promedio:")
    promedio_por_categoria.orderBy(
        F.desc("promedio_rating"),
        F.desc("total_resenas"),
    ).show(10, truncate=False)

    # 3.b.ii: categorias con los promedios mas bajos.
    print("\nTop 10 categorias con peor promedio:")
    promedio_por_categoria.orderBy(
        F.asc("promedio_rating"),
        F.desc("total_resenas"),
    ).show(10, truncate=False)
