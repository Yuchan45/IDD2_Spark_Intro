from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def resolver_parte_6(dataset: DataFrame) -> None:
    """Resuelve el analisis de votos utiles."""
    print("\n=== PARTE 6: Analisis de votos utiles ===")

    # 6.a: resumen estadistico de las columnas de votos.
    # Agarra las 2 columnas, y te calcula el count, min, mean (esto es avg) y max de cada una.
    print("\nResumen de helpful_votes y total_votes:")
    dataset.select("helpful_votes", "total_votes").summary(
        "count",
        "min",
        "mean",
        "max",
    ).show(truncate=False)

    # 6.b.i: proporcion global de votos utiles sobre votos totales.
    # Proporcion de votos utiles = helpful_votes / total_votes
    print("\nProporcion global de votos utiles:")
    dataset.agg(
        F.round(
            F.sum("helpful_votes") / F.sum("total_votes"),
            4,
        ).alias("proporcion_votos_utiles")
    ).show()

    # 6.b.ii: priorizar proporcion y luego cantidad total de votos.
    resenas_con_proporcion = (
        dataset.filter(F.col("total_votes") > 0) # Me quecdo con las reseñas con al menos un voto.
        .withColumn(
            "proporcion_util",
            F.round(F.col("helpful_votes") / F.col("total_votes"), 4), # Calculo la proporcion de votos utiles igual que antes y la llamo "proporcion_util"
        )
        .select(
            "review_id",
            "product_title",
            "review_headline",
            "helpful_votes",
            "total_votes",
            "proporcion_util",
        ) # Seleccionb las cols que muestro
    )

    
    print("\nTop 10 resenas con mejor proporcion de votos utiles:")
    resenas_con_proporcion.orderBy(
        F.desc("proporcion_util"),
        F.desc("total_votes"),
    ).show(10, truncate=60)

    # 6.c.i: resenas con la mayor cantidad absoluta de votos utiles.
    print("\nTop 10 resenas con mayor cantidad de votos utiles:")
    resenas_con_proporcion.orderBy(
        F.desc("helpful_votes"),
        F.desc("total_votes"),
    ).show(10, truncate=60)
