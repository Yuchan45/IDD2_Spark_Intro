from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def resolver_parte_5(dataset: DataFrame) -> None:
    """Resuelve el analisis de usuarios y compras verificadas."""
    print("\n=== PARTE 5: Analisis de usuarios y comportamiento ===")

    # Arrange 5.a:
    # Esto genera una tabla con la cantidad de resenas por cada usuario (customer_id).
    # customer_id, total_resenas
    # U1, 2
    # U2, 1
    # U3, 5
    resenas_por_usuario = (
        dataset.filter(F.col("customer_id").isNotNull()) # es = a "When customer_id is not null"
        .groupBy("customer_id")
        .count()
        .withColumnRenamed("count", "total_resenas")
    )



    # 5.a.i: mostrar una muestra de la cantidad de resenas por usuario.
    print("\nCantidad de resenas por usuario (primeros 20):")
    resenas_por_usuario.orderBy("customer_id").show(20, truncate=False)

    # 5.a.ii: identificar los usuarios que publicaron mas resenas.
    print("\nTop 10 usuarios mas activos:")
    resenas_por_usuario.orderBy(
        F.desc("total_resenas"),
        F.asc("customer_id"),
    ).show(10, truncate=False)



    # Arrange 5.b:
    # Dado el dataset original que tiene varios registros verified_purchase = True o False, se agrupa por esa columna para 
    # obtener el promedio de calificacion y la cantidad de resenas en cada grupo.
    # verified_purchase, stars_rating
    # True, 5
    # True, 4
    # False, 3.8

    # Genra una tabla resumen como:
    # verified_purchase, total_resenas, promedio_rating
    # True, 1000, 4.5
    # False, 500, 3.8
    relacion_compra_rating = (
        dataset.filter(F.col("verified_purchase").isNotNull())
        .groupBy("verified_purchase")
        .agg(
            F.count("*").alias("total_resenas"),
            F.round(F.avg("star_rating"), 2).alias("promedio_rating"),
        )
        .orderBy("verified_purchase")
    )

    # 5.b: comparar cantidad y promedio segun compra verificada.
    print("\nRelacion entre compra verificada y calificacion:")
    relacion_compra_rating.show(truncate=False)

    # Agarramos los promedios para cada grupo y los metemos en un diccionario
    promedios = {
        row["verified_purchase"]: row["promedio_rating"]
        for row in relacion_compra_rating.collect()
    }
    promedio_verificadas = promedios.get(True)
    promedio_no_verificadas = promedios.get(False)

    # Los comparamos para sacar una conclusion
    print("\nConclusion:")
    if promedio_verificadas is None or promedio_no_verificadas is None:
        print("No hay datos suficientes para comparar ambos grupos.")
    elif promedio_verificadas > promedio_no_verificadas:
        print("Las compras verificadas tienden a tener mejores calificaciones.")
    elif promedio_verificadas < promedio_no_verificadas:
        print("Las compras verificadas tienden a tener peores calificaciones.")
    else:
        print("Ambos grupos tienen el mismo promedio de calificacion.")
