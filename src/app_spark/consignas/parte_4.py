from pyspark.sql import DataFrame, SparkSession


def resolver_parte_4(
    spark: SparkSession,
    dataset: DataFrame,
) -> None:
    """Resuelve el analisis de calificaciones utilizando Spark SQL."""
    print("\n=== PARTE 4: Uso de Spark SQL ===")

    # 4.a: registrar el DataFrame para poder consultarlo con SQL.
    # Crear una vista temporal llamada "reviews" para consultas SQL.
    dataset.createOrReplaceTempView("reviews")
    print("\nVista temporal 'reviews' creada correctamente.")

    # 4.b: repetir con SQL las consultas de la Parte 3.
    print("\nPromedio de calificacion global:")
    spark.sql(
        """
        SELECT ROUND(AVG(star_rating), 2) AS promedio_global
        FROM reviews
        """
    ).show()

    print("\nCalificacion promedio por categoria:")
    spark.sql(
        """
        SELECT
            product_category,
            ROUND(AVG(star_rating), 2) AS promedio_rating,
            COUNT(*) AS total_resenas
        FROM reviews
        WHERE product_category IS NOT NULL
        GROUP BY product_category
        ORDER BY product_category
        """
    ).show(50, truncate=False)

    print("\nCantidad de resenas por rating:")
    spark.sql(
        """
        SELECT star_rating, COUNT(*) AS total_resenas
        FROM reviews
        GROUP BY star_rating
        ORDER BY star_rating
        """
    ).show(truncate=False)

    print("\nTop 10 categorias con mejor promedio:")
    spark.sql(
        """
        SELECT
            product_category,
            ROUND(AVG(star_rating), 2) AS promedio_rating,
            COUNT(*) AS total_resenas
        FROM reviews
        WHERE product_category IS NOT NULL
        GROUP BY product_category
        ORDER BY promedio_rating DESC, total_resenas DESC
        LIMIT 10
        """
    ).show(truncate=False)

    print("\nTop 10 categorias con peor promedio:")
    spark.sql(
        """
        SELECT
            product_category,
            ROUND(AVG(star_rating), 2) AS promedio_rating,
            COUNT(*) AS total_resenas
        FROM reviews
        WHERE product_category IS NOT NULL
        GROUP BY product_category
        ORDER BY promedio_rating ASC, total_resenas DESC
        LIMIT 10
        """
    ).show(truncate=False)

    # 4.c: productos con mayor cantidad de resenas.
    print("\nTop 10 productos con mayor cantidad de resenas:")
    spark.sql(
        """
        SELECT
            product_id,
            MAX(product_title) AS product_title,
            COUNT(*) AS total_resenas
        FROM reviews
        WHERE product_id IS NOT NULL
        GROUP BY product_id
        ORDER BY total_resenas DESC
        LIMIT 10
        """
    ).show(truncate=60)
