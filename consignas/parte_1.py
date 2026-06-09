from pyspark.sql import DataFrame


def resolver_parte_1(dataset: DataFrame) -> None:
    """Resuelve la carga y exploracion inicial del dataset."""
    print("\n=== PARTE 1: Carga y exploracion inicial ===")

    # Consigna 1.c: mostrar las primeras filas del subconjunto.
    print("\nPrimeras 5 filas:")
    dataset.show(5, truncate=50)

    # Consigna 1.d: mostrar la estructura completa del DataFrame.
    print("\nEsquema del dataset:")
    dataset.printSchema()

    # Consigna 1.e: listar cada columna junto con su tipo de dato.
    print("\nColumnas disponibles y tipos de datos:")
    for column_name, data_type in dataset.dtypes:
        print(f"- {column_name}: {data_type}")
