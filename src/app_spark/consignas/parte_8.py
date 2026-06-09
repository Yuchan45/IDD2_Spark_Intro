from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_PATH = PROJECT_ROOT / "output" / "promedio_rating_por_categoria.parquet"


def guardar_parquet(tabla: pa.Table) -> None:
    """Escribe primero un archivo temporal y luego reemplaza el anterior."""
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = OUTPUT_PATH.with_name(
        f"{OUTPUT_PATH.stem}.{uuid4().hex}.tmp.parquet"
    )

    try:
        pq.write_table(tabla, temporary_path)
    except PermissionError as error:
        temporary_path.unlink(missing_ok=True)
        raise PermissionError(
            f"No se puede escribir en la carpeta {OUTPUT_PATH.parent}. "
            "Revisa sus permisos de escritura."
        ) from error

    try:
        temporary_path.replace(OUTPUT_PATH)
    except PermissionError as error:
        temporary_path.unlink(missing_ok=True)
        raise PermissionError(
            f"No se pudo reemplazar {OUTPUT_PATH}. "
            "Cierra cualquier programa que tenga abierto ese archivo."
        ) from error


def resolver_parte_8(
    spark: SparkSession,
    dataset: DataFrame,
) -> None:
    """Genera, guarda y valida un dataset agregado en formato Parquet."""
    print("\n=== PARTE 8: Persistencia ===")

    # 8.1: generar un dataset agregado por categoria.
    # Creo un dataset con el product_category, el promedio de star_rating y el total de reseñas por categoria.
    promedio_por_categoria = (
        dataset.filter(F.col("product_category").isNotNull())
        .groupBy("product_category")
        .agg(
            F.round(F.avg("star_rating"), 2).alias("promedio_rating"),
            F.count("*").alias("total_resenas"),
        )
        .orderBy("product_category")
    )

    print("\nDataset agregado:")
    promedio_por_categoria.show(50, truncate=False)

    # 8.2: Guardado en .parquet (este queda en la carpeta output del proyecto).
    # Como el dataset agregado es pequeño, se puede llevar al driver y escribirlo con PyArrow.
    # (PyArrow evita incompatibilidades entre Hadoop y Windows al escribir)
    filas = promedio_por_categoria.collect()
    tabla = pa.table(
        {
            "product_category": [fila["product_category"] for fila in filas],
            "promedio_rating": [fila["promedio_rating"] for fila in filas],
            "total_resenas": [fila["total_resenas"] for fila in filas],
        }
    )

    guardar_parquet(tabla)
    print(f"\nDataset guardado en: {OUTPUT_PATH}")

    # 8.3: leer nuevamente el archivo y validar su contenido.
    dataset_guardado = spark.read.parquet(str(OUTPUT_PATH))

    print("\nContenido leido desde el Parquet generado:")
    dataset_guardado.orderBy("product_category").show(50, truncate=False)
    print(f"Categorias guardadas: {dataset_guardado.count()}")
