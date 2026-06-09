import pyspark
from pyspark.sql import SparkSession

import os
import sys
from pathlib import Path
from urllib.request import urlretrieve

from consignas.parte_1 import resolver_parte_1


DATASET_URL = (
    "https://datasets-documentation.s3.eu-west-3.amazonaws.com/"
    "amazon_reviews/amazon_reviews_2015.snappy.parquet"
)
DATASET_PATH = (
    Path(__file__).parent
    / "data"
    / "amazon_reviews_2015.snappy.parquet"
)
SAMPLE_FRACTION = 0.10


def show_download_progress(block_count: int, block_size: int, total_size: int):
    """Muestra el avance de la descarga en MB y porcentaje."""
    downloaded_size = block_count * block_size
    downloaded_mb = downloaded_size / (1024 * 1024)

    if total_size > 0:
        total_mb = total_size / (1024 * 1024)
        percentage = min(downloaded_size * 100 / total_size, 100)
        print(
            f"\rDescargando: {downloaded_mb:.1f} MB de {total_mb:.1f} MB "
            f"({percentage:.1f}%)",
            end="",
            flush=True,
        )
    else:
        print(
            f"\rDescargando: {downloaded_mb:.1f} MB",
            end="",
            flush=True,
        )


def is_valid_parquet(dataset_path: Path) -> bool:
    """Comprueba que el archivo tenga las firmas basicas de un Parquet."""
    if not dataset_path.exists() or dataset_path.stat().st_size < 8:
        return False

    with dataset_path.open("rb") as dataset_file:
        first_bytes = dataset_file.read(4)
        dataset_file.seek(-4, os.SEEK_END)
        last_bytes = dataset_file.read(4)

    return first_bytes == b"PAR1" and last_bytes == b"PAR1"


def download_dataset(dataset_url: str, dataset_path: Path) -> bool:
    """Descarga el dataset y reemplaza el archivo local solo si es valido."""
    temporary_path = dataset_path.with_suffix(".download")
    dataset_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        print("Descargando dataset...")
        urlretrieve(
            dataset_url,
            temporary_path,
            reporthook=show_download_progress,
        )
        print()

        if not is_valid_parquet(temporary_path):
            raise ValueError("El archivo descargado no es un Parquet valido.")

        # El archivo anterior se reemplaza solo cuando la descarga termino bien.
        temporary_path.replace(dataset_path)
        print("Dataset descargado correctamente.")
        return True
    except Exception as error:
        print(f"Error al descargar el dataset: {error}")
        temporary_path.unlink(missing_ok=True)
        return False


def load_dataset(spark: SparkSession, dataset_path: Path):
    """Carga el dataset local y devuelve una muestra del 10%."""
    dataset = spark.read.parquet(str(dataset_path))

    # La semilla permite trabajar siempre con el mismo subconjunto.
    dataset_sample = dataset.sample(
        withReplacement=False,
        fraction=SAMPLE_FRACTION,
        seed=42,
    )

    # Spark trabaja de forma diferida; esta accion comprueba la lectura.
    dataset_sample.limit(1).collect()
    return dataset_sample


def get_dataset(spark: SparkSession):
    """Obtiene un dataset local valido y lo carga con Spark."""
    # Si el archivo falta o esta incompleto, se vuelve a descargar.
    if not is_valid_parquet(DATASET_PATH):
        print("El dataset no existe o esta corrupto.")
        if not download_dataset(DATASET_URL, DATASET_PATH):
            return None

    try:
        return load_dataset(spark, DATASET_PATH)
    except Exception:
        # Spark puede detectar errores que la validacion basica no encuentra.
        print("Spark detecto que el dataset esta corrupto. Descargando nuevamente.")
        if not download_dataset(DATASET_URL, DATASET_PATH):
            return None

        try:
            return load_dataset(spark, DATASET_PATH)
        except Exception as error:
            print(f"No se pudo leer el dataset descargado: {error}")
            return None


def main() -> int:
    # Configuracion necesaria para ejecutar PySpark en esta computadora.
    # Si hace falta en tu maquina, descomenta y ajusta estas rutas.
    os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
    os.environ["HADOOP_HOME"] = r"C:\Hadoop\hadoop-3.3.6"
    os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
    os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    print(f"PySpark version: {pyspark.__version__}")

    # Se crea una sesion local de Spark usando dos nucleos.
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("AmazonReviewsIntro")
        .config("spark.driver.host", "localhost")
        # El Parquet guarda los textos como binarios sin metadata UTF-8.
        .config("spark.sql.parquet.binaryAsString", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        dataset = get_dataset(spark)

        if dataset is not None:
            print("Dataset cargado correctamente.")
            print("Se utilizara un subconjunto del 10% de los datos.")
            resolver_parte_1(dataset)
        else:
            print("No se pudo cargar el dataset.")
    finally:
        # La sesion se cierra siempre, incluso si ocurre un error.
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
