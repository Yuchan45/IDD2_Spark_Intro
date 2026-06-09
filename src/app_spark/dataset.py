import os
from pathlib import Path
from urllib.request import urlretrieve

from pyspark.sql import DataFrame, SparkSession


# Configuracion del dataset utilizada por todas las consignas.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATASET_URL = (
    "https://datasets-documentation.s3.eu-west-3.amazonaws.com/"
    "amazon_reviews/amazon_reviews_2015.snappy.parquet"
)
DATASET_PATH = PROJECT_ROOT / "data" / "amazon_reviews_2015.snappy.parquet"
SAMPLE_FRACTION = 0.10


def show_download_progress(
    block_count: int,
    block_size: int,
    total_size: int,
) -> None:
    """Muestra el avance de la descarga en MB y porcentaje."""
    downloaded_size = block_count * block_size
    downloaded_mb = downloaded_size / (1024 * 1024)

    if total_size > 0:
        total_mb = total_size / (1024 * 1024)
        percentage = min(downloaded_size * 100 / total_size, 100)
        message = (
            f"\rDescargando: {downloaded_mb:.1f} MB de {total_mb:.1f} MB "
            f"({percentage:.1f}%)"
        )
    else:
        message = f"\rDescargando: {downloaded_mb:.1f} MB"

    print(message, end="", flush=True)


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

        temporary_path.replace(dataset_path)
        print("Dataset descargado correctamente.")
        return True
    except Exception as error:
        print(f"Error al descargar el dataset: {error}")
        temporary_path.unlink(missing_ok=True)
        return False


def load_dataset(
    spark: SparkSession,
    dataset_path: Path,
) -> DataFrame:
    """Carga el dataset local y devuelve una muestra reproducible."""
    dataset = spark.read.parquet(str(dataset_path))

    dataset_sample = dataset.sample(
        withReplacement=False,
        fraction=SAMPLE_FRACTION,
        seed=42,
    )

    # Spark trabaja de forma diferida; esta accion comprueba la lectura.
    dataset_sample.limit(1).collect()
    return dataset_sample


def get_dataset(spark: SparkSession) -> DataFrame | None:
    """Descarga el archivo si es necesario y lo carga con Spark."""
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
