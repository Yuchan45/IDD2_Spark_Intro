import os
import sys
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession


# Permite importar el paquete ubicado dentro de src sin instalarlo.
SOURCE_PATH = Path(__file__).parent / "src"
sys.path.insert(0, str(SOURCE_PATH))

from app_spark.consignas.parte_1 import resolver_parte_1
from app_spark.dataset import SAMPLE_FRACTION, get_dataset


def main() -> int:
    # Configuracion necesaria para ejecutar PySpark en esta computadora.
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

        if dataset is None:
            print("No se pudo cargar el dataset.")
            return 1

        print("Dataset cargado correctamente.")
        print(f"Se utilizara un subconjunto del {SAMPLE_FRACTION:.0%} de los datos.")

        resolver_parte_1(dataset)
        return 0
    finally:
        # La sesion se cierra siempre, incluso si ocurre un error.
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
