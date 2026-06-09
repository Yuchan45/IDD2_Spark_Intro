# Actividad Apache Spark

Resolucion de la actividad practica de Ingenieria de Datos II para analizar
resenas de Amazon con PySpark.

El programa descarga el dataset cuando es necesario y trabaja con una muestra
aleatoria reproducible del 10%, debido a que el archivo original pesa
aproximadamente 8.8 GB.

## Requisitos

- Python 3.10 o superior.
- Java JDK 17.
- Hadoop para Windows con `winutils.exe`.
- Espacio disponible para descargar el dataset.

Las rutas de Java y Hadoop utilizadas por el proyecto se encuentran en
`main.py`:

```python
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["HADOOP_HOME"] = r"C:\Hadoop\hadoop-3.3.6"
```

Estas rutas deben ajustarse si las herramientas estan instaladas en otra
ubicacion.

## Instalacion

Desde PowerShell, en la carpeta del proyecto:

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
```

Las dependencias principales son:

- `pyspark`: procesamiento y consultas del dataset.
- `pyarrow`: escritura portable del resultado Parquet en Windows.

## Ejecucion

```powershell
py main.py
```

En la primera ejecucion, el programa:

1. Comprueba si el dataset local existe y es un Parquet valido.
2. Descarga el archivo si falta o esta corrupto.
3. Crea una sesion local de Spark con dos nucleos.
4. Selecciona una muestra reproducible del 10% con `seed=42`.
5. Ejecuta las consignas en orden.

Las barras con el formato `[Stage N: ...]` muestran el progreso de las tareas
que Spark ejecuta en paralelo.

## Consignas

- **Parte 1:** carga, primeras filas, esquema, columnas y tipos.
- **Parte 2:** registros, productos, categorias y distribucion de ratings.
- **Parte 3:** promedios globales y por categoria.
- **Parte 4:** consultas equivalentes con Spark SQL.
- **Parte 5:** actividad de usuarios y compras verificadas.
- **Parte 6:** analisis de votos utiles.
- **Parte 7:** comparacion entre lectura completa y seleccion parcial de
  columnas, tiempos y planes fisicos.
- **Parte 8:** generacion, persistencia y validacion de un dataset agregado.

## Archivos generados

El dataset original se almacena en:

```text
data/amazon_reviews_2015.snappy.parquet
```

La Parte 8 genera:

```text
output/promedio_rating_por_categoria.parquet
```

Las carpetas `data/` y `output/`, junto con todos los archivos `.parquet`,
estan excluidas de Git porque contienen datos grandes o generados.

## Estructura

```text
appSpark/
|-- data/
|-- output/
|-- main.py
|-- src/
|   `-- app_spark/
|       |-- dataset.py
|       `-- consignas/
|           |-- parte_1.py
|           |-- parte_2.py
|           |-- parte_3.py
|           |-- parte_4.py
|           |-- parte_5.py
|           |-- parte_6.py
|           |-- parte_7.py
|           `-- parte_8.py
|-- requirements.txt
`-- README.md
```

`main.py` contiene la configuracion del entorno, crea la sesion de Spark y
ejecuta cada parte. `dataset.py` se encarga de descargar, validar y cargar el
dataset. Cada consigna se encuentra separada en su propio modulo.
