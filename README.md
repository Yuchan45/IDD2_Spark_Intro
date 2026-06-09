# Actividad Spark

Resolucion modular de la actividad practica de Apache Spark.

## Requisitos

- Python 3.10 o superior
- Java 11 o superior
- `JAVA_HOME` configurado
- Si tu instalacion de Spark en Windows lo necesita, `HADOOP_HOME` y `winutils`

## Instalacion

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Ejecucion

```powershell
py main.py
```

## Que hace

1. Muestra la version de PySpark.
2. Abre una sesion local de Spark.
3. Descarga el archivo Parquet si no existe o esta corrupto.
4. Carga una muestra reproducible del 10%.
5. Resuelve las Partes 1 a 6 desde modulos separados.

## Estructura

```text
appSpark/
|-- data/
|-- main.py
|-- src/
|   `-- app_spark/
|       |-- consignas/
|       |   |-- parte_1.py
|       |   |-- parte_2.py
|       |   |-- parte_3.py
|       |   |-- parte_4.py
|       |   |-- parte_5.py
|       |   `-- parte_6.py
|       `-- dataset.py
|-- introCode.txt
|-- requirements.txt
`-- README.md
```
