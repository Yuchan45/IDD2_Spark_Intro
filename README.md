# Actividad Spark

Version simplificada del proyecto, dejando toda la logica en un solo archivo `main.py`, con un estilo parecido al ejemplo de `introCode.txt`.

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
3. Lee el archivo Parquet remoto directamente desde la URL.
4. Muestra algunas filas, el esquema y la cantidad total de registros.
5. Ejecuta consultas SQL simples sobre una vista temporal llamada `reviews`.

## Estructura

```text
appSpark/
|-- main.py
|-- introCode.txt
|-- requirements.txt
`-- README.md
```
