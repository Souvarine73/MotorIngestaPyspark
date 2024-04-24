# MotorIngesta
## Descripción
MotorIngesta es un paquete de Python diseñado para facilitar la ingesta de datos y la creación de un Delta Lake. Este paquete permite trabajar con archivos en formatos CSV, JSON y Parquet.

## Estructura del Proyecto
El proyecto consta de los siguientes archivos:

MotorIngesta.py: Este es el archivo principal donde se desarrolla el motor de ingesta de datos. Este archivo contiene la lógica principal para leer datos de la capa de aterrizaje, procesarlos y escribirlos en la capa de bronce.
csv_ingesta.py: Este archivo muestra un ejemplo de cómo sería un esquema para ingerir un archivo en formato JSON y llevarlo a la capa de bronce en formato CSV. Este archivo es útil para entender cómo se definen los esquemas para diferentes formatos de datos.
data_generation.ipynb: Este es un script de Jupyter Notebook que ayuda a generar un conjunto de datos de prueba en los formatos mencionados anteriormente. Este script puede ser útil para probar el funcionamiento del motor de ingesta.
main.py: Este es el archivo principal que ejecuta el motor y permite comprobar la creación de las capas de landing y bronze con el archivo persistido e ingerido. Este archivo es un buen punto de partida para entender cómo se utiliza el motor de ingesta.
Cómo Usar
Para usar este paquete, primero necesitarás instalar PySpark ya que el motor utiliza Spark Streaming para la ingesta de datos.

Una vez que hayas instalado PySpark, puedes ejecutar el script main.py para iniciar el motor y comenzar la ingesta de datos. El script data_generation.ipynb puede ser útil para generar un conjunto de datos de prueba.

Ejemplo de Uso
Aquí te dejo un ejemplo de cómo usar el paquete MotorIngesta:


````
python from MotorIngesta import MotorIngesta
from csv_ingesta import schema_csv

# Inicializa el motor
motor = MotorIngesta()

# Ejecuta el motor
motor.write_stream(spark, schema_csv)
```

Este código inicializa el motor de ingesta y luego lo ejecuta utilizando el esquema definido en csv_ingesta.py.
