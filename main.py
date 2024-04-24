from MotorIngesta import MotorIngesta, schema_csv
from pyspark.sql import SparkSession

if __name__ == "__main__":
    
    # Creamos Spark Session
    spark = SparkSession.builder.appName("Crear DataFrame").getOrCreate()

    #Instanciamos el objeto
    motor = MotorIngesta()
    
    # Persistimos los datos mediante la ingesta a una capa bronze
    motor.write_stream(spark, schema_csv)

    # Comprobamos que los datos estan correctamente
    df = spark.read.format("csv").option("header", True).load(r"C:\Proyectos\MotorIngestaSpark\bronze\csv")
    df.show()