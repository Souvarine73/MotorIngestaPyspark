from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class MotorIngesta:

    CURRENT_DIR = "C:\Proyectos\MotorIngestaSpark"

    def write_stream(self, spark: SparkSession, schema: dict):
        """
        :params: schema:
        """
        read_options = schema["options_read"]
        write_options = schema["options_write"]
        data_landing_location = f"{self.CURRENT_DIR}\{schema['data_landing_location']}"
        bronze_table_location = f"{self.CURRENT_DIR}\{schema['bronze_table_location']}"
        check_point_location = f"{self.CURRENT_DIR}\{schema['bronze_table_location']}\{schema['check_point_location']}"
        format_input = schema["format_input"]
        format_output = schema["format_output"]
        data_schema = schema["schema"]

        df = spark.readStream.format(format_input)\
                             .schema(data_schema)\
                             .options(**read_options)\
                             .load(data_landing_location)\
                             .withColumn("_ingested_at", F.current_timestamp())\
                             .withColumn("_filename", F.input_file_name())

        query = df.writeStream\
                  .outputMode("append")\
                  .format(format_output)\
                  .option("path", bronze_table_location)\
                  .option("checkpointLocation", check_point_location)\
                  .options(**write_options)\
                  .trigger(once=True)\
                  .start()

        query.awaitTermination()