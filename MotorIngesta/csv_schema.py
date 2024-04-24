from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

format_input = "json"

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("height", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True)
])

schema_csv = {
    "format_input": format_input,
    "format_output": "csv",
    "data_landing_location": f"landing\{format_input}",
    "bronze_table_location": "bronze\csv",
    "check_point_location": "_checkpoint",
    "options_read" : {
        "inferSchema": True,
    },
    "options_write" : {
        "mergeSchema": "true",
        "header": True,
    },
    "schema": schema,
}