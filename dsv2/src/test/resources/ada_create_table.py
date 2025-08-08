from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("example")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

num_rows = 100000

output_path = "/home/ada.ma/ada_1M_rows"

# For example, generate rows with an increasing ID and some dummy data
df = spark.range(num_rows).repartition(num_rows).write.format("delta").mode("append").save(output_path)