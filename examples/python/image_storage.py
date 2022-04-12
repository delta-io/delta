# This example shows
# 1 - how to load the TensorFlow Flowers Images into a dataframe
# 2 - manipulate the dataframe
# 3 - write the dataframe to a Delta Lake table
# 4 - read the new Delta Lake table

import pyspark.sql.functions as fn
import pyspark
from delta import configure_spark_with_delta_pip

builder = (
  pyspark.sql.SparkSession.builder
    .appName('quickstart')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Flowers dataset from the TensorFlow team - https://www.tensorflow.org/datasets/catalog/tf_flowers
imagePath = "/path/to/flower_photos/"
deltaPath = "/path/to/write/flower_photos_delta_table/"

# read the images from the flowers dataset
images = spark.read.format("binaryFile").\
  option("recursiveFileLookup", "true").\
  option("pathGlobFilter", "*.jpg").\
  load(imagePath)

# Knowing the file path, extract the flower type and filename using substring_index
# Remember, Spark dataframes are immutable, here we are just reusing the images dataframe
images = images.withColumn("flowerType_filename", fn.substring_index(images.path, "/", -2))
images = images.withColumn("flowerType", fn.substring_index(images.flowerType_filename, "/", 1))
images = images.withColumn("filename", fn.substring_index(images.flowerType_filename, "/", -1))
images = images.drop("flowerType_filename")
images.show()

# Select the columns we want to write out to
df = images.select("path", "content", "flowerType", "filename").repartition(4)
df.show()

# Write out the delta table to the given path, this will overwrite any table that is currently there
df.write.format("delta").mode("overwrite").save(deltaPath)

# Reads the delta table that was just written
dfDelta = spark.read.format("delta").load(deltaPath)
dfDelta.show()
