from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
import shutil
import random
import os
import time

testRoot = "/tmp/delta-uniform-hudi/"
warehousePath = testRoot + "uniform_tables"
shutil.rmtree(testRoot, ignore_errors=True)

# we need to set the following configs
spark = SparkSession.builder \
    .appName("delta-uniform-hudi") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", warehousePath) \
    .getOrCreate()

spark.sql("""CREATE TABLE `delta_table_with_hudi` (col1 INT) USING DELTA
  TBLPROPERTIES('delta.universalFormat.enabledFormats' = 'hudi') """)

spark.sql("""INSERT INTO `delta_table_with_hudi` VALUES (1); """)

time.sleep(5)

hudiTablePath = warehousePath + "/" + "delta_table_with_hudi"

hudiMetadataPath = hudiTablePath + "/.hoodie/metadata/files"

assert len(os.listdir(hudiMetadataPath)) > 0

# TODO: read with Hudi Spark to verify table content after Hudi supports Spark 3.5+