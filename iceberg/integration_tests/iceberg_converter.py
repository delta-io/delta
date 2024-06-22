#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
import shutil
import random

testRoot = "/tmp/delta-iceberg-converter/"
warehousePath = testRoot + "iceberg_tables"
shutil.rmtree(testRoot, ignore_errors=True)

# we need to set the following configs
spark = SparkSession.builder \
    .appName("delta-iceberg-converter") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", warehousePath) \
    .getOrCreate()

table = "local.db.table"
tablePath = "file://" + warehousePath + "/db/table"

try:
    print("Creating Iceberg table with partitions...")
    spark.sql(
        "CREATE TABLE {} (id BIGINT, data STRING) USING ICEBERG PARTITIONED BY (data)".format(table))
    spark.sql("INSERT INTO {} VALUES (1, 'a'), (2, 'b')".format(table))
    spark.sql("INSERT INTO {} VALUES (3, 'c')".format(table))

    print("Converting Iceberg table to Delta table...")
    spark.sql("CONVERT TO DELTA iceberg.`{}`".format(tablePath))

    print("Reading from converted Delta table...")
    spark.read.format("delta").load(tablePath).show()

    print("Modifying the converted table...")
    spark.sql("INSERT INTO delta.`{}` VALUES (4, 'd')".format(tablePath))

    print("Reading the final Delta table...")
    spark.read.format("delta").load(tablePath).show()

    print("Create an external catalog table using Delta...")
    spark.sql("CREATE TABLE converted_delta_table USING delta LOCATION '{}'".format(tablePath))

    print("Read from the catalog table...")
    spark.read.table("converted_delta_table").show()
finally:
    # cleanup
    shutil.rmtree(testRoot, ignore_errors=True)
