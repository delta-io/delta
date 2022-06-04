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
from pyspark.sql.functions import col, expr
from delta.tables import DeltaTable
import shutil


path = "/tmp/delta-change-data-feed"

# Clear any previous runs
shutil.rmtree(path, ignore_errors=True)

# Enable SQL commands and Update/Delete/Merge for the current spark session.
# we need to set the following configs
spark = SparkSession.builder \
    .appName("Change Data Feed") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


def read_cdc_by_path(starting_version):
    return spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", str(starting_version)) \
        .load(path) \
        .orderBy("_change_type", "id")


def read_cdc_by_table_name(starting_version):
    return spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", str(starting_version)) \
        .table("table") \
        .orderBy("_change_type", "id")


def stream_cdc_by_path(starting_version):
    return spark.readStream.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", str(starting_version)) \
        .load(path) \
        .writeStream \
        .format("console") \
        .start()


def stream_cdc_by_table_name(starting_version):
    return spark.readStream.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", str(starting_version)) \
        .table("table") \
        .writeStream \
        .format("console") \
        .start()

spark.sql('''CREATE TABLE table (id LONG)
             USING DELTA
             TBLPROPERTIES (delta.enableChangeDataFeed = true)
             LOCATION '{0}'
         '''.format(path))

spark.range(0, 10).write.format("delta").mode("append").save(path)  # v1

read_cdc_by_path(1).show()

spark.read.format("delta").load(path).orderBy("id").show()

table = DeltaTable.forPath(spark, path)

table.update(set={"id": expr("id + 1")})  # v2

read_cdc_by_path(2).show()

table.delete(condition=expr("id >= 5"))  # v3

read_cdc_by_table_name(3).show()

# TODO merge

print("Starting CDF stream 1")
cdfStream1 = stream_cdc_by_path(0)
cdfStream1.awaitTermination(10)
cdfStream1.stop()

print("Starting CDF stream 2")
cdfStream2 = stream_cdc_by_table_name(0)
cdfStream2.awaitTermination(10)
cdfStream2.stop()

# cleanup
shutil.rmtree(path, ignore_errors=True)

