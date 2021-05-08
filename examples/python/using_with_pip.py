#
# Copyright (2020) The Delta Lake Project Authors.
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

import shutil

# flake8: noqa
from pyspark.sql import SparkSession
from delta import *

builder = SparkSession.builder \
    .appName("with-pip") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \

# This configuration tells Spark to download the Delta Lake JAR that is needed to operate
# in Spark. Use this only when the Pypi package deltalake-spark is locally installed.
# This configuration is not needed if the this python program is executed with
# spark-submit or pyspark shell with the --package arguments.
spark = configure_spark_with_delta_pip(builder).getOrCreate()


# Clear previous run's delta-tables
try:
    shutil.rmtree("/tmp/delta-table")
except:
    pass

# Create a table
print("########### Create a Parquet table ##############")
data = spark.range(0, 5)
data.write.format("parquet").save("/tmp/delta-table")

# Convert to delta
print("########### Convert to Delta ###########")
DeltaTable.convertToDelta(spark, "parquet.`/tmp/delta-table`")

# Read the table
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()

deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")
print("######## Vacuum the table ########")
deltaTable.vacuum()

# cleanup
shutil.rmtree("/tmp/delta-table")
