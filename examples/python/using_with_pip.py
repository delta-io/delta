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

import shutil

# flake8: noqa
import os
from pyspark.sql import SparkSession
from delta import *

builder = SparkSession.builder \
    .appName("with-pip") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.repositories", \
        ("https://maven-central.storage-download.googleapis.com/maven2/,"
            "https://repo1.maven.org/maven2/")\
    )

# This is only for testing staged release artifacts. Ignore this completely.
if os.getenv('EXTRA_MAVEN_REPO'):
    builder = builder.config("spark.jars.repositories", os.getenv('EXTRA_MAVEN_REPO'))

# This configuration tells Spark to download the Delta Lake JAR that is needed to operate
# in Spark. Use this only when the Pypi package Delta Lake is locally installed with pip.
# This configuration is not needed if the this python program is executed with
# spark-submit or pyspark shell with the --package arguments.
spark = configure_spark_with_delta_pip(builder).getOrCreate()


# Clear previous run's delta-tables
shutil.rmtree("/tmp/delta-table", ignore_errors=True)

print("########### Create a Parquet table ##############")
data = spark.range(0, 5)
data.write.format("parquet").save("/tmp/delta-table")

print("########### Convert to Delta ###########")
DeltaTable.convertToDelta(spark, "parquet.`/tmp/delta-table`")

print("########### Read table with DataFrames ###########")
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()

print("########### Read table with DeltaTable ###########")
deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")
deltaTable.toDF().show()

spark.stop()

# cleanup
shutil.rmtree("/tmp/delta-table")
