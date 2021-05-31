#
# Copyright 2019 Databricks, Inc.
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

from pyspark import SparkContext
from pyspark.sql import Column, DataFrame, SparkSession, SQLContext, functions
from pyspark.sql.functions import *
from py4j.java_collections import MapConverter
from delta.tables import *
import shutil
import threading
from multiprocessing.pool import ThreadPool

import uuid

table_path = f"/tmp/delta-table-{uuid.uuid4()}"

# Clear previous run's delta-tables
try:
    shutil.rmtree(table_path)
except:
    pass

# Create SparkContext
# sc = SparkContext()
# sqlContext = SQLContext(sc)

# Enable SQL for the current spark session. we need to set the following configs to enable SQL
# Commands
# config io.delta.sql.DeltaSparkSessionExtension -- to enable custom Delta-specific SQL commands
# config parallelPartitionDiscovery.parallelism -- control the parallelism for vacuum
spark = SparkSession \
    .builder \
    .appName("utilities") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "8") \
    .config("spark.delta.logStore.class", "io.delta.storage.DynamoDBLogStore") \
    .getOrCreate()


# Apache Spark 2.4.x has a known issue (SPARK-25003) that requires explicit activation
# of the extension and cloning of the session. This will unnecessary in Apache Spark 3.x.
if spark.sparkContext.version < "3.":
    spark.sparkContext._jvm.io.delta.sql.DeltaSparkSessionExtension() \
        .apply(spark._jsparkSession.extensions())
    spark = SparkSession(spark.sparkContext, spark._jsparkSession.cloneSession())

data = spark.createDataFrame([], "id: int, a: int")
data.write.format("delta").partitionBy("id").save(table_path)

def write_tx(n):
    data = spark.createDataFrame([[n, n]], "id: int, a: int")
    data.write.format("delta").mode("append").partitionBy("id").save(table_path)

n = 32
concurrency = 4

pool = ThreadPool(concurrency)
pool.map(write_tx, range(n))

actual = spark.read.format("delta").load(table_path).count()
print("number of rows:", actual)
assert actual == n

# cleanup
shutil.rmtree(table_path)
