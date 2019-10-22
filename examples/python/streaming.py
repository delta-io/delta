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
import random
import threading

# Clear previous run delta-tables
files = ["/tmp/delta-table", "/tmp/delta-table2", "/tmp/delta-table3"]
for i in files:
    try:
        shutil.rmtree(i)
    except:
        pass

# Create SparkContext
sc = SparkContext()
sqlContext = SQLContext(sc)

spark = SparkSession \
    .builder \
    .appName("streaming") \
    .master("local[*]") \
    .getOrCreate()

# Create a table(key, value) of some data
data = spark.range(8)
data = data.withColumn("value", data.id + random.randint(0, 5000))
data.write.format("delta").save("/tmp/delta-table")

# Stream writes to the table
print("####### Streaming write ######")
streamingDf = spark.readStream.format("rate").load()
stream = streamingDf.selectExpr("value as id").writeStream\
    .format("delta")\
    .option("checkpointLocation", "/tmp/checkpoint")\
    .start("/tmp/delta-table2")
stream.awaitTermination(10)
stream.stop()

# Stream reads from a table
print("##### Reading from stream ######")
stream2 = spark.readStream.format("delta").load("/tmp/delta-table2")\
    .writeStream\
    .format("console")\
    .start()
stream2.awaitTermination(10)
stream2.stop()

# Streaming aggregates in Update mode
print("####### Streaming upgrades in update mode ########")


# Function to upsert microBatchOutputDF into Delta Lake table using merge
def upsertToDelta(microBatchOutputDF, batchId):
    t = deltaTable.alias("t").merge(microBatchOutputDF.alias("s"), "s.id = t.id")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()


streamingAggregatesDF = spark.readStream.format("rate").load()\
    .withColumn("id", col("value") % 10)\
    .drop("timestamp")
# Write the output of a streaming aggregation query into Delta Lake table
deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")
print("#############  Original Delta Table ###############")
deltaTable.toDF().show()
stream3 = streamingAggregatesDF.writeStream\
    .format("delta") \
    .foreachBatch(upsertToDelta) \
    .outputMode("update") \
    .start()
stream3.awaitTermination(10)
stream3.stop()
print("########### DeltaTable after streaming upsert #########")
deltaTable.toDF().show()

# cleanup
for i in files:
    try:
        shutil.rmtree(i)
    except:
        pass
