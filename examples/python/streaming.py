from pyspark import SparkContext
from pyspark.sql import Column, DataFrame, SparkSession, SQLContext, functions
from pyspark.sql.functions import *
from py4j.java_collections import MapConverter
from delta.tables import *
import shutil
import threading

# Create SparkContext
sc = SparkContext()
sqlContext = SQLContext(sc)

# Enable SQL for the current spark session.
spark = SparkSession \
    .builder \
    .appName("...") \
    .master("...") \
    .getOrCreate()

# Create a table
data = spark.range(0, 5)
data.write.format("delta").save("/tmp/delta-table")

# Read the table
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()

# Stream writes to the table
print("####### Streaming write ######")
streamingDf = spark.readStream.format("rate").load()
stream = streamingDf.selectExpr("value as id").writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoint").start("/tmp/delta-table2")
stream.awaitTermination(10)
stream.stop()

# Stream reads from a table
print("##### Reading from stream ######")
stream2 = spark.readStream.format("delta").load("/tmp/delta-table2").writeStream.format("console").start()
stream2.awaitTermination(10)
stream2.stop()

# Streaming aggregates in Update mode
print("####### Streaming upgrades in update mode ########")
# Function to upsert microBatchOutputDF into Delta Lake table using merge
# TODO Make a more meaningful example for streaming aggregates
def upsertToDelta(microBatchOutputDF, batchId):
  deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")

  t = deltaTable.alias("t")
  s = microBatchOutputDF.selectExpr("value as id").alias("s")

  t.merge(
      s,
      "s.id = t.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

streamingAggregatesDF = spark.readStream.format("rate").load()
# Write the output of a streaming aggregation query into Delta Lake table
stream3 = streamingAggregatesDF.writeStream \
  .format("delta") \
  .foreachBatch(upsertToDelta) \
  .outputMode("update") \
  .start()
stream3.awaitTermination(10)
stream3.stop()

# cleanup
shutil.rmtree("/tmp/delta-table2")
shutil.rmtree("/tmp/delta-table")
