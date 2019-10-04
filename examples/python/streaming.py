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
data = data.withColumn("key", data.id) \
           .withColumn("value", data.id + random.randint(0, 5000)) \
           .drop("id")
data.write.format("delta").save("/tmp/delta-table")

# Stream writes to the table
print("####### Streaming write ######")
streamingDf = spark.readStream.format("rate").load()
stream = streamingDf.selectExpr("value as id").writeStream \
            .format("delta") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .start("/tmp/delta-table2")
stream.awaitTermination(10)
stream.stop()

# Stream reads from a table
print("##### Reading from stream ######")
stream2 = spark.readStream.format("delta").load("/tmp/delta-table2") \
            .writeStream \
            .format("console") \
            .start()
stream2.awaitTermination(10)
stream2.stop()

# Streaming aggregates in Update mode
print("####### Streaming upgrades in update mode ########")
# Function to upsert microBatchOutputDF into Delta Lake table using merge
def upsertToDelta(microBatchOutputDF, batchId):
  t = deltaTable.alias("t") \
    .merge(microBatchOutputDF.alias("s"), "s.key = t.key") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

streamingAggregatesDF = spark.readStream.format("rate").load().withColumn("key", col("value") % 10).drop("timestamp")
# Write the output of a streaming aggregation query into Delta Lake table
deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")
print("#############  Original Delta Table ###############")
deltaTable.toDF().show()
stream3 = streamingAggregatesDF.writeStream \
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
