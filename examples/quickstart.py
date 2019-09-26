from pyspark import SparkContext
from pyspark.sql import Column, DataFrame, SparkSession, SQLContext, functions
from pyspark.sql.functions import *
from py4j.java_collections import MapConverter
from delta.tables import *
import shutil

# Create SparkContext
sc = SparkContext()
sqlContext = SQLContext(sc)

# Enable SQL for the current spark session.
spark = SparkSession \
    .builder \
    .appName("...") \
    .master("...") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

# Apache Spark 2.4.x has a known issue (SPARK-25003) that requires explicit activation
# of the extension and cloning of the session. This will unnecessary in Apache Spark 3.x.
if spark.sparkContext.version < "3.":
    spark.sparkContext._jvm.io.delta.sql.DeltaSparkSessionExtension() \
        .apply(spark._jsparkSession.extensions())
    spark = SparkSession(spark.sparkContext, spark._jsparkSession.cloneSession())

# Create a table
data = spark.range(0, 5)
data.write.format("parquet").save("/tmp/delta-table")

# Convert to delta
DeltaTable.convertToDelta(spark, "parquet.`/tmp/delta-table`")

# Read the table
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()

# Update table data
data = spark.range(5, 10)
data.write.format("delta").mode("overwrite").save("/tmp/delta-table")

deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")

# Update every even value by adding 100 to it
deltaTable.update(
  condition = expr("id % 2 == 0"),
  set = { "id": expr("id + 100") })

# Delete every even value
deltaTable.delete(condition = expr("id % 2 == 0"))

# Upsert (merge) new data
newData = spark.range(0, 20)


deltaTable.alias("oldData") \
  .merge(
    newData.alias("newData"),
    "oldData.id = newData.id") \
  .whenMatchedUpdate(set = { "id": col("newData.id") }) \
  .whenNotMatchedInsert(values = { "id": col("newData.id") }) \
  .execute()


deltaTable.toDF().show()

# Read old version of data using time travel
df = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
df.show()

# Utility commands
deltaTable.vacuum()

deltaTable.history().show()

# SQL Vacuum
spark.sql("VACUUM '%s' RETAIN 169 HOURS" % "/tmp/delta-table").collect()

# SQL describe history
spark.sql("desc history delta.`%s`" % ("/tmp/delta-table")).collect()

# cleanup
shutil.rmtree("/tmp/delta-table")
