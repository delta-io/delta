from pyspark import SparkContext, SparkConf
from pyspark.sql import Column, DataFrame, SparkSession, SQLContext, functions
from pyspark.sql.functions import *
from py4j.java_collections import MapConverter
from delta.tables import *
import shutil
import threading

tableName = "tbltestpython"

conf = SparkConf() \
    .setAppName("quickstart-metastore-sql") \
    .setMaster("local[*]") 

# Enable SQL/DML commands and Metastore tables for the current spark session.
# We need to set the following configs 

conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Clear any previous runs
try:
    spark.sql("DROP TABLE " + tableName)
except:
    pass

# Create a table
print("############# Creating a table ###############")
spark.range(0, 5).write.format("delta").mode("overwrite").saveAsTable(tableName)

# Read the table
print("############ Reading the table ###############")
spark.sql("SELECT * FROM %s" % tableName).show()

# Upsert (merge) new data
print("########### Upsert new data #############")
newData = spark.range(0, 20).write.format("delta").mode("overwrite").saveAsTable("newData")

spark.sql('''MERGE INTO {0} USING newData
        ON {0}.id = newData.id
        WHEN MATCHED THEN 
          UPDATE SET {0}.id = newData.id
        WHEN NOT MATCHED THEN INSERT *
    '''.format(tableName))

spark.sql("SELECT * FROM %s" % tableName).show()

# Update table data
print("########## Overwrite the table ###########")
data = spark.range(5, 10)
data.write.format("delta").mode("overwrite").saveAsTable(tableName)
spark.sql("SELECT * FROM %s" % tableName).show()


# Update every even value by adding 100 to it
print("########### Update to the table(add 100 to every even value) ##############")
spark.sql("UPDATE {0} SET id = (id + 100) WHERE (id % 2 == 0)".format(tableName))
spark.sql("SELECT * FROM %s" % tableName).show()

# Delete every even value
print("######### Delete every even value ##############")
spark.sql("DELETE FROM {0} WHERE (id % 2 == 0)".format(tableName))
spark.sql("SELECT * FROM %s" % tableName).show()


# Read old version of data using time travel
print("######## Read old data using time travel ############")
df = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
df.show()

# cleanup
try:
    spark.sql("DROP TABLE " + tableName)
except:
    pass
