pyspark --packages io.delta:delta-core_2.12:1.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

#1. Merge operation with the feature disabled

from delta.tables import *
#define the delta table on which the merge operation is being performed
deltaTable = DeltaTable.forPath(spark, "/temp/data/customers1")
#define the table from which the records are being merged to the destination delta table
mergeDF = spark.read.format("delta").load("/temp/data/customers_merge")

sql("SET spark.databricks.delta.merge.repartitionBeforeWrite.enabled=false")
##If you want to calculate time (in seconds) between operations, use the "time" function 
import time
start = time.time()
#Merge command
deltaTable.alias("customers1").merge(mergeDF.alias("c_merge"),"customers1.customer_sk = c_merge.customer_sk").whenNotMatchedInsertAll().execute()
end = time.time()
print(f"Runtime of the program is {end - start}")

#2. Merge operation with the feature disabled
deltaTable = DeltaTable.forPath(spark, "/temp/data/customers2")
sql("SET spark.databricks.delta.merge.repartitionBeforeWrite.enabled=true")
import time
start = time.time()
deltaTable.alias("customers2").merge(mergeDF.alias("c_merge"),"customers2.customer_sk = c_merge.customer_sk").whenNotMatchedInsertAll().execute()
end = time.time()
print(f"Runtime of the program is {end - start}")