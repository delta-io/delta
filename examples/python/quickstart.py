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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from delta.tables import DeltaTable
import shutil

# Clear any previous runs
try:
    shutil.rmtree("/tmp/delta-table")
except:
    pass

# Enable SQL commands and Update/Delete/Merge for the current spark session.
# we need to set the following configs
spark = SparkSession.builder \
    .appName("quickstart") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Create a table
print("############# Creating a table ###############")
data = spark.range(0, 5)
data.write.format("delta").save("/tmp/delta-table")

# Read the table
print("############ Reading the table ###############")
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()

# Upsert (merge) new data
print("########### Upsert new data #############")
newData = spark.range(0, 20)

deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")

deltaTable.alias("oldData")\
    .merge(
    newData.alias("newData"),
    "oldData.id = newData.id")\
    .whenMatchedUpdate(set={"id": col("newData.id")})\
    .whenNotMatchedInsert(values={"id": col("newData.id")})\
    .execute()

deltaTable.toDF().show()

# Update table data
print("########## Overwrite the table ###########")
data = spark.range(5, 10)
data.write.format("delta").mode("overwrite").save("/tmp/delta-table")
deltaTable.toDF().show()

deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")

# Update every even value by adding 100 to it
print("########### Update to the table(add 100 to every even value) ##############")
deltaTable.update(
    condition=expr("id % 2 == 0"),
    set={"id": expr("id + 100")})

deltaTable.toDF().show()

# Delete every even value
print("######### Delete every even value ##############")
deltaTable.delete(condition=expr("id % 2 == 0"))
deltaTable.toDF().show()

# Read old version of data using time travel
print("######## Read old data using time travel ############")
df = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
df.show()

# cleanup
shutil.rmtree("/tmp/delta-table")
