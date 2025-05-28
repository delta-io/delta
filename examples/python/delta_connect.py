#
# Copyright (2024) The Delta Lake Project Authors.
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

"""
To run this example you must follow these steps:

Requirements:
- Using Java 17
- Spark 4.0.0
- pyspark 4.0.0
- delta-connect-server 4.0.0rc1+

(1) Download the latest Spark 4 release and start a local Spark connect server using this command:
sbin/start-connect-server.sh \
  --packages io.delta:delta-connect-server_2.13:{DELTA_VERSION},com.google.protobuf:protobuf-java:3.25.1 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.connect.extensions.relation.classes"="org.apache.spark.sql.connect.delta.DeltaRelationPlugin" \
  --conf "spark.connect.extensions.command.classes"="org.apache.spark.sql.connect.delta.DeltaCommandPlugin"
* Be sure to replace DELTA_VERSION with the version you are using

(2) Set the SPARK_REMOTE environment variable to point to your local Spark server
export SPARK_REMOTE="sc://localhost:15002"

(3) Run this file i.e. python3 examples/python/delta_connect.py

(4) Stop the Spark Connect server once QA is complete.
sbin/stop-connect-server.sh
"""

import os
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import shutil

filePath = "/tmp/delta_connect"
tableName = "delta_connect_table"

def assert_dataframe_equals(df1, df2):
    assert(df1.collect().sort() == df2.collect().sort())

def cleanup(spark):
    shutil.rmtree(filePath, ignore_errors=True)
    spark.sql(f"DROP TABLE IF EXISTS {tableName}")
    spark.sql(f"DROP TABLE IF EXISTS delta.`{filePath}`")

# --------------------- Set up Spark Connect spark session ------------------------

assert os.getenv("SPARK_REMOTE"), "Must point to Spark Connect server using SPARK_REMOTE"

spark = SparkSession.builder \
    .appName("delta_connect") \
    .remote(os.getenv("SPARK_REMOTE")) \
    .getOrCreate()

# Clean up any previous runs
cleanup(spark)

# -------------- Try reading non-existent table (should fail with an exception) ----------------

# Using forPath
try:
    DeltaTable.forPath(spark, filePath).toDF().show()
except Exception as e:
    assert "DELTA_MISSING_DELTA_TABLE" in str(e)
else:
    assert False, "Expected exception to be thrown for missing table"

# Using forName
try:
    DeltaTable.forName(spark, tableName).toDF().show()
except Exception as e:
    assert "DELTA_MISSING_DELTA_TABLE" in str(e)
else:
    assert False, "Expected exception to be thrown for missing table"

# ------------------------ Write basic table and check that results match ----------------------

# By table name.
spark.range(5).write.format("delta").saveAsTable(tableName)
assert_dataframe_equals(DeltaTable.forName(spark, tableName).toDF(), spark.range(5))
assert_dataframe_equals(spark.read.format("delta").table(tableName), spark.range(5))
assert_dataframe_equals(spark.sql(f"SELECT * FROM {tableName}"), spark.range(5))

# By table path.
spark.range(6).write.format("delta").save(filePath)
assert_dataframe_equals(DeltaTable.forPath(spark, filePath).toDF(), spark.range(6))
assert_dataframe_equals(spark.read.format("delta").load(filePath), spark.range(6))
assert_dataframe_equals(spark.sql(f"SELECT * FROM delta.`{filePath}`"), spark.range(6))

deltaTable = DeltaTable.forPath(spark, filePath)

# Update every even value by adding 100 to it.
print("########### Update to the table(add 100 to every even value) ##############")
deltaTable.update(
    condition="id % 2 == 0",
    set={"id": "id + 100"}
)

expectedUpdateResult = spark.createDataFrame([(100,), (1,), (102,), (3,), (104,), (5,)], ["id"])
assert_dataframe_equals(deltaTable.toDF(), expectedUpdateResult)
assert_dataframe_equals(spark.read.format("delta").load(filePath), expectedUpdateResult)
assert_dataframe_equals(spark.sql(f"SELECT * FROM delta.`{filePath}`"), expectedUpdateResult)

# Delete every even value.
print("######### Delete every even value ##############")
deltaTable.delete(condition="id % 2 == 0")

expectedDeleteResult = spark.createDataFrame([(1,), (3,), (5,)], ["id"])
assert_dataframe_equals(deltaTable.toDF(), expectedDeleteResult)
assert_dataframe_equals(spark.read.format("delta").load(filePath), expectedDeleteResult)
assert_dataframe_equals(spark.sql(f"SELECT * FROM delta.`{filePath}`"), expectedDeleteResult)

# Read old version of data using time travel.
print("######## Read old data using time travel ############")
oldVersionDF = spark.read.format("delta").option("versionAsOf", 0).load(filePath)

assert_dataframe_equals(oldVersionDF, spark.range(6))

# ---------------------------------- Clean up ----------------------------------------
cleanup(spark)
