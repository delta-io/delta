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
- Spark 4.0.0-preview1+
- delta-spark (python package) 4.0.0rc1+ and pyspark 4.0.0.dev1+

(1) Start a local Spark connect server using this command:
sbin/start-connect-server.sh \
  --packages org.apache.spark:spark-connect_2.13:4.0.0-preview1,io.delta:delta-connect-server_2.13:{DELTA_VERSION},io.delta:delta-spark_2.13:{DELTA_VERSION},com.google.protobuf:protobuf-java:3.25.1 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.connect.extensions.relation.classes"="org.apache.spark.sql.connect.delta.DeltaRelationPlugin" \
  --conf "spark.connect.extensions.command.classes"="org.apache.spark.sql.connect.delta.DeltaCommandPlugin"
* Be sure to replace DELTA_VERSION with the version you are using

(2) Set the SPARK_REMOTE environment variable to point to your local Spark server
export SPARK_REMOTE="sc://localhost:15002"

(3) Run this file i.e. python3 examples/python/delta_connect.py
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

# By table name
spark.range(5).write.format("delta").saveAsTable(tableName)
assert_dataframe_equals(DeltaTable.forName(spark, tableName).toDF(), spark.range(5))
assert_dataframe_equals(spark.read.format("delta").table(tableName), spark.range(5))
assert_dataframe_equals(spark.sql(f"SELECT * FROM {tableName}"), spark.range(5))

# By table path
spark.range(10).write.format("delta").save(filePath)
assert_dataframe_equals(DeltaTable.forPath(spark, filePath).toDF(), spark.range(10))
assert_dataframe_equals(spark.read.format("delta").load(filePath), spark.range(10))
assert_dataframe_equals(spark.sql(f"SELECT * FROM delta.`{filePath}`"), spark.range(10))

# ---------------------------------- Clean up ----------------------------------------
cleanup(spark)
