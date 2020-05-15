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

from pyspark import SparkContext, SparkConf
from pyspark.sql import Column, DataFrame, SparkSession, SQLContext, functions
from pyspark.sql.functions import *
from py4j.java_collections import MapConverter
from delta.tables import *
import shutil
import threading

conf = SparkConf() \
    .setAppName("utilities") \
    .setMaster("local[*]")

# Enable SQL and for the current spark session. we need to set the following configs
# to enable SQL Commands
# config io.delta.sql.DeltaSparkSessionExtension -- to enable custom Delta-specific SQL commands
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Clear previous run's delta-tables
try:
    shutil.rmtree("/tmp/delta-table")
except:
    pass

# Create a table
print("########### Create a Parquet table ##############")
data = spark.range(0, 5)
data.write.format("parquet").save("/tmp/delta-table")

# Convert to delta
print("########### Convert to Delta ###########")
DeltaTable.convertToDelta(spark, "parquet.`/tmp/delta-table`")

# Read the table
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()

deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")
print("######## Vacuum the table ########")
deltaTable.vacuum()

print("######## Describe history for the table ######")
deltaTable.history().show()

# Generate manifest
print("######## Generating manifest ######")
deltaTable.generate("SYMLINK_FORMAT_MANIFEST")

# SQL Vacuum
print("####### SQL Vacuum #######")
spark.sql("VACUUM '%s' RETAIN 169 HOURS" % "/tmp/delta-table").collect()

# SQL describe history
print("####### SQL Describe History ########")
print(spark.sql("DESCRIBE HISTORY delta.`%s`" % ("/tmp/delta-table")).collect())

# cleanup
shutil.rmtree("/tmp/delta-table")
