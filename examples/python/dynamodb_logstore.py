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
import os
import sys
import threading

from pyspark import SparkContext
from pyspark.sql import Column, DataFrame, SparkSession, SQLContext, functions
from pyspark.sql.functions import *
from py4j.java_collections import MapConverter
from delta.tables import *
from multiprocessing.pool import ThreadPool
import time

"""
create required dynamodb table with:

$ aws --region us-west-2 dynamodb create-table \
    --table-name delta_log_test \
    --attribute-definitions AttributeName=tablePath,AttributeType=S \
                            AttributeName=fileName,AttributeType=S \
    --key-schema AttributeName=tablePath,KeyType=HASH \
                AttributeName=fileName,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

run this script in root dir of repository:

export VERSION=$(cat version.sbt|cut -d '"' -f 2)
export DELTA_CONCURRENT_WRITERS=2
export DELTA_CONCURRENT_READERS=2
export DELTA_TABLE_PATH=s3a://test-bucket/delta-test/
export DELTA_DYNAMO_TABLE=delta_log_test
export DELTA_STORAGE=io.delta.storage.DynamoDBLogStoreScala # TODO: remove `Scala` when Java version finished
export DELTA_NUM_ROWS=16

TODO: update this comment with proper delta-storage artifact ID (i.e. no _2.12 scala version)

./run-integration-tests.py \
  --test dynamodb_logstore.py \
  --python-only \
  --conf spark.jars.ivySettings=/workspace/ivy.settings \
         spark.driver.extraJavaOptions=-Dlog4j.configuration=file:debug/log4j.properties \
  --packages io.delta:delta-storage-dynamodb_2.12:${VERSION},org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.142
"""

# conf
delta_table_path = os.environ.get("DELTA_TABLE_PATH")
concurrent_writers = int(os.environ.get("DELTA_CONCURRENT_WRITERS", 2))
concurrent_readers = int(os.environ.get("DELTA_CONCURRENT_READERS", 2))
num_rows = int(os.environ.get("DELTA_NUM_ROWS", 32))

# TODO change back to default io.delta.storage.DynamoDBLogStore
delta_storage = os.environ.get("DELTA_STORAGE", "io.delta.storage.DynamoDBLogStoreScala")
dynamo_table_name = os.environ.get("DELTA_DYNAMO_TABLE", "delta_log_test")
dynamo_region = os.environ.get("DELTA_DYNAMO_REGION", "us-west-2")
dynamo_error_rates = os.environ.get("DELTA_DYNAMO_ERROR_RATES", "")

if delta_table_path is None:
    print(f"\nSkipping Python test {os.path.basename(__file__)} due to the missing env variable "
    f"`DELTA_TABLE_PATH`\n=====================")
    sys.exit(0)

test_log = f"""
--- LOG ---\n
delta table path: {delta_table_path}
concurrent writers: {concurrent_writers}
concurrent readers: {concurrent_readers}
number of rows: {num_rows}
delta storage: {delta_storage}
dynamo table name: {dynamo_table_name}
=====================
"""
print(test_log)

# TODO: update to spark.delta.DynamoDBLogStore.tableName (no `Scala`) when Java version finished

spark = SparkSession \
    .builder \
    .appName("utilities") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.delta.logStore.class", delta_storage) \
    .config("spark.delta.DynamoDBLogStoreScala.tableName", dynamo_table_name) \
    .config("spark.delta.DynamoDBLogStoreScala.region", dynamo_region) \
    .config("spark.delta.DynamoDBLogStoreScala.errorRates", dynamo_error_rates) \
    .getOrCreate()

data = spark.createDataFrame([], "id: int, a: int")
data.write.format("delta").mode("overwrite").partitionBy("id").save(delta_table_path)

def write_tx(n):
    data = spark.createDataFrame([[n, n]], "id: int, a: int")
    data.write.format("delta").mode("append").partitionBy("id").save(delta_table_path)


stop_reading = threading.Event()

def read_data():
    while not stop_reading.is_set():
        print("Reading {:d} rows ...".format(spark.read.format("delta").load(delta_table_path).distinct().count()))
        time.sleep(1)


def start_read_thread():
    thread = threading.Thread(target=read_data)
    thread.start()
    return thread


read_threads = [start_read_thread() for i in range(concurrent_readers)]

pool = ThreadPool(concurrent_writers)
start_t = time.time()
pool.map(write_tx, range(num_rows))
stop_reading.set()

for thread in read_threads:
    thread.join()

actual = spark.read.format("delta").load(delta_table_path).distinct().count()
print("Number of written rows:", actual)
assert actual == num_rows

t = time.time() - start_t
print(f"{num_rows / t:.02f} tx / sec")
