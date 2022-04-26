#
# Copyright (2021) The Delta Lake Project Authors.
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

from pyspark.sql import SparkSession
from multiprocessing.pool import ThreadPool
import time

"""
Create required dynamodb table with:

$ aws --region us-west-2 dynamodb create-table \
    --table-name delta_log_test \
    --attribute-definitions AttributeName=tablePath,AttributeType=S \
                            AttributeName=fileName,AttributeType=S \
    --key-schema AttributeName=tablePath,KeyType=HASH \
                AttributeName=fileName,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

Run this script in root dir of repository:

export VERSION=$(cat version.sbt|cut -d '"' -f 2)
export DELTA_CONCURRENT_WRITERS=2
export DELTA_CONCURRENT_READERS=2
export DELTA_TABLE_PATH=s3a://test-bucket/delta-test/
export DELTA_DYNAMO_TABLE=delta_log_test
export DELTA_DYNAMO_REGION=us-west-2
export DELTA_STORAGE=io.delta.storage.S3DynamoDBLogStore
export DELTA_NUM_ROWS=16

./run-integration-tests.py --run-storage-s3-dynamodb-integration-tests \
    --dbb-packages org.apache.hadoop:hadoop-aws:3.3.1 \
    --dbb-conf spark.jars.ivySettings=/workspace/ivy.settings \
        spark.driver.extraJavaOptions=-Dlog4j.configuration=file:debug/log4j.properties
"""

# conf
delta_table_path = os.environ.get("DELTA_TABLE_PATH")
concurrent_writers = int(os.environ.get("DELTA_CONCURRENT_WRITERS", 2))
concurrent_readers = int(os.environ.get("DELTA_CONCURRENT_READERS", 2))
num_rows = int(os.environ.get("DELTA_NUM_ROWS", 16))

# className to instantiate. io.delta.storage.S3DynamoDBLogStore or .FailingS3DynamoDBLogStore
delta_storage = os.environ.get("DELTA_STORAGE", "io.delta.storage.S3DynamoDBLogStore")
dynamo_table_name = os.environ.get("DELTA_DYNAMO_TABLE", "delta_log_test")
dynamo_region = os.environ.get("DELTA_DYNAMO_REGION", "us-west-2")
# used only by FailingS3DynamoDBLogStore
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
{"dynamo_error_rates: {}".format(dynamo_error_rates) if dynamo_error_rates else ""}
=====================
"""
print(test_log)

spark = SparkSession \
    .builder \
    .appName("utilities") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.delta.logStore.s3.impl", delta_storage) \
    .config("spark.delta.logStore.s3a.impl", delta_storage) \
    .config("spark.delta.logStore.s3n.impl", delta_storage) \
    .config("spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName", dynamo_table_name) \
    .config("spark.io.delta.storage.S3DynamoDBLogStore.ddb.region", dynamo_region) \
    .config("spark.io.delta.storage.S3DynamoDBLogStore.errorRates", dynamo_error_rates) \
    .config("spark.io.delta.storage.S3DynamoDBLogStore.provisionedThroughput.rcu", 12) \
    .config("spark.io.delta.storage.S3DynamoDBLogStore.provisionedThroughput.wcu", 13) \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

data = spark.createDataFrame([], "id: int, a: int")
print("writing:", data.collect())
data.write.format("delta").mode("overwrite").partitionBy("id").save(delta_table_path)


def write_tx(n):
    data = spark.createDataFrame([[n, n]], "id: int, a: int")
    print("writing:", data.collect())
    data.write.format("delta").mode("append").partitionBy("id").save(delta_table_path)


stop_reading = threading.Event()


def read_data():
    while not stop_reading.is_set():
        print("Reading {:d} rows ...".format(
            spark.read.format("delta").load(delta_table_path).distinct().count())
        )
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

import boto3
from botocore.config import Config
my_config = Config(
    region_name=dynamo_region,
)
dynamodb = boto3.resource('dynamodb',  config=my_config)
table = dynamodb.Table(dynamo_table_name)  # this ensures we actually used/created the input table
response = table.scan()
items = response['Items']
print(items[-1])  # print for manual validation
