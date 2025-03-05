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

$ aws dynamodb create-table \
    --region <region> \
    --table-name <table_name> \
    --attribute-definitions AttributeName=tablePath,AttributeType=S \
                            AttributeName=fileName,AttributeType=S \
    --key-schema AttributeName=tablePath,KeyType=HASH \
                AttributeName=fileName,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
    
Enable TTL with:

$ aws dynamodb update-time-to-live \
  --region <region> \
  --table-name <table_name> \
  --time-to-live-specification "Enabled=true, AttributeName=expireTime"

Run this script in root dir of repository:

# ===== Mandatory input from user =====
export RUN_ID=run001
export S3_BUCKET=delta-lake-dynamodb-test-00

# ===== Optional input from user =====
export DELTA_CONCURRENT_WRITERS=20
export DELTA_CONCURRENT_READERS=2
export DELTA_STORAGE=io.delta.storage.S3DynamoDBLogStore
export DELTA_NUM_ROWS=200
export DELTA_DYNAMO_REGION=us-west-2
export DELTA_DYNAMO_ERROR_RATES=0.00

# ===== Optional input from user (we calculate defaults using S3_BUCKET and RUN_ID) =====
export RELATIVE_DELTA_TABLE_PATH=___
export DELTA_DYNAMO_TABLE_NAME=___

./run-integration-tests.py --use-local \
    --run-storage-s3-dynamodb-integration-tests \
    --dbb-packages org.apache.hadoop:hadoop-aws:3.4.0 \
    --dbb-conf io.delta.storage.credentials.provider=software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider \
               spark.hadoop.fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
"""

# ===== Mandatory input from user =====
run_id = os.environ.get("RUN_ID")
s3_bucket = os.environ.get("S3_BUCKET")

# ===== Optional input from user =====
concurrent_writers = int(os.environ.get("DELTA_CONCURRENT_WRITERS", 2))
concurrent_readers = int(os.environ.get("DELTA_CONCURRENT_READERS", 2))
# className to instantiate. io.delta.storage.S3DynamoDBLogStore or .FailingS3DynamoDBLogStore
delta_storage = os.environ.get("DELTA_STORAGE", "io.delta.storage.S3DynamoDBLogStore")
num_rows = int(os.environ.get("DELTA_NUM_ROWS", 16))
dynamo_region = os.environ.get("DELTA_DYNAMO_REGION", "us-west-2")
# used only by FailingS3DynamoDBLogStore
dynamo_error_rates = os.environ.get("DELTA_DYNAMO_ERROR_RATES", "")

# ===== Optional input from user (we calculate defaults using RUN_ID) =====
relative_delta_table_path = os.environ.get("RELATIVE_DELTA_TABLE_PATH", "tables/table_" + run_id)\
    .rstrip("/")
dynamo_table_name = os.environ.get("DELTA_DYNAMO_TABLE_NAME", "ddb_table_" + run_id)

delta_table_path = "s3a://" + s3_bucket + "/" + relative_delta_table_path
relative_delta_log_path = relative_delta_table_path + "/_delta_log/"

if delta_table_path is None:
    print(f"\nSkipping Python test {os.path.basename(__file__)} due to the missing env variable "
          f"`DELTA_TABLE_PATH`\n=====================")
    sys.exit(0)

test_log = f"""
========================================== 
run id: {run_id}
delta table path: {delta_table_path}
dynamo table name: {dynamo_table_name}

concurrent writers: {concurrent_writers}
concurrent readers: {concurrent_readers}
number of rows: {num_rows}
delta storage: {delta_storage}
dynamo_error_rates: {dynamo_error_rates}

relative_delta_table_path: {relative_delta_table_path}
relative_delta_log_path: {relative_delta_log_path}
========================================== 
"""
print(test_log)

spark = SparkSession \
    .builder \
    .appName("utilities") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.s3.impl", delta_storage) \
    .config("spark.delta.logStore.s3a.impl", delta_storage) \
    .config("spark.delta.logStore.s3n.impl", delta_storage) \
    .config("spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName", dynamo_table_name) \
    .config("spark.io.delta.storage.S3DynamoDBLogStore.ddb.region", dynamo_region) \
    .config("spark.io.delta.storage.S3DynamoDBLogStore.errorRates", dynamo_error_rates) \
    .config("spark.io.delta.storage.S3DynamoDBLogStore.provisionedThroughput.rcu", 12) \
    .config("spark.io.delta.storage.S3DynamoDBLogStore.provisionedThroughput.wcu", 13) \
    .getOrCreate()

# spark.sparkContext.setLogLevel("INFO")

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


print("===================== Starting reads and writes =====================")
read_threads = [start_read_thread() for i in range(concurrent_readers)]
pool = ThreadPool(concurrent_writers)
start_t = time.time()
pool.map(write_tx, range(num_rows))
stop_reading.set()

for thread in read_threads:
    thread.join()

print("===================== Evaluating number of written rows =====================")
actual = spark.read.format("delta").load(delta_table_path).distinct().count()
print("Actual number of written rows:", actual)
print("Expected number of written rows:", num_rows)
assert actual == num_rows

t = time.time() - start_t
print(f"{num_rows / t:.02f} tx / sec")

print("===================== Evaluating DDB writes =====================")
import boto3
from botocore.config import Config
my_config = Config(
    region_name=dynamo_region,
)
dynamodb = boto3.resource('dynamodb',  config=my_config)
table = dynamodb.Table(dynamo_table_name)  # this ensures we actually used/created the input table
response = table.scan()
items = response['Items']
items = sorted(items, key=lambda x: x['fileName'])

print("========== All DDB items ==========")
for item in items:
    print(item)

print("===================== Evaluating _delta_log commits =====================")
s3_client = boto3.client("s3")
print(f"querying {s3_bucket}/{relative_delta_log_path}")
response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=relative_delta_log_path)
items = response['Contents']
print("========== Raw _delta_log contents ========== ")
for item in items:
    print(item)

delta_log_commits = filter(lambda x: ".json" in x['Key'] and ".tmp" not in x['Key'],
                           items)
delta_log_commits = sorted(delta_log_commits, key=lambda x: x['Key'])

print("========== _delta_log commits in version order ==========")
for commit in delta_log_commits:
    print(commit)

print("========== _delta_log commits in timestamp order ==========")
delta_log_commits_sorted_timestamp = sorted(delta_log_commits, key=lambda x: x['LastModified'])
for commit in delta_log_commits_sorted_timestamp:
    print(commit)

print("========== ASSERT that these orders (version vs timestamp) are the same ==========")
assert(delta_log_commits == delta_log_commits_sorted_timestamp)
