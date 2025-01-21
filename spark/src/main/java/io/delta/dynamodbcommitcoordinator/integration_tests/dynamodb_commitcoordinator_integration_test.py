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

import os
import sys
import threading
import json

from pyspark.sql import SparkSession
from multiprocessing.pool import ThreadPool
import time
import boto3
import uuid

"""

Run this script in root dir of repository:

# ===== Mandatory input from user =====
export RUN_ID=run001
export S3_BUCKET=delta-lake-dynamodb-test-00
export AWS_DEFAULT_REGION=us-west-2

# ===== Optional input from user =====
export DELTA_CONCURRENT_WRITERS=20
export DELTA_CONCURRENT_READERS=2
export DELTA_NUM_ROWS=200
export DELTA_DYNAMO_ENDPOINT=https://dynamodb.us-west-2.amazonaws.com

# ===== Optional input from user (we calculate defaults using S3_BUCKET and RUN_ID) =====
export RELATIVE_DELTA_TABLE_PATH=___
export DELTA_DYNAMO_TABLE_NAME=___

./run-integration-tests.py --use-local --run-dynamodb-commit-coordinator-integration-tests \
    --dbb-packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --dbb-conf io.delta.storage.credentials.provider=com.amazonaws.auth.profile.ProfileCredentialsProvider \
               spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.profile.ProfileCredentialsProvider
"""

# ===== Mandatory input from user =====
run_id = os.environ.get("RUN_ID")
s3_bucket = os.environ.get("S3_BUCKET")

# ===== Optional input from user =====
concurrent_writers = int(os.environ.get("DELTA_CONCURRENT_WRITERS", 2))
concurrent_readers = int(os.environ.get("DELTA_CONCURRENT_READERS", 2))
num_rows = int(os.environ.get("DELTA_NUM_ROWS", 16))
dynamo_endpoint = os.environ.get("DELTA_DYNAMO_ENDPOINT", "https://dynamodb.us-west-2.amazonaws.com")

# ===== Optional input from user (we calculate defaults using RUN_ID) =====
relative_delta_table_path = os.environ.get("RELATIVE_DELTA_TABLE_PATH", f"tables/table_ddb_cs_{run_id}_{str(uuid.uuid4())}")\
    .rstrip("/")
dynamo_table_name = os.environ.get("DELTA_DYNAMO_TABLE_NAME", "test_ddb_cs_table_" + run_id)

relative_delta_table1_path = relative_delta_table_path + "_tab1"
relative_delta_table2_path = relative_delta_table_path + "_tab2"
bucket_prefix = "s3a://" + s3_bucket + "/"
delta_table1_path = bucket_prefix + relative_delta_table1_path
delta_table2_path = bucket_prefix + relative_delta_table2_path

if delta_table1_path is None:
    print(f"\nSkipping Python test {os.path.basename(__file__)} due to the missing env variable "
          f"`DELTA_TABLE_PATH`\n=====================")
    sys.exit(0)

dynamodb_commit_coordinator_conf = json.dumps({
    "dynamoDBTableName": dynamo_table_name,
    "dynamoDBEndpoint": dynamo_endpoint
})

test_log = f"""
==========================================
run id: {run_id}
delta table1 path: {delta_table1_path}
delta table2 path: {delta_table1_path}
dynamo table name: {dynamo_table_name}

concurrent writers: {concurrent_writers}
concurrent readers: {concurrent_readers}
number of rows: {num_rows}

relative_delta_table_path: {relative_delta_table_path}
==========================================
"""
print(test_log)

commit_coordinator_property_key = "coordinatedCommits.commitCoordinator"
property_key_suffix = "-preview"

spark = SparkSession \
    .builder \
    .appName("utilities") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config(f"spark.databricks.delta.properties.defaults.{commit_coordinator_property_key}{property_key_suffix}", "dynamodb") \
    .config(f"spark.databricks.delta.properties.defaults.coordinatedCommits.commitCoordinatorConf{property_key_suffix}", dynamodb_commit_coordinator_conf) \
    .config(f"spark.databricks.delta.coordinatedCommits.commitCoordinator.dynamodb.awsCredentialsProviderName", "com.amazonaws.auth.profile.ProfileCredentialsProvider") \
    .getOrCreate()

print("Creating table at path ", delta_table1_path)
spark.sql(f"CREATE table delta.`{delta_table1_path}` (id int, a int) USING DELTA") # commit 0


def write_tx(n):
    print("writing:", [n, n])
    spark.sql(f"INSERT INTO delta.`{delta_table1_path}` VALUES ({n}, {n})")


stop_reading = threading.Event()


def read_data():
    while not stop_reading.is_set():
        print("Reading {:d} rows ...".format(
            spark.read.format("delta").load(delta_table1_path).distinct().count())
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
actual = spark.read.format("delta").load(delta_table1_path).distinct().count()
print("Actual number of written rows:", actual)
print("Expected number of written rows:", num_rows)
assert actual == num_rows

t = time.time() - start_t
print(f"{num_rows / t:.02f} tx / sec")

current_table_version = num_rows
dynamodb = boto3.resource('dynamodb', endpoint_url=dynamo_endpoint)
ddb_table = dynamodb.Table(dynamo_table_name)

def get_dynamo_db_table_entry_id(table_path):
    table_properties = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").select("properties").collect()[0][0]
    table_conf = table_properties.get(f"delta.coordinatedCommits.tableConf{property_key_suffix}", None)
    if table_conf is None:
        return None
    return json.loads(table_conf).get("tableId", None)

def validate_table_version_as_per_dynamodb(table_path, expected_version):
    table_id = get_dynamo_db_table_entry_id(table_path)
    assert table_id is not None
    print(f"Validating table version for tableId: {table_id}")
    item = ddb_table.get_item(
        Key={
            'tableId': table_id
        },
        AttributesToGet = ['tableVersion']
    )['Item']
    current_table_version = int(item['tableVersion'])
    assert current_table_version == expected_version

delta_table_version = num_rows
validate_table_version_as_per_dynamodb(delta_table1_path, delta_table_version)

def perform_insert_and_validate(table_path, insert_value):
    spark.sql(f"INSERT INTO delta.`{table_path}` VALUES ({insert_value}, {insert_value})")
    res = spark.sql(f"SELECT 1 FROM delta.`{table_path}` WHERE id = {insert_value} AND a = {insert_value}").collect()
    assert(len(res) == 1)

def check_for_delta_file_in_filesystem(delta_table_path, version, is_backfilled, should_exist):
    # Check for backfilled commit
    s3_client = boto3.client("s3")
    relative_table_path = delta_table_path.replace(bucket_prefix, "")
    relative_delta_log_path = relative_table_path + "/_delta_log/"
    relative_commit_folder_path = relative_delta_log_path if is_backfilled else os.path.join(relative_delta_log_path, "_commits")
    listing_prefix = os.path.join(relative_commit_folder_path, f"{version:020}.").lstrip("/")
    print(f"querying {listing_prefix} from bucket {s3_bucket} for version {version}")
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=listing_prefix)
    if 'Contents' not in response:
        assert(not should_exist, f"Listing for prefix {listing_prefix} did not return any files even though it should have.")
        return
    items = response['Contents']
    commits = filter(lambda key: ".json" in key and ".tmp" not in key, map(lambda x: os.path.basename(x['Key']), items))
    expected_count = 1 if should_exist else 0
    matching_files = list(filter(lambda key: key.split('.')[0].endswith(f"{version:020}"), commits))
    assert(len(matching_files) == expected_count)

def test_downgrades_and_upgrades(delta_table_path, delta_table_version):
    # Downgrade to filesystem based commits should work
    print("===================== Evaluating downgrade to filesystem based commits =====================")
    spark.sql(f"ALTER TABLE delta.`{delta_table_path}` UNSET TBLPROPERTIES ('delta.{commit_coordinator_property_key}{property_key_suffix}')")
    delta_table_version += 1

    perform_insert_and_validate(delta_table_path, 9990)
    delta_table_version += 1

    check_for_delta_file_in_filesystem(delta_table_path, delta_table_version, is_backfilled=True, should_exist=True)
    # No UUID delta file should have been created for this version
    check_for_delta_file_in_filesystem(delta_table_path, delta_table_version, is_backfilled=False, should_exist=False)
    print("[SUCCESS] Downgrade to filesystem based commits worked")

    # Upgrade to coordinated commits should work
    print("===================== Evaluating upgrade to coordinated commits =====================")
    spark.sql(f"ALTER TABLE delta.`{delta_table_path}` SET TBLPROPERTIES ('delta.{commit_coordinator_property_key}{property_key_suffix}' = 'dynamodb')")
    delta_table_version += 1
    check_for_delta_file_in_filesystem(delta_table_path, delta_table_version, is_backfilled=True, should_exist=True)
    # No UUID delta file should have been created for the enablement commit
    check_for_delta_file_in_filesystem(delta_table_path, delta_table_version, is_backfilled=False, should_exist=False)

    perform_insert_and_validate(delta_table_path, 9991)
    delta_table_version += 1
    check_for_delta_file_in_filesystem(delta_table_path, delta_table_version, is_backfilled=True, should_exist=True)
    check_for_delta_file_in_filesystem(delta_table_path, delta_table_version, is_backfilled=False, should_exist=True)

    perform_insert_and_validate(delta_table_path, 9992)
    delta_table_version += 1
    check_for_delta_file_in_filesystem(delta_table_path, delta_table_version, is_backfilled=True, should_exist=True)
    check_for_delta_file_in_filesystem(delta_table_path, delta_table_version, is_backfilled=False, should_exist=True)
    validate_table_version_as_per_dynamodb(delta_table_path, delta_table_version)

    print("[SUCCESS] Upgrade to coordinated commits worked")

test_downgrades_and_upgrades(delta_table1_path, delta_table_version)



print("[SUCCESS] All tests passed for Table 1")

print("===================== Evaluating Table 2 =====================")

# Table 2 is created with coordinated commits disabled
spark.conf.unset(f"spark.databricks.delta.properties.defaults.{commit_coordinator_property_key}{property_key_suffix}")

spark.sql(f"CREATE table delta.`{delta_table2_path}` (id int, a int) USING DELTA") # commit 0
table_2_version = 0

perform_insert_and_validate(delta_table2_path, 8000)
table_2_version += 1

check_for_delta_file_in_filesystem(delta_table2_path, table_2_version, is_backfilled=True, should_exist=True)
# No UUID delta file should have been created for this version
check_for_delta_file_in_filesystem(delta_table2_path, table_2_version, is_backfilled=False, should_exist=False)

print("===================== Evaluating Upgrade of Table 2 =====================")

spark.sql(f"ALTER TABLE delta.`{delta_table2_path}` SET TBLPROPERTIES ('delta.{commit_coordinator_property_key}{property_key_suffix}' = 'dynamodb')")
table_2_version += 1

perform_insert_and_validate(delta_table2_path, 8001)
table_2_version += 1

check_for_delta_file_in_filesystem(delta_table2_path, table_2_version, is_backfilled=True, should_exist=True)
# This version should have a UUID delta file
check_for_delta_file_in_filesystem(delta_table2_path, table_2_version, is_backfilled=True, should_exist=True)

test_downgrades_and_upgrades(delta_table2_path, table_2_version)

print("[SUCCESS] All tests passed for Table 2")
