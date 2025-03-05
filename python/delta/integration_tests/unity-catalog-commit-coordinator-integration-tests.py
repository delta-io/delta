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
import time
import uuid

"""
Run this script in root dir of repository:

===== Mandatory input from user =====
export CATALOG_NAME=___
export CATALOG_URI=___
export CATALOG_TOKEN=___
export TABLE_NAME=___
export SCHEMA=___

./run-integration-tests.py --use-local --unity-catalog-commit-coordinator-integration-tests \
    --packages \
    io.unitycatalog:unitycatalog-spark_2.12:0.2.1,org.apache.spark:spark-hadoop-cloud_2.12:3.5.4
"""

CATALOG_NAME = os.environ.get("CATALOG_NAME")
CATALOG_TOKEN = os.environ.get("CATALOG_TOKEN")
CATALOG_URI = os.environ.get("CATALOG_URI")
TABLE_NAME = os.environ.get("TABLE_NAME")
SCHEMA = os.environ.get("SCHEMA")

spark = SparkSession \
    .builder \
    .appName("coordinated_commit_tester") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "io.unitycatalog.spark.UCSingleCatalog") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.token", CATALOG_TOKEN) \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", CATALOG_URI) \
    .config(f"spark.sql.defaultCatalog", CATALOG_NAME) \
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.databricks.delta.commitcoordinator.unity-catalog.impl",
            "org.delta.catalog.UCCoordinatedCommitClient") \
    .getOrCreate()

expected_error_tag = "UNITY_CATALOG_EXTERNAL_COORDINATED_COMMITS_REQUEST_DENIED"


def create() -> None:
    try:
        spark.sql(f"CREATE TABLE {SCHEMA}.{TABLE_NAME} (a INT)")
    except Exception:
        print("[UNSUPPORTED] Creating managed table using UC commit coordinator isn't allowed")


def insert() -> None:
    try:
        spark.sql(f"INSERT INTO {SCHEMA}.{TABLE_NAME} VALUES (1), (2)")
    except Exception as error:
        assert(expected_error_tag in str(error))
        print("[UNSUPPORTED] Writing to managed table using UC commit coordinator isn't allowed")


def update() -> None:
    try:
        spark.sql(f"UPDATE {SCHEMA}.{TABLE_NAME} SET a=4")
    except Exception as error:
        assert(expected_error_tag in str(error))
        print("[UNSUPPORTED] Updating managed table using UC commit coordinator isn't allowed")


def delete() -> None:
    try:
        spark.sql(f"DELETE FROM {SCHEMA}.{TABLE_NAME} where a=1")
    except Exception as error:
        assert(expected_error_tag in str(error))
        print("[UNSUPPORTED] Deleting from managed table using UC commit coordinator isn't allowed")


def read() -> None:
    try:
        res = spark.sql(f"SELECT * FROM {SCHEMA}.{TABLE_NAME}")
    except Exception as error:
        assert(expected_error_tag in str(error))
        print("[UNSUPPORTED] Reading from managed table using UC commit coordinator isn't allowed")

create()
insert()
update()
read()
delete()
