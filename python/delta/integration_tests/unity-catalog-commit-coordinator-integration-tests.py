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
import unittest

import py4j.protocol
from pyspark.sql import SparkSession, DataFrame
import datetime
import uuid

from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType, StructType, StructField
from pyspark.testing import assertDataFrameEqual

"""
Run this script in root dir of repository:

===== Mandatory input from user =====
export CATALOG_TOKEN=___
export CATALOG_URI=___
export CATALOG_NAME=___
export SCHEMA=___
export MANAGED_CC_TABLE=___
export MANAGED_NON_CC_TABLE=___

./run-integration-tests.py --use-local --unity-catalog-commit-coordinator-integration-tests \
    --packages \
    io.unitycatalog:unitycatalog-spark_2.12:0.2.1,org.apache.spark:spark-hadoop-cloud_2.12:3.5.4
"""

CATALOG_NAME = os.environ.get("CATALOG_NAME")
CATALOG_TOKEN = os.environ.get("CATALOG_TOKEN")
CATALOG_URI = os.environ.get("CATALOG_URI")
MANAGED_CC_TABLE = os.environ.get("MANAGED_CC_TABLE")
SCHEMA = os.environ.get("SCHEMA")
MANAGED_NON_CC_TABLE = os.environ.get("MANAGED_NON_CC_TABLE")


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
    .config("spark.databricks.delta.replaceWhere.constraintCheck.enabled", True) \
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.databricks.delta.commitcoordinator.unity-catalog.impl",
            "org.delta.catalog.UCCoordinatedCommitClient") \
    .getOrCreate()


EXTERNAL_CLIENT_DELETE_ERROR = \
    f"Request to delete non-external table '{CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE}' " \
    "from outside of Databricks Unity Catalog enabled compute environment is not supported."
S3_FORBIDDEN_ACCESS_ERROR = ("Forbidden (Service: Amazon S3; Status Code: 403; "
                             "Error Code: 403 Forbidden;")
WRITE_FAILED_ERROR = "[TASK_WRITE_FAILED] Task failed while writing rows to s3"


def read(table_name) -> DataFrame:
    return spark.read.table(f"{CATALOG_NAME}.{SCHEMA}.{table_name}")


def read_with_timestamp(timestamp, table_name) -> DataFrame:
    return spark.read.option("timestampAsOf", timestamp).table(
        f"{CATALOG_NAME}.{SCHEMA}.{table_name}")


def read_with_cdf_timestamp(timestamp, table_name) -> DataFrame:
    return spark.read.option('readChangeFeed', 'true').option(
        "startingTimestamp", timestamp).table(f"{CATALOG_NAME}.{SCHEMA}.{table_name}")


def create_df_with_rows(list_of_rows) -> DataFrame:
    return spark.createDataFrame(list_of_rows,
                                 schema=StructType([StructField("id", IntegerType(), True)]))


class UnityCatalogCommitCoordinatorTestSuite(unittest.TestCase):
    setup_df = spark.createDataFrame([(1, ), (2, ), (3, )],
                                     schema=StructType([StructField("id", IntegerType(), True)]))

    def setUp(self) -> None:
        self.setup_df.write.mode("overwrite").insertInto(
            f"{CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE}")

    # DML Operations #
    def test_update(self) -> None:
        spark.sql(f"UPDATE {CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE} SET id=4 WHERE id=1")
        updated_tbl = read(MANAGED_CC_TABLE).toDF("id")
        assertDataFrameEqual(updated_tbl, create_df_with_rows([(4, ), (2, ), (3, )]))

    def test_delete(self) -> None:
        spark.sql(f"DELETE FROM {CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE} where id=1")
        updated_tbl = read(MANAGED_CC_TABLE).toDF("id")
        assertDataFrameEqual(updated_tbl, create_df_with_rows([(2, ), (3, )]))

    def test_merge(self) -> None:
        spark.sql(f"MERGE INTO {CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE} AS target "
                  f"USING (VALUES 2, 3, 4, 5 AS src(id)) AS src "
                  f"ON src.id = target.id WHEN NOT MATCHED THEN INSERT *;")
        updated_tbl = read(MANAGED_CC_TABLE).toDF("id")
        assertDataFrameEqual(updated_tbl, create_df_with_rows([(1, ), (2, ), (3, ), (4, ), (5, )]))

    # Utility Functions #
    def test_optimize(self):
        spark.sql(f"OPTIMIZE {CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE}")
        updated_tbl = read(MANAGED_CC_TABLE).toDF("id")
        assertDataFrameEqual(updated_tbl, create_df_with_rows([(1, ), (2, ), (3, )]))

    def test_history(self):
        spark.sql(f"DESCRIBE HISTORY {CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE};")

    def test_time_travel_read(self):
        current_timestamp = str(datetime.datetime.now())
        self.append(MANAGED_CC_TABLE)
        updated_tbl = read_with_timestamp(current_timestamp, MANAGED_CC_TABLE)
        assertDataFrameEqual(updated_tbl, self.setup_df)

    def test_restore(self):
        current_timestamp = str(datetime.datetime.now())
        self.append(MANAGED_CC_TABLE)
        spark.sql(f"RESTORE TABLE {CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE} "
                  f"TO TIMESTAMP AS OF '{current_timestamp}'")
        updated_tbl = read(MANAGED_CC_TABLE).toDF("id")
        assertDataFrameEqual(updated_tbl, self.setup_df)

    def test_change_data_feed(self):
        current_timestamp = str(datetime.datetime.now())
        self.append(MANAGED_CC_TABLE)
        updated_tbl = read_with_cdf_timestamp(
            current_timestamp, MANAGED_CC_TABLE).select("id").toDF("id")
        assertDataFrameEqual(updated_tbl, create_df_with_rows([(4, ), (5, )]))

    # Dataframe Writer V1 Tests #
    def test_insert_into_append(self):
        single_col_df = spark.createDataFrame([(4, ), (5, )], schema=["id"])
        single_col_df.write.mode("append").insertInto(f"{CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE}")
        updated_tbl = read(MANAGED_CC_TABLE).toDF("id")
        assertDataFrameEqual(updated_tbl, create_df_with_rows([(1, ), (2, ), (3, ), (4, ), (5, )]))

    def test_insert_into_overwrite(self):
        single_col_df = spark.createDataFrame([(5, )], schema=["id"])
        single_col_df.write.mode("overwrite").insertInto(
            f"{CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE}", True)
        updated_tbl = read(MANAGED_CC_TABLE).toDF("id")
        assertDataFrameEqual(updated_tbl, create_df_with_rows([(5, )]))

    def test_insert_into_overwrite_replace_where(self):
        single_col_df = spark.createDataFrame([(5, )], schema=["id"])
        single_col_df.write.mode("overwrite").option("replaceWhere", "id > 1").insertInto(
            f"{CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE}", True)
        updated_tbl = read(MANAGED_CC_TABLE).toDF("id")
        assertDataFrameEqual(updated_tbl, create_df_with_rows([(1, ), (5, )]))

    def test_insert_into_overwrite_partition_overwrite(self):
        single_col_df = spark.createDataFrame([(5,)], schema=["id"])
        single_col_df.write.mode("overwrite").option(
            "partitionOverwriteMode", "dynamic").insertInto(
            f"{CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE}", True)
        updated_tbl = read(MANAGED_CC_TABLE).toDF("id")
        assertDataFrameEqual(updated_tbl, create_df_with_rows([(5,)]))

    def test_save_as_table_append_existing_table(self):
        single_col_df = spark.createDataFrame(
            [(4, ), (5, )], schema=StructType([StructField("id", IntegerType(), True)]))
        single_col_df.write.format("delta").mode("append").saveAsTable(
            f"{CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE}")
        updated_tbl = read(MANAGED_CC_TABLE).toDF("id")
        assertDataFrameEqual(updated_tbl, create_df_with_rows([(1, ), (2, ), (3, ), (4, ), (5, )]))


    # Setting mode to append should work, however cc tables do not allow path based access.
    def test_save_append_using_path(self):
        single_col_df = spark.createDataFrame([(4, ), (5, )])
        # Fetch managed table path and attempt to side-step UC
        # and directly update table using path based access.
        tbl_path = spark.sql(
            f"DESCRIBE formatted {CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE}").collect()[5].data_type
        try:
            single_col_df.write.format("delta").save(mode="overwrite", path=tbl_path)
        except Exception as error:
            assert(S3_FORBIDDEN_ACCESS_ERROR in str(error))
        updated_tbl = read(MANAGED_CC_TABLE).toDF("id")
        assertDataFrameEqual(updated_tbl, self.setup_df)

    # DataFrame V2 Tests #
    def append(self, table_name):
        single_col_df = spark.createDataFrame(
            [(4, ),  (5, )], schema=StructType([StructField("id", IntegerType(), True)]))
        single_col_df.writeTo(f"{CATALOG_NAME}.{SCHEMA}.{table_name}").append()
        updated_tbl = read(table_name).toDF("id")
        assertDataFrameEqual(updated_tbl, create_df_with_rows([(1, ), (2, ), (3, ), (4, ), (5, )]))

    def test_overwrite(self):
        single_col_df = spark.createDataFrame(
            [(5,)], schema=StructType([StructField("id", IntegerType(), True)]))
        single_col_df.writeTo(f"{CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE}").overwrite(lit(True))
        updated_tbl = read(MANAGED_CC_TABLE).toDF("id")
        assertDataFrameEqual(updated_tbl, create_df_with_rows([(5,)]))

    def test_overwrite_partitions(self):
        single_col_df = spark.createDataFrame(
            [(5,)], schema=StructType([StructField("id", IntegerType(), True)]))
        single_col_df.writeTo(f"{CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE}").overwritePartitions()
        updated_tbl = read(MANAGED_CC_TABLE).toDF("id")
        assertDataFrameEqual(updated_tbl, create_df_with_rows([(5,)]))

    def test_create(self):
        single_col_df = spark.createDataFrame(
            [(5,)], schema=StructType([StructField("id", IntegerType(), True)]))
        try:
            single_col_df.writeTo(f"{CATALOG_NAME}.{SCHEMA}.created_table").create()
        except py4j.protocol.Py4JJavaError as error:
            assert("io.unitycatalog.spark.UCProxy.createTable" in str(error))


    def test_write_to_managed_table_without_cc(self):
        single_col_df = spark.createDataFrame(
            [(4,), (5,)], schema=StructType([StructField("id", IntegerType(), True)]))
        try:
            single_col_df.writeTo(f"{CATALOG_NAME}.{SCHEMA}.{MANAGED_NON_CC_TABLE}").append()
        except py4j.protocol.Py4JJavaError as error:
            assert(WRITE_FAILED_ERROR in str(error))

    def test_read_from_managed_table_without_cc(self):
        read(MANAGED_NON_CC_TABLE)

    def test_write_to_managed_cc_table(self):
        self.append(MANAGED_CC_TABLE)

    def test_read_from_managed_cc_table(self):
        read(MANAGED_CC_TABLE)

if __name__ == "__main__":
    unittest.main()
