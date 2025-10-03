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

import datetime
import os
import py4j
import unittest

from delta.tables import DeltaTable
from pyspark.errors.exceptions.captured import AnalysisException, UnsupportedOperationException
from pyspark.sql import SparkSession, DataFrame
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
    io.unitycatalog:unitycatalog-spark_2.13:0.3.0,org.apache.spark:spark-hadoop-cloud_2.13:4.0.0
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
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "io.unitycatalog.spark.UCSingleCatalog") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.token", CATALOG_TOKEN) \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", CATALOG_URI) \
    .config("spark.databricks.delta.replaceWhere.constraintCheck.enabled", True) \
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

MANAGED_CATALOG_OWNED_TABLE_FULL_NAME = f"{CATALOG_NAME}.{SCHEMA}.{MANAGED_CC_TABLE}"
MANAGED_NON_CATALOG_OWNED_TABLE_FULL_NAME = f"{CATALOG_NAME}.{SCHEMA}.{MANAGED_NON_CC_TABLE}"


class UnityCatalogManagedTableTestBase(unittest.TestCase):
    """
    Shared helpers and test setup for test suites below.
    """
    setup_df = spark.createDataFrame([(1, ), (2, ), (3, )],
                                     schema=StructType([StructField("id", IntegerType(), True)]))

    def setUp(self) -> None:
        self.setup_df.write.mode("overwrite").insertInto(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)

    # Helper methods
    def read(self, table_name: str) -> DataFrame:
        return spark.read.table(table_name)

    def read_with_cdf_timestamp(self, timestamp: str, table_name: str) -> DataFrame:
        return spark.read.option('readChangeFeed', 'true').option(
            "startingTimestamp", timestamp).table(table_name)

    def read_with_cdf_version(self, version: int, table_name: str) -> DataFrame:
        return spark.read.option('readChangeFeed', 'true').option(
            "startingVersion", version).table(table_name)

    def create_df_with_rows(self, list_of_rows: list) -> DataFrame:
        return spark.createDataFrame(list_of_rows,
                                     schema=StructType([StructField("id", IntegerType(), True)]))

    def get_table_history(self, table_name: str) -> DataFrame:
        return spark.sql(f"DESCRIBE HISTORY {table_name};")

    def append(self, table_name: str) -> None:
        single_col_df = spark.createDataFrame(
            [(4, ),  (5, )], schema=StructType([StructField("id", IntegerType(), True)]))
        single_col_df.writeTo(table_name).append()


class UnityCatalogManagedTableBasicSuite(UnityCatalogManagedTableTestBase):
    """
    Suite covering basic functionality of catalog owned tables.
    """

    def test_read_from_managed_table_without_catalog_owned(self) -> None:
        self.read(MANAGED_NON_CATALOG_OWNED_TABLE_FULL_NAME)

    def test_write_to_managed_catalog_owned_table(self) -> None:
        self.append(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl,
                             self.create_df_with_rows([(1, ), (2, ), (3, ), (4, ), (5, )]))

    def test_read_from_managed_catalog_owned_table(self) -> None:
        self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.setup_df)

    # Writing to tables that are not catalog owned is not supported.
    def test_write_to_managed_table_without_catalog_owned(self) -> None:
        try:
            self.append(MANAGED_NON_CATALOG_OWNED_TABLE_FULL_NAME)
        except py4j.protocol.Py4JJavaError as error:
            assert("[TASK_WRITE_FAILED] Task failed while writing rows to s3" in str(error))

    def test_unset_catalog_owned_feature(self) -> None:
        try:
            spark.sql(f"ALTER TABLE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} "
                      f"UNSET TBLPROPERTIES ('delta.feature.catalogOwned-preview')")
        except UnsupportedOperationException as error:
            assert("Altering a table is not supported yet" in str(error))

    def test_drop_catalog_owned_property(self) -> None:
        try:
            spark.sql(f"ALTER TABLE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} "
                      f"DROP FEATURE 'catalogOwned-preview'")
        except UnsupportedOperationException as error:
            assert("Altering a table is not supported yet" in str(error))


class UnityCatalogManagedTableDMLSuite(UnityCatalogManagedTableTestBase):
    """
    Suite covering DMLs (INSERT, MERGE, UPDATE, DELETE) on catalog owned tables.
    """

    def test_update(self) -> None:
        dt = DeltaTable.forName(spark, MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        dt.update(condition="id = 1", set={"id": "4"})
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.create_df_with_rows([(4, ), (2, ), (3, )]))

    def test_sql_update(self) -> None:
        spark.sql(f"UPDATE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} SET id=4 WHERE id=1")
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.create_df_with_rows([(4, ), (2, ), (3, )]))

    def test_delete(self) -> None:
        dt = DeltaTable.forName(spark, MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        dt.delete(condition="id = 1")
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.create_df_with_rows([(2, ), (3, )]))

    def test_sql_delete(self) -> None:
        spark.sql(f"DELETE FROM {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} where id=1")
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.create_df_with_rows([(2, ), (3, )]))

    def test_merge(self) -> None:
        dt = DeltaTable.forName(spark, MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        src = self.create_df_with_rows([(1, ), (2, ), (3, ), (4, ), (5, )])
        dt.alias("target") \
            .merge(
                source=src.alias("src"),
                condition="src.id = target.id") \
            .whenNotMatchedInsertAll() \
            .execute()

        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl,
                             self.create_df_with_rows([(1, ), (2, ), (3, ), (4, ), (5, )]))

    def test_sql_merge(self) -> None:
        spark.sql(f"MERGE INTO {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} AS target "
                  f"USING (VALUES 2, 3, 4, 5 AS src(id)) AS src "
                  f"ON src.id = target.id WHEN NOT MATCHED THEN INSERT *;")
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl,
                             self.create_df_with_rows([(1, ), (2, ), (3, ), (4, ), (5, )]))

    def test_merge_schema_evolution(self) -> None:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        try:
            spark.sql(f"MERGE INTO {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} AS target "
                      f"USING (VALUES (2, 2), (3, 3), (4, 4), (5, 5) AS src(id, extra)) AS src "
                      f"ON src.id = target.id WHEN NOT MATCHED THEN INSERT *;")
        except py4j.protocol.Py4JJavaError as error:
            assert(
                "A table's Delta metadata can only be changed from a cluster or warehouse"
                in str(error)
            )
        finally:
            spark.conf.unset("spark.databricks.delta.schema.autoMerge.enabled")

        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.setup_df)

    def test_insert_schema_evolution(self) -> None:
        two_cols_df = spark.createDataFrame([(4, 4), (5, 5)], schema=["id, extra"])
        try:
            two_cols_df\
                .write.mode("append").option("mergeSchema", "true")\
                .insertInto(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        except py4j.protocol.Py4JJavaError as error:
            assert(
                "A table's Delta metadata can only be changed from a cluster or warehouse"
                in str(error)
            )

        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.setup_df)

    def test_sql_insert(self) -> None:
        spark.sql(f"INSERT INTO {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} "
                  f"VALUES (4), (5)")
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl,
                             self.create_df_with_rows([(1,), (2,), (3,), (4,), (5,)]))

    def test_sql_insert_overwrite(self) -> None:
        spark.sql(f"INSERT OVERWRITE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} "
                  f"VALUES (2), (3), (4), (5)")
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl,
                             self.create_df_with_rows([(2,), (3,), (4,), (5,)]))

    def test_sql_insert_replace_where(self) -> None:
        spark.sql(f"INSERT INTO {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} "
                  f"REPLACE WHERE id = 1 "
                  f"VALUES (1)")
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl,
                             self.create_df_with_rows([(1,), (2,), (3,)]))

    def test_sql_insert_dynamic_partition_overwrite(self) -> None:
        spark.conf.set("spark.databricks.delta.dynamicPartitionOverwrite.enabled", "true")
        try:
            spark.sql(f"INSERT INTO {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} VALUES (5)")
            updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
            assertDataFrameEqual(updated_tbl,
                                 self.create_df_with_rows([(1,), (2,), (3,), (5,)]))
        finally:
            spark.conf.unset("spark.databricks.delta.dynamicPartitionOverwrite.enabled")

    # Dataframe Writer V1 Tests #
    def test_insert_into_append(self) -> None:
        single_col_df = spark.createDataFrame([(4, ), (5, )], schema=["id"])
        single_col_df.write.mode("append").insertInto(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl,
                             self.create_df_with_rows([(1, ), (2, ), (3, ), (4, ), (5, )]))

    def test_insert_into_overwrite(self) -> None:
        single_col_df = spark.createDataFrame([(5, )], schema=["id"])
        single_col_df.write.mode("overwrite").insertInto(
            MANAGED_CATALOG_OWNED_TABLE_FULL_NAME, True)
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.create_df_with_rows([(5, )]))

    def test_insert_into_overwrite_replace_where(self) -> None:
        single_col_df = spark.createDataFrame([(5, )], schema=["id"])
        single_col_df.write.mode("overwrite").option("replaceWhere", "id > 1").insertInto(
            f"{MANAGED_CATALOG_OWNED_TABLE_FULL_NAME}", True)
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.create_df_with_rows([(1, ), (5, )]))

    def test_insert_into_overwrite_partition_overwrite(self) -> None:
        single_col_df = spark.createDataFrame([(5,)], schema=["id"])
        single_col_df.write.mode("overwrite").option(
            "partitionOverwriteMode", "dynamic").insertInto(
            MANAGED_CATALOG_OWNED_TABLE_FULL_NAME, True)
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.create_df_with_rows([(5,)]))

    def test_save_as_table_append_existing_table(self) -> None:
        single_col_df = spark.createDataFrame(
            [(4, ), (5, )], schema=StructType([StructField("id", IntegerType(), True)]))
        single_col_df.write.format("delta").mode("append").saveAsTable(
            MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl,
                             self.create_df_with_rows([(1, ), (2, ), (3, ), (4, ), (5, )]))

    # Setting mode to append should work, however cc tables do not allow path based access.
    def test_save_append_using_path(self) -> None:
        single_col_df = spark.createDataFrame([(4, ), (5, )])
        # Fetch managed table path and attempt to side-step UC
        # and directly update table using path based access.
        tbl_path = spark.sql(
            f"DESCRIBE formatted {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME}").collect()[5].data_type
        try:
            single_col_df.write.format("delta").save(mode="append", path=tbl_path)
        except py4j.protocol.Py4JJavaError as error:
            assert("AccessDeniedException" in str(error))
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.setup_df)

    # DataFrame V2 Tests #
    def test_append(self) -> None:
        self.append(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl,
                             self.create_df_with_rows([(1, ), (2, ), (3, ), (4, ), (5, )]))

    def test_overwrite(self) -> None:
        single_col_df = spark.createDataFrame(
            [(5,)], schema=StructType([StructField("id", IntegerType(), True)]))
        single_col_df.writeTo(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).overwrite(lit(True))
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.create_df_with_rows([(5,)]))

    def test_overwrite_partitions(self) -> None:
        single_col_df = spark.createDataFrame(
            [(5,)], schema=StructType([StructField("id", IntegerType(), True)]))
        single_col_df.writeTo(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).overwritePartitions()
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.create_df_with_rows([(5,)]))


class UnityCatalogManagedTableDDLSuite(UnityCatalogManagedTableTestBase):
    """
    Suite covering DDLs (CREATE, REPLACE, CLONE, ALTER) on catalog owned tables.
    """

    def test_create_non_delta(self) -> None:
        single_col_df = spark.createDataFrame(
            [(5,)], schema=StructType([StructField("id", IntegerType(), True)]))
        try:
            # CREATE TABLE is currently not supported by UC.
            single_col_df.writeTo(f"{CATALOG_NAME}.{SCHEMA}.created_table").create()
        except py4j.protocol.Py4JJavaError as error:
            assert("io.unitycatalog.spark.UCProxy.createTable" in str(error))

    def test_create_delta(self) -> None:
        single_col_df = spark.createDataFrame(
            [(5,)], schema=StructType([StructField("id", IntegerType(), True)]))
        try:
            # CREATE TABLE is currently not supported by UC.
            single_col_df.writeTo(f"{CATALOG_NAME}.{SCHEMA}.created_table").using("delta").create()
        except AnalysisException as error:
            assert(
                f"[SCHEMA_NOT_FOUND] The schema `spark_catalog`.`{SCHEMA}` cannot be found"
                in str(error)
            )

    def test_sql_create(self) -> None:
        try:
            # This ignores the catalog name passed and tries to create the table under
            # 'spark_catalog'.
            spark.sql(f"CREATE TABLE {CATALOG_NAME}.{SCHEMA}.created_table (a int) USING DELTA")
        except AnalysisException as error:
            assert(
                f"[SCHEMA_NOT_FOUND] The schema `spark_catalog`.`{SCHEMA}` cannot be found"
                in str(error)
            )

    def test_create_non_catalog_owned(self) -> None:
        try:
            # This ignores the catalog name passed and tries to create the table under
            # 'spark_catalog'.
            spark.sql(f"CREATE TABLE {CATALOG_NAME}.{SCHEMA}.created_table (id int) USING DELTA")
        except AnalysisException as error:
            assert(
                f"[SCHEMA_NOT_FOUND] The schema `spark_catalog`.`{SCHEMA}` cannot be found"
                in str(error)
            )

    def test_clone_into_catalog_owned(self) -> None:
        try:
            # CLONE fails with an assertion error in UCSingleCatalog
            spark.sql(f"CREATE TABLE {CATALOG_NAME}.{SCHEMA}.created_table" +
                      f" SHALLOW CLONE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME}")
        except py4j.protocol.Py4JJavaError as error:
            assert("java.lang.AssertionError: assertion failed" in str(error))

    def test_clone_into_non_catalog_owned(self) -> None:
        try:
            # CLONE fails with an assertion error in UCSingleCatalog
            spark.sql(f"CREATE TABLE {CATALOG_NAME}.{SCHEMA}.created_table" +
                      f" SHALLOW CLONE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} "
                      f"TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'false')")
        except py4j.protocol.Py4JJavaError as error:
            assert("java.lang.AssertionError: assertion failed" in str(error))

    def test_alter_table_comment(self) -> None:
        try:
            spark.sql(f"ALTER TABLE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} "
                      f"ALTER COLUMN id COMMENT 'comment'")
        except UnsupportedOperationException as error:
            assert("Altering a table is not supported yet" in str(error))

    def test_alter_table_add_column(self) -> None:
        try:
            spark.sql(f"ALTER TABLE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} ADD COLUMN extra INT")
        except UnsupportedOperationException as error:
            assert("Altering a table is not supported yet" in str(error))

    def test_alter_table_set_tbl_properties(self) -> None:
        try:
            spark.sql(f"ALTER TABLE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} "
                      f"SET TBLPROPERTIES ('customProp' = 'customValue')")
        except UnsupportedOperationException as error:
            assert("Altering a table is not supported yet" in str(error))

        description = spark.sql(f"DESCRIBE EXTENDED {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME}")\
            .filter("col_name = 'Table Properties'").collect()[0][1]
        assert("customProp" not in description)


class UnityCatalogManagedTableUtilitySuite(UnityCatalogManagedTableTestBase):
    """
    Suite covering utility operations on a managed table in Unity Catalog: OPTIMIZE, ANALYZE,
    VACUUM, ...
    """
    def test_optimize(self) -> None:
        spark.sql(f"OPTIMIZE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME}")
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.create_df_with_rows([(1, ), (2, ), (3, )]))

    def test_optimize_sql(self) -> None:
        spark.sql(f"OPTIMIZE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME}")
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.create_df_with_rows([(1, ), (2, ), (3, )]))

    def test_zorder_by(self) -> None:
        spark.sql(f"OPTIMIZE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} ZORDER BY (id)")
        updated_tbl = self.read(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).toDF("id")
        assertDataFrameEqual(updated_tbl, self.create_df_with_rows([(1, ), (2, ), (3, )]))

    def test_analyze(self) -> None:
        try:
            spark.sql(f"ANALYZE TABLE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} COMPUTE STATISTICS")
        except AnalysisException as error:
            assert(
                "[NOT_SUPPORTED_COMMAND_FOR_V2_TABLE] ANALYZE TABLE is not supported for v2 tables."
                in str(error)
            )

    def test_describe_table(self) -> None:
        description = spark.sql(f"DESCRIBE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME}").collect()
        expected = spark.createDataFrame([("id", "int", None)],
                                         "col_name string, data_type string, comment string")
        assertDataFrameEqual(description, expected)

    def test_history(self) -> None:
        try:
            # DESCRIBE HISTORY is currently unsupported on catalog owned tables.
            self.get_table_history(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).collect()
        except py4j.protocol.Py4JJavaError as error:
            assert("Path based access is not supported for Catalog-Owned table" in str(error))

    def test_vacuum(self) -> None:
        try:
            # VACUUM is currently unsupported on catalog owned tables.
            spark.sql(f"VACUUM {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME}")
        except UnsupportedOperationException as error:
            assert("DELTA_UNSUPPORTED_VACUUM_ON_MANAGED_TABLE" in str(error))

    def test_restore(self) -> None:
        try:
            # Restore is currently unsupported on catalog owned tables.
            spark.sql(f"RESTORE TABLE {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} TO VERSION AS OF 0")
        except py4j.protocol.Py4JJavaError as error:
            assert("A table's Delta metadata can only be changed from a cluster or warehouse"
                   in str(error))


class UnityCatalogManagedTableReadSuite(UnityCatalogManagedTableTestBase):
    """
    Suite covering reading from a managed table in Unity Catalog/
    """

    def test_time_travel_read(self) -> None:
        dt = DeltaTable.forName(spark, MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        current_version = dt.history().selectExpr("max(version)").collect()[0][0]
        current_timestamp = str(datetime.datetime.now())
        self.append(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)

        result = spark.read.option("timestampAsOf", current_timestamp)\
            .table(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        assertDataFrameEqual(result, self.setup_df)

        result = spark.read.option("versionAsOf", current_version)\
            .table(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        assertDataFrameEqual(result, self.setup_df)

        result = spark.sql(f"SELECT * FROM {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} "
                           f"TIMESTAMP AS OF '{current_timestamp}'")
        assertDataFrameEqual(result, self.setup_df)

        result = spark.sql(f"SELECT * FROM {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME} "
                           f"VERSION AS OF {current_version}")
        assertDataFrameEqual(result, self.setup_df)

    # CDC (Timestamps, Versions) are currently unsupported for Catalog owned tables.
    def test_change_data_feed_with_timestamp(self) -> None:
        timestamp = str(datetime.datetime.now())
        self.append(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        try:
            self.read_with_cdf_timestamp(
                timestamp, MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).select("id", "_change_type")
        except py4j.protocol.Py4JJavaError as error:
            assert("Path based access is not supported for Catalog-Owned table" in str(error))

    def test_change_data_feed_with_version(self) -> None:
        self.append(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)
        try:
            self.read_with_cdf_version(
                0,
                MANAGED_CATALOG_OWNED_TABLE_FULL_NAME).select("id", "_change_type")
        except py4j.protocol.Py4JJavaError as error:
            assert("Path based access is not supported for Catalog-Owned table" in str(error))

    def test_delta_table_for_path(self) -> None:
        tbl_path = spark.sql(
            f"DESCRIBE formatted {MANAGED_CATALOG_OWNED_TABLE_FULL_NAME}").collect()[5].data_type
        try:
            DeltaTable.forPath(spark, tbl_path)
        except py4j.protocol.Py4JJavaError as error:
            # Path-based access isn't supported. This could throw a better error than just
            # 'access denied' though.
            assert("AccessDeniedException" in str(error))

    def test_streaming_read(self) -> None:
        try:
            spark.readStream\
                .table(MANAGED_CATALOG_OWNED_TABLE_FULL_NAME)\
                .writeStream\
                .option("checkpointLocation", "test")\
                .toTable("output_table")
        except py4j.protocol.Py4JJavaError as error:
            # Streaming from a catalog owned table fails as it attempts to access the table by path.
            # This could also throw a better error than jsut 'access denied'.
            assert("AccessDeniedException" in str(error))


if __name__ == "__main__":
    """
    Change this to select tests to run, for example:
    - '__main__': all tests in this file.
    - '__main__.UnityCatalogManagedTableDMLSuite': all tests in that single suites.
    - '__main__.UnityCatalogManagedTableDDLSuite.test_sql_create': only that single test.
    """
    test_name = "__main__"

    suite = unittest.TestLoader().loadTestsFromName(test_name)
    unittest.TextTestRunner(verbosity=2).run(suite)
