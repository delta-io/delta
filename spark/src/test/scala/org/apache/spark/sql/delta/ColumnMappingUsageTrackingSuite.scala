/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

class ColumnMappingUsageTrackingSuite extends QueryTest with DeltaSQLCommandTest {
  import testImplicits._

  private val COLUMN_MAPPING_USAGE_TRACKING_FEATURE_KEY =
    TableFeatureProtocolUtils.propertyKey(ColumnMappingUsageTrackingTableFeature)

  private val COLUMN_MAPPING_USAGE_TRACKING_FEATURE_DEFAULT_KEY =
    TableFeatureProtocolUtils.defaultPropertyKey(ColumnMappingUsageTrackingTableFeature)

  private val TABLE_NAME = "t"

  test("add column without dropping or renaming column") {
    withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name",
      COLUMN_MAPPING_USAGE_TRACKING_FEATURE_DEFAULT_KEY -> "supported") {

      withTable(TABLE_NAME) {
        sql(s"CREATE TABLE $TABLE_NAME (id INT, name STRING) USING delta")
        assert(!hasDroppedOrRenamedColumn())
        assert(getPhysicalColumnName("id") === "id")
        assert(getPhysicalColumnName("name") === "name")

        sql(s"ALTER TABLE $TABLE_NAME ADD COLUMN last_name STRING")
        assert(!hasDroppedOrRenamedColumn())
        assert(getPhysicalColumnName("last_name") === "last_name")
      }
    }
  }

  test("add column after dropping column") {
    withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name",
      COLUMN_MAPPING_USAGE_TRACKING_FEATURE_DEFAULT_KEY -> "supported") {

      withTable(TABLE_NAME) {
        sql(s"CREATE TABLE $TABLE_NAME (id INT, name STRING) USING delta")
        assert(!hasDroppedOrRenamedColumn())
        assert(getPhysicalColumnName("id") === "id")
        assert(getPhysicalColumnName("name") === "name")

        sql(s"ALTER TABLE $TABLE_NAME DROP COLUMN id")
        assert(hasDroppedOrRenamedColumn())

        sql(s"ALTER TABLE $TABLE_NAME ADD COLUMN last_name STRING")
        assert(hasDroppedOrRenamedColumn())
        assert(getPhysicalColumnName("last_name") !== "last_name")
      }
    }
  }

  test("add column after renaming column") {
    withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name",
      COLUMN_MAPPING_USAGE_TRACKING_FEATURE_DEFAULT_KEY -> "supported") {

      withTable(TABLE_NAME) {
        sql(s"CREATE TABLE $TABLE_NAME (id INT, name STRING) USING delta")
        assert(!hasDroppedOrRenamedColumn())
        assert(getPhysicalColumnName("id") === "id")
        assert(getPhysicalColumnName("name") === "name")

        sql(s"ALTER TABLE $TABLE_NAME RENAME COLUMN name TO first_name")
        assert(hasDroppedOrRenamedColumn())
        assert(getPhysicalColumnName("first_name") === "name")

        sql(s"ALTER TABLE $TABLE_NAME ADD COLUMN last_name STRING")
        assert(hasDroppedOrRenamedColumn())
        assert(getPhysicalColumnName("last_name") !== "last_name")
      }
    }
  }

  test("create table with invalid column names") {
    withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name",
      COLUMN_MAPPING_USAGE_TRACKING_FEATURE_DEFAULT_KEY -> "supported") {

      withTable(TABLE_NAME) {
        sql(s"CREATE TABLE $TABLE_NAME (id INT, `first name` STRING) USING delta")
        assert(!hasDroppedOrRenamedColumn())
        assert(getPhysicalColumnName("id") === "id")
        assert(getPhysicalColumnName("first name") !== "first name")
      }
    }
  }

  test("create table like") {
    withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name",
      COLUMN_MAPPING_USAGE_TRACKING_FEATURE_DEFAULT_KEY -> "supported") {

      withTable("s", TABLE_NAME) {
        sql("CREATE TABLE s (id INT, name STRING) USING delta")
        sql("ALTER TABLE s RENAME COLUMN name TO first_name")
        sql("ALTER TABLE s ADD COLUMN last_name STRING")

        sql(s"CREATE TABLE $TABLE_NAME LIKE s")
        sql(s"ALTER TABLE $TABLE_NAME ADD COLUMN age INT")
        assert(!hasDroppedOrRenamedColumn())
        // The schema was copied including the physical column names.
        assert(getPhysicalColumnName("id") === "id")
        assert(getPhysicalColumnName("first_name") !== "first_name")
        assert(getPhysicalColumnName("last_name") !== "last_name")
      }
    }
  }

  test("create table using dataframe") {
    withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name",
      COLUMN_MAPPING_USAGE_TRACKING_FEATURE_DEFAULT_KEY -> "supported") {

      withTable(TABLE_NAME) {
        Seq((0, "name")).toDF("id", "name").write.format("delta").saveAsTable(TABLE_NAME)
        assert(!hasDroppedOrRenamedColumn())
        assert(getPhysicalColumnName("id") === "id")
        assert(getPhysicalColumnName("name") === "name")
      }
    }
  }

  test("clone table with renamed column") {
    withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name",
      COLUMN_MAPPING_USAGE_TRACKING_FEATURE_DEFAULT_KEY -> "supported") {

      withTable("s", TABLE_NAME) {
        sql("CREATE TABLE s (id INT, name STRING) USING delta")
        sql("ALTER TABLE s RENAME COLUMN name TO first_name")
        sql("ALTER TABLE s ADD COLUMN last_name STRING")

        sql(s"CREATE TABLE $TABLE_NAME CLONE s")
        sql(s"ALTER TABLE $TABLE_NAME ADD COLUMN age INT")
        assert(hasDroppedOrRenamedColumn())
        assert(getPhysicalColumnName("id") === "id")
        assert(getPhysicalColumnName("first_name") !== "first_name")
        assert(getPhysicalColumnName("last_name") !== "last_name")
        assert(getPhysicalColumnName("age") !== "age")
      }
    }
  }

  test("clone table without dropped or renamed column") {
    withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name",
      COLUMN_MAPPING_USAGE_TRACKING_FEATURE_DEFAULT_KEY -> "supported") {

      withTable("s", TABLE_NAME) {
        sql("CREATE TABLE s (id INT, name STRING) USING delta")
        sql("ALTER TABLE s ADD COLUMN last_name STRING")

        sql(s"CREATE TABLE $TABLE_NAME CLONE s")
        sql(s"ALTER TABLE $TABLE_NAME ADD COLUMN age INT")
        assert(!hasDroppedOrRenamedColumn())
        assert(getPhysicalColumnName("id") === "id")
        assert(getPhysicalColumnName("name") === "name")
        assert(getPhysicalColumnName("last_name") === "last_name")
        assert(getPhysicalColumnName("age") === "age")
      }
    }
  }

  test("clone table without column mapping tracking enabled") {
    withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name") {
      withTable("s", TABLE_NAME) {
        sql("CREATE TABLE s (id INT, name STRING) USING delta")
        sql("ALTER TABLE s RENAME COLUMN name TO first_name")
        sql("ALTER TABLE s ADD COLUMN last_name STRING")

        withSQLConf(COLUMN_MAPPING_USAGE_TRACKING_FEATURE_DEFAULT_KEY -> "supported") {
          sql(s"CREATE TABLE $TABLE_NAME CLONE s")
          sql(s"ALTER TABLE $TABLE_NAME ADD COLUMN age INT")
          assert(!hasDroppedOrRenamedColumn())
          // The schema was copied including the physical column names.
          assert(getPhysicalColumnName("id") !== "id")
          assert(getPhysicalColumnName("first_name") !== "first_name")
          assert(getPhysicalColumnName("last_name") !== "last_name")
          assert(getPhysicalColumnName("age") === "age")
        }
      }
    }
  }

  test("enabled after creation") {
    withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name") {
      withTable(TABLE_NAME) {
        sql(s"CREATE TABLE $TABLE_NAME (id INT, name STRING) USING delta")
        assert(getPhysicalColumnName("id") !== "id")
        assert(getPhysicalColumnName("name") !== "name")

        sql(s"ALTER TABLE $TABLE_NAME " +
          s"SET TBLPROPERTIES ('$COLUMN_MAPPING_USAGE_TRACKING_FEATURE_KEY' = 'supported')")
        assert(hasDroppedOrRenamedColumn())

        sql(s"ALTER TABLE $TABLE_NAME ADD COLUMN last_name STRING")
        assert(getPhysicalColumnName("last_name") !== "last_name")
      }
    }
  }

  private def getPhysicalColumnName(columnName: String): String = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(TABLE_NAME))
    val schema = deltaLog.update().schema
    DeltaColumnMapping.getPhysicalName(schema(columnName))
  }

  private def hasDroppedOrRenamedColumn(): Boolean = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(TABLE_NAME))
    val configuration = deltaLog.update().metadata.configuration
    configuration(DeltaConfigs.COLUMN_MAPPING_HAS_DROPPED_OR_RENAMED.key).toBoolean
  }
}
