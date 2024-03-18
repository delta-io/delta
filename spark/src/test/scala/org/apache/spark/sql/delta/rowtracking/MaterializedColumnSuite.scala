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

package org.apache.spark.sql.delta.rowtracking

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaRuntimeException, MaterializedRowCommitVersion, MaterializedRowId, RowTracking}
import org.apache.spark.sql.delta.rowid.RowIdTestUtils

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest

class MaterializedColumnSuite extends RowIdTestUtils
  with ParquetTest {

  private val testTableName = "target"
  private val testDataColumnName = "test_data"

  private def withTestTable(testFunction: => Unit): Unit = {
    withTable(testTableName) {
      spark.range(end = 10).toDF(testDataColumnName)
        .write.format("delta").saveAsTable(testTableName)
      testFunction
    }
  }

  private def getMaterializedRowIdColumnName(tableName: String): Option[String] = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tableName))
    snapshot.metadata.configuration.get(MaterializedRowId.MATERIALIZED_COLUMN_NAME_PROP)
  }

  private def getMaterializedRowCommitVersionColumnName(tableName: String): Option[String] = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tableName))
    snapshot.metadata.configuration.get(
      MaterializedRowCommitVersion.MATERIALIZED_COLUMN_NAME_PROP)
  }

  for ((name, getMaterializedColumnName) <- Map(
    "row ids" -> getMaterializedRowIdColumnName _,
    "row commit versions" -> getMaterializedRowCommitVersionColumnName _
  )) {
    test(s"materialized $name column name is stored when row tracking is enabled") {
      withRowTrackingEnabled(enabled = true) {
        withTestTable {
          assert(getMaterializedColumnName(testTableName).isDefined)
        }
      }
    }

    test(s"materialized $name column name is not stored when row tracking is disabled") {
      withRowTrackingEnabled(enabled = false) {
        withTestTable {
          assert(getMaterializedColumnName(testTableName).isEmpty)
        }
      }
    }

    test(s"adding a column with the same name as the materialized $name column name fails") {
      withRowTrackingEnabled(enabled = true) {
        withTestTable {
          val materializedColumnName = getMaterializedColumnName(testTableName).get
          val error = intercept[DeltaRuntimeException] {
            sql(s"ALTER TABLE $testTableName ADD COLUMN (`$materializedColumnName` BIGINT)")
          }
          checkError(error, "DELTA_ADDING_COLUMN_WITH_INTERNAL_NAME_FAILED",
            parameters = Map("colName" -> materializedColumnName))
        }
      }
    }

    test(s"renaming a column to the materialized $name column name fails") {
      withRowTrackingEnabled(enabled = true) {
        withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name") {
          withTestTable {
            val materializedColumnName = getMaterializedColumnName(testTableName).get
            val error = intercept[DeltaRuntimeException] {
              sql(s"ALTER TABLE $testTableName " +
                s"RENAME COLUMN $testDataColumnName TO `$materializedColumnName`")
            }
            checkError(error, errorClass = "DELTA_ADDING_COLUMN_WITH_INTERNAL_NAME_FAILED",
              parameters = Map("colName" -> materializedColumnName))
          }
        }
      }
    }

    test(s"cloning a table with a column equal to the materialized $name column name fails") {
      val targetName = "target"
      val sourceName = "source"
      withTable(targetName, sourceName) {
        withRowTrackingEnabled(enabled = true) {
          spark.range(0).toDF("val")
            .write.format("delta").saveAsTable(targetName)

          val materializedColumnName = getMaterializedColumnName(targetName).get
          spark.range(0).toDF(materializedColumnName)
            .write.format("delta").saveAsTable(sourceName)

          val error = intercept[DeltaRuntimeException] {
            sql(s"CREATE OR REPLACE TABLE $targetName SHALLOW CLONE $sourceName")
          }
          checkError(error, errorClass = "DELTA_ADDING_COLUMN_WITH_INTERNAL_NAME_FAILED",
            parameters = Map("colName" -> materializedColumnName))
        }
      }
    }

    test(s"replace keeps the materialized $name column name") {
      withRowTrackingEnabled(enabled = true) {
        withTestTable {
          val materializedColumnNameBefore = getMaterializedColumnName(testTableName)
          sql(
            s"""
               |CREATE OR REPLACE TABLE $testTableName
               |USING delta AS
               |SELECT * FROM VALUES (0), (1)
               |""".stripMargin)
          val materializedColumnNameAfter = getMaterializedColumnName(testTableName)
          assert(materializedColumnNameBefore == materializedColumnNameAfter)
        }
      }
    }

    test(s"restore keeps the materialized $name column name") {
      withRowTrackingEnabled(enabled = true) {
        withTestTable {
          spark.range(end = 100).toDF(testDataColumnName)
            .write.format("delta").mode("overwrite").saveAsTable(testTableName)

          val materializedColumnNameBefore = getMaterializedColumnName(testTableName)
          io.delta.tables.DeltaTable.forName(testTableName).restoreToVersion(0)
          val materializedColumnNameAfter = getMaterializedColumnName(testTableName)
          assert(materializedColumnNameBefore == materializedColumnNameAfter)
        }
      }
    }

    test(s"clone assigns a materialized $name column when table property is set") {
      val sourceTableName = "source"
      val targetTableName = "target"

      withTable(sourceTableName, targetTableName) {
        withRowTrackingEnabled(enabled = false) {
          spark.range(end = 1).write.format("delta").saveAsTable(sourceTableName)
          assert(getMaterializedColumnName(sourceTableName).isEmpty)

          sql(s"CREATE OR REPLACE TABLE $targetTableName SHALLOW CLONE $sourceTableName " +
            s"TBLPROPERTIES ('${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = 'true')")

          assert(getMaterializedColumnName(targetTableName).isDefined)
        }
      }
    }

    test(s"clone assigns a materialized $name column when source enables row tracking") {
      val sourceTableName = "source"
      val targetTableName = "target"

      withTable(sourceTableName, targetTableName) {
        withRowTrackingEnabled(enabled = true) {
          spark.range(end = 1).toDF("col1").write.format("delta").saveAsTable(sourceTableName)

          sql(s"CREATE TABLE $targetTableName SHALLOW CLONE $sourceTableName")

          val sourceTableColumnName = getMaterializedColumnName(sourceTableName)
          val targetTableColumnName = getMaterializedColumnName(targetTableName)

          assert(sourceTableColumnName.isDefined)
          assert(targetTableColumnName.isDefined)
          assert(sourceTableColumnName !== targetTableColumnName)
        }
      }
    }
  }
}
