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

package org.apache.spark.sql.delta.columnmapping

import org.apache.spark.sql.delta.DeltaConfigs
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.RowId
import org.apache.spark.sql.delta.RowId.RowTrackingMetadataDomain
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION
import org.apache.spark.sql.delta.rowid.RowIdTestUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier

class RemoveColumnMappingRowTrackingSuite extends RowIdTestUtils
  with RemoveColumnMappingSuiteUtils {
  test("row ids are preserved") {
    createTestTable(tblProperties = Seq(s"'${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name'",
      s"'${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = 'true'"))

    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName = testTableName))

    checkRowTrackingMarkedAsPreservedForCommit(deltaLog) {
      checkRowIdHighWaterMarkBeforeAndAfterOperation(deltaLog, diffRows = totalRows) {
        checkRowIdsStayTheSameBeforeAndAfterOperation {
          testRemovingColumnMapping()
        }
      }
    }

    // High watermark increased 3 times from the original value. Rewrite, UPDATE, Rewrite.
    checkRowIdHighWaterMarkBeforeAndAfterOperation(deltaLog, diffRows = totalRows * 2) {
      // Add back column mapping and remove it again. Row IDs should stay the same.
      checkRowIdsStayTheSameBeforeAndAfterOperation {
        sql(
          s"""ALTER TABLE $testTableName
             |SET TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name')""".stripMargin)
        // Update a row from each file to force materialize the Row IDs and metadata.
        val predicate = s"$logicalColumnName % $rowsPerFile == 0"
        withSQLConf(UPDATE_USE_PERSISTENT_DELETION_VECTORS.key -> "false") {
          sql(s"UPDATE $testTableName SET $secondColumn = -1 WHERE $predicate ")
        }

        checkRowTrackingMarkedAsPreservedForCommit(deltaLog) {
          testRemovingColumnMapping()
        }
      }
    }
  }

  test("Row tracking marked as not preserved when row tracking disabled") {
    withRowTrackingEnabled(enabled = false) {
      withTable(testTableName) {
        createTestTable()

        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))

        assert(!rowTrackingMarkedAsPreservedForCommit(deltaLog) {
          testRemovingColumnMapping()
        })
      }
    }
  }

  test("Row tracking marked as not preserved when row tracking is supported " +
    "but disabled") {
    withRowTrackingEnabled(enabled = false) {
      withTable(testTableName) {
        createTestTable(tblProperties = Seq(s"'$rowTrackingFeatureName' = 'supported'",
          s"'delta.minWriterVersion' = $TABLE_FEATURES_MIN_WRITER_VERSION"))

        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))

        assert(!rowTrackingMarkedAsPreservedForCommit(deltaLog) {
          checkRowIdHighWaterMarkBeforeAndAfterOperation(deltaLog, diffRows = totalRows) {
            testRemovingColumnMapping()
          }
        })
      }
    }
  }
  private def verifyRowIdsStayTheSame(originalRowIds: Array[Row]) = {
    val newRowIds = spark.read.table(testTableName)
      .select(logicalColumnName, RowId.QUALIFIED_COLUMN_NAME)
    checkAnswer(newRowIds, originalRowIds)
  }

  private def createTestTable(tblProperties: Seq[String] = Seq.empty): Unit = {
    val tblPropertiesStr = if (tblProperties.nonEmpty) {
      s"TBLPROPERTIES ${tblProperties.mkString("(", ",", ")")}"
    } else {
      ""
    }
    sql(
      s"""
         |CREATE TABLE $testTableName
         |USING delta
         |$tblPropertiesStr
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
  }

  private def checkRowIdsStayTheSameBeforeAndAfterOperation(operation: => Unit): Unit = {
    val originalDf = spark.read.table(testTableName)
    val originalRowIds = originalDf.select(logicalColumnName, RowId.QUALIFIED_COLUMN_NAME)
      .collect()

    operation

    verifyRowIdsStayTheSame(originalRowIds)
  }

  private def checkRowIdHighWaterMarkBeforeAndAfterOperation
      (log: DeltaLog, diffRows: Int)(operation: => Unit): Unit = {
    val originalDomainMetadata = RowTrackingMetadataDomain.fromSnapshot(log.update()).get

    operation

    val snapshot = log.update()
    val newDomainMetadata = RowTrackingMetadataDomain.fromSnapshot(snapshot).get
    assert(newDomainMetadata.rowIdHighWaterMark ===
      originalDomainMetadata.rowIdHighWaterMark + diffRows,
      "Should increase the high watermark by the number of rewritten rows.")
  }
}
