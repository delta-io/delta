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
import org.apache.spark.sql.delta.sources.DeltaSQLConf._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier

class RemoveColumnMappingRowTrackingSuite extends RemoveColumnMappingSuiteUtils {
  test("row ids are preserved") {
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES (
         |'${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name',
         |'${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = 'true'
         |)
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)

    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName = testTableName))

    val snapshot = deltaLog.update()
    val originalDf = spark.read.table(testTableName)
    val originalRowIds = originalDf.select(logicalColumnName, RowId.QUALIFIED_COLUMN_NAME)
      .collect()
    val originalDomainMetadata = RowTrackingMetadataDomain.fromSnapshot(snapshot).get

    testRemovingColumnMapping()

    verifyRowIdsStayTheSame(originalRowIds)
    verifyDomainMetadata(deltaLog.update(), originalDomainMetadata, diffRows = totalRows)

    // Add back column mapping and remove it again. Row ids should stay the same
    sql(
      s"""ALTER TABLE $testTableName
         |SET TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name')""".stripMargin)
    // Update a row from each file to force materialize the row ids and metadata.
    val predicate = s"$logicalColumnName % $rowsPerFile == 0"
    withSQLConf(UPDATE_USE_PERSISTENT_DELETION_VECTORS.key -> "false") {
      sql(s"UPDATE $testTableName SET $secondColumn = -1 WHERE $predicate ")
    }
    testRemovingColumnMapping()
    verifyRowIdsStayTheSame(originalRowIds)
    // High watermark increased 3 times from the original value. Rewrite, UPDATE, Rewrite.
    verifyDomainMetadata(deltaLog.update(), originalDomainMetadata, diffRows = totalRows * 3)
  }

  private def verifyRowIdsStayTheSame(originalRowIds: Array[Row]) = {
    val newRowIds = spark.read.table(testTableName)
      .select(logicalColumnName, RowId.QUALIFIED_COLUMN_NAME)
    checkAnswer(newRowIds, originalRowIds)
  }

  private def verifyDomainMetadata(
      snapshot: Snapshot,
      originalDomainMetadata: RowTrackingMetadataDomain,
      diffRows: Int) = {
    val newDomainMetadata = RowTrackingMetadataDomain.fromSnapshot(snapshot).get
    assert(newDomainMetadata.rowIdHighWaterMark ===
      originalDomainMetadata.rowIdHighWaterMark + diffRows,
      "Should increase the high watermark by the number of rewritten rows.")
  }
}
