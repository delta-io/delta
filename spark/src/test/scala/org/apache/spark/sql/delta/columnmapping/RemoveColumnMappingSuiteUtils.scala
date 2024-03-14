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

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaOperations.RemoveColumnMapping
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf._
import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col

/**
 * A base trait for testing removing column mapping.
 * Takes care of setting basic SQL configs and dropping the [[testTableName]] after each test.
 */
trait RemoveColumnMappingSuiteUtils extends QueryTest with DeltaColumnMappingSuiteUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(ALLOW_COLUMN_MAPPING_REMOVAL.key, "true")
  }

  override def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $testTableName")
    super.afterEach()
  }

  protected val numFiles = 10
  protected val totalRows = 100
  protected val rowsPerFile = totalRows / numFiles
  protected val logicalColumnName = "logical_column_name"
  protected val secondColumn = "second_column_name"
  protected val testTableName: String = "test_table_" + this.getClass.getSimpleName

  import testImplicits._

  protected def testRemovingColumnMapping(
    unsetTableProperty: Boolean = false): Any = {
    // Verify the input data is as expected.
    checkAnswer(
      spark.table(tableName = testTableName).select(logicalColumnName),
      spark.range(totalRows).select(col("id").as(logicalColumnName)))
    // Add a schema comment and verify it is preserved after the rewrite.
    val comment = "test comment"
    sql(s"ALTER TABLE $testTableName ALTER COLUMN $logicalColumnName COMMENT '$comment'")

    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName = testTableName))
    val originalSnapshot = deltaLog.update()

    assert(originalSnapshot.schema.head.getComment().get == comment,
      "Renamed column should preserved comment.")
    val originalFiles = getFiles(originalSnapshot)
    val startingVersion = deltaLog.update().version

    unsetColumnMappingProperty(useUnset = unsetTableProperty)

    verifyRewrite(
      unsetTableProperty,
      deltaLog,
      originalFiles,
      startingVersion)
    // Verify the schema comment is preserved after the rewrite.
    assert(deltaLog.update().schema.head.getComment().get == comment,
      "Should preserve the schema comment.")
  }

  /**
   * Verify the table still contains the same data after the rewrite, column mapping is removed
   * from table properties and the operation recorded properly.
   */
  protected def verifyRewrite(
      unsetTableProperty: Boolean,
      deltaLog: DeltaLog,
      originalFiles: Array[AddFile],
      startingVersion: Long): Unit = {
    checkAnswer(
      spark.table(tableName = testTableName).select(logicalColumnName),
      spark.range(totalRows).select(col("id").as(logicalColumnName)))

    val newSnapshot = deltaLog.update()
    assert(newSnapshot.version - startingVersion == 1, "Should rewrite the table in one commit.")

    val history = deltaLog.history.getHistory(deltaLog.update().version)
    verifyColumnMappingOperationIsRecordedInHistory(history)

    assert(newSnapshot.schema.head.name == logicalColumnName, "Should rename the first column.")

    verifyColumnMappingSchemaMetadataIsRemoved(newSnapshot)

    verifyColumnMappingTablePropertiesAbsent(newSnapshot, unsetTableProperty)
    assert(originalFiles.map(_.numLogicalRecords.get).sum ==
      newSnapshot.allFiles.map(_.numLogicalRecords.get).collect().sum,
      "Should have the same number of records.")
  }

  protected def unsetColumnMappingProperty(useUnset: Boolean): Unit = {
    val unsetStr = if (useUnset) {
      s"UNSET TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}')"
    } else {
      s"SET TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'none')"
    }
    sql(
      s"""
         |ALTER TABLE $testTableName $unsetStr
         |""".stripMargin)
  }

  /**
   * Get all files in snapshot.
   */
  protected def getFiles(snapshot: Snapshot): Array[AddFile] = snapshot.allFiles.collect()

  protected def verifyColumnMappingOperationIsRecordedInHistory(history: Seq[DeltaHistory]) = {
    val op = RemoveColumnMapping()
    assert(history.head.operation === op.name)
    assert(history.head.operationParameters === op.parameters.mapValues(_.toString).toMap)
  }

  protected def verifyColumnMappingSchemaMetadataIsRemoved(newSnapshot: Snapshot) = {
    SchemaMergingUtils.explode(newSnapshot.schema).foreach { case(_, col) =>
      assert(!DeltaColumnMapping.hasPhysicalName(col))
      assert(!DeltaColumnMapping.hasColumnId(col))
    }
  }

  protected def verifyColumnMappingTablePropertiesAbsent(
      newSnapshot: Snapshot,
      unsetTablePropertyUsed: Boolean) = {
    val columnMappingPropertyKey = DeltaConfigs.COLUMN_MAPPING_MODE.key
    val columnMappingMaxIdPropertyKey = DeltaConfigs.COLUMN_MAPPING_MAX_ID.key
    val newColumnMappingModeOpt = newSnapshot.metadata.configuration.get(columnMappingPropertyKey)
    if (unsetTablePropertyUsed) {
      assert(newColumnMappingModeOpt.isEmpty)
    } else {
      assert(newColumnMappingModeOpt.contains("none"))
    }
    assert(!newSnapshot.metadata.configuration.contains(columnMappingMaxIdPropertyKey))
  }
}
