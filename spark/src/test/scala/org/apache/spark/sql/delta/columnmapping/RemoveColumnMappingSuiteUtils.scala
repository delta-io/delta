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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col

/**
 * A base trait for testing removing column mapping.
 * Takes care of setting basic SQL configs and dropping the [[testTableName]] after each test.
 */
trait RemoveColumnMappingSuiteUtils extends QueryTest with DeltaColumnMappingSuiteUtils {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(ALLOW_COLUMN_MAPPING_REMOVAL.key, "true")
  }

  override protected def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $testTableName")
    super.afterEach()
  }

  protected val numFiles = 10
  protected val totalRows = 100
  protected val rowsPerFile = totalRows / numFiles
  protected val logicalColumnName = "logical_column_name"
  protected val secondColumn = "second_column_name"
  protected val firstColumn = "first_column_name"
  protected val thirdColumn = "third_column_name"
  protected val renamedThirdColumn = "renamed_third_column_name"

  protected val testTableName: String = "test_table_" + this.getClass.getSimpleName
  protected def deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))

  import testImplicits._

  protected def testRemovingColumnMapping(unsetTableProperty: Boolean = false): Any = {
    // Verify the input data is as expected.
    val originalData = spark.table(tableName = testTableName).select(logicalColumnName).collect()
    // Add a schema comment and verify it is preserved after the rewrite.
    val comment = "test comment"
    sql(s"ALTER TABLE $testTableName ALTER COLUMN $logicalColumnName COMMENT '$comment'")

    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName = testTableName))
    val originalSnapshot = deltaLog.update()

    assert(originalSnapshot.schema.head.getComment().get == comment,
      "Renamed column should preserve comment.")
    val originalFiles = getFiles(originalSnapshot)
    val startingVersion = deltaLog.update().version

    unsetColumnMappingProperty(useUnset = unsetTableProperty)

    verifyRewrite(
      unsetTableProperty = unsetTableProperty,
      deltaLog,
      originalFiles,
      startingVersion,
      originalData = originalData)
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
      startingVersion: Long,
      originalData: Array[Row],
      droppedFeature: Boolean = false): Unit = {
    checkAnswer(
      spark.table(tableName = testTableName).select(logicalColumnName),
      originalData)
    val newSnapshot = deltaLog.update()
    val versionsAddedByRewrite = if (droppedFeature) {
      2
    } else {
      1
    }
    assert(newSnapshot.version - startingVersion == versionsAddedByRewrite,
      s"Should rewrite the table in $versionsAddedByRewrite commits.")

    val rewriteVersion = deltaLog.update().version - versionsAddedByRewrite + 1
    val history = deltaLog.history.getHistory(rewriteVersion, Some(rewriteVersion))
    verifyColumnMappingOperationIsRecordedInHistory(history)

    assert(newSnapshot.schema.head.name == logicalColumnName, "Should rename the first column.")

    verifyColumnMappingSchemaMetadataIsRemoved(newSnapshot)

    verifyColumnMappingTablePropertiesAbsent(newSnapshot, unsetTableProperty || droppedFeature)
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

  protected def enableColumnMapping(): Unit = {
    sql(
      s"""ALTER TABLE $testTableName
        SET TBLPROPERTIES (
        '${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5')""")
  }

  protected def renameColumn(): Unit = {
    sql(s"ALTER TABLE $testTableName RENAME COLUMN $thirdColumn TO $renamedThirdColumn")
  }

  protected def dropColumn(): Unit = {
    sql(s"ALTER TABLE $testTableName DROP COLUMN $thirdColumn")
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
