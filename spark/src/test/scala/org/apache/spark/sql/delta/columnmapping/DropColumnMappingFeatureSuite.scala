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

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaConfigs._
import org.apache.spark.sql.delta.sources.DeltaSQLConf._

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.util.ManualClock

/**
 * Test dropping column mapping feature from a table.
 */
class DropColumnMappingFeatureSuite extends RemoveColumnMappingSuiteUtils {

  val clock = new ManualClock(System.currentTimeMillis())
  test("column mapping cannot be dropped without the feature flag") {
    withSQLConf(ALLOW_COLUMN_MAPPING_REMOVAL.key -> "false") {
      sql(s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name',
         |        'delta.minReaderVersion' = '3',
         |        'delta.minWriterVersion' = '7')
         |AS SELECT 1 as a
         |""".stripMargin)

      intercept[DeltaColumnMappingUnsupportedException] {
        dropColumnMappingTableFeature()
      }
    }
  }

  test("table without column mapping enabled") {
    sql(s"""CREATE TABLE $testTableName
           |USING delta
           |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'none')
           |AS SELECT 1 as a
           |""".stripMargin)

    val e = intercept[DeltaTableFeatureException] {
      dropColumnMappingTableFeature()
    }
    checkError(e,
      errorClass = DeltaErrors.dropTableFeatureFeatureNotSupportedByProtocol(".")
      .getErrorClass,
      parameters = Map("feature" -> "columnMapping"))
  }

  test("invalid column names") {
    val invalidColName1 = colName("col1")
    val invalidColName2 = colName("col2")
    sql(
      s"""CREATE TABLE $testTableName (a INT, `$invalidColName1` INT, `$invalidColName2` INT)
         |USING delta
         |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
         |""".stripMargin)
    val e = intercept[DeltaAnalysisException] {
      dropColumnMappingTableFeature()
    }
    checkError(e,
      errorClass = "DELTA_INVALID_COLUMN_NAMES_WHEN_REMOVING_COLUMN_MAPPING",
      parameters = Map("invalidColumnNames" -> "col1 with special chars ,;{}()\n\t="))
  }

  test("drop column mapping from a table without table feature") {
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name',
         |        '${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'false',
         |        'delta.minReaderVersion' = '3',
         |        'delta.minWriterVersion' = '7')
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
    testDroppingColumnMapping()
  }

  test("drop column mapping from a table with table feature") {
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name',
         |        '${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'false',
         |        'delta.minReaderVersion' = '3',
         |        'delta.minWriterVersion' = '7')
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
    testDroppingColumnMapping()
  }

  test("drop column mapping from a table without column mapping table property") {
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name',
         |        '${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'false',
         |        'delta.minReaderVersion' = '3',
         |        'delta.minWriterVersion' = '7')
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
    unsetColumnMappingProperty(useUnset = true)
    val e = intercept[DeltaTableFeatureException] {
      dropColumnMappingTableFeature()
    }
    checkError(
      e,
      errorClass = "DELTA_FEATURE_DROP_HISTORICAL_VERSIONS_EXIST",
      parameters = Map(
        "feature" -> "columnMapping",
        "logRetentionPeriodKey" -> "delta.logRetentionDuration",
        "logRetentionPeriod" -> "30 days",
        "truncateHistoryLogRetentionPeriod" -> "24 hours")
    )
  }

  test("drop column mapping in id mode") {
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'id',
         |        '${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'false',
         |        'delta.minReaderVersion' = '3',
         |        'delta.minWriterVersion' = '7')
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
    testDroppingColumnMapping()
  }

  def testDroppingColumnMapping(): Unit = {
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

    val e = intercept[DeltaTableFeatureException] {
      dropColumnMappingTableFeature()
    }
    checkError(
      e,
      errorClass = "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
      parameters = Map(
        "feature" -> "columnMapping",
        "logRetentionPeriodKey" -> "delta.logRetentionDuration",
        "logRetentionPeriod" -> "30 days",
        "truncateHistoryLogRetentionPeriod" -> "24 hours")
    )

    verifyRewrite(
      unsetTableProperty = true,
      deltaLog,
      originalFiles,
      startingVersion,
      originalData = originalData,
      droppedFeature = true)
    // Verify the schema comment is preserved after the rewrite.
    assert(deltaLog.update().schema.head.getComment().get == comment,
      "Should preserve the schema comment.")
    verifyDropFeatureTruncateHistory()
  }

  protected def verifyDropFeatureTruncateHistory() = {
    val deltaLog1 = DeltaLog.forTable(spark, TableIdentifier(tableName = testTableName), clock)
    // Populate the delta cache with the delta log with the right data path so it stores the clock.
    // This is currently the only way to make sure the drop feature command uses the clock.
    DeltaLog.clearCache()
    DeltaLog.forTable(spark, deltaLog1.dataPath, clock)
    // Set the log retention to 0 so that we can test truncate history.
    sql(
      s"""
         |ALTER TABLE $testTableName SET TBLPROPERTIES (
         |  '${TABLE_FEATURE_DROP_TRUNCATE_HISTORY_LOG_RETENTION.key}' = '0 hours',
         |  '${LOG_RETENTION.key}' = '0 hours')
         |""".stripMargin)
    // Pretend enough time has passed for the history to be truncated.
    clock.advance(TimeUnit.MINUTES.toMillis(5))
    sql(
      s"""
         |ALTER TABLE $testTableName DROP FEATURE ${ColumnMappingTableFeature.name} TRUNCATE HISTORY
         |""".stripMargin)
    val newSnapshot = deltaLog.update()
    assert(newSnapshot.protocol.readerAndWriterFeatures.isEmpty, "Should drop the feature.")
    assert(newSnapshot.protocol.minWriterVersion == 1)
    assert(newSnapshot.protocol.minReaderVersion == 1)
  }

  protected def dropColumnMappingTableFeature(): Unit = {
    sql(
      s"""
         |ALTER TABLE $testTableName DROP FEATURE ${ColumnMappingTableFeature.name}
         |""".stripMargin)
  }
}
