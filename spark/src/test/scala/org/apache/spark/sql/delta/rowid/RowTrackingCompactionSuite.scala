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

package org.apache.spark.sql.delta.rowid

import java.io.File

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import org.apache.spark.sql.delta.hooks.AutoCompact
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

trait RowTrackingCompactionTestsBase
  extends QueryTest
  with SharedSparkSession
  with RowIdTestUtils {

  protected def commandName: String

  protected val numSoftDeletedRows: Int = 0

  protected def createTable(
      dir: File,
      rowTrackingEnabled: Boolean,
      partitioned: Boolean,
      withMaterializedRowTrackingColumns: Boolean): Unit = {
    withRowTrackingEnabled(rowTrackingEnabled) {
      val partitionClause = if (partitioned) "PARTITIONED BY (key)" else ""
      spark.sql(
        s"""CREATE TABLE delta.`${dir.getAbsolutePath}` (key LONG, value LONG)
           |USING DELTA
           |$partitionClause""".stripMargin)

      def writeValues(start: Long, end: Long): Unit = {
        val df = spark.range(start, end, step = 1, numPartitions = 1)
          .select(col("id") % 10 as "key", col("id") as "value")
        writeDf(dir, partitioned, withMaterializedRowTrackingColumns, df)
      }
      // Write 3 times to create 3 commits with different versions
      writeValues(start = 0, end = 100)
      writeValues(start = 100, end = 200)
      writeValues(start = 200, end = 300)
    }
  }

  protected def writeDf(
      dir: File,
      partitioned: Boolean,
      withMaterializedRowTrackingColumns: Boolean,
      _df: DataFrame): Unit = {
    var df = _df
    if (withMaterializedRowTrackingColumns) {
      val deltaLog = DeltaLog.forTable(spark, dir)
      val snapshot = deltaLog.update()
      val materializedRowIdColName = MaterializedRowId.getMaterializedColumnNameOrThrow(
        snapshot.protocol, snapshot.metadata, deltaLog.tableId)
      df = df.withMaterializedRowIdColumn(materializedRowIdColName, col("value"))
      val materializedRowCommitVersionColName =
        MaterializedRowCommitVersion.getMaterializedColumnNameOrThrow(
          snapshot.protocol, snapshot.metadata, deltaLog.tableId)
      df = df.withMaterializedRowCommitVersionColumn(
        materializedRowCommitVersionColName, col("value"))
    }

    var writer = df.write.format("delta").mode("append")
    if (partitioned) {
      writer = writer.partitionBy("key")
    }
    writer.save(dir.getAbsolutePath)
  }

  protected def runStatement(statement: String): OptimizeMetrics = {
    import testImplicits._
    spark.sql(statement)
      .select(col("metrics.*"))
      .as[OptimizeMetrics]
      .head()
  }

  protected def runCompaction(dir: File, applyFilter: Boolean): OptimizeMetrics

  protected def checkCompactionRowTrackingPreservation(dir: File, applyFilter: Boolean): Unit = {
    val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

    def readWithRowTrackingColumns(): DataFrame = {
      spark.read.format("delta").load(dir.getAbsolutePath)
        .select("value", RowId.QUALIFIED_COLUMN_NAME, RowCommitVersion.QUALIFIED_COLUMN_NAME)
    }

    val rowsBeforeFirstCompaction = readWithRowTrackingColumns().collect()

    // Run compaction, check that at least one file in the table was rewritten, and check that
    // the fresh row IDs have been preserved.
    checkRowTrackingMarkedAsPreservedForCommit(deltaLog) {
      val metrics = runCompaction(dir, applyFilter)
      assert(metrics.numFilesRemoved > 0)
    }
    checkAnswer(readWithRowTrackingColumns(), rowsBeforeFirstCompaction)

    assertRowIdsAreValid(deltaLog)
  }

  protected def runTest(
      partitioned: Boolean,
      withMaterializedRowTrackingColumns: Boolean,
      applyFilter: Boolean): Unit = {
    withTempDir { dir =>
      createTable(dir, rowTrackingEnabled = true, partitioned, withMaterializedRowTrackingColumns)
      checkCompactionRowTrackingPreservation(dir, applyFilter)
    }
  }
}

trait RowTrackingCompactionTests extends RowTrackingCompactionTestsBase {
  test(s"$commandName unpartitioned table with fresh row IDs") {
    runTest(partitioned = false, withMaterializedRowTrackingColumns = false, applyFilter = false)
  }

  test(s"$commandName unpartitioned table with stable row IDs") {
    runTest(partitioned = false, withMaterializedRowTrackingColumns = true, applyFilter = false)
  }

  test(s"$commandName partitioned table with fresh row IDs") {
    runTest(partitioned = true, withMaterializedRowTrackingColumns = false, applyFilter = false)
  }

  test(s"$commandName partitioned table with stable row IDs") {
    runTest(partitioned = true, withMaterializedRowTrackingColumns = true, applyFilter = false)
  }

  test(s"$commandName partitioned table with fresh row IDs and filter") {
    runTest(partitioned = true, withMaterializedRowTrackingColumns = false, applyFilter = true)
  }

  test(s"$commandName partitioned table with stable row IDs and filter") {
    runTest(partitioned = true, withMaterializedRowTrackingColumns = true, applyFilter = true)
  }

  test("Row tracking marked as not preserved when row tracking disabled") {
    withTempDir { dir =>
      withRowTrackingEnabled(enabled = false) {
        createTable(
          dir,
          rowTrackingEnabled = false,
          partitioned = false,
          withMaterializedRowTrackingColumns = false)
      }
      val log = DeltaLog.forTable(spark, dir)
      assert(!rowTrackingMarkedAsPreservedForCommit(log) {
        runCompaction(dir, applyFilter = false)
      })
    }
  }

  test("Row tracking marked as not preserved when row tracking is supported but " +
    "disabled") {
    withTempDir { dir =>
      withRowTrackingEnabled(enabled = false) {
        createTable(
          dir,
          rowTrackingEnabled = false,
          partitioned = false,
          withMaterializedRowTrackingColumns = false)
      }

      sql(
        s"""
           |ALTER TABLE delta.`${dir.getAbsolutePath}`
           |SET TBLPROPERTIES (
           |'$rowTrackingFeatureName' = 'supported',
           |'delta.minWriterVersion' = $TABLE_FEATURES_MIN_WRITER_VERSION)""".stripMargin)

      val log = DeltaLog.forTable(spark, dir)
      assert(!rowTrackingMarkedAsPreservedForCommit(log) {
        runCompaction(dir, applyFilter = false)
      })
    }
  }

  test(s"$commandName preserves row tracking on backfill enabled tables") {
    withSQLConf(DeltaSQLConf.DELTA_ROW_TRACKING_BACKFILL_ENABLED.key -> "true") {
      withTempDir { dir =>
        createTable(
          dir,
          rowTrackingEnabled = false,
          partitioned = false,
          withMaterializedRowTrackingColumns = false)

        val log = DeltaLog.forTable(spark, dir)
        val snapshot = log.update()
        assert(!RowTracking.isEnabled(snapshot.protocol, snapshot.metadata))

        val numRows = spark.read.format("delta").load(dir.getAbsolutePath).count()
        validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 1) {
          triggerBackfillOnTestTableUsingAlterTable(
            targetTableName = s"delta.`${dir.getAbsolutePath}`",
            numRowsInTable = numRows.toInt + numSoftDeletedRows,
            log)
        }

        checkCompactionRowTrackingPreservation(dir, applyFilter = false)
      }
    }
  }
}

trait RowTrackingOptimizeTests extends RowTrackingCompactionTests {
  override protected def commandName: String = "optimize"

  override def runCompaction(dir: File, applyFilter: Boolean): OptimizeMetrics = {
    runStatement(
      s"""OPTIMIZE delta.`${dir.getAbsolutePath}`
         |${if (applyFilter) "WHERE key = 5" else ""}""".stripMargin)
  }
}

trait RowTrackingZorderTests extends RowTrackingCompactionTests {
  override protected def commandName: String = "z-order"

  override def runCompaction(dir: File, applyFilter: Boolean): OptimizeMetrics = {
    runStatement(
      s"""OPTIMIZE delta.`${dir.getAbsolutePath}`
         |${if (applyFilter) "WHERE key = 5" else ""}
         |ZORDER BY (value)""".stripMargin)
  }
}

trait RowTrackingAutoCompactionTests extends RowTrackingCompactionTests {
  override protected def commandName: String = "auto-compact"

  override def runCompaction(dir: File, applyFilter: Boolean): OptimizeMetrics = {
    val log = DeltaLog.forTable(spark, dir)
    val partitionPredicates = if (applyFilter) {
      Seq(EqualTo(UnresolvedAttribute("key"), Literal(5L)))
    } else {
      Nil
    }

    var metrics: OptimizeMetrics = null
    withSQLConf(DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> "0") {
      metrics = AutoCompact.compact(
        spark,
        log,
        partitionPredicates).head
    }
    metrics
  }
}

trait RowTrackingPurgeTests extends RowTrackingCompactionTests with DeletionVectorsTestUtils {

  override protected val numSoftDeletedRows: Int = 3

  override protected def commandName: String = "purge"

  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectorsInNewTables(spark.conf)
  }

  override protected def createTable(
      dir: File,
      rowTrackingEnabled: Boolean,
      partitioned: Boolean,
      withMaterializedRowTrackingColumns: Boolean): Unit = {
    withRowTrackingEnabled(enabled = rowTrackingEnabled) {
      val partitionClause = if (partitioned) "PARTITIONED BY (key)" else ""
      spark.sql(
        s"""CREATE TABLE delta.`${dir.getAbsolutePath}` (key LONG, value LONG, value2 LONG)
           |USING DELTA
           |$partitionClause""".stripMargin)

      def writeValues(start: Long, end: Long): Unit = {
        val df = spark.range(start, end, step = 1, numPartitions = 1)
          .select(col("id") % 10 as "key", col("id") as "value", col("id") as "value2")
        writeDf(dir, partitioned, withMaterializedRowTrackingColumns, df)
      }
      // Write 3 times to create 3 commits with different versions
      writeValues(start = 0, end = 100)
      writeValues(start = 100, end = 200)
      writeValues(start = 200, end = 300)

      // Add Deletion Vectors to the table so that we can trigger purge.
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)
      removeRowsFromAllFilesInLog(deltaLog, numRowsToRemovePerFile = 1)
    }
  }

  override def runCompaction(dir: File, applyFilter: Boolean): OptimizeMetrics = {
    val statement =
      s"""REORG TABLE delta.`${dir.getAbsolutePath}`
         |${if (applyFilter) "WHERE key = 5" else ""}
         |APPLY (PURGE)""".stripMargin

    val metricsFirstRun = runStatement(statement)

    // Check that a second of run of PURGE does not modify the table.
    // This could be an issue with row IDs, as the materialized row ID is not part of the schema
    // of the table.
    val metricsSecondRun = runStatement(statement)
    assert(metricsSecondRun.numFilesRemoved === 0)
    assert(metricsSecondRun.numFilesAdded === 0)

    metricsFirstRun
  }
}

trait RowTrackingCompactionTestsWithNameColumnMapping
  extends RowTrackingCompactionTestsBase
  with DeltaColumnMappingEnableNameMode

trait RowIdCompactionTestsWithIdColumnMapping
  extends RowTrackingCompactionTestsBase
  with DeltaColumnMappingEnableIdMode

class RowTrackingOptimizeSuite extends RowTrackingOptimizeTests
class RowTrackingOptimizeSuiteWithIdColumnMapping extends RowTrackingOptimizeTests
  with RowIdCompactionTestsWithIdColumnMapping
class RowTrackingOptimizeSuiteWithNameColumnMapping extends RowTrackingOptimizeTests
  with RowTrackingCompactionTestsWithNameColumnMapping

class RowTrackingZorderSuite extends RowTrackingZorderTests
class RowTrackingZorderSuiteWithIdColumnMapping extends RowTrackingZorderTests
  with RowIdCompactionTestsWithIdColumnMapping
class RowTrackingZorderSuiteWithNameColumnMapping extends RowTrackingZorderTests
  with RowTrackingCompactionTestsWithNameColumnMapping

class RowTrackingAutoCompactionSuite extends RowTrackingAutoCompactionTests
class RowTrackingAutoCompactionSuiteWithIdColumnMapping extends RowTrackingAutoCompactionTests
  with RowIdCompactionTestsWithIdColumnMapping
class RowTrackingAutoCompactionSuiteWithNameColumnMapping extends RowTrackingAutoCompactionTests
  with RowTrackingCompactionTestsWithNameColumnMapping

class RowTrackingPurgeSuite extends RowTrackingPurgeTests
class RowTrackingPurgeSuiteWithIdColumnMapping extends RowTrackingPurgeTests
  with RowIdCompactionTestsWithIdColumnMapping
class RowTrackingPurgeSuiteWithNameColumnMapping extends RowTrackingPurgeTests
  with RowTrackingCompactionTestsWithNameColumnMapping
