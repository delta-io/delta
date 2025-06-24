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

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION
import org.apache.spark.sql.delta.cdc.UpdateCDCSuite
import org.apache.spark.sql.delta.rowtracking.RowTrackingTestUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.{col, lit}

trait RowTrackingUpdateSuiteBase
  extends RowIdTestUtils {

  protected def dvsEnabled: Boolean = false

  protected val numRowsTarget = 3000
  protected val numRowsPerFile = 250
  protected val numFiles: Int = numRowsTarget / numRowsPerFile

  protected val targetTableName = "target"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey, value = "true")
      .set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey,
        dvsEnabled.toString)
      .set(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key, dvsEnabled.toString)
      .set(DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS.key, dvsEnabled.toString)
      .set(DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS.key, dvsEnabled.toString)
  }

  protected def writeTestTable(
      tableName: String,
      isPartitioned: Boolean,
      lastModifiedVersion: Long = 0L): Unit = {
    // Disable optimized writes to write out the specified number of files.
    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false") {
      val df = spark.range(
        start = 0, end = numRowsTarget, step = 1, numPartitions = numFiles)
        .withColumn("last_modified_version", lit(lastModifiedVersion))
        .withColumn("partition", (col("id") / (numRowsTarget / 3)).cast("int"))
        .write.format("delta")
      if (isPartitioned) {
        df.partitionBy("partition").saveAsTable(tableName)
      } else {
        df.saveAsTable(tableName)
      }
      val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tableName))
      assert(snapshot.allFiles.count() === numFiles)
    }
  }

  protected def withRowIdTestTable(isPartitioned: Boolean)(f: => Unit): Unit = {
    withTable(targetTableName) {
      writeTestTable(targetTableName, isPartitioned)
      f
    }
  }

  protected def checkAndExecuteUpdate(
      tableName: String, condition: Option[String], newVersion: Long = 1L): Unit = {
    val expectedRowIds =
      spark.read.table(tableName).select("id", RowId.QUALIFIED_COLUMN_NAME).collect()

    val log = DeltaLog.forTable(spark, TableIdentifier(targetTableName))
    checkRowTrackingMarkedAsPreservedForCommit(log) {
      checkFileActionInvariantBeforeAndAfterOperation(log) {
        executeUpdate(tableName, condition, newVersion)
      }
    }

    val actualRowIds = spark.read.table(tableName).select("id", RowId.QUALIFIED_COLUMN_NAME)
    checkAnswer(actualRowIds, expectedRowIds)
    assertRowIdsAreValid(log)

    val actualRowCommitVersions =
      spark.read.table(tableName).select("id", RowCommitVersion.QUALIFIED_COLUMN_NAME)
    val expectedRowCommitVersions =
      spark.read.table(tableName).select("id", "last_modified_version").collect()
    checkAnswer(actualRowCommitVersions, expectedRowCommitVersions)
  }

  protected def executeUpdate(tableName: String, where: Option[String], newVersion: Long): Unit = {
    val whereClause = where.map(c => s"WHERE $c").getOrElse("")
    sql(s"""UPDATE $tableName as t
         |SET last_modified_version = $newVersion
         |$whereClause""".stripMargin)
  }
}

trait RowTrackingUpdateCommonTests extends RowTrackingUpdateSuiteBase {

  for {
    isPartitioned <- BOOLEAN_DOMAIN
    whereClause <- Seq(
      Some(s"id < ${(numFiles / 2) * numRowsPerFile}"), // 50% of files match
      Some(s"id < ${numRowsPerFile / 2}"), // One file matches
      None // No condition, 100% of files match
    )
  } {
    test(s"Preserves row IDs, whereClause = $whereClause, isPartitioned = $isPartitioned") {
      withRowIdTestTable(isPartitioned = isPartitioned) {
        checkAndExecuteUpdate(tableName = targetTableName, condition = whereClause)
      }
    }
  }

  for (isPartitioned <- BOOLEAN_DOMAIN)
  test(s"Preserves row IDs across multiple updates, isPartitioned = $isPartitioned") {
    withRowIdTestTable(isPartitioned = false) {
      checkAndExecuteUpdate(targetTableName, condition = Some("id % 20 = 0"))

      checkAndExecuteUpdate(targetTableName, condition = Some("id % 10 = 0"), newVersion = 2L)
    }
  }

  test("Preserves row IDs in update on partition column, whole file update") {
    withRowIdTestTable(isPartitioned = true) {
      checkAndExecuteUpdate(tableName = targetTableName, condition = Some("partition = 0"))
    }
  }


  test(s"Preserves row IDs on unpartitioned table with optimized writes") {
    withRowIdTestTable(isPartitioned = false) {
      val whereClause = Some(s"id = 0 OR id = $numRowsTarget - 1")
      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true") {
        checkAndExecuteUpdate(targetTableName, condition = whereClause)
      }

      val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(targetTableName))

      val expectedNumFiles = if (dvsEnabled) numFiles + 1 else numFiles - 1
      assert(snapshot.allFiles.count() === expectedNumFiles)
    }
  }

  test("Row tracking marked as not preserved when row tracking disabled") {
    withRowTrackingEnabled(enabled = false) {
      withRowIdTestTable(isPartitioned = false) {
        val log = DeltaLog.forTable(spark, TableIdentifier(targetTableName))
        assert(!rowTrackingMarkedAsPreservedForCommit(log) {
          executeUpdate(targetTableName, where = None, newVersion = -1L)
        })
      }
    }
  }

  test("Row tracking marked as not preserved when row tracking is supported " +
    "but disabled") {
    withRowTrackingEnabled(enabled = false) {
      withRowIdTestTable(isPartitioned = false) {
        sql(
          s"""
             |ALTER TABLE $targetTableName
             |SET TBLPROPERTIES (
             |'$rowTrackingFeatureName' = 'supported',
             |'delta.minWriterVersion' = $TABLE_FEATURES_MIN_WRITER_VERSION)""".stripMargin)

        val log = DeltaLog.forTable(spark, TableIdentifier(targetTableName))
        assert(!rowTrackingMarkedAsPreservedForCommit(log) {
          executeUpdate(targetTableName, where = None, newVersion = -1L)
        })
      }
    }
  }

  test("Preserving Row Tracking - Subqueries are not supported in UPDATE") {
    withRowTrackingEnabled(enabled = true) {
      withRowIdTestTable(isPartitioned = false) {
        val ex = intercept[AnalysisException] {
          checkAndExecuteUpdate(
            tableName = targetTableName,
            condition = Some(
              s"""id in (SELECT id FROM $targetTableName s
              WHERE s.id = 0 OR s.id = $numRowsPerFile)"""))
        }.getMessage
        assert(ex.contains("Subqueries are not supported in the UPDATE"))
      }
    }
  }

  for {
    isPartitioned <- BOOLEAN_DOMAIN
  } {
    test("UPDATE preserves Row Tracking on tables enabled using backfill, "
        + s"isPartitioned=$isPartitioned") {
      withSQLConf(DeltaSQLConf.DELTA_ROW_TRACKING_BACKFILL_ENABLED.key -> "true") {
        // This is the expected delta log history by the end of the test.
        // version 0: Table Creation
        // version 1: Protocol upgrade
        // version 2: Backfill commit
        // version 3: Metadata upgrade (tbl properties)
        // version 4: Update
        val backfillCommitVersion = 2L
        withRowTrackingEnabled(enabled = false) {
          withTable(targetTableName) {
            writeTestTable(
              targetTableName, isPartitioned, lastModifiedVersion = backfillCommitVersion)

            val (log, snapshot) =
              DeltaLog.forTableWithSnapshot(spark, TableIdentifier(targetTableName))
            assert(!RowTracking.isEnabled(snapshot.protocol, snapshot.metadata))
            validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 1) {
              triggerBackfillOnTestTableUsingAlterTable(targetTableName, numRowsTarget, log)
            }

            val whereClause = s"id < ${numRowsPerFile / 2}"
            // The newVersion should be 4, the commit associated with the UPDATE.
            val newVersion = 4L
            checkAndExecuteUpdate(
              tableName = targetTableName, condition = Some(whereClause), newVersion)
          }
        }
      }
    }
  }
}

trait RowTrackingUpdateDVTests extends RowTrackingUpdateSuiteBase
  with DeletionVectorsTestUtils {

  override protected def dvsEnabled: Boolean = true

}

trait RowTrackingCDFTests extends RowTrackingUpdateSuiteBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")
  }
}

class RowTrackingUpdateSuite extends RowTrackingUpdateCommonTests

class RowTrackingUpdateCDFSuite extends RowTrackingUpdateCommonTests with RowTrackingCDFTests

class RowTrackingUpdateDVSuite extends RowTrackingUpdateCommonTests
  with RowTrackingUpdateDVTests

class RowTrackingUpdateCDFDVSuite extends RowTrackingUpdateCommonTests
  with RowTrackingUpdateDVTests with RowTrackingCDFTests

class RowTrackingUpdateIdColumnMappingSuite extends RowTrackingUpdateCommonTests
  with DeltaColumnMappingEnableIdMode

class RowTrackingUpdateNameColumnMappingSuite extends RowTrackingUpdateCommonTests
  with DeltaColumnMappingEnableNameMode

class RowTrackingUpdateCDFDVIdColumnMappingSuite extends RowTrackingUpdateCommonTests
  with RowTrackingCDFTests with RowTrackingUpdateDVTests with DeltaColumnMappingEnableIdMode

class RowTrackingUpdateCDFDVNameColumnMappingSuite extends RowTrackingUpdateCommonTests
  with RowTrackingCDFTests with RowTrackingUpdateDVTests with DeltaColumnMappingEnableNameMode

class RowTrackingUpdateCDFIdColumnMappingSuite extends RowTrackingUpdateCommonTests
  with RowTrackingCDFTests with DeltaColumnMappingEnableIdMode

class RowTrackingUpdateCDFNameColumnMappingSuite extends RowTrackingUpdateCommonTests
  with RowTrackingCDFTests with DeltaColumnMappingEnableNameMode

// Base trait for UPDATE tests that will run post-merge only
trait UpdateWithRowTrackingTests extends UpdateSQLSuite with RowTrackingTestUtils {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey, "true")

  override def excluded: Seq[String] = super.excluded ++
    Seq(
      // TODO: UPDATE on views can't find metadata column
      "test update on temp view - view with too many internal aliases - Dataset TempView",
      "test update on temp view - view with too many internal aliases - SQL TempView",
      "test update on temp view - view with too many internal aliases " +
        "with write amplification reduction - Dataset TempView",
      "test update on temp view - view with too many internal aliases " +
        "with write amplification reduction - SQL TempView",
      "test update on temp view - basic - Partition=true - SQL TempView",
      "test update on temp view - basic - Partition=false - SQL TempView",
      "test update on temp view - superset cols - Dataset TempView",
      "test update on temp view - superset cols - SQL TempView",
      "test update on temp view - nontrivial projection - Dataset TempView",
      "test update on temp view - nontrivial projection - SQL TempView",
      "test update on temp view - nontrivial projection " +
        "with write amplification reduction - Dataset TempView",
      "test update on temp view - nontrivial projection " +
        "with write amplification reduction - SQL TempView",
      "update a SQL temp view",
      // Checks file size written out
      "usage metrics"
      )
}

// UPDATE + row tracking
class UpdateWithRowTrackingSuite extends UpdateSQLSuite with UpdateWithRowTrackingTests {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey, "false")
}

// UPDATE + CDC + row tracking
class UpdateWithRowTrackingCDCSuite extends UpdateCDCSuite with UpdateWithRowTrackingTests {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey, "false")
}

// Tests with only the table feature enabled. Should not break any tests, unless row count stats
// are missing.
trait UpdateWithRowTrackingTableFeatureTests extends UpdateSQLSuite with RowTrackingTestUtils {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey, "false")
    .set(defaultRowTrackingFeatureProperty, "supported")
}

// UPDATE + row tracking table feature
class UpdateWithRowTrackingTableFeatureSuite
  extends UpdateSQLSuite
  with UpdateWithRowTrackingTableFeatureTests {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey, "false")
}

// UPDATE + CDC + row tracking table feature
class UpdateWithRowTrackingTableFeatureCDCSuite
  extends UpdateCDCSuite
  with UpdateWithRowTrackingTableFeatureTests {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey, "false")
}
