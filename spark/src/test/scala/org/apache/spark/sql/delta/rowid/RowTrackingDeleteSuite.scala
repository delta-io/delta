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
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col

trait RowTrackingDeleteTestDimension
  extends QueryTest
  with RowIdTestUtils {
  protected def deletionVectorEnabled: Boolean = false
  protected def cdfEnabled: Boolean = false
  val testTableName = "rowIdDeleteTable"
  val initialNumRows = 5000

  /**
   * Create a table and validate that it has Row IDs and the expected number of files.
   */
  def createTestTable(
      tableName: String,
      isPartitioned: Boolean,
      multipleFilesPerPartition: Boolean): Unit = {
    // We disable Optimize Write to ensure the right number of files are created.
    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false") {
      val numFilesPerPartition = if (isPartitioned && multipleFilesPerPartition) 2 else 1
      val numRowsPerPartition = 100
      val expectedNumFiles = if (isPartitioned) {
        numFilesPerPartition * (initialNumRows / numRowsPerPartition)
      } else {
        10
      }
      val partitionColumnValue = (col("id") / numRowsPerPartition).cast("int")

      val df = spark.range(0, initialNumRows, 1, expectedNumFiles)
                    .withColumn("part", partitionColumnValue)
      if (isPartitioned) {
        df.repartition(numFilesPerPartition)
          .write
          .format("delta")
          .partitionBy("part")
          .saveAsTable(tableName)
      } else {
        df.write
          .format("delta")
          .saveAsTable(tableName)
      }

      val (log, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tableName))
      assert(snapshot.allFiles.count() === expectedNumFiles)
    }
  }

  def withRowIdTestTable(isPartitioned: Boolean)(f: => Unit): Unit = {
    withRowTrackingEnabled(enabled = true) {
      withTable(testTableName) {
        createTestTable(testTableName, isPartitioned, multipleFilesPerPartition = false)
        f
      }
    }
  }

  /**
   * Read the stable row IDs before and after the DELETE operation.
   * Validate the row IDs are the same.
   */
  def deleteAndValidateStableRowId(whereCondition: Option[String]): Unit = {
    val expectedRows: Array[Row] = spark.table(testTableName)
      .select("id", RowId.QUALIFIED_COLUMN_NAME, RowCommitVersion.QUALIFIED_COLUMN_NAME)
      .where(s"NOT (${whereCondition.getOrElse("true")})")
      .collect()

    val log = DeltaLog.forTable(spark, TableIdentifier(testTableName))
    checkRowTrackingMarkedAsPreservedForCommit(log) {
      checkFileActionInvariantBeforeAndAfterOperation(log) {
        checkHighWatermarkBeforeAndAfterOperation(log) {
          executeDelete(whereCondition)
        }
      }
    }

    val actualDF = spark.table(testTableName)
      .select("id", RowId.QUALIFIED_COLUMN_NAME, RowCommitVersion.QUALIFIED_COLUMN_NAME)
    checkAnswer(actualDF, expectedRows)
  }

  def executeDelete(whereCondition: Option[String]): Unit = {
    val whereClause = whereCondition.map(cond => s" WHERE $cond").getOrElse("")
    spark.sql(s"DELETE FROM $testTableName$whereClause")
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, cdfEnabled.toString)
      .set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey,
        deletionVectorEnabled.toString)
  }
}

trait RowTrackingDeleteSuiteBase extends RowTrackingDeleteTestDimension {
  val subqueryTableName = "subqueryTable"

  for {
    isPartitioned <- BOOLEAN_DOMAIN
    whereClause <- Seq(
      "id IN (5, 7, 11, 57, 66, 77, 79, 88, 91, 95)", // 0.2%, 10 rows match
      "part = 5", // 10%, 500 rows match
      "id % 20 = 0", // 20%, 1000 rows match
      "id >= 0" // 100%, 5000 rows match
    )
  } {
    test(s"DELETE preserves Row IDs, isPartitioned=$isPartitioned, whereClause=`$whereClause`") {
      withRowIdTestTable(isPartitioned) {
        deleteAndValidateStableRowId(Some(whereClause))
      }
    }
  }

  test("Preserving Row Tracking - Subqueries are not supported in DELETE") {
    withRowIdTestTable(isPartitioned = false) {
      withTable(subqueryTableName) {
        createTestTable(subqueryTableName, isPartitioned = false, multipleFilesPerPartition = false)
        val ex = intercept[AnalysisException] {
          deleteAndValidateStableRowId(Some(
            s"id in (SELECT id FROM $subqueryTableName WHERE id = 7 OR id = 11)"))
        }.getMessage
        assert(ex.contains("Subqueries are not supported in the DELETE"))
      }
    }
  }

  for (isPartitioned <- BOOLEAN_DOMAIN)  {
    test(s"Multiple DELETEs preserve Row IDs, isPartitioned=$isPartitioned") {
      withRowIdTestTable(isPartitioned) {
        val whereClause1 = "id % 20 = 0"
        deleteAndValidateStableRowId(Some(whereClause1))
        val whereClause2 = "id % 10 = 0"
        deleteAndValidateStableRowId(Some(whereClause2))
      }
    }
  }

  for (isPartitioned <- BOOLEAN_DOMAIN) {
    test(s"Insert after DELETE on whole table, isPartitioned=$isPartitioned") {
      withRowIdTestTable(isPartitioned) {
        // Delete whole table.
        deleteAndValidateStableRowId(whereCondition = None)

        spark.sql(s"INSERT INTO $testTableName VALUES (1, 0), (2, 0), (3, 0), (4, 0)")

        // The new rows should have new row IDs.
        val actualDF = spark.table(testTableName)
          .select("id", RowId.QUALIFIED_COLUMN_NAME)
        assert(actualDF.filter(s"row_id < $initialNumRows").count() <= 0)
      }
    }
  }

  for {
    isPartitioned <- BOOLEAN_DOMAIN
  } {
    test(s"DELETE with optimized writes preserves Row ID, isPartitioned=$isPartitioned") {
      withRowTrackingEnabled(enabled = true) {
        withTable(testTableName) {
          createTestTable(testTableName, isPartitioned, multipleFilesPerPartition = true)
          val whereClause = "id % 20 = 0"
          withSQLConf(
              DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true"
          ) {
            deleteAndValidateStableRowId(whereCondition = Some(whereClause))

            val (log, snapshot) =
              DeltaLog.forTableWithSnapshot(spark, TableIdentifier(testTableName))
            val currentNumFiles = snapshot.allFiles.count()

            val expectedNumFiles = if (deletionVectorEnabled) {
              if (isPartitioned) 100 else 10
            } else {
              if (isPartitioned) 53 else 1
            }

            assert(currentNumFiles === expectedNumFiles,
              s"The current num files $currentNumFiles is unexpected for optimized writes")
          }
        }
      }
    }
  }

  test("Row tracking marked as not preserved when row tracking disabled") {
    withRowTrackingEnabled(enabled = false) {
      withTable(testTableName) {
        createTestTable(testTableName, isPartitioned = false, multipleFilesPerPartition = false)
        val log = DeltaLog.forTable(spark, TableIdentifier(testTableName))
        assert(!rowTrackingMarkedAsPreservedForCommit(log) {
          executeDelete(whereCondition = Some("id = 5"))
        })
      }
    }
  }

  test("Row tracking marked as not preserved when row tracking is supported but disabled") {
    withRowTrackingEnabled(enabled = false) {
      withTable(testTableName) {
        createTestTable(testTableName, isPartitioned = false, multipleFilesPerPartition = false)

        sql(
          s"""
             |ALTER TABLE $testTableName
             |SET TBLPROPERTIES (
             |'$rowTrackingFeatureName' = 'supported',
             |'delta.minWriterVersion' = $TABLE_FEATURES_MIN_WRITER_VERSION)""".stripMargin)

        val log = DeltaLog.forTable(spark, TableIdentifier(testTableName))
        assert(!rowTrackingMarkedAsPreservedForCommit(log) {
          executeDelete(whereCondition = Some("id = 5"))
        })
      }
    }
  }

  for {
    isPartitioned <- BOOLEAN_DOMAIN
  } {
    test("DELETE preserves Row ID on tables with row IDs enabled using backfill,"
      + s"isPartitioned=$isPartitioned") {
      withSQLConf(DeltaSQLConf.DELTA_ROW_TRACKING_BACKFILL_ENABLED.key -> "true") {
        withRowTrackingEnabled(enabled = false) {
          withTable(testTableName) {
            createTestTable(testTableName, isPartitioned, multipleFilesPerPartition = false)
            val (log, snapshot) = DeltaLog.
              forTableWithSnapshot(spark, TableIdentifier(testTableName))
            assert(!RowTracking.isEnabled(snapshot.protocol, snapshot.metadata))
            validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 1) {
              triggerBackfillOnTestTableUsingAlterTable(testTableName, initialNumRows, log)
            }
            deleteAndValidateStableRowId(whereCondition = Some("id % 10 = 4"))
          }
        }
      }
    }
  }
}

trait RowTrackingDeleteDvBase
  extends RowTrackingDeleteTestDimension
  with DeletionVectorsTestUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectorsInNewTables(spark.conf)
  }

  override protected def deletionVectorEnabled = true

  for (isPartitioned <- BOOLEAN_DOMAIN) {
    test(s"DELETE with persistent DVs disabled, isPartitioned=$isPartitioned") {
      val whereClause = "id % 20 = 0"
      withDeletionVectorsEnabled(enabled = false) {
        withRowIdTestTable(isPartitioned) {
          deleteAndValidateStableRowId(whereCondition = Some(whereClause))
        }
      }
    }
  }
}

trait RowTrackingDeleteCDCBase extends RowTrackingDeleteTestDimension {
  override protected def cdfEnabled = true
}

// No Column Mapping concrete test suites
class RowTrackingDeleteSuite extends RowTrackingDeleteSuiteBase

class RowTrackingDeleteDvSuite extends RowTrackingDeleteSuiteBase
  with RowTrackingDeleteDvBase

class RowTrackingDeleteCDCSuite extends RowTrackingDeleteSuiteBase
  with RowTrackingDeleteCDCBase

class RowTrackingDeleteCDCDvSuite extends RowTrackingDeleteSuiteBase
  with RowTrackingDeleteCDCBase
  with RowTrackingDeleteDvBase

// Name Column Mapping concrete test suites
class RowTrackingDeleteNameColumnMappingSuite extends RowTrackingDeleteSuiteBase
  with DeltaColumnMappingEnableNameMode

class RowTrackingDeleteCDCDvNameColumnMappingSuite extends RowTrackingDeleteSuiteBase
  with RowTrackingDeleteCDCBase
  with RowTrackingDeleteDvBase
  with DeltaColumnMappingEnableNameMode

// ID Column Mapping concrete test suites
class RowTrackingDeleteIdColumnMappingSuite extends RowTrackingDeleteSuiteBase
  with DeltaColumnMappingEnableIdMode

class RowTrackingDeleteCDCDvIdColumnMappingSuite extends RowTrackingDeleteSuiteBase
  with RowTrackingDeleteCDCBase
  with RowTrackingDeleteDvBase
  with DeltaColumnMappingEnableIdMode
