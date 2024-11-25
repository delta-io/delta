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

import scala.collection.mutable.ArrayBuffer

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaStatistics.{MIN, NULL_COUNT, NUM_RECORDS, TIGHT_BOUNDS}
import org.apache.spark.sql.delta.stats.StatisticsCollection
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.databind.node.ObjectNode

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.{col, lit, map_values, when}
import org.apache.spark.sql.test.SharedSparkSession

class TightBoundsSuite
    extends QueryTest
    with SharedSparkSession
    with DeletionVectorsTestUtils
    with DeltaSQLCommandTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectors(spark.conf)
  }

  test("Validate TIGHT_BOUND column") {
    val targetDF = createTestDF(0, 100, 2)
    val sourceDF = targetDF

    def runDelete(target: io.delta.tables.DeltaTable): Int = {
      target.delete("id >= 75")
      2 // Expected number of files.
    }

    val operations = ArrayBuffer[io.delta.tables.DeltaTable => Int](runDelete)
    for {
      // Make sure it works for all operations that add DVs
      runOperation <- operations
      // Make sure tightBounds update is backwards compatible
      tightBoundDisabled <- BOOLEAN_DOMAIN
    } {
      val conf = Seq(
        DeltaSQLConf.TIGHT_BOUND_COLUMN_ON_FILE_INIT_DISABLED.key -> tightBoundDisabled.toString)

      withSQLConf(conf: _*) {
        withTempDeltaTable(targetDF) { (targetTable, targetLog) =>
          val snapshotBeforeOperation = targetLog.update()
          val statsColumnName = snapshotBeforeOperation.getBaseStatsColumnName
          val tightBoundsValuesBeforeOperation = snapshotBeforeOperation.withStatsDeduplicated
            .select(col(s"${statsColumnName}.$TIGHT_BOUNDS"))
            .collect()

          assert(tightBoundsValuesBeforeOperation.length === 2)
          val expectedTightBoundsValue = if (tightBoundDisabled) "[null]" else "[true]"
          tightBoundsValuesBeforeOperation
            .foreach(r => assert(r.toString == expectedTightBoundsValue))

          val expectedNumberOfFiles = runOperation(targetTable())
          // All operations only touch the second file.
          assert(getFilesWithDeletionVectors(targetLog).size == 1)

          val snapshotAfterOperation = targetLog.update()
          val tightBoundsValuesAfterOperation = snapshotAfterOperation.withStatsDeduplicated
            // Order by returns non-null DVs last. Thus, the file with the wide bounds
            // should be the last one.
            .orderBy(col("deletionVector").asc_nulls_first)
            .select(col(s"${statsColumnName}.$TIGHT_BOUNDS"))
            .collect()

          // Make sure tightsBounds is generated even for files that initially
          // did not contain the column. Note, we expect 2 files each from merge and delete
          // operations and three from update. This is because update creates a new file for the
          // updated rows.
          assert(tightBoundsValuesAfterOperation.length === expectedNumberOfFiles)
          assert(tightBoundsValuesAfterOperation.head.toString === expectedTightBoundsValue)
          assert(tightBoundsValuesAfterOperation.last.toString === "[false]")
        }
      }
    }
  }

  test("Verify exception is thrown if we commit files with DVs and tight bounds") {
    val targetDF = createTestDF(0, 100, 2)
    withTempDeltaTable(targetDF, enableDVs = true) { (targetTable, targetLog) =>
      // Remove one record from each file.
      targetTable().delete("id in (0, 50)")
      verifyDVsExist(targetLog, 2)

      // Commit actions with DVs and tight bounds.
      val txn = targetLog.startTransaction()
      val addFiles = txn.snapshot.allFiles.collect().toSeq.map { action =>
        action.copy(stats =
          s"""{"${NUM_RECORDS}":${action.numPhysicalRecords.get},
             | "${TIGHT_BOUNDS}":true}""".stripMargin)
      }

      val exception = intercept[DeltaIllegalStateException] {
        txn.commitActions(DeltaOperations.TestOperation(), addFiles: _*)
      }
      assert(exception.getErrorClass ===
        "DELTA_ADDING_DELETION_VECTORS_WITH_TIGHT_BOUNDS_DISALLOWED")
    }
  }

  protected def getStats(snapshot: Snapshot, statName: String): Array[Row] = {
    val statsColumnName = snapshot.getBaseStatsColumnName
    snapshot
      .withStatsDeduplicated
      .select(s"$statsColumnName.$statName")
      .collect()
  }

  protected def getStatFromLastFile(snapshot: Snapshot, statName: String): Row = {
    val statsColumnName = snapshot.getBaseStatsColumnName
    snapshot
      .withStatsDeduplicated
      .select(s"$statsColumnName.$statName")
      .orderBy(s"$statsColumnName.$MIN")
      .collect()
      .last
  }

  protected def getStatFromLastFileWithDVs(snapshot: Snapshot, statName: String): Row = {
    val statsColumnName = snapshot.getBaseStatsColumnName
    snapshot
      .withStatsDeduplicated
      .filter("isNotNull(deletionVector)")
      .select(s"$statsColumnName.$statName")
      .collect()
      .last
  }

  /**
   * Helper method that returns stats for every file in the snapshot as row objects.
   *
   * Return value schema is {
   *  numRecords: Int,
   *  RminValues: Row(Int, Int, ...), // Min value for each column
   *  maxValues: Row(Int, Int, ...), // Max value for each column
   *  nullCount: Row(Int, Int, ...), // Null count for each column
   *  tightBounds: boolean
   * }
   */
  protected def getStatsInPartitionOrder(snapshot: Snapshot): Array[Row] = {
    val statsColumnName = snapshot.getBaseStatsColumnName
    snapshot
      .withStatsDeduplicated
      .orderBy(map_values(col("partitionValues")))
      .select(s"$statsColumnName.*")
      .collect()
  }

  protected def getNullCountFromFirstFileWithDVs(snapshot: Snapshot): Row = {
    // Note, struct columns in Spark are returned with datatype Row.
    getStatFromLastFile(snapshot, NULL_COUNT)
      .getAs[Row](NULL_COUNT)
  }

  test("NULL COUNT is updated correctly when all values are nulls"
  ) {
    val targetDF = spark.range(0, 100, 1, 2)
      .withColumn("value", when(col("id") < 25, col("id"))
        .otherwise(null))

      withTempDeltaTable(targetDF, enableDVs = true) { (targetTable, targetLog) =>
        targetTable().delete("id >= 80")
        assert(getNullCountFromFirstFileWithDVs(targetLog.update()) === Row(0, 50))

        targetTable().delete("id >= 70")
        assert(getNullCountFromFirstFileWithDVs(targetLog.update()) === Row(0, 50))
      }
  }

  test("NULL COUNT is updated correctly where there are no nulls"
  ) {
    val targetDF = spark.range(0, 100, 1, 2)
      .withColumn("value", col("id"))

      withTempDeltaTable(targetDF, enableDVs = true) { (targetTable, targetLog) =>
        val expectedResult = Row(0, 0)
        targetTable().delete("id >= 80")
        assert(getNullCountFromFirstFileWithDVs(targetLog.update()) === expectedResult)

        targetTable().delete("id >= 70")
        assert(getNullCountFromFirstFileWithDVs(targetLog.update()) === expectedResult)
      }
  }

  test("NULL COUNT is updated correctly when some values are nulls"
  ) {
    val targetDF = spark.range(0, 100, 1, 2)
      .withColumn("value", when(col("id") < 75, col("id"))
        .otherwise(null))

      withTempDeltaTable(targetDF, enableDVs = true) { (targetTable, targetLog) =>
        targetTable().delete("id >= 80")
        assert(getNullCountFromFirstFileWithDVs(targetLog.update()) === Row(0, 25))

        targetTable().delete("id >= 70")
        assert(getNullCountFromFirstFileWithDVs(targetLog.update()) === Row(0, 25))
      }
  }

  test("DML operations fetch stats on tables with partial stats") {
    val targetDF = createTestDF(0, 200, 4)
      .withColumn("v", col("id"))
      .withColumn("partCol", (col("id") / lit(50)).cast("Int"))

    val conf = Seq(DeltaSQLConf.DELTA_COLLECT_STATS.key -> false.toString)
    withTempDeltaTable(targetDF, Seq("partCol"), conf = conf) { (targetTable, targetLog) =>
      val statsBeforeFirstDelete = getStatsInPartitionOrder(targetLog.update())
      val expectedStatsBeforeFirstDelete = Seq(
        Row(null, null, null, null, null), // File 1.
        Row(null, null, null, null, null), // File 2.
        Row(null, null, null, null, null), // File 3.
        Row(null, null, null, null, null) // File 4.
      )
      assert(statsBeforeFirstDelete === expectedStatsBeforeFirstDelete)

      // This operation touches files 2 and 3. Files 1 and 4 should still have not stats.
      targetTable().delete("id in (50, 100)")

      // Expect the stats for every file that got a DV added to it with tightBounds = false
      val statsAfterFirstDelete = getStatsInPartitionOrder(targetLog.update())
      val expectedStatsAfterFirstDelete = Seq(
        Row(null, null, null, null, null), // File 1.
        Row(50, Row(50, 50), Row(99, 99), Row(0, 0), false), // File 2.
        Row(50, Row(100, 100), Row(149, 149), Row(0, 0), false), // File 3.
        Row(null, null, null, null, null) // File 4.
      )
      assert(statsAfterFirstDelete === expectedStatsAfterFirstDelete)
    }
  }

  test("Update file without minValue and maxValue stats to wide bounds") {
    // The table has only binary columns, for which Delta does not collect minValue or maxValue
    // stats. The file stats should still include numRecords, nullCount, and tightBounds.
    withTempDeltaTable(
      dataDF = spark.range(0, 10, 1, 1).toDF("id")
        .select(col("id").cast("string").cast("binary").as("b")),
      enableDVs = true
    ) { (targetTable, targetLog) =>
      val statsBeforeDelete = getStatsInPartitionOrder(targetLog.update())
      val expectedStatsBeforeDelete = Seq(Row(10, Row(0), true))
      assert(statsBeforeDelete === expectedStatsBeforeDelete)

      // The DELETE command updates file stats to wide bounds.
      targetTable().delete(col("b") === lit("1").cast("string").cast("binary"))

      val statsAfterDelete = getStatsInPartitionOrder(targetLog.update())
      val expectedStatsAfterDelete = Seq(Row(10, Row(0), false))
      assert(statsAfterDelete === expectedStatsAfterDelete)
    }
  }

  test("Update file without column stats to wide bounds") {
    // We disable gathering stats for any of the columns in this table.
    // In this case, the file stats should include numRecords and tightBounds only,
    // but not minValue, maxValue or nullCount.
    withTempDeltaTable(
      dataDF = spark.range(0, 10, 1, 1).toDF("id"),
      conf = Map(DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.defaultTablePropertyKey -> "0").toSeq,
      enableDVs = true
    ) { (targetTable, targetLog) =>
      val statsBeforeDelete = getStatsInPartitionOrder(targetLog.update())
      val expectedStatsBeforeDelete = Seq(Row(10, true))
      assert(statsBeforeDelete === expectedStatsBeforeDelete)

      // The DELETE command updates file stats to wide bounds.
      targetTable().delete("id = 1")

      val statsAfterDelete = getStatsInPartitionOrder(targetLog.update())
      val expectedStatsAfterDelete = Seq(Row(10, false))
      assert(statsAfterDelete === expectedStatsAfterDelete)
    }
  }

  def tableAddDVAndTightStats(
      targetTable: () => io.delta.tables.DeltaTable,
      targetLog: DeltaLog,
      deleteCond: String): Unit = {
    // Add DVs. Stats should have tightBounds = false afterwards.
    targetTable().delete(deleteCond)
    val initialStats = getStats(targetLog.update(), "*")
    assert(initialStats.forall(_.get(4) === false)) // tightBounds

    // Other systems may support Compute Stats that recomputes tightBounds stats on tables with DVs.
    // Simulate this with a manual update commit that introduces tight stats.
    val txn = targetLog.startTransaction()
    val addFiles = txn.snapshot.allFiles.collect().toSeq.map { action =>
      val node = JsonUtils.mapper.readTree(action.stats).asInstanceOf[ObjectNode]
      assert(node.has("numRecords"))
      val numRecords = node.get("numRecords").asInt()
      action.copy(stats = s"""{ "numRecords" : $numRecords, "tightBounds" : true }""")
    }
    txn.commitActions(DeltaOperations.ManualUpdate, addFiles: _*)
  }

  test("CLONE on table with DVs and tightBound stats") {
    val targetDF = spark.range(0, 100, 1, 1).toDF()
    withTempDeltaTable(targetDF) { (targetTable, targetLog) =>
      val targetPath = targetLog.dataPath.toString
      tableAddDVAndTightStats(targetTable, targetLog, "id >= 80")
      // CLONE shouldn't throw
      // DELTA_ADDING_DELETION_VECTORS_WITH_TIGHT_BOUNDS_DISALLOWED
      withTempPath("cloned") { clonedPath =>
        sql(s"CREATE TABLE delta.`$clonedPath` SHALLOW CLONE delta.`$targetPath`")
      }
    }
  }

  test("RESTORE TABLE on table with DVs and tightBound stats") {
    val targetDF = spark.range(0, 100, 1, 1).toDF()
    withTempDeltaTable(targetDF) { (targetTable, targetLog) =>
      val targetPath = targetLog.dataPath.toString
      // adds version 1 (delete) and 2 (compute stats)
      tableAddDVAndTightStats(targetTable, targetLog, "id >= 80")
      // adds version 3 (delete more)
      targetTable().delete("id < 20")
      // Restore back to version 2 (after compute stats)
      // After 2nd delete, new DVs are added to the file, so the restore will
      // have to recommit the file with old DVs.
      targetTable().restoreToVersion(2)
      // Verify that the restored table has DVs and tight bounds.
      val stats = getStatFromLastFileWithDVs(targetLog.update(), "*")
      assert(stats.get(4) === true) // tightBounds
    }
  }

  test("Row Tracking backfill on table with DVs and tightBound stats") {
    // Enabling Row Tracking and backfill shouldn't throw
    // DELTA_ADDING_DELETION_VECTORS_WITH_TIGHT_BOUNDS_DISALLOWED
    withSQLConf(DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey -> "false") {
      val targetDF = spark.range(0, 100, 1, 1).toDF()
      withTempDeltaTable(targetDF) { (targetTable, targetLog) =>
        val targetPath = targetLog.dataPath.toString
        tableAddDVAndTightStats(targetTable, targetLog, "id >= 80")
        // Make sure that we start with no RowTracking feature.
        assert(!RowTracking.isSupported(targetLog.unsafeVolatileSnapshot.protocol))
        assert(!RowId.isEnabled(targetLog.unsafeVolatileSnapshot.protocol,
          targetLog.unsafeVolatileSnapshot.metadata))

        sql(s"ALTER TABLE delta.`$targetPath` SET TBLPROPERTIES " +
          "('delta.enableRowTracking' = 'true')")
        assert(targetLog.history.getHistory(None)
          .count(_.operation == DeltaOperations.ROW_TRACKING_BACKFILL_OPERATION_NAME) == 1)
      }
    }
  }
}

class TightBoundsColumnMappingSuite extends TightBoundsSuite with DeltaColumnMappingEnableIdMode
