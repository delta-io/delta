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

import scala.collection.mutable

import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.{DeletedRecordCountsHistogram, DeletedRecordCountsHistogramUtils}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaEncoder

import org.apache.spark.sql.{DataFrame, Encoder, QueryTest, Row}
import org.apache.spark.sql.functions.{coalesce, col, count, lit, sum}
import org.apache.spark.sql.test.SharedSparkSession

case class StatsSchema(
    numDeletedRecords: Long,
    numDeletionVectors: Long,
    deletedRecordCountsHistogramOpt: Option[DeletedRecordCountsHistogram])

class ChecksumDVMetricsSuite
  extends QueryTest
    with SharedSparkSession
    with DeletionVectorsTestUtils
    with DeltaSQLCommandTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectors(spark.conf)
  }

  protected implicit def statsSchemaEncoder: Encoder[StatsSchema] =
    (new DeltaEncoder[StatsSchema]).get

  /*
   * Compare statistics in the checksum by comparing to the log statistics.
   */
  protected def validateChecksum(
      snapshot: Snapshot,
      statisticsExpected: Boolean = true,
      histogramEnabled: Boolean = true): Unit = {
    val checksum = snapshot.checksumOpt match {
      case Some(checksum) => checksum
      case None => snapshot.computeChecksum
    }

    if (statisticsExpected) {
      val histogramAggregationOpt =
        if (histogramEnabled) {
          Some(DeletedRecordCountsHistogramUtils.histogramAggregate(
            coalesce(col("deletionVector.cardinality"), lit(0L))
          ).as("deletedRecordCountsHistogramOpt"))
        } else {
          Some(lit(null)
            .cast(DeletedRecordCountsHistogram.schema)
            .as("deletedRecordCountsHistogramOpt"))
        }

      val aggregations = Seq(
        sum(coalesce(col("deletionVector.cardinality"), lit(0))).as("numDeletedRecords"),
        count(col("deletionVector")).as("numDeletionVectors")) ++
        histogramAggregationOpt

      val stats = snapshot.withStatsDeduplicated
        .select(aggregations: _*)
        .as[StatsSchema]
        .first()

      val numDeletedRecords = stats.numDeletedRecords
      val numDeletionVectors = stats.numDeletionVectors
      val deletionVectorHistogram = stats.deletedRecordCountsHistogramOpt

      assert(checksum.numDeletedRecordsOpt === Some(numDeletedRecords))
      assert(checksum.numDeletionVectorsOpt === Some(numDeletionVectors))
      assert(checksum.deletedRecordCountsHistogramOpt === deletionVectorHistogram)
    } else {
      assert(checksum.numDeletedRecordsOpt === None)
      assert(checksum.numDeletionVectorsOpt === None)
      assert(checksum.deletedRecordCountsHistogramOpt === None)
    }
  }

  protected def runMerge(
      target: io.delta.tables.DeltaTable,
      source: DataFrame,
      deleteFromID: Int): Unit = {
    target.as("t").merge(source.as("s"), "t.id = s.id")
      .whenMatched(s"s.id >= ${deleteFromID}").delete()
      .execute()
  }

  protected def runDelete(
      target: io.delta.tables.DeltaTable,
      source: DataFrame = null,
      deleteFromID: Int): Unit = {
    target.delete(s"id >= ${deleteFromID}")
  }

  protected def runUpdate(
      target: io.delta.tables.DeltaTable,
      source: DataFrame = null,
      deleteFromID: Int): Unit = {
    target.update(col("id") >= lit(deleteFromID), Map("v" -> lit(-1)))
  }

  for {
    enableDVsOnTableDefault <- BOOLEAN_DOMAIN
    enableDVCreation <- BOOLEAN_DOMAIN
    enableIncrementalCommit <- BOOLEAN_DOMAIN
    allowDVsOnOperation <- BOOLEAN_DOMAIN
  } test(s"Commit checksum captures DV statistics " +
      s"enableDVsOnTableDefault: ${enableDVsOnTableDefault} " +
      s"enableDVCreation: ${enableDVCreation} " +
      s"enableIncrementalCommit: ${enableIncrementalCommit} " +
      s"allowDVsOnOperation: ${allowDVsOnOperation}") {
    val targetDF = createTestDF(0, 100, 2)
    val sourceDF = targetDF

    val operations: Seq[(io.delta.tables.DeltaTable, DataFrame, Int) => Unit] =
      Seq(runMerge, runDelete, runUpdate)

    // We validate checksum validation for different feature combinations.
    for (runOperation <- operations) {
      withSQLConf(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey ->
          enableDVsOnTableDefault.toString,
        DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> enableIncrementalCommit.toString) {

        withTempDeltaTable(targetDF, enableDVs = enableDVCreation) { (targetTable, targetLog) =>
          validateChecksum(targetLog.update(), enableDVCreation)

          // The first operation only deletes half the records from the second file.
          runOperation(targetTable(), sourceDF, 75)
          validateChecksum(targetLog.update(), enableDVCreation)

          // The second operation deletes the remaining records from the second file.
          withSQLConf(
            DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key ->
              allowDVsOnOperation.toString,
            DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS.key ->
              allowDVsOnOperation.toString) {
            runOperation(targetTable(), sourceDF, 50)
            validateChecksum(targetLog.update(), enableDVCreation)
          }
        }
      }
    }
  }

  test(s"Verify checksum DV statistics are not produced when the relevant config is disabled") {
    val targetDF = createTestDF(0, 100, 2)

    withSQLConf(DeltaSQLConf.DELTA_DELETED_RECORD_COUNTS_HISTOGRAM_ENABLED.key -> false.toString) {
      withTempDeltaTable(targetDF, enableDVs = true) { (targetTable, targetLog) =>
        runDelete(targetTable(), deleteFromID = 75)
        validateChecksum(targetLog.update(), histogramEnabled = false)
      }
    }
  }

  for {
    enableDVCreation <- BOOLEAN_DOMAIN
    enableIncrementalCommit <- BOOLEAN_DOMAIN
  } test(s"Checksum is backward compatible " +
      s"enableDVCreation: $enableDVCreation " +
      s"enableIncrementalCommit: $enableIncrementalCommit") {
    val targetDF = createTestDF(0, 100, 2)
    withSQLConf(DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> enableIncrementalCommit.toString) {
      withTempDeltaTable(targetDF, enableDVs = enableDVCreation) { (targetTable, targetLog) =>
        validateChecksum(targetLog.update(), statisticsExpected = enableDVCreation)

        runDelete(targetTable(), deleteFromID = 75)
        validateChecksum(targetLog.update(), statisticsExpected = enableDVCreation)

        // Flip DV setting on an existing table.
        enableDeletionVectorsInTable(targetLog, enable = !enableDVCreation)

        runDelete(targetTable(), deleteFromID = 50)

        // When INCREMENTAL_COMMIT_ENABLED, enabling DVs midway would normally yield
        // empty stats. This is due to the incremental nature of the computation and due to the
        // fact we do not store stats for tables with no DVs. However, in this scenario we try
        // to take advantage any recent snapshot reconstruction and harvest the stats from there.
        // In the opposite scenario, disabling DVs midway, we maintain the previously computed
        // statistics so we do not lose incrementality if DVs are enabled again.
        // When incremental commit is disabled, both enabling and disabling DVs
        // midway is not an issue. When DVs are enabled we produce results and when DVs are
        // disabled we do not.
        validateChecksum(targetLog.update(),
          statisticsExpected = !(enableDVCreation && !enableIncrementalCommit))
      }
    }
  }

  test("Checksum is computed in incremental commit when full state recomputation is triggered") {
    val targetDF = createTestDF(0, 100, 2)
    withSQLConf(DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> true.toString,
      DeltaSQLConf.INCREMENTAL_COMMIT_VERIFY.key -> false.toString,
      DeltaSQLConf.INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS.key -> false.toString,
      DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_VERIFICATION_MODE_ENABLED.key -> false.toString) {
      withTempDeltaTable(targetDF, enableDVs = false) { (targetTable, targetLog) =>
        validateChecksum(targetLog.update(), statisticsExpected = false)

        runDelete(targetTable(), deleteFromID = 75)
        validateChecksum(targetLog.update(), statisticsExpected = false)

        // Flip DV setting on an existing table.
        enableDeletionVectorsInTable(targetLog)
        runDelete(targetTable(), deleteFromID = 60)
        validateChecksum(targetLog.update(), statisticsExpected = true)
      }
    }
  }

  for (enableIncrementalCommit <- BOOLEAN_DOMAIN)
  test(s"Verify checksum validation " +
    s"incrementalCommit: $enableIncrementalCommit") {
    val targetDF = createTestDF(0, 100, 2)

    withSQLConf(DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> enableIncrementalCommit.toString) {
      withTempDeltaTable(targetDF, enableDVs = true) { (targetTable, targetLog) =>
        runDelete(targetTable(), deleteFromID = 75)
        verifyDVsExist(targetLog, 1)

        val snapshot = targetLog.update()
        assert(snapshot.validateChecksum())
      }
    }
  }

  for (enableIncrementalCommit <- BOOLEAN_DOMAIN)
  test(s"Verify checksum validation when DVs are enabled on existing tables " +
    s"incrementalCommit: $enableIncrementalCommit") {
    val targetDF = createTestDF(0, 100, 2)

    withSQLConf(DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> enableIncrementalCommit.toString) {
      withTempDeltaTable(targetDF, enableDVs = false) { (targetTable, targetLog) =>
        runDelete(targetTable(), deleteFromID = 75)

        // This validation should not take into account DV statistics.
        assert(targetLog.update().validateChecksum())

        // Enable DVs, delete with DVs and validate checksum again.
        enableDeletionVectorsInTable(targetLog)
        runDelete(targetTable(), deleteFromID = 60)
        verifyDVsExist(targetLog, 1)

        // When incremental commit is enabled DV statistics should remain None since DVs were
        // enabled midway. Checksum validation should not include DV statistics in the
        // validation process.
        assert(targetLog.update().validateChecksum())
      }
    }
  }

  test("Verify DeletedRecordsCountHistogram correctness") {
    val histogram1 = DeletedRecordCountsHistogramUtils.emptyHistogram

    // Initialize histogram with 100 files.
    (1 to 100).foreach(_ => histogram1.insert(0))
    assert(histogram1.deletedRecordCounts === Seq(100, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    // Simulate record deletions from 10 files. This would generate 10 RemoveFile actions with
    // zero DV cardinality. Then we generate 10 add files, 5 in the range 1-9 and 5 more in
    // the range 1000-9999.
    (1 to 10).foreach(_ => histogram1.remove(0))
    (5 to 9).foreach(n => histogram1.insert(n))
    (1000 to 1004).foreach(n => histogram1.insert(n))
    assert(histogram1.deletedRecordCounts === Seq(90, 5, 0, 0, 5, 0, 0, 0, 0, 0))

    (1 to 5).foreach(_ => histogram1.remove(0))
    (100000 to 100004).foreach(n => histogram1.insert(n))
    assert(histogram1.deletedRecordCounts === Seq(85, 5, 0, 0, 5, 0, 5, 0, 0, 0))

    // Negative values should be ignored.
    histogram1.insert(-123)
    histogram1.remove(-14)
    assert(histogram1.deletedRecordCounts === Seq(85, 5, 0, 0, 5, 0, 5, 0, 0, 0))

    // Verify small numbers "catch all" bucket works.
    (1 to 5).foreach(_ => histogram1.remove(0))
    histogram1.insert(10000000L)
    histogram1.insert(422290000L)
    histogram1.insert(300000999L)
    assert(histogram1.deletedRecordCounts === Seq(80, 5, 0, 0, 5, 0, 5, 0, 3, 0))

    // Verify large numbers "catch all" bucket works.
    histogram1.insert(252763333339L)
    assert(histogram1.deletedRecordCounts === Seq(80, 5, 0, 0, 5, 0, 5, 0, 3, 1))

    // Check edges.
    val histogram2 = DeletedRecordCountsHistogramUtils.emptyHistogram
    // Bin 1.
    histogram2.insert(0)
    assert(histogram2.deletedRecordCounts === Seq(1, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    // Bin 2.
    histogram2.insert(1)
    histogram2.insert(9)
    assert(histogram2.deletedRecordCounts === Seq(1, 2, 0, 0, 0, 0, 0, 0, 0, 0))
    // Bin 3.
    histogram2.insert(10)
    histogram2.insert(99)
    assert(histogram2.deletedRecordCounts === Seq(1, 2, 2, 0, 0, 0, 0, 0, 0, 0))
    // Bin 4.
    histogram2.insert(100)
    histogram2.insert(999)
    assert(histogram2.deletedRecordCounts === Seq(1, 2, 2, 2, 0, 0, 0, 0, 0, 0))
    // Bin 5.
    histogram2.insert(1000)
    histogram2.insert(9999)
    assert(histogram2.deletedRecordCounts === Seq(1, 2, 2, 2, 2, 0, 0, 0, 0, 0))
    // Bin 6.
    histogram2.insert(10000)
    histogram2.insert(99999)
    assert(histogram2.deletedRecordCounts === Seq(1, 2, 2, 2, 2, 2, 0, 0, 0, 0))
    // Bin 7.
    histogram2.insert(100000)
    histogram2.insert(999999)
    assert(histogram2.deletedRecordCounts === Seq(1, 2, 2, 2, 2, 2, 2, 0, 0, 0))
    // Bin 8.
    histogram2.insert(1000000)
    histogram2.insert(9999999)
    assert(histogram2.deletedRecordCounts === Seq(1, 2, 2, 2, 2, 2, 2, 2, 0, 0))
    // Bin 9.
    histogram2.insert(10000000)
    histogram2.insert(100000000)
    histogram2.insert(1000000000)
    histogram2.insert(Int.MaxValue - 1)
    assert(histogram2.deletedRecordCounts === Seq(1, 2, 2, 2, 2, 2, 2, 2, 4, 0))
    // Bin 10.
    histogram2.insert(Int.MaxValue)
    histogram2.insert(Long.MaxValue)
    assert(histogram2.deletedRecordCounts === Seq(1, 2, 2, 2, 2, 2, 2, 2, 4, 2))
  }

  test("Verify DeletedRecordsCountHistogram aggregate correctness") {
    import org.apache.spark.sql.delta.implicits._
    val data = Seq(
      0L, 1L, 9L, 10L, 99L, 100L, 999L, 1000L, 9999L, 10000L, 99999L, 100000L, 999999L,
      1000000L, 9999999L, 10000000L, Int.MaxValue - 1, Int.MaxValue, Long.MaxValue)

    val df = spark.createDataset(data).toDF("dvCardinality")
    val histogram = df
      .select(DeletedRecordCountsHistogramUtils.histogramAggregate(col("dvCardinality")))
      .first()

    val deletedRecordCounts = histogram
      .getAs[Row](0)
      .getAs[mutable.WrappedArray[Long]]("deletedRecordCounts")

    assert(deletedRecordCounts === Seq(1, 2, 2, 2, 2, 2, 2, 2, 2, 2))
  }
}
