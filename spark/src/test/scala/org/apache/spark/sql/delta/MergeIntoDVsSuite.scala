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

import org.apache.spark.sql.delta.cdc.MergeCDCTests
import org.apache.spark.sql.delta.sources.DeltaSQLConf

trait MergeIntoDVsTests extends MergeIntoSQLSuite with DeletionVectorsTestUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectors(spark, merge = true)
  }

  override def excluded: Seq[String] = {
    val miscFailures = Seq(
      "basic case - merge to view on a Delta table by path, " +
        "partitioned: true skippingEnabled: false useSqlView: true",
      "basic case - merge to view on a Delta table by path, " +
        "partitioned: true skippingEnabled: false useSqlView: false",
      "basic case - merge to view on a Delta table by path, " +
        "partitioned: false skippingEnabled: false useSqlView: true",
      "basic case - merge to view on a Delta table by path, " +
        "partitioned: false skippingEnabled: false useSqlView: false",
      "basic case - merge to Delta table by name, isPartitioned: false skippingEnabled: false",
      "basic case - merge to Delta table by name, isPartitioned: true skippingEnabled: false",
      "not matched by source - all 3 clauses - no changes - " +
        "isPartitioned: true - cdcEnabled: true",
      "not matched by source - all 3 clauses - no changes - " +
        "isPartitioned: false - cdcEnabled: true",
      "test merge on temp view - view with too many internal aliases - Dataset TempView"
    )

    super.excluded ++ miscFailures
  }

  protected override lazy val expectedOpTypes: Set[String] = Set(
    "delta.dml.merge.findTouchedFiles",
    "delta.dml.merge.writeModifiedRowsOnly",
    "delta.dml.merge.writeDeletionVectors",
    "delta.dml.merge")
}

class MergeIntoDVsSuite extends MergeIntoDVsTests {
  import testImplicits._

  def assertOperationalDVMetrics(
      tablePath: String,
      numDeletedRows: Long,
      numUpdatedRows: Long,
      numCopiedRows: Long,
      numTargetFilesRemoved: Long,
      numDeletionVectorsAdded: Long,
      numDeletionVectorsRemoved: Long,
      numDeletionVectorsUpdated: Long): Unit = {
    val table = io.delta.tables.DeltaTable.forPath(tablePath)
    val mergeMetrics = DeltaMetricsUtils.getLastOperationMetrics(table)
    assert(mergeMetrics.getOrElse("numTargetRowsDeleted", -1) === numDeletedRows)
    assert(mergeMetrics.getOrElse("numTargetRowsUpdated", -1) === numUpdatedRows)
    assert(mergeMetrics.getOrElse("numTargetRowsCopied", -1) === numCopiedRows)
    assert(mergeMetrics.getOrElse("numTargetFilesRemoved", -1) === numTargetFilesRemoved)
    assert(mergeMetrics.getOrElse("numTargetDeletionVectorsAdded", -1) === numDeletionVectorsAdded)
    assert(
      mergeMetrics.getOrElse("numTargetDeletionVectorsRemoved", -1) === numDeletionVectorsRemoved)
    assert(
      mergeMetrics.getOrElse("numTargetDeletionVectorsUpdated", -1) === numDeletionVectorsUpdated)
  }

  test(s"Merge with DVs metrics - Incremental Updates") {
    withTempDir { dir =>
      val sourcePath = s"$dir/source"
      val targetPath = s"$dir/target"

      spark.range(0, 10, 2).write.format("delta").save(sourcePath)
      spark.range(10).write.format("delta").save(targetPath)

      executeMerge(
        tgt = s"delta.`$targetPath` t",
        src = s"delta.`$sourcePath` s",
        cond = "t.id = s.id",
        clauses = updateNotMatched(set = "id = t.id * 10"))

      checkAnswer(readDeltaTable(targetPath), Seq(0, 10, 2, 30, 4, 50, 6, 70, 8, 90).toDF("id"))

      assertOperationalDVMetrics(
        targetPath,
        numDeletedRows = 0,
        numUpdatedRows = 5,
        numCopiedRows = 0,
        numTargetFilesRemoved = 0, // No files were fully deleted.
        numDeletionVectorsAdded = 2,
        numDeletionVectorsRemoved = 0,
        numDeletionVectorsUpdated = 0)

      executeMerge(
        tgt = s"delta.`$targetPath` t",
        src = s"delta.`$sourcePath` s",
        cond = "t.id = s.id",
        clauses = delete(condition = "t.id = 2"))

      checkAnswer(readDeltaTable(targetPath), Seq(0, 10, 30, 4, 50, 6, 70, 8, 90).toDF("id"))

      assertOperationalDVMetrics(
        targetPath,
        numDeletedRows = 1,
        numUpdatedRows = 0,
        numCopiedRows = 0,
        numTargetFilesRemoved = 0,
        numDeletionVectorsAdded = 1, // Updating a DV equals removing and adding.
        numDeletionVectorsRemoved = 1, // Updating a DV equals removing and adding.
        numDeletionVectorsUpdated = 1)

      // Delete all rows from a file.
      executeMerge(
        tgt = s"delta.`$targetPath` t",
        src = s"delta.`$sourcePath` s",
        cond = "t.id = s.id",
        clauses = delete(condition = "t.id < 5"))

      checkAnswer(readDeltaTable(targetPath), Seq(10, 30, 50, 6, 70, 8, 90).toDF("id"))

      assertOperationalDVMetrics(
        targetPath,
        numDeletedRows = 2,
        numUpdatedRows = 0,
        numCopiedRows = 0,
        numTargetFilesRemoved = 1,
        numDeletionVectorsAdded = 0,
        numDeletionVectorsRemoved = 1,
        numDeletionVectorsUpdated = 0)
    }
  }

  test(s"Merge with DVs metrics - delete entire file") {
    withTempDir { dir =>
      val sourcePath = s"$dir/source"
      val targetPath = s"$dir/target"

      spark.range(0, 7).write.format("delta").save(sourcePath)
      spark.range(10).write.format("delta").save(targetPath)

      executeMerge(
        tgt = s"delta.`$targetPath` t",
        src = s"delta.`$sourcePath` s",
        cond = "t.id = s.id",
        clauses = update(set = "id = t.id * 10"))

      checkAnswer(readDeltaTable(targetPath), Seq(0, 10, 20, 30, 40, 50, 60, 7, 8, 9).toDF("id"))

      assertOperationalDVMetrics(
        targetPath,
        numDeletedRows = 0,
        numUpdatedRows = 7,
        numCopiedRows = 0, // No rows were copied.
        numTargetFilesRemoved = 1, // 1 file was removed entirely.
        numDeletionVectorsAdded = 1, // 1 files was deleted partially.
        numDeletionVectorsRemoved = 0,
        numDeletionVectorsUpdated = 0)
    }
  }
}

trait MergeCDCWithDVsTests extends MergeCDCTests with DeletionVectorsTestUtils {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectors(spark, merge = true)
  }

  override def excluded: Seq[String] = {
    /**
     * Merge commands that result to no actions do not generate a new commit when DVs are enabled.
     * We correct affected tests by changing the expected CDC result (Create table CDC).
     */
    val miscFailures = "merge CDC - all conditions failed for all rows"

    super.excluded :+ miscFailures
  }
}

/**
 * Includes the entire MergeIntoSQLSuite with CDC enabled.
 */
class MergeIntoDVsCDCSuite extends MergeIntoDVsTests with MergeCDCWithDVsTests
