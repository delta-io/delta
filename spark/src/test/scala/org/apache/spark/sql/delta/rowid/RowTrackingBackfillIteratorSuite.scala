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

import java.util.UUID

import org.apache.spark.sql.delta.FileMetadataMaterializationTracker
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.backfill.{BackfillBatchIterator, RowTrackingBackfillBatch}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class RowTrackingBackfillIteratorSuite extends QueryTest
    with SharedSparkSession
    with RowIdTestUtils {

  import testImplicits._

  private val numFiles = 1000
  private val fileSize = 1L

  // dummy files for testing.
  private val files = Seq.fill(numFiles) {
    AddFile(
      "p-" + UUID.randomUUID().toString,
      Map.empty,
      fileSize,
      modificationTime = System.currentTimeMillis(),
      dataChange = false)
  }

  private def getRowTrackingBackfillBatches(
      maxNumFilesPerBin: Int): Seq[RowTrackingBackfillBatch] = {
    new BackfillBatchIterator(
      files.toDS(),
      FileMetadataMaterializationTracker.noopTracker,
      maxNumFilesPerBin,
      constructBatch = RowTrackingBackfillBatch(_)).toList
  }

  test("Row Tracking Backfill bins files into multiple batches") {
    val maxNumFilesPerBin = 500.0
    val expectedNumBatches = Math.ceil(numFiles / maxNumFilesPerBin)
    val batches = getRowTrackingBackfillBatches(maxNumFilesPerBin.toInt)
    assert(batches.size === expectedNumBatches)
    batches.foreach(b => assert(b.filesInBatch.size === maxNumFilesPerBin))
  }

  test("Row Tracking Backfill creates the minimum number of batches") {
    // All files in one batch.
    val batches = getRowTrackingBackfillBatches(numFiles)
    assert(batches.size === 1)
    batches.foreach(b => assert(b.filesInBatch.size === numFiles))
  }

  test("Row Tracking Backfill will create a batch smaller than maxFilesPerBin, if necessary") {
    val maxNumFilesPerBin = 150
    val expectedNumBatches = (numFiles / maxNumFilesPerBin) + 1
    val batches = getRowTrackingBackfillBatches(maxNumFilesPerBin)
    assert(batches.size === expectedNumBatches)
    assert(batches.map(_.filesInBatch.size).sum === numFiles)
    assert(batches.exists(_.filesInBatch.size < maxNumFilesPerBin),
      "there should be a batch that isn't full.")
  }

  test("Row Tracking Backfill will create no batch if there are no files") {
    val maxNumFilesPerBin = 10
    val emptyDataset = Seq.empty[AddFile].toDS()
    val batches = new BackfillBatchIterator(
      emptyDataset,
      FileMetadataMaterializationTracker.noopTracker,
      maxNumFilesPerBin,
      constructBatch = RowTrackingBackfillBatch(_)).toList

    assert(batches.isEmpty, "there should not be any batch produced if there are no files to batch")
  }
}
