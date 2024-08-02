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

package org.apache.spark.sql.delta.rowtracking

import org.apache.spark.sql.delta.{DeletionVectorsTestUtils, DeltaLog, DeltaOperations, DeltaTestUtils, FileMetadataMaterializationTracker, OptimisticTransaction, RowId, RowTrackingFeature}
import org.apache.spark.sql.delta.DeltaOperations.{ManualUpdate, Truncate}
import org.apache.spark.sql.delta.actions.{Action, AddFile}
import org.apache.spark.sql.delta.actions.{Metadata, RemoveFile}
import org.apache.spark.sql.delta.commands.backfill.{BackfillBatchIterator, BackfillCommandStats, RowTrackingBackfillBatch, RowTrackingBackfillExecutor}
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.delta.rowid.RowIdTestUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

class RowTrackingConflictResolutionSuite extends QueryTest
  with DeletionVectorsTestUtils
  with SharedSparkSession
  with RowIdTestUtils {

  private val testTableName = "test_table"

  private def deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))
  private def latestSnapshot = deltaLog.update()

  private def withTestTable(testBlock: => Unit): Unit = {
    withTable(testTableName) {
      withRowTrackingEnabled(enabled = false) {
        // Table is initially empty.
        spark.range(end = 0).toDF().write.format("delta").saveAsTable(testTableName)

        testBlock
      }
    }
  }

  /** Create an AddFile action for testing purposes. */
  private def addFile(path: String): AddFile = {
    AddFile(
      path = path,
      partitionValues = Map.empty,
      size = 1337,
      modificationTime = 1,
      dataChange = true,
      stats = """{ "numRecords": 1 }"""
    )
  }

  /** Add Row tracking table feature support. */
  private def activateRowTracking(): Unit = {
    require(!latestSnapshot.protocol.isFeatureSupported(RowTrackingFeature))
    deltaLog.upgradeProtocol(Action.supportedProtocolVersion())
  }

  // Add 'numRecords' records to the table.
  private def commitRecords(numRecords: Int): Unit = {
    spark.range(numRecords).write.format("delta").mode("append").saveAsTable(testTableName)
  }

  test("Set baseRowId if table feature was committed concurrently") {
    withTestTable {
      val txn = deltaLog.startTransaction()
      activateRowTracking()
      txn.commit(Seq(addFile(path = "file_path")), DeltaOperations.ManualUpdate)

      assertRowIdsAreValid(deltaLog)
    }
  }

  test("Set valid baseRowId if table feature and RowIdHighWaterMark are committed concurrently") {
    withTestTable {
      val filePath = "file_path"
      val numConcurrentRecords = 11

      val txn = deltaLog.startTransaction()
      activateRowTracking()
      commitRecords(numConcurrentRecords)
      txn.commit(Seq(addFile(filePath)), DeltaOperations.ManualUpdate)

      assertRowIdsAreValid(deltaLog)
      val committedAddFile = latestSnapshot.allFiles.collect().filter(_.path == filePath)
      assert(committedAddFile.size === 1)
      assert(committedAddFile.head.baseRowId === Some(numConcurrentRecords))
    }
  }

  test("Conflict resolution if table feature and initial AddFiles are in the same commit") {
    withTestTable {
      val filePath = "file_path"

      val txn = deltaLog.startTransaction()
      deltaLog.startTransaction().commit(
        Seq(Action.supportedProtocolVersion(), addFile("other_path")), DeltaOperations.ManualUpdate)
      txn.commit(Seq(addFile(filePath)), DeltaOperations.ManualUpdate)

      assertRowIdsAreValid(deltaLog)
      val committedAddFile = latestSnapshot.allFiles.collect().filter(_.path == filePath)
      assert(committedAddFile.size === 1)
      assert(committedAddFile.head.baseRowId === Some(1))
    }
  }

  test("Conflict resolution with concurrent INSERT") {
    withTestTable {
      val filePath = "file_path"
      val numInitialRecords = 7
      val numConcurrentRecords = 11

      activateRowTracking()
      commitRecords(numInitialRecords)
      val txn = deltaLog.startTransaction()
      commitRecords(numConcurrentRecords)
      txn.commit(Seq(addFile(filePath)), DeltaOperations.ManualUpdate)

      assertRowIdsAreValid(deltaLog)
      val committedAddFile = latestSnapshot.allFiles.collect().filter(_.path == filePath)
      assert(committedAddFile.size === 1)
      assert(committedAddFile.head.baseRowId === Some(numInitialRecords + numConcurrentRecords))
      val currentHighWaterMark = RowId.extractHighWatermark(latestSnapshot).get
      assert(currentHighWaterMark === numInitialRecords + numConcurrentRecords)
    }
  }

  test("Handle commits that do not bump the high water mark") {
    withTestTable {
      val filePath = "file_path"
      val numInitialRecords = 7
      activateRowTracking()
      commitRecords(numInitialRecords)

      val txn = deltaLog.startTransaction()
      val concurrentTxn = deltaLog.startTransaction()
      val updatedProtocol = latestSnapshot.protocol
      concurrentTxn.commit(Seq(updatedProtocol), DeltaOperations.ManualUpdate)
      txn.commit(Seq(addFile(filePath)), DeltaOperations.ManualUpdate)

      assertRowIdsAreValid(deltaLog)
    }
  }

  /**
   * Setup a test table with four files and return these files to the caller.
   */
  private def setupTableAndGetAllFiles(log: DeltaLog): (AddFile, AddFile, AddFile, AddFile) = {
    val f1 = DeltaTestUtils.createTestAddFile(encodedPath = "a", partitionValues = Map("x" -> "1"))
    val f2 = DeltaTestUtils.createTestAddFile(encodedPath = "b", partitionValues = Map("x" -> "1"))
    val f3 = DeltaTestUtils.createTestAddFile(encodedPath = "c", partitionValues = Map("x" -> "2"))
    val f4 = DeltaTestUtils.createTestAddFile(encodedPath = "d", partitionValues = Map("x" -> "2"))

    val setupActions: Seq[Action] = Seq(
      Metadata(
        schemaString = new StructType().add("x", IntegerType).json,
        partitionColumns = Seq("x")),
      f1,
      f2,
      f3,
      f4,
      Action.supportedProtocolVersion().withFeature(RowTrackingFeature)
    )

    log.startTransaction().commit(setupActions, ManualUpdate)

    (f1, f2, f3, f4)
  }

  /** Add a dummy DV to a file in a table. */
  private def addDVToFileInTable(deltaLog: DeltaLog, file: AddFile): (AddFile, RemoveFile) = {
    val dv = writeDV(deltaLog, RoaringBitmapArray(0L))
    updateFileDV(file, dv)
  }

  /** Execute backfill on the table associated with the delta log passed in. */
  private def executeBackfill(log: DeltaLog, backfillTxn: OptimisticTransaction): Unit = {
    val maxBatchesInParallel = 1
    val backfillStats = BackfillCommandStats(
      backfillTxn.txnId,
      nameOfTriggeringOperation = DeltaOperations.OP_SET_TBLPROPERTIES,
      maxNumBatchesInParallel = maxBatchesInParallel)
    val backfillExecutor = new RowTrackingBackfillExecutor(
      spark,
      backfillTxn,
      FileMetadataMaterializationTracker.noopTracker,
      maxBatchesInParallel,
      backfillStats
    )
    val filesToBackfill =
      RowTrackingBackfillExecutor.getCandidateFilesToBackfill(log.update())
    val batches = new BackfillBatchIterator(
      filesToBackfill,
      FileMetadataMaterializationTracker.noopTracker,
      maxNumFilesPerBin = 4,
      constructBatch = RowTrackingBackfillBatch(_))

    backfillExecutor.run(batches)
  }

  /** Check if base row IDs and default row commit versions have been assigned. */
  def assertBaseRowIDsAndDefaultRowCommitVersionsAssigned(finalFiles: Seq[AddFile]): Unit = {
    finalFiles.foreach(addedFile => assert(addedFile.baseRowId.nonEmpty))
    finalFiles.foreach(addedFile => assert(addedFile.defaultRowCommitVersion.nonEmpty))
  }

  test("Backfill conflict with a delete, Delete wins") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)

      // Setup
      val (file1, file2, file3, file4) = setupTableAndGetAllFiles(log)

      // Start Backfill.
      val backfillTxn = log.startTransaction()

      // A delete occurs in parallel. Delete wins.
      val deleteTxn = log.startTransaction()
      deleteTxn.filterFiles(EqualTo('x, Literal(1)) :: Nil)
      val deleteActions = Seq(file1.remove, file2.remove)
      // Truncate is a data-changing operation.
      deleteTxn.commit(deleteActions, Truncate())

      // Finish backfill.
      executeBackfill(log, backfillTxn)

      val finalFiles = log.update().allFiles.collect()
      assertBaseRowIDsAndDefaultRowCommitVersionsAssigned(finalFiles)
      assertRowIdsAreValid(log)
      assert(finalFiles.map(_.path).toSet === Seq(file3, file4).map(_.path).toSet)
    }
  }

  test("Backfill conflicts with a delete, Backfill wins") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      // Setup
      val (file1, file2, file3, file4) = setupTableAndGetAllFiles(log)

      // Start delete
      val deleteTxn = log.startTransaction()
      deleteTxn.filterFiles(EqualTo('x, Literal(1)) :: Nil)

      // Backfill occurs in parallel and wins.
      val backfillTxn = log.startTransaction()
      executeBackfill(log, backfillTxn)

      val deleteActions = Seq(file1.remove, file2.remove)
      // Truncate is a data-changing operation.
      deleteTxn.commit(deleteActions, Truncate())

      val finalFiles = log.update().allFiles.collect()
      assertBaseRowIDsAndDefaultRowCommitVersionsAssigned(finalFiles)
      assertRowIdsAreValid(log)
      assert(finalFiles.map(_.path).toSet === Seq(file3, file4).map(_.path).toSet)
    }
  }

  test("Backfill conflicts with a DV delete, Delete wins") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)

      // Setup
      val (file1, file2, file3, file4) = setupTableAndGetAllFiles(log)
      enableDeletionVectorsInTable(log)

      // Start Backfill
      val backfillTxn = log.startTransaction()

      // A delete occurs in parallel. Delete wins.
      val deleteTxn = log.startTransaction()
      deleteTxn.filterFiles(EqualTo('x, Literal(1)) :: Nil)
      val (addFile1WithDV, removeFile1) = addDVToFileInTable(log, file1)
      val (addFile2WithDV, removeFile2) = addDVToFileInTable(log, file2)
      val deleteActions = Seq(addFile1WithDV, removeFile1, addFile2WithDV, removeFile2)
      // Truncate is a data-changing operation.
      deleteTxn.commit(deleteActions, Truncate())

      // Finish Backfill
      executeBackfill(log, backfillTxn)

      val finalFiles = log.update().allFiles.collect()
      assertBaseRowIDsAndDefaultRowCommitVersionsAssigned(finalFiles)
      assertRowIdsAreValid(log)
      val allFiles = Seq(file1, file2, file3, file4)
      assert(finalFiles.map(_.path).toSet === allFiles.map(_.path).toSet)
    }
  }

  test("Backfill conflicts with a DV delete, Backfill wins") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      // Setup
      val (file1, file2, file3, file4) = setupTableAndGetAllFiles(log)
      enableDeletionVectorsInTable(log)

      // Start delete
      val deleteTxn = log.startTransaction()
      deleteTxn.filterFiles(EqualTo('x, Literal(1)) :: Nil)

      // Backfill occurs in parallel and wins.
      val backfillTxn = log.startTransaction()
      executeBackfill(log, backfillTxn)

      val (addFile1WithDV, removeFile1) = addDVToFileInTable(log, file1)
      val (addFile2WithDV, removeFile2) = addDVToFileInTable(log, file2)
      val deleteActions = Seq(addFile1WithDV, removeFile1, addFile2WithDV, removeFile2)
      // Truncate is a data-changing operation.
      deleteTxn.commit(deleteActions, Truncate())

      val finalFiles = log.update().allFiles.collect()
      assertBaseRowIDsAndDefaultRowCommitVersionsAssigned(finalFiles)
      assertRowIdsAreValid(log)
      val allFiles = Seq(file1, file2, file3, file4)
      assert(finalFiles.map(_.path).toSet === allFiles.map(_.path).toSet)
    }
  }
}
