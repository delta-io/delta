/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.concurrency.rowlevelconcurrency

import java.util.UUID

import scala.util.Random

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.RowLevelConcurrency.{RebaseBudgets, RebaseFailure, RebaseStatus}
import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Unit tests covering the per-commit budgets (file count, wall-clock deadline),
 * the tightBounds stats degradation, and a 3-writer convergence fuzz test.
 */
class RowLevelConcurrencyBudgetsAndFuzzSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  // ---------- helpers ----------

  private def bitmap(rows: Long*): RoaringBitmapArray = {
    val b = new RoaringBitmapArray()
    rows.foreach(b.add)
    b
  }

  private def inlineDv(rows: Long*): DeletionVectorDescriptor = {
    val b = bitmap(rows: _*)
    if (b.isEmpty) {
      DeletionVectorDescriptor.EMPTY
    } else {
      val bytes = b.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
      DeletionVectorDescriptor.inlineInLog(bytes, b.cardinality)
    }
  }

  private def addFile(
      path: String,
      dv: DeletionVectorDescriptor = DeletionVectorDescriptor.EMPTY,
      stats: String = "{\"numRecords\": 100}"): AddFile =
    AddFile(
      path = path,
      partitionValues = Map.empty,
      size = 1L,
      modificationTime = 1L,
      dataChange = true,
      stats = stats,
      deletionVector = dv)

  private def removeFile(path: String, dv: DeletionVectorDescriptor): RemoveFile =
    RemoveFile(
      path = path,
      deletionTimestamp = Some(1L),
      dataChange = true,
      deletionVector = dv)

  private def newHadoopConf(): org.apache.hadoop.conf.Configuration = {
    // scalastyle:off deltahadoopconfiguration
    spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
  }

  private def onDiskDv(
      tablePath: Path,
      hadoopConf: org.apache.hadoop.conf.Configuration,
      rows: Long*): DeletionVectorDescriptor = {
    val bitmapArray = bitmap(rows: _*)
    val store = DeletionVectorStore.createInstance(hadoopConf)
    val fileId = UUID.randomUUID()
    val writer = store.createWriter(
      store.generateFileNameInTable(store.pathWithFileSystem(tablePath), fileId))
    try {
      val bytes = DeletionVectorUtils.serialize(
        bitmapArray,
        RoaringBitmapArrayFormat.Portable,
        tablePath = Some(tablePath))
      val range = writer.write(bytes)
      DeletionVectorDescriptor.onDiskWithRelativePath(
        id = fileId,
        sizeInBytes = bytes.length,
        cardinality = bitmapArray.cardinality,
        offset = Some(range.offset))
    } finally {
      writer.close()
    }
  }

  private def decodedRows(
      dv: DeletionVectorDescriptor,
      tablePath: Path,
      hadoopConf: org.apache.hadoop.conf.Configuration): Set[Long] =
    DeletionVectorStore.createInstance(hadoopConf).read(dv, tablePath).toArray.toSet

  // ---------- budgets: external DV reads ----------

  test("budget: maxDvReads counts distinct external reads, not candidate paths") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()

      val priorDv = inlineDv()
      val loserActions: Seq[Action] = Seq(
        removeFile("f1", priorDv), addFile("f1", onDiskDv(tablePath, hadoopConf, 1L)),
        removeFile("f2", priorDv), addFile("f2", onDiskDv(tablePath, hadoopConf, 2L)))
      val winnerAdded = Seq(
        addFile("f1", onDiskDv(tablePath, hadoopConf, 11L)),
        addFile("f2", onDiskDv(tablePath, hadoopConf, 12L)))
      val winnerRemoved = Seq(removeFile("f1", priorDv), removeFile("f2", priorDv))

      val budgets = RebaseBudgets(
        maxDvBytesPerFile = 1024L * 1024L,
        maxDvReads = 3,
        deadlineNanos = Long.MaxValue)

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, budgets)

      assert(result.status == RebaseStatus.Aborted)
      assert(result.failure.contains(RebaseFailure.DvReadBudgetExceeded))
      assert(result.stats.numDvFilesRead == 0, "read budget must be preflighted before I/O")
      assert(result.numDvFilesWritten == 0)

      val exactBudgetResult = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf,
        budgets.copy(maxDvReads = 4))
      assert(exactBudgetResult.status == RebaseStatus.Succeeded)
      assert(exactBudgetResult.stats.numDvFilesRead == 4)
    }
  }

  // ---------- budgets: wall-clock deadline ----------

  test("budget: deadline is checked immediately after each DV read") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()

      val priorDv = onDiskDv(tablePath, hadoopConf, 0L)
      val loserActions: Seq[Action] =
        Seq(removeFile("f1", priorDv), addFile("f1", onDiskDv(tablePath, hadoopConf, 0L, 1L)))
      val winnerAdded = Seq(addFile("f1", onDiskDv(tablePath, hadoopConf, 0L, 2L)))
      val winnerRemoved = Seq(removeFile("f1", priorDv))
      var clockReads = 0
      val clock = () => {
        clockReads += 1
        if (clockReads < 3) 0L else 2L
      }
      val budgets = RebaseBudgets(
        maxDvBytesPerFile = 1024L * 1024L,
        maxDvReads = Int.MaxValue,
        deadlineNanos = 1L,
        nanoTime = clock)

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, budgets)

      assert(result.status == RebaseStatus.Aborted)
      assert(result.failure.contains(RebaseFailure.DeadlineExceeded))
      assert(result.stats.numDvFilesRead == 1)
      assert(result.numDvFilesWritten == 0)
      assert(result.newActions == loserActions)
    }
  }

  // ---------- tightBounds degradation ----------

  test("tightBounds: degraded to false when loserDelta is non-empty") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()

      val priorDv = inlineDv(0L)
      val winnerDv = inlineDv(0L, 1L)
      val loserDv = inlineDv(0L, 5L)

      // Winner stats claim tightBounds=true; after RLC the rebased file must have
      // tightBounds=false because the loser's delta marks additional rows as deleted
      // whose values may have contributed to the tight min/max.
      val winnerStats =
        """{"numRecords":100,"minValues":{"x":1},"maxValues":{"x":100},"tightBounds":true}"""
      val loserActions: Seq[Action] = Seq(removeFile("f1", priorDv), addFile("f1", loserDv))
      val winnerAdded = Seq(addFile("f1", winnerDv, stats = winnerStats))
      val winnerRemoved = Seq(removeFile("f1", priorDv))

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, maxDvBytes = 1024L * 1024L)

      assert(result.resolvedFileCount == 1)
      val rebasedAdd = result.newActions.collect { case a: AddFile => a }
        .find(_.path == "f1").get
      assert(rebasedAdd.stats.contains("\"tightBounds\":false"),
        s"Expected tightBounds=false in rebased stats but got: ${rebasedAdd.stats}")
    }
  }

  test("tightBounds: unchanged when loserDelta is empty") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()

      val priorDv = inlineDv(0L)
      val winnerDv = inlineDv(0L, 1L)
      val loserDv = priorDv  // Loser made no new deletions

      val winnerStats = """{"numRecords":100,"tightBounds":true}"""
      val loserActions: Seq[Action] = Seq(removeFile("f1", priorDv), addFile("f1", loserDv))
      val winnerAdded = Seq(addFile("f1", winnerDv, stats = winnerStats))
      val winnerRemoved = Seq(removeFile("f1", priorDv))

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, maxDvBytes = 1024L * 1024L)

      assert(result.resolvedFileCount == 1)
      val rebasedAdd = result.newActions.collect { case a: AddFile => a }
        .find(_.path == "f1").get
      assert(rebasedAdd.stats == winnerStats,
        s"Stats should pass through unchanged when loserDelta is empty")
    }
  }

  test("retry: an already rebased pair can rebase against the next winning commit") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()
      val prior = inlineDv(0L)
      val loserActions: Seq[Action] =
        Seq(removeFile("f", prior), addFile("f", inlineDv(0L, 30L)))

      val first = RowLevelConcurrency.tryRebase(
        loserActions,
        Seq(addFile("f", inlineDv(0L, 10L))),
        Seq(removeFile("f", prior)),
        tablePath,
        hadoopConf,
        maxDvBytes = 1024L * 1024L)
      assert(first.status == RebaseStatus.Succeeded)

      val firstRemove = first.newActions.collectFirst { case remove: RemoveFile => remove }.get
      val firstAdd = first.newActions.collectFirst { case add: AddFile => add }.get
      val secondWinnerAdd = addFile("f", inlineDv(0L, 10L, 20L))
      val second = RowLevelConcurrency.tryRebase(
        first.newActions,
        Seq(secondWinnerAdd),
        Seq(removeFile("f", firstRemove.deletionVector)),
        tablePath,
        hadoopConf,
        maxDvBytes = 1024L * 1024L)

      assert(second.status == RebaseStatus.Succeeded)
      val finalRemove = second.newActions.collectFirst { case remove: RemoveFile => remove }.get
      val finalAdd = second.newActions.collectFirst { case add: AddFile => add }.get
      assert(finalRemove.deletionVector == secondWinnerAdd.deletionVector)
      assert(decodedRows(finalAdd.deletionVector, tablePath, hadoopConf) ==
        Set(0L, 10L, 20L, 30L))
      assert(firstAdd.path == finalAdd.path)
    }
  }

  // ---------- 3-writer convergence fuzz ----------

  test("fuzz: 3-writer convergence -- ABC disjoint always commits all 3 after sequencing") {
    // Property (multi-writer convergence): if three writers A, B, C
    // touch disjoint row sets in the same physical file, then after the ConflictChecker
    // loop processes the winning commits one-at-a-time, the final loser's rebased pair
    // encodes (priorDV union A union B union C) against the latest winner's AddFile.
    //
    // The ConflictChecker iterates over winning commits serially (see
    // OptimisticTransaction.scala:2793-2810): for each winner w, it calls tryRebase with
    // the *current* (possibly already-rebased) loser actions vs that one winner's
    // commit summary. We model that loop here:
    //   1. A commits first (no rebase).
    //   2. B rebases against A.
    //   3. C rebases against A, then C-after-rebase rebases against B-after-rebase.
    val rng = new Random(seed = 42L)
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()

      for (iter <- 1 to 50) {
        val rowsPerWriter = 8
        val universe: IndexedSeq[Long] = (0L until 1000L).toIndexedSeq
        val shuffled: IndexedSeq[Long] = rng.shuffle(universe)
        // Three disjoint slices of 8 rows each = 24 unique rows touched.
        val aRows = shuffled.slice(0, rowsPerWriter)
        val bRows = shuffled.slice(rowsPerWriter, 2 * rowsPerWriter)
        val cRows = shuffled.slice(2 * rowsPerWriter, 3 * rowsPerWriter)
        val priorRows = shuffled.slice(3 * rowsPerWriter, 3 * rowsPerWriter + 5)

        val priorDv = inlineDv(priorRows: _*)
        val aDv = inlineDv((priorRows ++ aRows): _*)
        val bDv = inlineDv((priorRows ++ bRows): _*)
        val cDv = inlineDv((priorRows ++ cRows): _*)

        // Helper: extract the rebased (RemoveFile, AddFile) pair from a rebase result.
        def extractPair(actions: Seq[Action]): (RemoveFile, AddFile) = {
          val r = actions.collect { case x: RemoveFile => x }.head
          val a = actions.collect { case x: AddFile => x }.head
          (r, a)
        }

        // Step 1 (no rebase): A commits first.
        val aAdd = addFile("f", aDv)
        val aRemove = removeFile("f", priorDv)

        // Step 2: B rebases against A. B starts with (priorDv -> bDv).
        val bActions: Seq[Action] = Seq(removeFile("f", priorDv), addFile("f", bDv))
        val bResult = RowLevelConcurrency.tryRebase(
          bActions, Seq(aAdd), Seq(aRemove), tablePath, hadoopConf,
          maxDvBytes = 1024L * 1024L)
        assert(bResult.resolvedFileCount == 1, s"iter=$iter: B should rebase against A")
        val (bRebasedRemove, bRebasedAdd) = extractPair(bResult.newActions)

        // Step 3a: C rebases against A. C also starts with (priorDv -> cDv).
        val cActions: Seq[Action] = Seq(removeFile("f", priorDv), addFile("f", cDv))
        val cVsAResult = RowLevelConcurrency.tryRebase(
          cActions, Seq(aAdd), Seq(aRemove), tablePath, hadoopConf,
          maxDvBytes = 1024L * 1024L)
        assert(cVsAResult.resolvedFileCount == 1,
          s"iter=$iter: C should rebase against A")
        val (cAfterARemove, cAfterAAdd) = extractPair(cVsAResult.newActions)

        // Step 3b: C-after-rebase rebases against B-after-rebase. This is the second
        // iteration of the ConflictChecker loop for C: its loser actions are now the
        // post-rebase pair from step 3a, and the winner is B's already-rebased pair
        // from step 2 (since B committed at the next version).
        val cAfterAActions: Seq[Action] = Seq(cAfterARemove, cAfterAAdd)
        val cFinalResult = RowLevelConcurrency.tryRebase(
          cAfterAActions,
          Seq(bRebasedAdd), Seq(bRebasedRemove),
          tablePath, hadoopConf, maxDvBytes = 1024L * 1024L)
        assert(cFinalResult.resolvedFileCount == 1,
          s"iter=$iter: C-after-A should rebase against B-after-A")

        // Verify the final bitmap exactly equals prior union A union B union C.
        val (_, finalAdd) = extractPair(cFinalResult.newActions)
        val expectedAllRows = (priorRows ++ aRows ++ bRows ++ cRows).distinct.toSet
        val actualRows = decodedRows(finalAdd.deletionVector, tablePath, hadoopConf)
        assert(actualRows == expectedAllRows,
          s"iter=$iter: row-set mismatch; missing=${expectedAllRows -- actualRows}, " +
            s"unexpected=${actualRows -- expectedAllRows}")
        assert(cFinalResult.newActions.collect {
          case add: AddFile if add.path == "f" => add
        }.size == 1, s"iter=$iter: exactly one active AddFile action is required per path")
      }
    }
  }

  test("fuzz: overlap on at least one writer -> not all three converge") {
    // Property: if A's delta overlaps B's delta (say they both delete row 7), then B's
    // rebase against A fails and B falls back to the legacy abort path.
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()

      val priorDv = inlineDv(0L)
      val aDv = inlineDv(0L, 7L)
      val bDv = inlineDv(0L, 7L)  // Same row as A -- overlap!

      val bActions: Seq[Action] = Seq(removeFile("f", priorDv), addFile("f", bDv))
      val aAdded = Seq(addFile("f", aDv))
      val aRemoved = Seq(removeFile("f", priorDv))

      val result = RowLevelConcurrency.tryRebase(
        bActions, aAdded, aRemoved, tablePath, hadoopConf, maxDvBytes = 1024L * 1024L)

      assert(result.resolvedFileCount == 0)
      assert(result.failure.contains(RebaseFailure.Overlap))
    }
  }
}
