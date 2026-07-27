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

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.RowLevelConcurrency.{RebaseFailure, RebaseStatus}
import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Unit tests for [[RowLevelConcurrency.tryRebase]] -- the DELETE rebase
 * algorithm. These tests construct synthetic Action sequences with inline DVs (so the
 * decode side does no I/O) and exercise the rebase against a local-filesystem temp dir
 * (the write side issues real PUTs to a temp DV file).
 */
class RowLevelConcurrencyDeleteRebaseSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  // ---------- helpers ----------

  /** Build a RoaringBitmapArray containing the given row indices. */
  private def bitmap(rows: Long*): RoaringBitmapArray = {
    val b = new RoaringBitmapArray()
    rows.foreach(b.add)
    b
  }

  /** Encode a bitmap as an inline DeletionVectorDescriptor (no I/O required for decode). */
  private def inlineDv(rows: Long*): DeletionVectorDescriptor = {
    val b = bitmap(rows: _*)
    if (b.isEmpty) {
      DeletionVectorDescriptor.EMPTY
    } else {
      val bytes = b.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
      DeletionVectorDescriptor.inlineInLog(bytes, b.cardinality)
    }
  }

  private def addFile(path: String, dv: DeletionVectorDescriptor): AddFile =
    AddFile(
      path = path,
      partitionValues = Map.empty,
      size = 1L,
      modificationTime = 1L,
      dataChange = true,
      stats = "{\"numRecords\": 100}",
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

  private def MAX_DV_BYTES = 1024L * 1024L

  private def decodedRows(
      dv: DeletionVectorDescriptor,
      tablePath: Path,
      hadoopConf: org.apache.hadoop.conf.Configuration): Set[Long] =
    DeletionVectorStore.createInstance(hadoopConf).read(dv, tablePath).toArray.toSet

  // ---------- happy path ----------

  test("tryRebase: disjoint deltas resolve to unioned DV") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()
      val path = "f1.parquet"

      // Prior: rows 0,1 deleted. Winner adds row 2 (delta = {2}).
      // Loser adds row 5 (delta = {5}). Disjoint -> resolvable.
      val priorDv = inlineDv(0L, 1L)
      val winnerDv = inlineDv(0L, 1L, 2L)
      val loserDv = inlineDv(0L, 1L, 5L)

      val loserActions: Seq[Action] = Seq(
        removeFile(path, priorDv),
        addFile(path, loserDv))
      val winnerAdded = Seq(addFile(path, winnerDv))
      val winnerRemoved = Seq(removeFile(path, priorDv))

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, MAX_DV_BYTES)

      assert(result.resolvedFileCount == 1)
      assert(result.status == RebaseStatus.Succeeded)
      assert(result.numDvFilesWritten == 1)  // Non-empty union -> 1 PUT
      assert(result.skipReasons.isEmpty)
      assert(result.resolvedAddFiles.size == 1)

      // Verify the new actions: same path RemoveFile + AddFile pair, plus unionedDV
      val newRemoves = result.newActions.collect { case r: RemoveFile => r }
      val newAdds = result.newActions.collect { case a: AddFile => a }
      assert(newRemoves.size == 1)
      assert(newAdds.size == 1)
      assert(newRemoves.head.path == path)
      assert(newAdds.head.path == path)
      // RemoveFile's DV should point at the winner's AddFile DV (the active version)
      assert(newRemoves.head.deletionVector == winnerDv)
      assert(decodedRows(newAdds.head.deletionVector, tablePath, hadoopConf) == Set(0L, 1L, 2L, 5L))
    }
  }

  test("tryRebase: no new deletions on either side still rebases to the prior DV") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()
      val path = "f1.parquet"

      // Neither side added deletions, so the union equals the (non-empty) prior DV.
      val priorDv = inlineDv(0L, 1L)
      val winnerDv = priorDv
      val loserDv = priorDv

      val loserActions: Seq[Action] = Seq(removeFile(path, priorDv), addFile(path, loserDv))
      val winnerAdded = Seq(addFile(path, winnerDv))
      val winnerRemoved = Seq(removeFile(path, priorDv))

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, MAX_DV_BYTES)

      assert(result.resolvedFileCount == 1)
      // The union is non-empty, so it is materialized as a single DV write.
      assert(result.numDvFilesWritten == 1)
      val newAdds = result.newActions.collect { case a: AddFile => a }
      assert(decodedRows(newAdds.head.deletionVector, tablePath, hadoopConf) == Set(0L, 1L))
    }
  }

  test("tryRebase: fully empty DVs rebase to the EMPTY descriptor with zero PUTs") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()
      val path = "f1.parquet"

      val emptyDv = inlineDv()
      val loserActions: Seq[Action] = Seq(removeFile(path, emptyDv), addFile(path, emptyDv))
      val winnerAdded = Seq(addFile(path, emptyDv))
      val winnerRemoved = Seq(removeFile(path, emptyDv))

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, MAX_DV_BYTES)

      assert(result.resolvedFileCount == 1)
      assert(result.numDvFilesWritten == 0, "An empty union must not write a DV file")
      val newAdds = result.newActions.collect { case a: AddFile => a }
      assert(newAdds.head.deletionVector == DeletionVectorDescriptor.EMPTY)
    }
  }

  test("tryRebase: loser DV that drops a prior deletion aborts atomically") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()
      val path = "f1.parquet"

      // The loser's post-image DV omits row 1, which the prior DV already deleted.
      // Committing it would resurrect a deleted row, so the rebase must abort.
      val priorDv = inlineDv(0L, 1L)
      val winnerDv = inlineDv(0L, 1L, 2L)
      val loserDv = inlineDv(0L)

      val loserActions: Seq[Action] = Seq(removeFile(path, priorDv), addFile(path, loserDv))
      val winnerAdded = Seq(addFile(path, winnerDv))
      val winnerRemoved = Seq(removeFile(path, priorDv))

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, MAX_DV_BYTES)

      assert(result.status == RowLevelConcurrency.RebaseStatus.Aborted)
      assert(result.failure.contains(RowLevelConcurrency.RebaseFailure.LoserShrunkDv))
      assert(result.resolvedFileCount == 0)
      assert(result.numDvFilesWritten == 0)
      assert(result.newActions == loserActions, "Aborted rebase must not mutate actions")
    }
  }

  // ---------- precondition failures ----------

  test("tryRebase: overlapping deltas (P4 fail) records Overlap and does not mutate") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()
      val path = "f1.parquet"

      // Both winner and loser deleted row 2 -- they overlap.
      val priorDv = inlineDv(0L)
      val winnerDv = inlineDv(0L, 2L)
      val loserDv = inlineDv(0L, 2L)

      val loserActions: Seq[Action] = Seq(removeFile(path, priorDv), addFile(path, loserDv))
      val winnerAdded = Seq(addFile(path, winnerDv))
      val winnerRemoved = Seq(removeFile(path, priorDv))

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, MAX_DV_BYTES)

      assert(result.resolvedFileCount == 0)
      assert(result.numDvFilesWritten == 0)
      assert(result.failure.contains(RebaseFailure.Overlap))
      assert(result.newActions == loserActions)
    }
  }

  test("tryRebase: winner shrinks DV (P2 fail) records WinnerShrunkDv") {
    // P2 precondition (winner monotonicity): the winner's same-path AddFile DV must
    // be a SUPERSET of the prior (read-time) RemoveFile DV. If the winner's DV instead
    // shrinks (e.g. a phantom in-place "undelete" leaked through), RLC must NOT rebase
    // because the loser's reads may have been against rows the winner has now reverted.
    // We synthesize that exact shape: prior contains row 5, winner's new DV does not.
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()
      val path = "f1.parquet"

      val priorDv = inlineDv(0L, 5L)         // baseline: rows {0, 5}
      val winnerDv = inlineDv(0L, 1L)        // winner shrinks 5, adds 1 -- prior \ winner != empty
      val loserDv = inlineDv(0L, 5L, 9L)     // loser added row 9 over the same baseline

      val loserActions: Seq[Action] = Seq(removeFile(path, priorDv), addFile(path, loserDv))
      val winnerAdded = Seq(addFile(path, winnerDv))
      val winnerRemoved = Seq(removeFile(path, priorDv))  // same priorDv => P3 passes

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, MAX_DV_BYTES)

      assert(result.resolvedFileCount == 0)
      assert(result.failure.contains(RebaseFailure.WinnerShrunkDv),
        s"Expected WinnerShrunkDv but got: ${result.failure}")
      // P2 fires before P4 -- assert no false "Overlap" or "DifferentBaseDv" reading
      assert(result.newActions == loserActions)
    }
  }

  test("tryRebase: different base DV (P3 fail) records DifferentBaseDv") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()
      val path = "f1.parquet"

      val loserRemoveDv = inlineDv(0L)
      val winnerRemoveDv = inlineDv(0L, 1L)  // Different base!

      val loserActions: Seq[Action] = Seq(
        removeFile(path, loserRemoveDv),
        addFile(path, inlineDv(0L, 5L)))
      val winnerAdded = Seq(addFile(path, inlineDv(0L, 1L, 2L)))
      val winnerRemoved = Seq(removeFile(path, winnerRemoveDv))

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, MAX_DV_BYTES)

      assert(result.resolvedFileCount == 0)
      assert(result.failure.contains(RebaseFailure.DifferentBaseDv))
      assert(result.newActions == loserActions)
    }
  }

  test("tryRebase: winner CoW (different add path) records WinnerCowOrCompacted") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()
      val path = "f1.parquet"

      val loserActions: Seq[Action] = Seq(removeFile(path, inlineDv()), addFile(path, inlineDv(5L)))
      val winnerAdded = Seq(addFile("f1_rewritten.parquet", inlineDv()))
      val winnerRemoved = Seq(removeFile(path, inlineDv()))

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, MAX_DV_BYTES)

      assert(result.resolvedFileCount == 0)
      assert(result.failure.contains(RebaseFailure.WinnerCowOrCompacted))
      assert(result.newActions == loserActions)
    }
  }

  // ---------- multi-file ----------

  test("tryRebase: one disjoint and one overlapping path aborts atomically") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()

      // f1: disjoint -> resolved
      val f1Prior = inlineDv(0L)
      val f1Winner = inlineDv(0L, 1L)
      val f1Loser = inlineDv(0L, 5L)
      // f2: overlap -> skipped
      val f2Prior = inlineDv(0L)
      val f2Winner = inlineDv(0L, 2L)
      val f2Loser = inlineDv(0L, 2L)

      val loserActions: Seq[Action] = Seq(
        removeFile("f1", f1Prior), addFile("f1", f1Loser),
        removeFile("f2", f2Prior), addFile("f2", f2Loser))
      val winnerAdded = Seq(addFile("f1", f1Winner), addFile("f2", f2Winner))
      val winnerRemoved = Seq(removeFile("f1", f1Prior), removeFile("f2", f2Prior))

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, MAX_DV_BYTES)

      assert(result.status == RebaseStatus.Aborted)
      assert(result.failure.contains(RebaseFailure.Overlap))
      assert(result.resolvedFileCount == 0)
      assert(result.numDvFilesWritten == 0)
      assert(result.newActions == loserActions)
    }
  }

  // ---------- budget enforcement ----------

  test("tryRebase: DV exceeding maxDvBytes aborts with byte-budget outcome") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()
      val path = "f1.parquet"

      val priorDv = inlineDv(0L)
      val winnerDv = inlineDv(0L, 1L)
      val loserDv = inlineDv(0L, 5L)

      val loserActions: Seq[Action] = Seq(removeFile(path, priorDv), addFile(path, loserDv))
      val winnerAdded = Seq(addFile(path, winnerDv))
      val winnerRemoved = Seq(removeFile(path, priorDv))

      // Tiny budget: 1 byte. Should reject all decodes for non-empty DVs.
      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, maxDvBytes = 1L)

      assert(result.resolvedFileCount == 0)
      assert(result.newActions == loserActions)
      assert(result.failure.contains(RebaseFailure.ByteBudgetExceeded))
    }
  }

  test("tryRebase: one resolvable and one byte-budget path aborts atomically") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()
      val prior = inlineDv()
      val smallWinner = inlineDv(1L)
      val smallLoser = inlineDv(2L)
      val largeWinner = inlineDv((1000L until 2000L): _*)
      val largeLoser = inlineDv(3L)
      val loserActions: Seq[Action] = Seq(
        removeFile("f1", prior), addFile("f1", smallLoser),
        removeFile("f2", prior), addFile("f2", largeLoser))
      val winnerAdded = Seq(addFile("f1", smallWinner), addFile("f2", largeWinner))
      val winnerRemoved = Seq(removeFile("f1", prior), removeFile("f2", prior))
      val maxBytes = math.max(smallWinner.sizeInBytes, smallLoser.sizeInBytes).toLong

      val result = RowLevelConcurrency.tryRebase(
        loserActions, winnerAdded, winnerRemoved, tablePath, hadoopConf, maxDvBytes = maxBytes)

      assert(result.status == RebaseStatus.Aborted)
      assert(result.failure.contains(RebaseFailure.ByteBudgetExceeded))
      assert(result.stats.numDvFilesRead == 0)
      assert(result.numDvFilesWritten == 0)
      assert(result.newActions == loserActions)
    }
  }

  test("tryRebase: malformed inline DV reports decode failure") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()
      val prior = inlineDv()
      val malformed = DeletionVectorDescriptor.inlineInLog(Array[Byte](1, 2, 3), 1L)
      val loserActions: Seq[Action] =
        Seq(removeFile("f", prior), addFile("f", inlineDv(2L)))

      val result = RowLevelConcurrency.tryRebase(
        loserActions,
        Seq(addFile("f", malformed)),
        Seq(removeFile("f", prior)),
        tablePath,
        hadoopConf,
        MAX_DV_BYTES)

      assert(result.failure.contains(RebaseFailure.DecodeFailure))
      assert(result.newActions == loserActions)
    }
  }

  test("tryRebase: missing external DV reports read failure") {
    withTempDir { dir =>
      val tablePath = new Path("file:" + dir.getAbsolutePath)
      val hadoopConf = newHadoopConf()
      val prior = inlineDv()
      val missing = DeletionVectorDescriptor.onDiskWithAbsolutePath(
        path = new Path(tablePath, "missing.dv").toString,
        sizeInBytes = 32,
        cardinality = 1L,
        offset = Some(0))
      val loserActions: Seq[Action] =
        Seq(removeFile("f", prior), addFile("f", inlineDv(2L)))

      val result = RowLevelConcurrency.tryRebase(
        loserActions,
        Seq(addFile("f", missing)),
        Seq(removeFile("f", prior)),
        tablePath,
        hadoopConf,
        MAX_DV_BYTES)

      assert(result.failure.contains(RebaseFailure.DvReadFailure))
      assert(result.stats.numDvFilesRead == 1)
      assert(result.newActions == loserActions)
    }
  }

  test("tryRebase: DV write failure aborts without mutating actions") {
    val tablePath = new Path("rlc-missing-filesystem://bucket/table")
    val hadoopConf = newHadoopConf()
    val prior = inlineDv()
    val loserActions: Seq[Action] =
      Seq(removeFile("f", prior), addFile("f", inlineDv(2L)))

    val result = RowLevelConcurrency.tryRebase(
      loserActions,
      Seq(addFile("f", inlineDv(1L))),
      Seq(removeFile("f", prior)),
      tablePath,
      hadoopConf,
      MAX_DV_BYTES)

    assert(result.failure.contains(RebaseFailure.DvWriteFailure))
    assert(result.newActions == loserActions)
  }
}
