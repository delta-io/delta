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

package org.apache.spark.sql.delta

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.delta.coordinatedcommits.{CommitCoordinatorProvider,
  CoordinatedCommitsBaseSuite, InMemoryCommitCoordinator, TrackingCommitCoordinatorClient}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LocalLogStore
import org.apache.spark.sql.delta.storage.LogStore.logStoreClassConfKey
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}

/**
 * On a Coordinated-Commits table the fs listing must not promote a staged-commit file
 * (`_staged_commits/N.<uuid>.json`) to a commit. [[RecursiveListingLocalLogStore]] reproduces a
 * recursive-listing LogStore hermetically (no real object store).
 */
class StagedCommitListingSuite extends QueryTest
    with DeltaSQLCommandTest
    with CoordinatedCommitsBaseSuite {

  // Large batch size keeps commits staged (un-backfilled) for these tests.
  override def coordinatedCommitsBackfillBatchSize: Option[Int] = Some(100)

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(logStoreClassConfKey, classOf[RecursiveListingLocalLogStore].getName)
      // Keep minor compactions out of the picture; these tests reason about commit versions.
      .set(DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_READS.key, "false")

  private def stagedCommitFiles(deltaLog: DeltaLog): Seq[File] =
    Option(new File(FileNames.commitDirPath(deltaLog.logPath).toUri).listFiles())
      .getOrElse(Array.empty[File])
      .filter(_.getName.endsWith(".json"))
      .sortBy(_.getName)
      .toSeq

  private def stagedCommitFile(deltaLog: DeltaLog, version: Long): File =
    stagedCommitFiles(deltaLog)
      .find(f => FileNames.deltaVersion(new Path(f.toURI)) == version)
      .getOrElse(fail(s"expected staged commit v$version"))

  private def backfilledDeltaFile(deltaLog: DeltaLog, version: Long): File =
    new File(FileNames.unsafeDeltaFile(deltaLog.logPath, version).toUri)

  /** Creates a table with backfilled v0 and staged, ratified v1. */
  private def withStagedV1Table(f: DeltaLog => Unit): Unit = withTempDir { tempDir =>
    val path = tempDir.getCanonicalPath
    spark.range(0, 10).write.format("delta").save(path)
    spark.range(10, 20).write.format("delta").mode("append").save(path)
    DeltaLog.invalidateCache(spark, new Path(path))
    f(DeltaLog.forTable(spark, path))
  }

  private def checkIds(deltaLog: DeltaLog, endExclusive: Int): Unit =
    checkAnswer(
      spark.read.format("delta").load(deltaLog.dataPath.toString).orderBy("id"),
      (0 until endExclusive).map(i => Row(i.toLong)))

  /** The [[InMemoryCommitCoordinator]] backing the builder, for manipulating its ledger. */
  private def getInMemoryCoordinator: InMemoryCommitCoordinator = {
    val client = CommitCoordinatorProvider
      .getCommitCoordinatorClient(
        defaultCommitsCoordinatorName, defaultCommitsCoordinatorConf, spark)
    client match {
      case tracking: TrackingCommitCoordinatorClient =>
        tracking.delegatingCommitCoordinatorClient.asInstanceOf[InMemoryCommitCoordinator]
      case inMemory: InMemoryCommitCoordinator => inMemory
      case other =>
        throw new IllegalStateException(
          s"Unexpected commit coordinator client type: ${other.getClass.getName}")
    }
  }

  // Sanity check: the harness actually surfaces staged files, else the tests below pass vacuously.
  test("test harness: recursive listing surfaces _staged_commits files") {
    withStagedV1Table { deltaLog =>
      assert(stagedCommitFiles(deltaLog).nonEmpty,
        "expected at least one staged commit file to exist on disk")

      val listed = deltaLog.store
        .listFrom(FileNames.listingPrefix(deltaLog.logPath, 0L), deltaLog.newDeltaHadoopConf())
        .map(_.getPath)
        .toSeq
      assert(listed.exists(p => p.getParent.getName == FileNames.COMMIT_SUBDIR),
        s"recursive listing should surface _staged_commits files, but listed: $listed")
    }
  }

  test("backfilled + staged copy of the same version does not throw NOT_CONTIGUOUS") {
    withStagedV1Table { deltaLog =>
      // Also materialise the backfilled `00...01.json` so v1 exists both backfilled and staged.
      val stagedV1 = stagedCommitFile(deltaLog, version = 1L)
      val backfilledV1 = backfilledDeltaFile(deltaLog, version = 1L)
      assert(!backfilledV1.exists(), "v1 should initially exist only as a staged commit")
      Files.copy(stagedV1.toPath, backfilledV1.toPath)
      assert(backfilledV1.exists() && stagedV1.exists(),
        "v1 must exist both backfilled and staged for this scenario")

      DeltaLog.invalidateCache(spark, deltaLog.dataPath)

      // Before the fix this threw DELTA_VERSIONS_NOT_CONTIGUOUS (versions [0, 1, 1]).
      val reloadedLog = DeltaLog.forTable(spark, deltaLog.dataPath)
      val snapshot = reloadedLog.update()
      assert(snapshot.version == 1L)
      checkIds(reloadedLog, endExclusive = 20)
    }
  }

  test("orphaned staged commit (not in getCommits) is not read") {
    withStagedV1Table { deltaLog =>
      // Orphan v1: drop it from the coordinator, leaving only the well-formed staged file under
      // `_staged_commits/`.
      val coordinator = getInMemoryCoordinator
      coordinator.removeCommitTestOnly(deltaLog.logPath, commitVersion = 1L)
      assert(!backfilledDeltaFile(deltaLog, version = 1L).exists(),
        "v1 should not be backfilled")
      stagedCommitFile(deltaLog, version = 1L)

      DeltaLog.invalidateCache(spark, deltaLog.dataPath)

      val reloadedLog = DeltaLog.forTable(spark, deltaLog.dataPath)
      val snapshot = reloadedLog.update()
      assert(snapshot.version == 0L,
        s"orphaned staged commit must not advance the snapshot; got v${snapshot.version}")
      checkIds(reloadedLog, endExclusive = 10)
    }
  }

  test("ratified, not-yet-backfilled staged commit is still read via the coordinator") {
    withStagedV1Table { deltaLog =>
      // v1 is ratified by the coordinator but, with batch size 100, still only exists staged.
      stagedCommitFile(deltaLog, version = 1L)
      assert(!backfilledDeltaFile(deltaLog, version = 1L).exists(),
        "v1 should NOT be backfilled yet (batch size is large)")

      // Channel check: the raw fs listing must NOT surface v1 (else we wouldn't be testing the
      // coordinator path).
      val (fsListingOpt, _) = deltaLog.listFromFileSystemInternal(
        startVersion = 0L,
        versionToLoad = None,
        includeMinorCompactions = false)
      val fsDeltaVersions = fsListingOpt.getOrElse(Array.empty)
        .collect { case (f, FileNames.FileType.DELTA, v) => v }.toSeq
      assert(!fsDeltaVersions.contains(1L),
        s"raw fs listing must not surface staged v1 as a delta; got $fsDeltaVersions")

      val snapshot = deltaLog.update()
      assert(snapshot.version == 1L,
        s"ratified staged v1 must be read via the coordinator; got v${snapshot.version}")
      // The v1 delta is the staged (unbackfilled) file, i.e. it came from the coordinator.
      val v1Delta = snapshot.logSegment.deltas
        .find(f => FileNames.deltaVersion(f.getPath) == 1L)
        .getOrElse(fail("v1 delta missing from the log segment"))
      verifyUnbackfilled(v1Delta)

      checkIds(deltaLog, endExclusive = 20)
    }
  }
}

/**
 * A [[LocalLogStore]] whose `listFrom` recurses into subdirectories, reproducing a recursive
 * LogStore that surfaces `_staged_commits/` files (the default [[LocalLogStore]] lists only
 * immediate children and masks the bug). Top-level so it can be set via the logStore class conf.
 */
class RecursiveListingLocalLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
    extends LocalLogStore(sparkConf, hadoopConf) {

  override def listFrom(path: Path, hadoopConf: Configuration): Iterator[FileStatus] = {
    val fs = path.getFileSystem(hadoopConf)
    val parent = path.getParent
    if (!fs.exists(parent)) {
      throw DeltaErrors.fileOrDirectoryNotFoundException(s"$parent")
    }
    // Recursively collect leaf files under the parent (mirrors a no-delimiter prefix listing).
    val collected = scala.collection.mutable.ArrayBuffer.empty[FileStatus]
    def recurse(dir: Path): Unit = {
      fs.listStatus(dir).foreach { st =>
        if (st.isDirectory) recurse(st.getPath) else collected += st
      }
    }
    recurse(parent)
    // Return files with name >= the start name, sorted by leaf name (as the S3 log store does).
    collected
      .filter(_.getPath.getName >= path.getName)
      .sortBy(_.getPath.getName)
      .iterator
  }
}
