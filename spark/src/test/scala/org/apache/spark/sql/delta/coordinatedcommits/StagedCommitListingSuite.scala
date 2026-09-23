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


package org.apache.spark.sql.delta.coordinatedcommits

import java.io.File

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaTestUtils.verifyUnbackfilled
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LocalLogStore
import org.apache.spark.sql.delta.storage.LogStore.logStoreClassConfKey
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest

/**
 * On a Catalog Owned table the fs listing must not promote a staged-commit file
 * (`_staged_commits/N.<uuid>.json`) to a commit. [[RecursiveListingLocalLogStore]] reproduces a
 * recursive-listing LogStore hermetically (no real object store).
 */
class StagedCommitListingSuite extends QueryTest
    with CatalogOwnedTestBaseSuite
    with WithCatalogOwnedBatch100
    with DeltaSQLCommandTest {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(logStoreClassConfKey, classOf[RecursiveListingLocalLogStore].getName)

  /** Runs `f` with Catalog Owned (CCv2) and staged-commit filtering enabled. */
  private def withCatalogOwnedCommits(f: => Unit): Unit = withDefaultCCTableFeature {
    withSQLConf(
      DeltaSQLConf.DELTA_SNAPSHOT_FILESYSTEM_LISTING_FILTER_STAGED_COMMITS_ENABLED.key ->
        "true") {
      f
    }
  }

  private def logDir(path: String): File = new File(DeltaLog.logPathFor(path).toString)

  private def stagedCommitsDir(path: String): File =
    new File(logDir(path), FileNames.COMMIT_SUBDIR)

  private def stagedCommitFiles(path: String): Seq[File] =
    Option(stagedCommitsDir(path).listFiles()).getOrElse(Array.empty)
      .filter(_.getName.endsWith(".json"))
      .sortBy(_.getName)
      .toSeq

  private def assertStagedVersions(path: String, expectedVersions: Seq[Long]): Seq[File] = {
    val staged = stagedCommitFiles(path)
    assert(staged.map(f => FileNames.deltaVersion(new Path(f.getName))) === expectedVersions)
    staged
  }

  private def backfilledDeltaFile(path: String, version: Long): File =
    new File(FileNames.unsafeDeltaFile(new Path(logDir(path).toString), version).toString)

  /** The [[InMemoryCommitCoordinator]] backing the builder, for manipulating its ledger. */
  private def getInMemoryCoordinator: InMemoryCommitCoordinator = {
    val client = getCatalogOwnedCommitCoordinatorClient(
      CatalogOwnedTableUtils.DEFAULT_CATALOG_NAME_FOR_TESTING)
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
    withCatalogOwnedCommits {
      withTempDir { tempDir =>
        val path = tempDir.getCanonicalPath
        spark.range(0, 10).write.format("delta").save(path)
        // Append v1; with batch size 100 it stays staged (un-backfilled).
        spark.range(10, 20).write.format("delta").mode("append").save(path)

        assertStagedVersions(path, Seq(1L))

        val deltaLog = DeltaLog.forTable(spark, path)
        val logPath = deltaLog.logPath
        val listed = deltaLog.store
          .listFrom(FileNames.listingPrefix(logPath, 0L), deltaLog.newDeltaHadoopConf())
          .map(_.getPath)
          .toSeq
        val listedStagedVersions = listed
          .filter(_.getParent.getName == FileNames.COMMIT_SUBDIR)
          .map(FileNames.deltaVersion)
        assert(listedStagedVersions === Seq(1L),
          s"recursive listing should surface exactly staged version 1, but listed: $listed")
      }
    }
  }

  test("filesystem listing keeps checkpoints and minor compactions") {
    withCatalogOwnedCommits {
      withTempDir { tempDir =>
        val path = tempDir.getCanonicalPath
        spark.range(0, 10).write.format("delta").save(path) // v0
        spark.range(10, 20).write.format("delta").mode("append").save(path) // v1 (staged)
        assertStagedVersions(path, Seq(1L))

        val deltaLog = DeltaLog.forTable(spark, path)
        val hadoopConf = deltaLog.newDeltaHadoopConf()
        deltaLog.store.write(
          FileNames.checkpointFileSingular(deltaLog.logPath, 0L),
          Iterator("{}"),
          overwrite = false,
          hadoopConf)
        deltaLog.store.write(
          FileNames.compactedDeltaFile(deltaLog.logPath, 0L, 0L),
          Iterator("{}"),
          overwrite = false,
          hadoopConf)

        def listedFileTypes(
            includeMinorCompactions: Boolean): Set[(FileNames.FileType.Value, Long)] = {
          deltaLog.listFromFileSystemInternal(
            startVersion = 0L,
            versionToLoad = None,
            includeMinorCompactions = includeMinorCompactions)
            ._1
            .getOrElse(Array.empty)
            .map { case (_, fileType, version) => fileType -> version }
            .toSet
        }

        assert(listedFileTypes(includeMinorCompactions = false) === Set(
          FileNames.FileType.DELTA -> 0L,
          FileNames.FileType.CHECKPOINT -> 0L))
        assert(listedFileTypes(includeMinorCompactions = true) === Set(
          FileNames.FileType.DELTA -> 0L,
          FileNames.FileType.CHECKPOINT -> 0L,
          FileNames.FileType.COMPACTED_DELTA -> 0L))
      }
    }
  }

  testWithCatalogOwned(backfillBatchSize = 1)(
      "rollout flag controls staged-commit filtering for a backfilled version") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(0, 10).write.format("delta").save(path) // v0
      spark.range(10, 20).write.format("delta").mode("append").save(path) // v1 (backfilled)

      val stagedV1 = assertStagedVersions(path, Seq(1L)).head
      val backfilledV1 = backfilledDeltaFile(path, 1L)
      assert(backfilledV1.exists() && stagedV1.exists(),
        "batch size 1 must leave both backfilled and staged copies of v1")

      DeltaLog.invalidateCache(spark, new Path(path))
      withSQLConf(
          DeltaSQLConf.DELTA_SNAPSHOT_FILESYSTEM_LISTING_FILTER_STAGED_COMMITS_ENABLED.key ->
            "false") {
        val error = intercept[DeltaIllegalStateException] {
          DeltaLog.forTable(spark, path).update()
        }
        assert(error.getErrorClass.startsWith("DELTA_VERSIONS_NOT_CONTIGUOUS"))
      }

      DeltaLog.invalidateCache(spark, new Path(path))
      withSQLConf(
          DeltaSQLConf.DELTA_SNAPSHOT_FILESYSTEM_LISTING_FILTER_STAGED_COMMITS_ENABLED.key ->
            "true") {
        val snapshot = DeltaLog.forTable(spark, path).update()
        assert(snapshot.version == 1L)
        checkAnswer(
          spark.read.format("delta").load(path).orderBy("id"),
          (0 until 20).map(i => org.apache.spark.sql.Row(i.toLong)))
      }
    }
  }

  test("orphaned staged commit (not in getCommits) is not read") {
    withCatalogOwnedCommits {
      withTempDir { tempDir =>
        val path = tempDir.getCanonicalPath
        spark.range(0, 10).write.format("delta").save(path) // v0
        // Write a genuine, well-formed v1 commit. With batch size 100 it stays staged.
        spark.range(10, 20).write.format("delta").mode("append").save(path) // v1 (staged)

        val deltaLog = DeltaLog.forTable(spark, path)
        val logPath = deltaLog.logPath

        val stagedV1 = assertStagedVersions(path, Seq(1L)).head
        val backfilledV1 = backfilledDeltaFile(path, 1L)
        assert(!backfilledV1.exists(), "batch size 100 must not backfill v1")

        // Orphan v1 by dropping it from the coordinator. Its staged file remains on disk.
        val coordinator = getInMemoryCoordinator
        coordinator.removeCommitTestOnly(logPath, commitVersion = 1L)
        assert(stagedV1.exists(), "orphaning v1 must not delete its staged file")
        assertStagedVersions(path, Seq(1L))
        assert(!backfilledV1.exists(), "orphaned v1 must not have a backfilled copy")

        DeltaLog.invalidateCache(spark, new Path(path))

        val snapshot = DeltaLog.forTable(spark, path).update()
        assert(snapshot.version == 0L,
          s"orphaned staged commit must not advance the snapshot; got v${snapshot.version}")
        checkAnswer(
          spark.read.format("delta").load(path).orderBy("id"),
          (0 until 10).map(i => org.apache.spark.sql.Row(i.toLong)))
      }
    }
  }

  test("ratified, not-yet-backfilled staged commit is still read via the coordinator") {
    withCatalogOwnedCommits {
      withTempDir { tempDir =>
        val path = tempDir.getCanonicalPath
        spark.range(0, 10).write.format("delta").save(path) // v0
        spark.range(10, 20).write.format("delta").mode("append").save(path) // v1 (ratified, staged)

        // v1 is ratified by the coordinator but, with batch size 100, still only exists staged.
        assertStagedVersions(path, Seq(1L))
        assert(!backfilledDeltaFile(path, 1L).exists(),
          "v1 should NOT be backfilled yet (batch size is large)")

        DeltaLog.invalidateCache(spark, new Path(path))
        val deltaLog = DeltaLog.forTable(spark, path)

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

        checkAnswer(
          spark.read.format("delta").load(path).orderBy("id"),
          (0 until 20).map(i => org.apache.spark.sql.Row(i.toLong)))
      }
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
    // `path` is the first file name to return. List from its containing log directory.
    val logDir = path.getParent
    if (!fs.exists(logDir)) {
      throw DeltaErrors.fileOrDirectoryNotFoundException(s"$logDir")
    }

    // This mirrors an object-store prefix listing without a delimiter.
    val collected = scala.collection.mutable.ArrayBuffer.empty[FileStatus]
    val remoteFiles = fs.listFiles(logDir, true)
    while (remoteFiles.hasNext) {
      collected += remoteFiles.next()
    }
    // Return files with name >= the start name, sorted by leaf name (as the S3 log store does).
    collected
      .filter(_.getPath.getName >= path.getName)
      .sortBy(_.getPath.getName)
      .iterator
  }
}
