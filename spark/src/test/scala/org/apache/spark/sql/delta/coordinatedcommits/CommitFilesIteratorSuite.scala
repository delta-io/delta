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

import java.util.UUID

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Action, Checkpoint}
import org.apache.spark.sql.delta.amt.AMTCheckpointTestBase
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/** Tests for API `CoordinatedCommitsUtils.commitFilesIterator()`. */
class CommitFilesIteratorSuite extends AMTCheckpointTestBase {

  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(100)

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, "true")
    .set(DeltaSQLConf.COMMIT_FILES_ITERATOR_BACKFILL_GAP_FIX_ENABLED.key, "true")

  private sealed trait TableKind
  private case object Filesystem extends TableKind
  private case object CatalogManaged extends TableKind
  private case object AMT extends TableKind

  private val checkpointModes = Seq(
    AMTCheckpointScenario.InlineIncremental,
    AMTCheckpointScenario.DeferredIncremental,
    AMTCheckpointScenario.DeferredFull)

  // -------------------------------- Setup helpers -------------------------------

  private def withPreparedDeltaLog(
      tableKind: TableKind,
      backfillBatchSize: Int = 100)(body: (String, DeltaLog) => Unit): Unit = {
    val tableName = "commit_files_iterator_" + UUID.randomUUID().toString.replace("-", "")
    clearBuilders()
    registerBuilder(TrackingInMemoryCommitCoordinatorBuilder(backfillBatchSize))
    try {
      withTable(tableName) {
        createTable(tableName, tableKind)
        val deltaLog = deltaLogForName(tableName)
        val initialSnapshot = deltaLog.update()
        assert(initialSnapshot.version == 0L)
        assert(initialSnapshot.isCatalogOwned == (tableKind != Filesystem))
        body(tableName, deltaLog)
      }
    } finally {
      clearBuilders()
      DeltaLog.clearCache()
    }
  }

  private def createTable(tableName: String, tableKind: TableKind): Unit = {
    def createNonAMTTable(): Unit = sql(
      s"CREATE TABLE $tableName (id INT) USING delta TBLPROPERTIES " +
        s"('delta.checkpointInterval' = '${Int.MaxValue}')")

    tableKind match {
      case AMT => createAMTTable(tableName, checkpointInterval = Int.MaxValue)
      case CatalogManaged => createNonAMTTable()
      case Filesystem => withoutDefaultCCTableFeature { createNonAMTTable() }
    }
  }

  private def appendCommits(tableName: String, numCommits: Int): Unit = {
    val deltaLog = deltaLogForName(tableName)
    val firstVersion = deltaLog.update().version + 1
    (firstVersion until firstVersion + numCommits).foreach { version =>
      sql(s"INSERT INTO $tableName VALUES ($version)")
    }
    assert(deltaLog.update().version == firstVersion + numCommits - 1)
  }

  private def writeManifestCommit(
      tableName: String,
      checkpointMode: AMTCheckpointScenario): Long = {
    val deltaLog = deltaLogForName(tableName)
    val manifestVersion = deltaLog.update().version + 1
    checkpointMode match {
      case AMTCheckpointScenario.InlineIncremental =>
        withInline { sql(s"INSERT INTO $tableName VALUES ($manifestVersion)") }
      case _ => commitCheckpoint(deltaLog, incremental = checkpointMode.isIncremental)
    }
    val snapshot = deltaLog.update()
    val rootVersion = if (checkpointMode.isInline) manifestVersion else manifestVersion - 1
    assert(snapshot.version == manifestVersion)
    assert(amtProvider(snapshot).exists(_.version == rootVersion))
    // Read directly: checkpointAt calls the iterator under test.
    val actions = deltaLog.store
      .read(stagedDeltaPath(deltaLog, manifestVersion), deltaLog.newDeltaHadoopConf())
      .map(Action.fromJson)
    assert(actions.collect { case cp: Checkpoint => cp.version } == Seq(rootVersion))
    manifestVersion
  }

  private def writeBootstrapCheckpoint(tableName: String): Unit = {
    assert(writeManifestCommit(tableName, AMTCheckpointScenario.DeferredFull) == 1L)
  }

  private def backfillToVersion(tableName: String, version: Long): Unit = {
    val catalogTable = catalogTableFor(tableName)
    val snapshot = deltaLogForName(tableName).update(catalogTableOpt = Some(catalogTable))
    CatalogOwnedTableUtils
      .populateTableCommitCoordinatorFromCatalog(spark, Some(catalogTable), snapshot).get
      .backfillToVersion(Some(catalogTable.identifier), version)
  }

  private def reopenDeltaLog(tableName: String): DeltaLog = {
    DeltaLog.clearCache()
    deltaLogForName(tableName)
  }

  // ------------------------------- File assertions ------------------------------

  private def canonicalDeltaPath(deltaLog: DeltaLog, version: Long): Path =
    FileNames.unsafeDeltaFile(deltaLog.logPath, version)

  private def stagedDeltaPath(deltaLog: DeltaLog, version: Long): Path = {
    val commitDirectoryPath = FileNames.commitDirPath(deltaLog.logPath)
    val matchingCommitFiles = commitDirectoryPath.getFileSystem(deltaLog.newDeltaHadoopConf())
      .listStatus(commitDirectoryPath)
      .filter { status =>
        FileNames.isUnbackfilledDeltaFile(status) && FileNames.deltaVersion(status) == version
      }
    assert(matchingCommitFiles.length == 1, s"Expected one staged file at version $version")
    matchingCommitFiles.head.getPath
  }

  private def expectedCommitFiles(
      deltaLog: DeltaLog,
      versions: Seq[Long],
      lastBackfilledVersion: Long): Seq[(Long, Path)] = {
    versions.map { version =>
      val path = if (version <= lastBackfilledVersion) {
        canonicalDeltaPath(deltaLog, version)
      } else {
        stagedDeltaPath(deltaLog, version)
      }
      version -> path
    }
  }

  private def commitFilesIterator(
      deltaLog: DeltaLog,
      catalogTable: CatalogTable,
      startVersion: Long): Iterator[(Long, Path)] = {
    CoordinatedCommitsUtils
      .commitFilesIterator(deltaLog, Some(catalogTable), startVersion)
      .map { case (status, version) => version -> status.getPath }
  }

  private def assertCommitFiles(
      deltaLog: DeltaLog,
      tableName: String,
      startVersion: Long,
      expected: Seq[(Long, Path)]): Unit = {
    val actual = commitFilesIterator(deltaLog, catalogTableFor(tableName), startVersion).toVector
    assert(actual == expected)
  }

  // ------------------------- Filesystem and catalog-managed ----------------------

  for (tableKind <- Seq(Filesystem, CatalogManaged)) {
    test(s"$tableKind: version zero and inclusive start-version filtering") {
      withPreparedDeltaLog(tableKind, backfillBatchSize = 1) { (tableName, deltaLog) =>
        assertCommitFiles(
          deltaLog,
          tableName,
          startVersion = 0,
          expected = expectedCommitFiles(deltaLog, versions = Seq(0L), lastBackfilledVersion = 0))
        assertCommitFiles(
          deltaLog,
          tableName,
          startVersion = 1,
          expected = expectedCommitFiles(deltaLog, versions = Seq.empty, lastBackfilledVersion = 0))

        appendCommits(tableName, numCommits = 3)
        for (startVersion <- Seq(0L, 2L, 3L, 4L)) {
          assertCommitFiles(
            deltaLog,
            tableName,
            startVersion,
            expected = expectedCommitFiles(
              deltaLog, versions = startVersion to 3L, lastBackfilledVersion = 3))
        }
      }
    }

    test(s"$tableKind: `log.update()` is only invoked at exhaustion on CatalogManaged tables") {
      withPreparedDeltaLog(tableKind, backfillBatchSize = 1) { (tableName, deltaLog) =>
        appendCommits(tableName, numCommits = 3)
        val catalogTable = catalogTableFor(tableName)
        // Include iterator construction in the first tracked read.
        lazy val iterator = commitFilesIterator(deltaLog, catalogTable, startVersion = 0)
        val prefixUsageRecords = collectUsageLogs("deltaLog.update") {
          val actual = iterator.take(2).toVector
          assert(actual == expectedCommitFiles(
            deltaLog, versions = 0L to 1L, lastBackfilledVersion = 3))
        }
        assert(prefixUsageRecords.isEmpty)

        val remainingUsageRecords = collectUsageLogs("deltaLog.update") {
          val actual = iterator.toVector
          assert(actual == expectedCommitFiles(
            deltaLog, versions = 2L to 3L, lastBackfilledVersion = 3))
        }
        val expectedUpdateCount = if (tableKind == CatalogManaged) 1 else 0
        assert(remainingUsageRecords.size == expectedUpdateCount)
      }
    }
  }

  test("CatalogManaged: canonical prefix, staged suffix, and complete backfill") {
    withPreparedDeltaLog(CatalogManaged) { (tableName, deltaLog) =>
      appendCommits(tableName, numCommits = 3)
      backfillToVersion(tableName, 1)
      assertCommitFiles(
        deltaLog,
        tableName,
        startVersion = 0,
        expected = expectedCommitFiles(deltaLog, versions = 0L to 3L, lastBackfilledVersion = 1))
      assertCommitFiles(
        deltaLog,
        tableName,
        startVersion = 2,
        expected = expectedCommitFiles(deltaLog, versions = 2L to 3L, lastBackfilledVersion = 1))

      backfillToVersion(tableName, 3)
      assertCommitFiles(
        deltaLog,
        tableName,
        startVersion = 0,
        expected = expectedCommitFiles(deltaLog, versions = 0L to 3L, lastBackfilledVersion = 3))
    }
  }

  for (tableKind <- Seq(Filesystem, CatalogManaged, AMT)) {
    test(s"$tableKind: a stale reader discovers newer commits") {
      withPreparedDeltaLog(tableKind) { (tableName, readerDeltaLog) =>
        reopenDeltaLog(tableName)
        appendCommits(tableName, numCommits = 3)
        assert(readerDeltaLog.unsafeVolatileSnapshot.version == 0L)
        val lastBackfilledVersion = if (tableKind == Filesystem) 3L else 0L
        assertCommitFiles(
          readerDeltaLog,
          tableName,
          startVersion = 0,
          expected = expectedCommitFiles(
            readerDeltaLog,
            versions = 0L to 3L,
            lastBackfilledVersion = lastBackfilledVersion))
      }
    }
  }

  // -------------------------------- AMT checkpoints -----------------------------

  test("AMT: a bootstrap checkpoint at zero retains canonical commit zero") {
    withPreparedDeltaLog(AMT) { (tableName, deltaLog) =>
      writeBootstrapCheckpoint(tableName) // manifest v1, root v0
      assert(deltaLog.update().checkpointProvider.version == 0L)
      assertCommitFiles(
        deltaLog,
        tableName,
        startVersion = 0,
        expected = expectedCommitFiles(deltaLog, versions = 0L to 1L, lastBackfilledVersion = 0))
    }
  }

  for (checkpointMode <- checkpointModes) {
    test(s"AMT ${checkpointMode.name}: staged manifest, trailing commits, and backfill") {
      withPreparedDeltaLog(AMT) { (tableName, deltaLog) =>
        writeBootstrapCheckpoint(tableName)                          // manifest v1, root v0
        appendCommits(tableName, numCommits = 2)                     // v2, v3
        assert(writeManifestCommit(tableName, checkpointMode) == 4L) // manifest v4
        val snapshot = deltaLog.update()
        // A manifest commit backfills all previous commits up to v3.
        checkpointMode match {
          case AMTCheckpointScenario.InlineIncremental =>
            assert(snapshot.logSegment.deltas.isEmpty)
            assert(snapshot.logSegment.deltaAtCheckpointVersionOpt.exists(
              FileNames.deltaVersion(_) == 4L))
            assert(snapshot.logSegment.deltaAtCheckpointVersionOpt.exists(
              FileNames.isUnbackfilledDeltaFile))
          case _ =>
            assert(snapshot.logSegment.deltas.map(FileNames.deltaVersion(_)) == Seq(4L))
            assert(snapshot.logSegment.deltaAtCheckpointVersionOpt.exists(
              FileNames.deltaVersion(_) == 3L))
            // v3 being backfilled doesn't guarantee the cached snapshot has the backfilled delta.
        }
        for (startVersion <- Seq(0L, 3L, 4L, 5L)) {
          assertCommitFiles(
            deltaLog,
            tableName,
            startVersion,
            expected = expectedCommitFiles(
              deltaLog, versions = startVersion to 4L, lastBackfilledVersion = 3))
        }

        val readerDeltaLog = reopenDeltaLog(tableName)
        assertCommitFiles(
          readerDeltaLog,
          tableName,
          startVersion = 0,
          expected = expectedCommitFiles(deltaLog, versions = 0L to 4L, lastBackfilledVersion = 3))
        appendCommits(tableName, numCommits = 2)
        assertCommitFiles(
          readerDeltaLog,
          tableName,
          startVersion = 4,
          expected = expectedCommitFiles(
            readerDeltaLog, versions = 4L to 6L, lastBackfilledVersion = 3))

        backfillToVersion(tableName, 6)
        assertCommitFiles(
          readerDeltaLog,
          tableName,
          startVersion = 0,
          expected = expectedCommitFiles(
            readerDeltaLog, versions = 0L to 6L, lastBackfilledVersion = 6))
      }
    }

    test(s"AMT ${checkpointMode.name}: a stale reader discovers a newer manifest") {
      withPreparedDeltaLog(AMT) { (tableName, readerDeltaLog) =>
        writeBootstrapCheckpoint(tableName)
        reopenDeltaLog(tableName)
        appendCommits(tableName, numCommits = 2)
        assert(writeManifestCommit(tableName, checkpointMode) == 4L)
        assert(readerDeltaLog.unsafeVolatileSnapshot.version == 1L)
        assertCommitFiles(
          readerDeltaLog,
          tableName,
          startVersion = 0,
          expected = expectedCommitFiles(
            readerDeltaLog, versions = 0L to 4L, lastBackfilledVersion = 3))
      }
    }
  }

  test("AMT: advancing an inline checkpoint retains older manifest commits") {
    withPreparedDeltaLog(AMT) { (tableName, deltaLog) =>
      writeBootstrapCheckpoint(tableName)
      appendCommits(tableName, numCommits = 1)
      assert(writeManifestCommit(tableName, AMTCheckpointScenario.InlineIncremental) == 3L)
      appendCommits(tableName, numCommits = 1)
      assert(writeManifestCommit(tableName, AMTCheckpointScenario.InlineIncremental) == 5L)
      assertCommitFiles(
        deltaLog,
        tableName,
        startVersion = 0,
        expected = expectedCommitFiles(deltaLog, versions = 0L to 5L, lastBackfilledVersion = 4))
    }
  }

  // --------------------------- Backfill between phases --------------------------

  // The local log store captures the listing eagerly. Each test consumes only v0 before
  // interleaving writes, leaving the snapshot phase unevaluated until the iterator is drained.
  for (tableKind <- Seq(CatalogManaged, AMT)) {
    for (backfilledVersion <- Seq(2L, 4L)) {
      test(s"$tableKind: gap-listing after concurrent backfills through v$backfilledVersion") {
        withPreparedDeltaLog(tableKind) { (tableName, readerDeltaLog) =>
          if (tableKind == AMT) writeBootstrapCheckpoint(tableName)
          else appendCommits(tableName, numCommits = 1)
          // Keep the reader at v1; writes through the newly cached DeltaLog advance to v4.
          reopenDeltaLog(tableName)
          appendCommits(tableName, numCommits = 3)
          val iterator = commitFilesIterator(
            readerDeltaLog, catalogTable = catalogTableFor(tableName), startVersion = 0)
          assert(iterator.take(1).toVector == expectedCommitFiles(
            readerDeltaLog, versions = Seq(0L), lastBackfilledVersion = 0))

          // Phase 1 saw only v0. Backfill either v1-v2 (leaving a staged tail) or all v1-v4.
          backfillToVersion(tableName, version = backfilledVersion)
          assert(readerDeltaLog.unsafeVolatileSnapshot.version == 1L)
          // Phase 2 advances the reader from v1 to v4. Its staged tail omits backfilled commits,
          // so a separate gap listing must recover v1-v2 (partial) or v1-v4 (complete).
          assert(iterator.toVector == expectedCommitFiles(
            readerDeltaLog,
            versions = 1L to 4L,
            lastBackfilledVersion = backfilledVersion))
          assert(readerDeltaLog.unsafeVolatileSnapshot.logSegment.unbackfilledDeltas.map(_._2) ==
            ((backfilledVersion + 1) to 4L))
        }
      }
    }

    test(s"$tableKind: gap-listing does not duplicate commits in a stale unbackfilled tail") {
      withPreparedDeltaLog(tableKind) { (tableName, readerDeltaLog) =>
        if (tableKind == AMT) writeBootstrapCheckpoint(tableName)
        else appendCommits(tableName, numCommits = 1)
        reopenDeltaLog(tableName)
        appendCommits(tableName, numCommits = 3)
        val catalogTable = catalogTableFor(tableName)
        val iterator = commitFilesIterator(readerDeltaLog, catalogTable, startVersion = 0)
        assert(iterator.take(1).toVector == expectedCommitFiles(
          readerDeltaLog, versions = Seq(0L), lastBackfilledVersion = 0))

        // Backfill v1-v2 and refresh the reader to v4 with v3-v4 staged.
        backfillToVersion(tableName, version = 2)
        val snapshot = readerDeltaLog.update(catalogTableOpt = Some(catalogTable))
        assert(snapshot.logSegment.unbackfilledDeltas.map(_._2) == Seq(3L, 4L))

        // Backfill v3 without advancing the table version.
        backfillToVersion(tableName, version = 3)
        // After `update()`, the reader keeps the cached snapshot with stale unbackfilled commits.
        // Even though the snapshot knows that v3 is backfilled, its log segment still contains the
        // unbackfilled delta. The same will happen with the `update()` call from the iterator.
        val staleSnapshot = readerDeltaLog.update(catalogTableOpt = Some(catalogTable))
        assert(staleSnapshot.getLastKnownBackfilledVersion == 3L)
        assert(staleSnapshot.logSegment.unbackfilledDeltas.map(_._2) == Seq(3L, 4L))
        val v3BackfilledDeltaPath = canonicalDeltaPath(readerDeltaLog, 3)
        assert(v3BackfilledDeltaPath.getFileSystem(readerDeltaLog.newDeltaHadoopConf())
          .exists(v3BackfilledDeltaPath))

        // Phase 2 reuses the v4 snapshot's staged v3-v4. The gap-listing must stop at v2, even
        // though the backfilled v3 now exists, so v3 is returned only once.
        assert(iterator.toVector == expectedCommitFiles(
          readerDeltaLog, versions = 1L to 4L, lastBackfilledVersion = 2))
      }
    }
  }

  for (checkpointMode <- checkpointModes) {
    for (backfillManifest <- Seq(false, true)) {
      test(s"AMT ${checkpointMode.name}: manifest commits published between iterator phases, " +
          s"backfillManifest=$backfillManifest") {
        withPreparedDeltaLog(AMT) { (tableName, readerDeltaLog) =>
          writeBootstrapCheckpoint(tableName) // manifest v1, root v0
          reopenDeltaLog(tableName)
          appendCommits(tableName, numCommits = 2) // v2, v3 remain staged
          val iterator = commitFilesIterator(
            readerDeltaLog, catalogTable = catalogTableFor(tableName), startVersion = 0)
          assert(iterator.take(1).toVector == expectedCommitFiles(
            readerDeltaLog, versions = Seq(0L), lastBackfilledVersion = 0))

          // Publishing v4 backfills v1-v3 after phase 1 has already listed the filesystem.
          assert(writeManifestCommit(tableName, checkpointMode) == 4L)
          if (backfillManifest) backfillToVersion(tableName, version = 4)
          assert(readerDeltaLog.unsafeVolatileSnapshot.version == 1L)
          val lastBackfilledVersion = if (backfillManifest) 4L else 3L
          assert(iterator.toVector == expectedCommitFiles(
            readerDeltaLog,
            versions = 1L to 4L,
            lastBackfilledVersion = lastBackfilledVersion))

          // The reader's snapshot was updated during the iterator consumption above.
          val snapshot = readerDeltaLog.unsafeVolatileSnapshot
          assert(snapshot.version == 4L)
          val expectedStagedVersions = if (backfillManifest) Seq.empty else Seq(4L)
          assert(snapshot.logSegment.unbackfilledDeltas.map(_._2) == expectedStagedVersions)
          if (checkpointMode.isInline) {
            // The manifest is at the checkpoint version, outside the replay suffix.
            assert(snapshot.logSegment.deltas.isEmpty)
          }
        }
      }
    }
  }
}
