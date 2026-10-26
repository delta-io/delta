/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults.catalogManaged

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import io.delta.kernel.{CommitRange, Operation, SnapshotBuilder, TableManager}
import io.delta.kernel.CommitRangeBuilder.CommitBoundary
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.MetricsEngine
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO
import io.delta.kernel.defaults.internal.json.JsonUtils
import io.delta.kernel.defaults.utils.{TestRow, TestUtilsWithTableManagerAPIs, WriteUtilsWithV2Builders}
import io.delta.kernel.exceptions.{KernelException, MaxCatalogVersionException, UnsupportedTableFeatureException}
import io.delta.kernel.internal.{DeltaHistoryManager, SnapshotImpl}
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction
import io.delta.kernel.internal.actions.Protocol
import io.delta.kernel.internal.actions.SingleAction.createProtocolSingleAction
import io.delta.kernel.internal.commitrange.CommitRangeImpl
import io.delta.kernel.internal.files.{ParsedCatalogCommitData, ParsedLogData}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.table.SnapshotBuilderImpl
import io.delta.kernel.internal.tablefeatures.TableFeatures.{isCatalogManagedSupported, CATALOG_MANAGED_RW_FEATURE, IN_COMMIT_TIMESTAMP_W_FEATURE, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION}
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.CloseableIterable.emptyIterable
import io.delta.kernel.utils.FileStatus

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for end-to-end reads of catalog-managed Delta tables.
 *
 * The goal of this suite is to simulate how a real "Catalog-Managed-Client" would read a
 * catalog-managed Delta table, without introducing a full, or even partial (e.g. in-memory)
 * catalog client implementation.
 *
 * The catalog boundary is simulated by tests manually providing [[ParsedLogData]]. For example,
 * there can be X commits in the _staged_commits directory, and a given test can decide that Y
 * commits (subset of X) are in fact "ratified". The test can then turn those commits into
 * [[ParsedLogData]] and inject them into the [[SnapshotBuilder]]. This is,
 * in essence, doing exactly what we would expect a "Catalog-Managed-Client" to do.
 */
class CatalogManagedE2EReadSuite extends AnyFunSuite
    with TestUtilsWithTableManagerAPIs with WriteUtilsWithV2Builders {

  def withCatalogOwnedPreviewTestTable(testFx: (String, List[ParsedLogData]) => Unit): Unit = {
    val tablePath = getTestResourceFilePath("catalog-owned-preview")

    // Note: We need to *resolve* each test resource file path, because the table root file path
    //       will itself be resolved when we create the Snapshot. If we resolved some paths but
    //       not others, we would get an error like `File <commit-file> doesn't belong in the
    //       transaction log at <log-path>`.

    val parsedLogData = Seq(
      // scalastyle:off line.size.limit
      getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000001.4cb9708e-b478-44de-b203-53f9ba9b2876.json"),
      getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000002.5b9bba4a-0085-430d-a65e-b0d38c1afbe9.json"))
      // scalastyle:on line.size.limit
      .map { path => defaultEngine.getFileSystemClient.resolvePath(path) }
      .map { p => FileStatus.of(p) }
      .map { fs => ParsedLogData.forFileStatus(fs) }
      .toList
    testFx(tablePath, parsedLogData)
  }

  test("simple e2e read of catalogManaged table with staged ratified commits") {
    withCatalogOwnedPreviewTestTable { (tablePath, parsedLogData) =>
      // ===== WHEN =====
      val snapshot = TableManager
        .loadSnapshot(tablePath)
        .asInstanceOf[SnapshotBuilderImpl]
        .atVersion(2)
        .withLogData(parsedLogData.asJava)
        .withMaxCatalogVersion(2)
        .build(defaultEngine)

      // ===== THEN =====
      assert(snapshot.getVersion === 2)
      assert(snapshot.getLogSegment.getDeltas.size() === 3)
      assert(snapshot.getTimestamp(defaultEngine) === 1749830881799L)

      val protocol = snapshot.getProtocol
      assert(protocol.getMinReaderVersion == TABLE_FEATURES_MIN_READER_VERSION)
      assert(protocol.getMinWriterVersion == TABLE_FEATURES_MIN_WRITER_VERSION)
      assert(protocol.getReaderFeatures.contains(CATALOG_MANAGED_RW_FEATURE.featureName()))
      assert(protocol.getWriterFeatures.contains(CATALOG_MANAGED_RW_FEATURE.featureName()))
      assert(protocol.getWriterFeatures.contains(IN_COMMIT_TIMESTAMP_W_FEATURE.featureName()))

      val actualResult = readSnapshot(snapshot)
      val expectedResult = (0 to 199).map { x => TestRow(x / 100, x) }
      checkAnswer(actualResult, expectedResult)
    }
  }

  test("e2e DeltaHistoryManager.getActiveCommitAtTimestamp with catalogManaged table " +
    "with staged ratified commits") {
    withCatalogOwnedPreviewTestTable { (tablePath, parsedLogData) =>
      val logPath = new Path(tablePath, "_delta_log")

      val parsedRatifiedCatalogCommits = parsedLogData
        .filter(_.isInstanceOf[ParsedCatalogCommitData])
        .map(_.asInstanceOf[ParsedCatalogCommitData])

      val latestSnapshot = TableManager
        .loadSnapshot(tablePath)
        .asInstanceOf[SnapshotBuilderImpl]
        .withLogData(parsedLogData.asJava)
        .withMaxCatalogVersion(2)
        .build(defaultEngine)

      def checkGetActiveCommitAtTimestamp(
          timestamp: Long,
          expectedVersion: Long,
          canReturnLastCommit: Boolean = false,
          canReturnEarliestCommit: Boolean = false): Unit = {
        val activeCommit = DeltaHistoryManager.getActiveCommitAtTimestamp(
          defaultEngine,
          latestSnapshot,
          logPath,
          timestamp,
          true, /* mustBeRecreatable */
          canReturnLastCommit,
          canReturnEarliestCommit,
          parsedRatifiedCatalogCommits.asJava)
        assert(activeCommit.getVersion == expectedVersion)
      }

      val v0Ts = 1749830855993L // published commit
      val v1Ts = 1749830871085L // staged commit
      val v2Ts = 1749830881799L // staged commit

      // Query a timestamp before V0 should fail if canReturnEarliestCommit = false
      val e1 = intercept[KernelException] {
        checkGetActiveCommitAtTimestamp(v0Ts - 1, 0)
      }
      assert(e1.getMessage.contains("before the earliest available version"))

      // Query a timestamp before V0 with canReturnEarliestCommit = true
      checkGetActiveCommitAtTimestamp(v0Ts - 1, 0, canReturnEarliestCommit = true)

      // Query @ V0
      checkGetActiveCommitAtTimestamp(v0Ts, 0)

      // Query between V0 and V1
      checkGetActiveCommitAtTimestamp(v0Ts + 1, 0)

      // Query at V1
      checkGetActiveCommitAtTimestamp(v1Ts, 1)

      // Query between V1 and V2
      checkGetActiveCommitAtTimestamp(v1Ts + 1, 1)

      // Query at V2
      checkGetActiveCommitAtTimestamp(v2Ts, 2)

      // Query a timestamp after V2 should fail with canReturnLastCommit = false
      val e2 = intercept[KernelException] {
        checkGetActiveCommitAtTimestamp(v2Ts + 1, 2)
      }
      assert(e2.getMessage.contains("is after the latest available version"))

      // Query a timestamp after V2 with canReturnLastCommit = true
      checkGetActiveCommitAtTimestamp(v2Ts + 1, 2, canReturnLastCommit = true)

    }
  }

  test("time-travel by ts read of catalogManaged table with ratified commits") {
    withCatalogOwnedPreviewTestTable { (tablePath, parsedLogData) =>
      val v0Ts = 1749830855993L // published commit
      val v1Ts = 1749830871085L // ratified staged commit
      val v2Ts = 1749830881799L // ratified staged commit

      val latestSnapshot = TableManager
        .loadSnapshot(tablePath)
        .asInstanceOf[SnapshotBuilderImpl]
        .withLogData(parsedLogData.asJava)
        .withMaxCatalogVersion(2)
        .build(defaultEngine)

      def checkTimeTravelByTimestamp(
          timestamp: Long,
          expectedVersion: Long,
          expectedSnapshotTimestamp: Long): Unit = {
        val snapshot = TableManager
          .loadSnapshot(tablePath)
          .atTimestamp(timestamp, latestSnapshot)
          .withMaxCatalogVersion(2)
          .withLogData(parsedLogData.asJava)
          .build(defaultEngine)
        assert(snapshot.getVersion == expectedVersion)
        assert(snapshot.getTimestamp(defaultEngine) == expectedSnapshotTimestamp)
      }

      // Between v0 and v1 should return v0 (between published & ratified)
      checkTimeTravelByTimestamp(v0Ts + 1, 0, v0Ts)

      // Exactly v1 should return v1
      checkTimeTravelByTimestamp(v1Ts, 1, v1Ts)

      // Between v1 and v2 should return v1 (between 2 ratified commits)
      checkTimeTravelByTimestamp(v1Ts + 1, 1, v1Ts)

      // Exactly v2 should return v2
      checkTimeTravelByTimestamp(v2Ts, 2, v2Ts)

      // After v2 should fail
      val e = intercept[KernelException] {
        checkTimeTravelByTimestamp(v2Ts + 1, 2, v2Ts)
      }
      assert(e.getMessage.contains("is after the latest available version"))
    }
  }

  test("e2e CommitRange test with catalogManaged table with staged ratified commits") {
    withCatalogOwnedPreviewTestTable { (tablePath, parsedLogData) =>
      val v0Ts = 1749830855993L // published commit
      val v1Ts = 1749830871085L // staged commit
      val v2Ts = 1749830881799L // staged commit

      val latestSnapshot = TableManager
        .loadSnapshot(tablePath)
        .withLogData(parsedLogData.asJava)
        .withMaxCatalogVersion(2)
        .build(defaultEngine)

      def checkStartBoundary(timestamp: Long, expectedVersion: Long): Unit = {
        assert(TableManager.loadCommitRange(
          tablePath,
          CommitBoundary.atTimestamp(timestamp, latestSnapshot))
          .withLogData(parsedLogData.asJava)
          .withMaxCatalogVersion(2)
          .build(defaultEngine)
          .getStartVersion == expectedVersion)
      }
      def checkEndBoundary(timestamp: Long, expectedVersion: Long): Unit = {
        assert(TableManager.loadCommitRange(tablePath, CommitBoundary.atVersion(0))
          .withLogData(parsedLogData.asJava)
          .withMaxCatalogVersion(2)
          .withEndBoundary(CommitBoundary.atTimestamp(timestamp, latestSnapshot))
          .build(defaultEngine)
          .getEndVersion == expectedVersion)
      }

      // startTimestamp is before V0
      checkStartBoundary(v0Ts - 1, 0)
      // endTimestamp is before V0
      intercept[KernelException] {
        checkEndBoundary(v0Ts - 1, -1)
      }

      // startTimestamp is at V0
      checkStartBoundary(v0Ts, 0)
      // endTimestamp is at V0
      checkEndBoundary(v0Ts, 0)

      // startTimestamp is between V0 and V1
      checkStartBoundary(v0Ts + 100L, 1)
      // endTimestamp is between V0 and V1
      checkEndBoundary(v0Ts + 100L, 0)

      // startTimestamp is at V1
      checkStartBoundary(v1Ts, 1)
      // endTimestamp is at V1
      checkEndBoundary(v1Ts, 1)

      // startTimestamp is between V1 and V2
      checkStartBoundary(v1Ts + 100L, 2)
      // endTimestamp is between V1 and V2
      checkEndBoundary(v1Ts + 100L, 1)

      // startTimestamp is at V2
      checkStartBoundary(v2Ts, 2)
      // endTimestamp is at V2
      checkEndBoundary(v2Ts, 2)

      // startTimestamp is after V2
      intercept[KernelException] {
        checkStartBoundary(v2Ts + 10, -1)
      }
      // endTimestamp is after V2
      checkEndBoundary(v2Ts + 10, 2)

      // Verify the fileList in the CommitRange
      val commitRange = TableManager
        .loadCommitRange(tablePath, CommitBoundary.atVersion(0))
        .withLogData(parsedLogData.asJava)
        .withMaxCatalogVersion(2)
        .build(defaultEngine)

      val expectedFileList = Seq(
        // scalastyle:off line.size.limit
        getTestResourceFilePath("catalog-owned-preview/_delta_log/00000000000000000000.json"),
        getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000001.4cb9708e-b478-44de-b203-53f9ba9b2876.json"),
        getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000002.5b9bba4a-0085-430d-a65e-b0d38c1afbe9.json")
        // scalastyle:on line.size.limit
      ).map(path => defaultEngine.getFileSystemClient.resolvePath(path))

      assert(commitRange.asInstanceOf[CommitRangeImpl].getDeltaFiles().asScala.map(_.getPath) ==
        expectedFileList)
    }
  }

  test("reading a catalogManaged table without providing maxCatalogVersion fails") {
    withCatalogOwnedPreviewTestTable { (tablePath, parsedLogData) =>
      // With logData
      val latestError = intercept[MaxCatalogVersionException] {
        TableManager
          .loadSnapshot(tablePath)
          .withLogData(parsedLogData.asJava)
          .build(defaultEngine)
      }
      assert(latestError.getMessage === "Must provide maxCatalogVersion for catalogManaged tables")
      // Without logData
      val noLogDataError = intercept[MaxCatalogVersionException] {
        TableManager
          .loadSnapshot(tablePath)
          .build(defaultEngine)
      }
      assert(
        noLogDataError.getMessage === "Must provide maxCatalogVersion for catalogManaged tables")
      // With time-travel-version
      val e1 = intercept[MaxCatalogVersionException] {
        TableManager
          .loadSnapshot(tablePath)
          .atVersion(0)
          .build(defaultEngine)
      }
      assert(e1.getMessage === "Must provide maxCatalogVersion for catalogManaged tables")
      // With time-travel-version and logData
      val e2 = intercept[MaxCatalogVersionException] {
        TableManager
          .loadSnapshot(tablePath)
          .atVersion(2)
          .withLogData(parsedLogData.asJava)
          .build(defaultEngine)
      }
      assert(e2.getMessage === "Must provide maxCatalogVersion for catalogManaged tables")
    }
  }

  test("reading a file-system managed table and providing maxCatalogVersion fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create a basic file-system managed table
      createEmptyTable(tablePath = tablePath, schema = testSchema)
      // Try to read it and provide the maxCatalogVersion
      val error = intercept[MaxCatalogVersionException] {
        TableManager
          .loadSnapshot(tablePath)
          .withMaxCatalogVersion(0)
          .build(engine)
      }
      assert(error.getMessage ===
        "Should not provide maxCatalogVersion for file-system managed tables")
      // Version time travel to a file-system managed version is unaffected by maxCatalogVersion
      val versionSnapshot = TableManager
        .loadSnapshot(tablePath)
        .atVersion(0)
        .withMaxCatalogVersion(0)
        .build(engine)
      assert(versionSnapshot.getVersion === 0)
    }
  }

  /**
   * Writes a commit containing only commitInfo and an optional protocol. When `publish` is false,
   * the commit is written to _staged_commits and returned as ratified log data.
   */
  private def writeCommit(
      tablePath: String,
      version: Long,
      inCommitTimestamp: Long,
      protocolOpt: Option[Protocol] = None,
      publish: Boolean = true): List[ParsedLogData] = {
    val logPath = new Path(tablePath, "_delta_log")
    val commitInfo =
      s"""{"commitInfo":{"inCommitTimestamp":$inCommitTimestamp,"timestamp":$inCommitTimestamp}}"""
    val protocolActions =
      protocolOpt.map(p => JsonUtils.rowToJson(createProtocolSingleAction(p.toRow))).toSeq
    val commitFile = if (publish) {
      FileNames.deltaFile(logPath, version)
    } else {
      Files.createDirectories(Paths.get(FileNames.stagedCommitDirectory(logPath)))
      FileNames.stagedCommitFile(logPath, version)
    }
    Files.write(
      Paths.get(commitFile),
      (commitInfo +: protocolActions).mkString("", "\n", "\n").getBytes(UTF_8))
    if (publish) {
      Nil
    } else {
      val resolvedCommitFile = defaultEngine.getFileSystemClient.resolvePath(commitFile)
      List(ParsedLogData.forFileStatus(FileStatus.of(resolvedCommitFile)))
    }
  }

  private def snapshotAt(
      tablePath: String,
      version: Long,
      parsedLogData: List[ParsedLogData] = Nil): SnapshotImpl = {
    TableManager
      .loadSnapshot(tablePath)
      .atVersion(version)
      .withLogData(parsedLogData.asJava)
      .withMaxCatalogVersion(version)
      .build(defaultEngine)
      .asInstanceOf[SnapshotImpl]
  }

  private def protocolAt(
      tablePath: String,
      version: Long,
      parsedLogData: List[ParsedLogData] = Nil): Protocol = {
    snapshotAt(tablePath, version, parsedLogData).getProtocol
  }

  private def withoutCatalogManaged(protocol: Protocol): Protocol = {
    val catalogManaged = CATALOG_MANAGED_RW_FEATURE.featureName
    new Protocol(
      protocol.getMinReaderVersion,
      protocol.getMinWriterVersion,
      (protocol.getReaderFeatures.asScala - catalogManaged).asJava,
      (protocol.getWriterFeatures.asScala - catalogManaged).asJava)
  }

  private def withUnsupportedReaderFeature(protocol: Protocol): Protocol = {
    val unsupportedFeature = "unsupportedReaderFeature"
    new Protocol(
      protocol.getMinReaderVersion,
      protocol.getMinWriterVersion,
      (protocol.getReaderFeatures.asScala + unsupportedFeature).asJava,
      (protocol.getWriterFeatures.asScala + unsupportedFeature).asJava)
  }

  /**
   * Creates a file-system managed table at version 0 and upgrades it to catalogManaged at version
   * 1. When `publishUpgrade` is false, the upgrade commit is moved into _staged_commits and passed
   * to `testFx` as ratified log data, simulating a catalog that has not yet published it.
   */
  private def withUpgradedToCatalogManagedTable(publishUpgrade: Boolean)(
      testFx: (String, Long, List[ParsedLogData]) => Unit): Unit = {
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
      createEmptyTable(
        tablePath = tablePath,
        schema = testSchema,
        tableProperties = Map("delta.enableInCommitTimestamps" -> "true"))
      val v0Timestamp =
        TableManager.loadSnapshot(tablePath).build(defaultEngine).getTimestamp(defaultEngine)
      upgradeToCatalogManaged(tablePath)
      val parsedLogData = if (publishUpgrade) Nil else unpublish(tablePath, version = 1)
      testFx(tablePath, v0Timestamp, parsedLogData)
    }
  }

  /** Upgrades the file-system managed table at `tablePath` to catalogManaged. */
  private def upgradeToCatalogManaged(tablePath: String): Unit = {
    TableManager
      .loadSnapshot(tablePath)
      .withCommitter(customCatalogCommitter)
      .build(defaultEngine)
      .buildUpdateTableTransaction("engineInfo", Operation.MANUAL_UPDATE)
      .withTablePropertiesAdded(
        (customCatalogCommitter.getRequiredTableProperties.asScala ++
          Map(CATALOG_MANAGED_RW_FEATURE.getTableFeatureSupportKey -> "supported")).asJava)
      .build(defaultEngine)
      .commit(defaultEngine, emptyIterable[Row])
  }

  /**
   * Writes a commit at `version` that removes catalogManaged from the protocol of the previous
   * version.
   */
  private def downgradeFromCatalogManaged(
      tablePath: String,
      version: Long,
      parsedLogData: List[ParsedLogData] = Nil): Unit = {
    val previousSnapshot = snapshotAt(tablePath, version - 1, parsedLogData)
    writeCommit(
      tablePath,
      version,
      inCommitTimestamp = previousSnapshot.getTimestamp(defaultEngine) + 1,
      protocolOpt = Some(withoutCatalogManaged(previousSnapshot.getProtocol)))
  }

  /**
   * Appends `seqOfUnpartitionedDataBatch1` to the latest version. Pass `maxCatalogVersionOpt` when
   * the latest version is catalogManaged.
   */
  private def appendRows(tablePath: String, maxCatalogVersionOpt: Option[Long] = None): Unit = {
    val snapshotBuilder = TableManager.loadSnapshot(tablePath)
    val snapshot = maxCatalogVersionOpt
      .map(snapshotBuilder.withMaxCatalogVersion(_).withCommitter(customCatalogCommitter))
      .getOrElse(snapshotBuilder)
      .build(defaultEngine)
    val txn = snapshot
      .buildUpdateTableTransaction("engineInfo", Operation.WRITE)
      .build(defaultEngine)
    commitAppendData(defaultEngine, txn, seqOfUnpartitionedDataBatch1)
  }

  /**
   * Moves the published delta at `version` into _staged_commits and returns it as ratified log
   * data, simulating a catalog that has not yet published it.
   */
  private def unpublish(tablePath: String, version: Long): List[ParsedLogData] = {
    val logPath = new Path(tablePath, "_delta_log")
    val stagedCommit = FileNames.stagedCommitFile(logPath, version)
    Files.createDirectories(Paths.get(FileNames.stagedCommitDirectory(logPath)))
    Files.move(Paths.get(FileNames.deltaFile(logPath, version)), Paths.get(stagedCommit))
    val resolvedStagedCommit = defaultEngine.getFileSystemClient.resolvePath(stagedCommit)
    List(ParsedLogData.forFileStatus(FileStatus.of(resolvedStagedCommit)))
  }

  private def publish(tablePath: String, stagedCommit: ParsedLogData): Unit = {
    val logPath = new Path(tablePath, "_delta_log")
    Files.copy(
      Paths.get(new Path(stagedCommit.getFileStatus.getPath).toUri),
      Paths.get(FileNames.deltaFile(logPath, stagedCommit.getVersion)))
  }

  /**
   * Creates a table with the following published versions:
   *
   * v0: file-system managed, empty
   * v1: upgrade to catalogManaged
   * v2: catalogManaged append
   * v3: downgrade to file-system managed
   * v4: file-system managed append
   */
  private def createUpgradedThenDowngradedTable(tablePath: String): Unit = {
    createEmptyTable(
      tablePath = tablePath,
      schema = testSchema,
      tableProperties = Map("delta.enableInCommitTimestamps" -> "true"))
    upgradeToCatalogManaged(tablePath)
    appendRows(tablePath, maxCatalogVersionOpt = Some(1))
    downgradeFromCatalogManaged(tablePath, version = 3)
    appendRows(tablePath)
  }

  /** Row count at each version of the table created by [[createUpgradedThenDowngradedTable]] */
  private val upgradedThenDowngradedTableNumRows = Seq(0, 0, 600, 600, 1200)

  /**
   * Copies the catalogManaged catalog-owned-preview table and downgrades it to file-system managed
   * at version 1. When `publishDowngrade` is false, the downgrade commit is written to
   * _staged_commits and passed to `testFx` as ratified log data.
   */
  private def withDowngradedFromCatalogManagedTable(publishDowngrade: Boolean)(
      testFx: (String, Long, List[ParsedLogData]) => Unit): Unit = {
    withTempDir { tempDir =>
      FileUtils.copyDirectory(new File(getTestResourceFilePath("catalog-owned-preview")), tempDir)
      val tablePath = tempDir.getCanonicalPath
      // inCommitTimestamp of the catalog-owned-preview table's version 0
      val v0Timestamp = 1749830855993L
      val parsedLogData = writeCommit(
        tablePath,
        version = 1,
        inCommitTimestamp = v0Timestamp + 1,
        protocolOpt = Some(withoutCatalogManaged(protocolAt(tablePath, 0))),
        publish = publishDowngrade)
      testFx(tablePath, v0Timestamp, parsedLogData)
    }
  }

  test("time travel across catalogManaged upgrade") {
    withUpgradedToCatalogManagedTable(publishUpgrade = true) { (tablePath, v0Timestamp, _) =>
      val latestSnapshot = TableManager
        .loadSnapshot(tablePath)
        .withMaxCatalogVersion(1)
        .build(defaultEngine)
      assert(isCatalogManagedSupported(latestSnapshot.asInstanceOf[SnapshotImpl].getProtocol))

      val versionSnapshot = TableManager
        .loadSnapshot(tablePath)
        .atVersion(0)
        .withMaxCatalogVersion(1)
        .build(defaultEngine)
      assert(versionSnapshot.getVersion === 0)

      // Pre-upgrade versions are fully published file-system managed history, so version time
      // travel to them does not load the latest protocol.
      val pathBasedVersionSnapshot = TableManager
        .loadSnapshot(tablePath)
        .atVersion(0)
        .build(defaultEngine)
      assert(pathBasedVersionSnapshot.getVersion === 0)

      val e1 = intercept[MaxCatalogVersionException] {
        TableManager
          .loadSnapshot(tablePath)
          .atVersion(1)
          .build(defaultEngine)
      }
      assert(e1.getMessage === "Must provide maxCatalogVersion for catalogManaged tables")

      val timestampSnapshot = TableManager
        .loadSnapshot(tablePath)
        .atTimestamp(v0Timestamp, latestSnapshot)
        .withMaxCatalogVersion(1)
        .build(defaultEngine)
      assert(timestampSnapshot.getVersion === 0)

      val e2 = intercept[MaxCatalogVersionException] {
        TableManager
          .loadSnapshot(tablePath)
          .atTimestamp(v0Timestamp, latestSnapshot)
          .build(defaultEngine)
      }
      assert(e2.getMessage === "Must provide maxCatalogVersion for catalogManaged tables")
    }
  }

  test("time travel across unpublished catalogManaged upgrade") {
    withUpgradedToCatalogManagedTable(publishUpgrade = false) { (tablePath, _, parsedLogData) =>
      val e = intercept[MaxCatalogVersionException] {
        TableManager
          .loadSnapshot(tablePath)
          .atVersion(1)
          .withLogData(parsedLogData.asJava)
          .build(defaultEngine)
      }
      assert(e.getMessage === "Must provide maxCatalogVersion for catalogManaged tables")

      val versionSnapshot = TableManager
        .loadSnapshot(tablePath)
        .atVersion(0)
        .withLogData(parsedLogData.asJava)
        .withMaxCatalogVersion(1)
        .build(defaultEngine)
      assert(versionSnapshot.getVersion === 0)
    }
  }

  test("time travel to a published version with only the catalog commits it needs") {
    withUpgradedToCatalogManagedTable(publishUpgrade = false) { (tablePath, _, parsedLogData) =>
      // Version 2 is ratified but neither published nor provided, like a catalog client that only
      // fetches commits up to the requested version.
      writeCommit(tablePath, version = 2, System.currentTimeMillis(), publish = false)
      Seq(Nil, parsedLogData).foreach { logData =>
        val versionSnapshot = TableManager
          .loadSnapshot(tablePath)
          .atVersion(0)
          .withLogData(logData.asJava)
          .withMaxCatalogVersion(2)
          .build(defaultEngine)
        assert(versionSnapshot.getVersion === 0)
      }
    }
  }

  Seq(true, false).foreach { publish =>
    test("time travel to a readable version when a later catalogManaged version is unreadable " +
      s"- publish=$publish") {
      withUpgradedToCatalogManagedTable(publishUpgrade = publish) {
        (tablePath, _, upgradeLogData) =>
          val unreadableLogData = writeCommit(
            tablePath,
            version = 2,
            System.currentTimeMillis(),
            protocolOpt =
              Some(withUnsupportedReaderFeature(protocolAt(tablePath, 1, upgradeLogData))),
            publish = publish)
          val parsedLogData = (upgradeLogData ++ unreadableLogData).asJava

          val versionSnapshot = TableManager
            .loadSnapshot(tablePath)
            .atVersion(0)
            .withLogData(parsedLogData)
            .withMaxCatalogVersion(2)
            .build(defaultEngine)
          assert(versionSnapshot.getVersion === 0)

          intercept[UnsupportedTableFeatureException] {
            TableManager
              .loadSnapshot(tablePath)
              .atVersion(2)
              .withLogData(parsedLogData)
              .withMaxCatalogVersion(2)
              .build(defaultEngine)
          }
      }
    }
  }

  test("time travel below maxCatalogVersion when a later version downgrades the table") {
    withUpgradedToCatalogManagedTable(publishUpgrade = true) { (tablePath, _, _) =>
      writeCommit(
        tablePath,
        version = 2,
        System.currentTimeMillis(),
        protocolOpt = Some(withoutCatalogManaged(protocolAt(tablePath, 1))))

      val versionSnapshot = TableManager
        .loadSnapshot(tablePath)
        .atVersion(0)
        .withMaxCatalogVersion(1)
        .build(defaultEngine)
      assert(versionSnapshot.getVersion === 0)

      // Version 1 is catalogManaged but predates the downgrade, so it can be read by path
      val pathBasedVersionSnapshot = TableManager
        .loadSnapshot(tablePath)
        .atVersion(1)
        .build(defaultEngine)
      assert(pathBasedVersionSnapshot.getVersion === 1)
    }
  }

  test("time travel across catalogManaged downgrade") {
    withDowngradedFromCatalogManagedTable(publishDowngrade = true) {
      (tablePath, v0Timestamp, _) =>
        val pathBasedVersionSnapshot = TableManager
          .loadSnapshot(tablePath)
          .atVersion(0)
          .build(defaultEngine)
        assert(pathBasedVersionSnapshot.getVersion === 0)

        Seq(0L, 1L).foreach { version =>
          val versionSnapshot = TableManager
            .loadSnapshot(tablePath)
            .atVersion(version)
            .withMaxCatalogVersion(1)
            .build(defaultEngine)
          assert(versionSnapshot.getVersion === version)
        }

        val latestSnapshot = TableManager
          .loadSnapshot(tablePath)
          .build(defaultEngine)
        val timestampSnapshot = TableManager
          .loadSnapshot(tablePath)
          .atTimestamp(v0Timestamp, latestSnapshot)
          .build(defaultEngine)
        assert(timestampSnapshot.getVersion === 0)

        val e = intercept[MaxCatalogVersionException] {
          TableManager
            .loadSnapshot(tablePath)
            .atTimestamp(v0Timestamp, latestSnapshot)
            .withMaxCatalogVersion(1)
            .build(defaultEngine)
        }
        assert(
          e.getMessage === "Should not provide maxCatalogVersion for file-system managed tables")
    }
  }

  test("time travel across unpublished catalogManaged downgrade") {
    withDowngradedFromCatalogManagedTable(publishDowngrade = false) {
      (tablePath, _, parsedLogData) =>
        // The ratified downgrade is only visible through the provided catalog commits
        val e = intercept[MaxCatalogVersionException] {
          TableManager
            .loadSnapshot(tablePath)
            .atVersion(0)
            .build(defaultEngine)
        }
        assert(e.getMessage === "Must provide maxCatalogVersion for catalogManaged tables")

        val versionSnapshot = TableManager
          .loadSnapshot(tablePath)
          .atVersion(0)
          .withLogData(parsedLogData.asJava)
          .build(defaultEngine)
        assert(versionSnapshot.getVersion === 0)
    }
  }

  test("time travel to a version before a downgrade when the latest version is unreadable") {
    withDowngradedFromCatalogManagedTable(publishDowngrade = true) {
      (tablePath, v0Timestamp, _) =>
        writeCommit(
          tablePath,
          version = 2,
          v0Timestamp + 2,
          protocolOpt = Some(withUnsupportedReaderFeature(protocolAt(tablePath, 1))))

        val engine = new MetricsEngine(new HadoopFileIO(new Configuration()))
        val versionSnapshot = TableManager
          .loadSnapshot(tablePath)
          .atVersion(0)
          .build(engine)
        assert(versionSnapshot.getVersion === 0)
        // The latest version check stops at the protocol in version 2 instead of also searching
        // versions 1 and 0 for metadata.
        assert(engine.getJsonHandler.getVersionsRead === Seq(0, 2))
    }
  }

  test("time travel across a catalogManaged upgrade and a later downgrade") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
      createUpgradedThenDowngradedTable(tablePath)
      val timestamps = (0L to 4L).map(snapshotAt(tablePath, _).getTimestamp(defaultEngine))
      val latestSnapshot = TableManager.loadSnapshot(tablePath).build(defaultEngine)

      (0 to 4).foreach { version =>
        // Versions 1 and 2 are catalogManaged but predate the downgrade, so they can be read by path
        val versionSnapshot = TableManager
          .loadSnapshot(tablePath)
          .atVersion(version)
          .build(defaultEngine)
        assert(versionSnapshot.getVersion === version)
        assert(readSnapshot(versionSnapshot).size === upgradedThenDowngradedTableNumRows(version))

        val timestampSnapshot = TableManager
          .loadSnapshot(tablePath)
          .atTimestamp(timestamps(version), latestSnapshot)
          .build(defaultEngine)
        assert(timestampSnapshot.getVersion === version)

        val e = intercept[MaxCatalogVersionException] {
          TableManager
            .loadSnapshot(tablePath)
            .atTimestamp(timestamps(version), latestSnapshot)
            .withMaxCatalogVersion(4)
            .build(defaultEngine)
        }
        assert(
          e.getMessage === "Should not provide maxCatalogVersion for file-system managed tables")
      }
    }
  }

  Seq(true, false).foreach { publishReupgrade =>
    test("time travel across a catalogManaged downgrade and a later upgrade " +
      s"- publishReupgrade=$publishReupgrade") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getCanonicalPath
        createUpgradedThenDowngradedTable(tablePath)
        val timestamps = (0L to 4L).map(snapshotAt(tablePath, _).getTimestamp(defaultEngine))
        upgradeToCatalogManaged(tablePath)
        val parsedLogData =
          if (publishReupgrade) Nil else unpublish(tablePath, version = 5)
        val latestSnapshot = TableManager
          .loadSnapshot(tablePath)
          .withLogData(parsedLogData.asJava)
          .withMaxCatalogVersion(5)
          .build(defaultEngine)

        // Versions 1 and 2 predate the downgrade, but the table is catalogManaged again
        Seq(1, 2, 5).foreach { version =>
          val e = intercept[MaxCatalogVersionException] {
            TableManager
              .loadSnapshot(tablePath)
              .atVersion(version)
              .withLogData(parsedLogData.asJava)
              .build(defaultEngine)
          }
          assert(e.getMessage === "Must provide maxCatalogVersion for catalogManaged tables")
        }

        (0 to 4).foreach { version =>
          val versionSnapshot = TableManager
            .loadSnapshot(tablePath)
            .atVersion(version)
            .withLogData(parsedLogData.asJava)
            .withMaxCatalogVersion(5)
            .build(defaultEngine)
          assert(versionSnapshot.getVersion === version)
          assert(readSnapshot(versionSnapshot).size === upgradedThenDowngradedTableNumRows(version))

          val timestampSnapshot = TableManager
            .loadSnapshot(tablePath)
            .atTimestamp(timestamps(version), latestSnapshot)
            .withLogData(parsedLogData.asJava)
            .withMaxCatalogVersion(5)
            .build(defaultEngine)
          assert(timestampSnapshot.getVersion === version)

          val e = intercept[MaxCatalogVersionException] {
            TableManager
              .loadSnapshot(tablePath)
              .atTimestamp(timestamps(version), latestSnapshot)
              .withLogData(parsedLogData.asJava)
              .build(defaultEngine)
          }
          assert(e.getMessage === "Must provide maxCatalogVersion for catalogManaged tables")
        }

        // File-system managed versions are fully published and can be read by path
        Seq(0, 3, 4).foreach { version =>
          val pathBasedVersionSnapshot = TableManager
            .loadSnapshot(tablePath)
            .atVersion(version)
            .build(defaultEngine)
          assert(pathBasedVersionSnapshot.getVersion === version)
        }
      }
    }
  }

  test("commit ranges across a catalogManaged upgrade and downgrade") {
    def commitVersions(commitRange: CommitRange): Seq[Long] = {
      commitRange
        .getCommitActions(defaultEngine, Set(DeltaAction.ADD, DeltaAction.PROTOCOL).asJava)
        .toSeq
        .map(_.getVersion)
    }

    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
      createEmptyTable(
        tablePath = tablePath,
        schema = testSchema,
        tableProperties = Map("delta.enableInCommitTimestamps" -> "true"))
      upgradeToCatalogManaged(tablePath)
      appendRows(tablePath, maxCatalogVersionOpt = Some(1))
      val v2LogData = unpublish(tablePath, version = 2)

      // Version 2 is ratified but not yet published
      val ratifiedRange = TableManager
        .loadCommitRange(tablePath, CommitBoundary.atVersion(0))
        .withEndBoundary(CommitBoundary.atVersion(2))
        .withLogData(v2LogData.asJava)
        .withMaxCatalogVersion(2)
        .build(defaultEngine)
      assert(commitVersions(ratifiedRange) === Seq(0, 1, 2))
      assert(
        ratifiedRange.asInstanceOf[CommitRangeImpl].getDeltaFiles.asScala.last.getPath ===
          v2LogData.head.getFileStatus.getPath)

      publish(tablePath, v2LogData.head)
      downgradeFromCatalogManaged(tablePath, version = 3)
      appendRows(tablePath)

      val postDowngradeRange = TableManager
        .loadCommitRange(tablePath, CommitBoundary.atVersion(3))
        .build(defaultEngine)
      assert(commitVersions(postDowngradeRange) === Seq(3, 4))

      val fullRange = TableManager
        .loadCommitRange(tablePath, CommitBoundary.atVersion(0))
        .withEndBoundary(CommitBoundary.atVersion(4))
        .build(defaultEngine)
      assert(commitVersions(fullRange) === Seq(0, 1, 2, 3, 4))
    }
  }

  test("for latest queries we do not load past the maxRatifiedVersion even if " +
    "later versions exist on the file-system") {
    withTempDir { tempDir =>
      withCatalogOwnedPreviewTestTable { (resourceTablePath, resourceLogData) =>
        // Copy the catalog-owned-preview test resource table to the temp directory
        org.apache.commons.io.FileUtils.copyDirectory(
          new java.io.File(resourceTablePath),
          tempDir)

        // "Publish" v1 and v2 (we do both to maintain ordered backfill)
        val deltaLogPath = new Path(tempDir.getPath, "_delta_log")
        val stagedCommitPath = new Path(deltaLogPath, "_staged_commits")
        resourceLogData.foreach { stagedCommit =>
          val stagedCommitFile = new java.io.File(
            stagedCommitPath.toString,
            new Path(stagedCommit.getFileStatus.getPath).getName)
          val publishedCommitFile = new java.io.File(
            FileNames.deltaFile(deltaLogPath.toString, stagedCommit.getVersion))
          org.apache.commons.io.FileUtils.copyFile(stagedCommitFile, publishedCommitFile)
        }

        def convertResourceLogData(logData: ParsedLogData): ParsedLogData = {
          val path = new Path(stagedCommitPath, new Path(logData.getFileStatus.getPath).getName)
          ParsedLogData.forFileStatus(FileStatus.of(
            defaultEngine.getFileSystemClient.resolvePath(path.toString)))
        }

        Seq(0, 1, 2).foreach { maxCatalogVersion =>
          {
            // Try to read the table with no parsedLogData
            val snapshot = TableManager
              .loadSnapshot(tempDir.getPath)
              .withMaxCatalogVersion(maxCatalogVersion)
              .build(defaultEngine)
            assert(snapshot.getVersion == maxCatalogVersion)

          }
          {
            // Try to read the table with parsedLogData
            val parsedLogData = resourceLogData
              .filter(_.getVersion <= maxCatalogVersion)
              .map(convertResourceLogData)
            val snapshot = TableManager
              .loadSnapshot(tempDir.getPath)
              .withMaxCatalogVersion(maxCatalogVersion)
              .withLogData(parsedLogData.asJava)
              .build(defaultEngine)
            assert(snapshot.getVersion == maxCatalogVersion)
          }
        }
      }
    }
  }

  test("for latest queries if we cannot load the maxRatifiedVersion we fail") {
    withCatalogOwnedPreviewTestTable { (tablePath, _) =>
      // We can only test this when no logData is provided. Otherwise we require logData to end
      // with maxRatifiedVersion ==> it should be able to be read.
      val e = intercept[KernelException] {
        TableManager
          .loadSnapshot(tablePath)
          .withMaxCatalogVersion(2)
          .build(defaultEngine)
      }
      assert(e.getMessage.contains("Cannot load table version 2"))
    }
  }

  test("for latest queries we read the _last_checkpoint file") {
    withCatalogOwnedPreviewTestTable { (resourceTablePath, resourceLogData) =>
      // It doesn't matter if the checkpoint actually exists; we just want to check that during
      // log segment building we try to read _last_checkpoint
      import io.delta.kernel.defaults.MetricsEngine
      val engine = new MetricsEngine(new HadoopFileIO(new Configuration()))
      val snapshot = TableManager
        .loadSnapshot(resourceTablePath)
        .withMaxCatalogVersion(2)
        .withLogData(resourceLogData.asJava)
        .build(engine)
      assert(snapshot.getVersion == 2)
      assert(engine.getJsonHandler.getLastCheckpointMetadataReadCalls == 1)
    }
  }

  test("for commitRange queries with no end boundary we do not load past the maxRatifiedVersion " +
    "even if later versions exist on the file-system") {
    withTempDir { tempDir =>
      withCatalogOwnedPreviewTestTable { (resourceTablePath, resourceLogData) =>
        // Copy the catalog-owned-preview test resource table to the temp directory
        org.apache.commons.io.FileUtils.copyDirectory(
          new java.io.File(resourceTablePath),
          tempDir)

        // "Publish" v1 and v2 (we do both to maintain ordered backfill)
        val deltaLogPath = new Path(tempDir.getPath, "_delta_log")
        val stagedCommitPath = new Path(deltaLogPath, "_staged_commits")
        resourceLogData.foreach { stagedCommit =>
          val stagedCommitFile = new java.io.File(
            stagedCommitPath.toString,
            new Path(stagedCommit.getFileStatus.getPath).getName)
          val publishedCommitFile = new java.io.File(
            FileNames.deltaFile(deltaLogPath.toString, stagedCommit.getVersion))
          org.apache.commons.io.FileUtils.copyFile(stagedCommitFile, publishedCommitFile)
        }

        def convertResourceLogData(logData: ParsedLogData): ParsedLogData = {
          val path = new Path(stagedCommitPath, new Path(logData.getFileStatus.getPath).getName)
          ParsedLogData.forFileStatus(FileStatus.of(
            defaultEngine.getFileSystemClient.resolvePath(path.toString)))
        }

        Seq(0, 1, 2).foreach { maxCatalogVersion =>
          {
            // Try to read the table with no parsedLogData
            val commitRange = TableManager
              .loadCommitRange(tempDir.getPath, CommitBoundary.atVersion(0))
              .withMaxCatalogVersion(maxCatalogVersion)
              .build(defaultEngine)
            assert(commitRange.getEndVersion == maxCatalogVersion)
          }
          {
            // Try to read the table with parsedLogData
            val parsedLogData = resourceLogData
              .filter(_.getVersion <= maxCatalogVersion)
              .map(convertResourceLogData)
            val commitRange = TableManager
              .loadCommitRange(tempDir.getPath, CommitBoundary.atVersion(0))
              .withMaxCatalogVersion(maxCatalogVersion)
              .withLogData(parsedLogData.asJava)
              .build(defaultEngine)
            assert(commitRange.getEndVersion == maxCatalogVersion)
          }
        }
      }
    }
  }

  test("for commitRange queries with no end boundary if we cannot load the maxRatifiedVersion we " +
    "fail") {
    withCatalogOwnedPreviewTestTable { (tablePath, _) =>
      // We can only test this when no logData is provided. Otherwise we require logData to end
      // with maxRatifiedVersion ==> it should be able to be read.
      val e = intercept[KernelException] {
        TableManager
          .loadCommitRange(tablePath, CommitBoundary.atVersion(0))
          .withMaxCatalogVersion(2)
          .build(defaultEngine)
      }
      assert(e.getMessage.contains(
        "Requested table changes ending with endVersion=2 but no log file found for version 2"))
    }
  }
}
