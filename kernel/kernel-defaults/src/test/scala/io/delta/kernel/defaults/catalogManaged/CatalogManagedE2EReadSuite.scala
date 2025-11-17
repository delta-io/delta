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

import scala.collection.JavaConverters._

import io.delta.kernel.{SnapshotBuilder, TableManager}
import io.delta.kernel.CommitRangeBuilder.CommitBoundary
import io.delta.kernel.defaults.utils.{TestRow, TestUtilsWithTableManagerAPIs, WriteUtilsWithV2Builders}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.DeltaHistoryManager
import io.delta.kernel.internal.commitrange.CommitRangeImpl
import io.delta.kernel.internal.files.{ParsedCatalogCommitData, ParsedLogData}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.table.SnapshotBuilderImpl
import io.delta.kernel.internal.tablefeatures.TableFeatures.{isCatalogManagedSupported, CATALOG_MANAGED_R_W_FEATURE_PREVIEW, IN_COMMIT_TIMESTAMP_W_FEATURE, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION}
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.FileStatus

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

  test("simple e2e read of catalogOwned-preview table with staged ratified commits") {
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
      assert(protocol.getReaderFeatures.contains(CATALOG_MANAGED_R_W_FEATURE_PREVIEW.featureName()))
      assert(protocol.getWriterFeatures.contains(CATALOG_MANAGED_R_W_FEATURE_PREVIEW.featureName()))
      assert(protocol.getWriterFeatures.contains(IN_COMMIT_TIMESTAMP_W_FEATURE.featureName()))

      val actualResult = readSnapshot(snapshot)
      val expectedResult = (0 to 199).map { x => TestRow(x / 100, x) }
      checkAnswer(actualResult, expectedResult)
    }
  }

  test("e2e DeltaHistoryManager.getActiveCommitAtTimestamp with catalogOwned-preview table " +
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

  test("time-travel by ts read of catalogOwned-preview table with ratified commits") {
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

  test("e2e CommitRange test with catalogOwned-preview table with staged ratified commits") {
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
        assert(TableManager.loadCommitRange(tablePath)
          .withLogData(parsedLogData.asJava)
          .withStartBoundary(CommitBoundary.atTimestamp(timestamp, latestSnapshot))
          .build(defaultEngine).getStartVersion == expectedVersion)
      }
      def checkEndBoundary(timestamp: Long, expectedVersion: Long): Unit = {
        assert(TableManager.loadCommitRange(tablePath)
          .withLogData(parsedLogData.asJava)
          .withEndBoundary(CommitBoundary.atTimestamp(timestamp, latestSnapshot))
          .build(defaultEngine).getEndVersion == expectedVersion)
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
        .loadCommitRange(tablePath)
        .withLogData(parsedLogData.asJava)
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

  // We test this in the unit tests as well, but since those use the withProtocolAndMetadata API
  // we also test it here with a real table where we load the P&M from the log
  test("reading a catalogManaged table without providing maxCatalogVersion fails") {
    withCatalogOwnedPreviewTestTable { (tablePath, parsedLogData) =>
      // With logData
      intercept[IllegalArgumentException] {
        TableManager
          .loadSnapshot(tablePath)
          .withLogData(parsedLogData.asJava)
          .build(defaultEngine)
      }
      // Without logData (and with time-travel-version)
      intercept[IllegalArgumentException] {
        TableManager
          .loadSnapshot(tablePath)
          .atVersion(0)
          .build(defaultEngine)
      }
    }
  }

  test("reading a file-system managed table and providing maxCatalogVersion fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create a basic file-system managed table
      createEmptyTable(tablePath = tablePath, schema = testSchema)
      // Try to read it and provide the maxCatalogVersion
      intercept[IllegalArgumentException] {
        TableManager
          .loadSnapshot(tablePath)
          .withMaxCatalogVersion(0)
          .build(engine)
      }
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
    withCatalogOwnedPreviewTestTable { (tablePath, logData) =>
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
}
