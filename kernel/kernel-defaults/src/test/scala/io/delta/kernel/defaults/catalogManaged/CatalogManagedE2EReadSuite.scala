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

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.{SnapshotBuilder, TableManager}
import io.delta.kernel.defaults.utils.{TestRow, TestUtilsWithTableManagerAPIs}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.DeltaHistoryManager
import io.delta.kernel.internal.files.{ParsedDeltaData, ParsedLogData}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.table.SnapshotBuilderImpl
import io.delta.kernel.internal.tablefeatures.TableFeatures.{CATALOG_MANAGED_R_W_FEATURE_PREVIEW, IN_COMMIT_TIMESTAMP_W_FEATURE, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION}
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
class CatalogManagedE2EReadSuite extends AnyFunSuite with TestUtilsWithTableManagerAPIs {

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

      val parsedDeltaData = parsedLogData
        .filter(_.isInstanceOf[ParsedDeltaData])
        .map(_.asInstanceOf[ParsedDeltaData])

      val latestSnapshot = TableManager
        .loadSnapshot(tablePath)
        .asInstanceOf[SnapshotBuilderImpl]
        .withLogData(parsedLogData.asJava)
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
          parsedDeltaData.asJava)
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
}
