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
package io.delta.kernel.spark.snapshot.unitycatalog

import java.util.Optional

import io.delta.kernel.{CommitRange, CommitRangeBuilder, Snapshot}
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.fs.{Path => KernelPath}
import io.delta.kernel.spark.exception.VersionNotFoundException
import io.delta.kernel.test.MockSnapshotUtils
import io.delta.kernel.unitycatalog.{InMemoryUCClient, UCCatalogManagedClient, UCCatalogManagedTestUtils}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for [[UCManagedTableSnapshotManager]].
 *
 * Tests the checkVersionExists logic which validates version bounds for UC-managed tables.
 * For UC tables, earliest version is always 0 (all ratified commits available).
 *
 * Note: getActiveCommitAtTime is not unit-tested here because it's a thin delegation layer.
 * It calls loadLatestSnapshot() (tested) then delegates to DeltaHistoryManager
 * .getActiveCommitAtTimestamp() which is comprehensively tested in DeltaHistoryManagerSuite.
 */
class UCManagedTableSnapshotManagerSuite extends AnyFunSuite with UCCatalogManagedTestUtils {

  private val testTableId = "test_uc_table_id"
  private val testTablePath = "/test/path/to/table"
  private val testUcUri = "https://uc.example.com"
  private val testUcToken = "test_token"

  /**
   * Execution trace for checkVersionExists:
   * → UCManagedTableSnapshotManager.checkVersionExists(version, mustBeRecreatable, allowOutOfRange)
   *   → loadLatestSnapshot() at line 115
   *     → UCCatalogManagedClient.loadSnapshot(engine, tableId, tablePath, empty, empty)
   *     ← returns Snapshot(version=latestVersion)
   *   ← latestSnapshot.getVersion() = latestVersion
   *   → earliestVersion = 0 (hardcoded for UC at line 119)
   *   → Check: version < earliestVersion? || (version > latestVersion && !allowOutOfRange)?
   *   ← If true: throw VersionNotFoundException(version, earliestVersion, latestVersion)
   *   ← If false: return normally
   */

  // Test cases: (testName, versionToCheck, mustBeRecreatable, allowOutOfRange, shouldThrow)
  private val checkVersionExistsTestCases = Seq(
    ("versionWithinRange", 3L, true, false, false),
    ("versionEqualsLatest", 5L, true, false, false),
    ("versionIsZero_earliestForUC", 0L, true, false, false),
    ("versionBelowZero", -1L, true, false, true),
    ("versionAboveLatest_notAllowOutOfRange", 10L, true, false, true),
    ("versionAboveLatest_allowOutOfRange", 10L, true, true, false),
    ("versionIsZero_mustBeRecreatable_false", 0L, false, false, false),
    ("versionAboveLatest_mustBeRecreatable_false", 10L, false, false, true)
  )

  checkVersionExistsTestCases.foreach { case (testName, versionToCheck, mustBeRecreatable,
      allowOutOfRange, shouldThrow) =>
    test(s"checkVersionExists - $testName") {
      // Create a test client that returns a snapshot at version 5
      val latestVersion = 5L
      val testClient = new TestUCCatalogManagedClient(latestVersion)
      val tableInfo = new UCTableInfo(testTableId, testTablePath, testUcUri, testUcToken)
      val hadoopConf = new Configuration()

      val manager = new UCManagedTableSnapshotManager(testClient, tableInfo, hadoopConf)

      if (shouldThrow) {
        val exception = intercept[VersionNotFoundException] {
          manager.checkVersionExists(versionToCheck, mustBeRecreatable, allowOutOfRange)
        }
        assert(exception.getMessage.contains(versionToCheck.toString),
          s"Exception should mention the requested version $versionToCheck")
      } else {
        // Should not throw
        manager.checkVersionExists(versionToCheck, mustBeRecreatable, allowOutOfRange)
      }
    }
  }

  test("loadLatestSnapshot returns snapshot from client") {
    val expectedVersion = 7L
    val testClient = new TestUCCatalogManagedClient(expectedVersion)
    val tableInfo = new UCTableInfo(testTableId, testTablePath, testUcUri, testUcToken)
    val hadoopConf = new Configuration()

    val manager = new UCManagedTableSnapshotManager(testClient, tableInfo, hadoopConf)
    val snapshot = manager.loadLatestSnapshot()

    assert(snapshot != null)
    assert(snapshot.getVersion == expectedVersion)
  }

  test("loadSnapshotAt returns snapshot at requested version") {
    val latestVersion = 10L
    val requestedVersion = 5L
    val testClient = new TestUCCatalogManagedClient(latestVersion)
    val tableInfo = new UCTableInfo(testTableId, testTablePath, testUcUri, testUcToken)
    val hadoopConf = new Configuration()

    val manager = new UCManagedTableSnapshotManager(testClient, tableInfo, hadoopConf)
    val snapshot = manager.loadSnapshotAt(requestedVersion)

    assert(snapshot != null)
    // TestUCCatalogManagedClient returns the requested version
    assert(snapshot.getVersion == requestedVersion)
  }

  /**
   * Execution trace for getTableChanges:
   * → UCManagedTableSnapshotManager.getTableChanges(engine, startVersion, endVersion) :127
   *   → ucCatalogManagedClient.loadCommitRange(engine, tableId, tablePath,
   *       Optional.of(startVersion), empty, endVersion, empty) :128-135
   *   ← returns CommitRange with (startVersion, endVersion)
   */
  test("getTableChanges with endVersion specified delegates to client") {
    val latestVersion = 10L
    val startVersion = 2L
    val endVersion = 7L
    val testClient = new TestUCCatalogManagedClientWithCommitRange(
      latestVersion, startVersion, endVersion)
    val tableInfo = new UCTableInfo(testTableId, testTablePath, testUcUri, testUcToken)
    val hadoopConf = new Configuration()

    val manager = new UCManagedTableSnapshotManager(testClient, tableInfo, hadoopConf)
    val commitRange = manager.getTableChanges(
      testClient.getTestEngine(hadoopConf), startVersion, Optional.of(endVersion))

    assert(commitRange != null)
    assert(commitRange.getStartVersion == startVersion)
    assert(commitRange.getEndVersion == endVersion)
  }

  test("getTableChanges with empty endVersion delegates to client") {
    val latestVersion = 10L
    val startVersion = 3L
    val expectedEndVersion = latestVersion // When endVersion is empty, client returns latest
    val testClient = new TestUCCatalogManagedClientWithCommitRange(
      latestVersion, startVersion, expectedEndVersion)
    val tableInfo = new UCTableInfo(testTableId, testTablePath, testUcUri, testUcToken)
    val hadoopConf = new Configuration()

    val manager = new UCManagedTableSnapshotManager(testClient, tableInfo, hadoopConf)
    val commitRange = manager.getTableChanges(
      testClient.getTestEngine(hadoopConf), startVersion, Optional.empty())

    assert(commitRange != null)
    assert(commitRange.getStartVersion == startVersion)
    assert(commitRange.getEndVersion == expectedEndVersion)
  }

  /**
   * Test implementation of UCCatalogManagedClient that returns controlled mock snapshots.
   * This allows testing UCManagedTableSnapshotManager logic without requiring real Delta table
   * files or a real Unity Catalog connection.
   */
  private class TestUCCatalogManagedClient(defaultVersion: Long)
      extends UCCatalogManagedClient(new InMemoryUCClient("test_metastore")) {

    override def loadSnapshot(
        engine: Engine,
        ucTableId: String,
        tablePath: String,
        versionOpt: Optional[java.lang.Long],
        timestampOpt: Optional[java.lang.Long]): Snapshot = {
      // Return a mock snapshot at the requested version, or defaultVersion if not specified
      val version = if (versionOpt.isPresent) versionOpt.get().toLong else defaultVersion
      MockSnapshotUtils.getMockSnapshot(new KernelPath(tablePath), version)
    }
  }

  /**
   * Extended test client that also provides mock CommitRange for getTableChanges tests.
   */
  private class TestUCCatalogManagedClientWithCommitRange(
      defaultVersion: Long,
      expectedStartVersion: Long,
      expectedEndVersion: Long)
      extends TestUCCatalogManagedClient(defaultVersion) {

    import io.delta.kernel.defaults.engine.DefaultEngine

    def getTestEngine(hadoopConf: Configuration): Engine = {
      DefaultEngine.create(hadoopConf)
    }

    override def loadCommitRange(
        engine: Engine,
        ucTableId: String,
        tablePath: String,
        startVersionOpt: Optional[java.lang.Long],
        startTimestampOpt: Optional[java.lang.Long],
        endVersionOpt: Optional[java.lang.Long],
        endTimestampOpt: Optional[java.lang.Long]): CommitRange = {
      // Return a mock CommitRange with the expected versions
      new MockCommitRange(expectedStartVersion, expectedEndVersion)
    }
  }

  /**
   * Minimal CommitRange implementation for testing.
   * Only implements the version accessors needed for our tests.
   */
  private class MockCommitRange(startVer: Long, endVer: Long) extends CommitRange {
    import java.util.Set
    import io.delta.kernel.CommitActions
    import io.delta.kernel.data.ColumnarBatch
    import io.delta.kernel.internal.DeltaLogActionUtils
    import io.delta.kernel.utils.CloseableIterator

    override def getStartVersion: Long = startVer
    override def getEndVersion: Long = endVer

    override def getQueryStartBoundary: CommitRangeBuilder.CommitBoundary = {
      CommitRangeBuilder.CommitBoundary.atVersion(startVer)
    }

    override def getQueryEndBoundary: Optional[CommitRangeBuilder.CommitBoundary] = {
      Optional.of(CommitRangeBuilder.CommitBoundary.atVersion(endVer))
    }

    override def getActions(
        engine: Engine,
        startSnapshot: Snapshot,
        actionSet: Set[DeltaLogActionUtils.DeltaAction]): CloseableIterator[ColumnarBatch] = {
      throw new UnsupportedOperationException("Not implemented for unit tests")
    }

    override def getCommitActions(
        engine: Engine,
        startSnapshot: Snapshot,
        actionSet: Set[DeltaLogActionUtils.DeltaAction]): CloseableIterator[CommitActions] = {
      throw new UnsupportedOperationException("Not implemented for unit tests")
    }
  }
}
