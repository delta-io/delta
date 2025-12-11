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

import io.delta.kernel.{CommitActions, CommitRangeBuilder, Operation, ScanBuilder}
import io.delta.kernel.{CommitRange, Snapshot}
import io.delta.kernel.data.ColumnarBatch
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.DeltaLogActionUtils
import io.delta.kernel.spark.exception.VersionNotFoundException
import io.delta.kernel.statistics.SnapshotStatistics
import io.delta.kernel.transaction.UpdateTableTransactionBuilder
import io.delta.kernel.types.StructType
import io.delta.kernel.unitycatalog.InMemoryUCClient
import io.delta.kernel.unitycatalog.UCCatalogManagedClient
import io.delta.kernel.utils.CloseableIterator

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Integration tests for [[UCManagedTableSnapshotManager]].
 *
 * These tests use mock implementations of [[UCCatalogManagedClient]], [[Snapshot]], and
 * [[CommitRange]] to verify the behavior of all [[DeltaSnapshotManager]] interface methods.
 */
class UCManagedTableSnapshotManagerSuite extends SparkFunSuite with SharedSparkSession {

  // Test constants
  private val TEST_TABLE_ID = "test_uc_table_id"
  private val TEST_TABLE_PATH = "/test/path/to/table"
  private val TEST_UC_URI = "https://uc.example.com"
  private val TEST_UC_TOKEN = "test_token"
  private val TEST_LATEST_VERSION = 10L

  // ==================== Mock Classes ====================

  /**
   * Mock Snapshot implementation that returns a configured version.
   */
  class MockSnapshot(version: Long, path: String = TEST_TABLE_PATH) extends Snapshot {
    override def getPath: String = path
    override def getVersion: Long = version
    override def getPartitionColumnNames: java.util.List[String] = java.util.Collections.emptyList()
    override def getTimestamp(engine: Engine): Long = System.currentTimeMillis()
    override def getSchema: StructType = new StructType()
    override def getDomainMetadata(domain: String): Optional[String] = Optional.empty()
    override def getTableProperties: java.util.Map[String, String] =
      java.util.Collections.emptyMap()
    override def getStatistics: SnapshotStatistics =
      throw new UnsupportedOperationException("Not implemented in mock")
    override def getScanBuilder: ScanBuilder =
      throw new UnsupportedOperationException("Not implemented in mock")
    override def buildUpdateTableTransaction(
        engineInfo: String,
        operation: Operation): UpdateTableTransactionBuilder =
      throw new UnsupportedOperationException("Not implemented in mock")
    override def publish(engine: Engine): Unit = {}
    override def writeChecksum(engine: Engine, mode: Snapshot.ChecksumWriteMode): Unit = {}
  }

  /**
   * Mock CommitRange implementation that returns configured start/end versions.
   */
  class MockCommitRange(startVersion: Long, endVersion: Long) extends CommitRange {
    override def getStartVersion: Long = startVersion
    override def getEndVersion: Long = endVersion
    override def getQueryStartBoundary: CommitRangeBuilder.CommitBoundary =
      CommitRangeBuilder.CommitBoundary.atVersion(startVersion)
    override def getQueryEndBoundary: Optional[CommitRangeBuilder.CommitBoundary] =
      Optional.of(CommitRangeBuilder.CommitBoundary.atVersion(endVersion))
    override def getActions(
        engine: Engine,
        startSnapshot: Snapshot,
        actionSet: java.util.Set[DeltaLogActionUtils.DeltaAction])
        : CloseableIterator[ColumnarBatch] =
      throw new UnsupportedOperationException("Not implemented in mock")
    override def getCommitActions(
        engine: Engine,
        startSnapshot: Snapshot,
        actionSet: java.util.Set[DeltaLogActionUtils.DeltaAction])
        : CloseableIterator[CommitActions] =
      throw new UnsupportedOperationException("Not implemented in mock")
  }

  /**
   * Test UCCatalogManagedClient that returns controlled snapshots and commit ranges.
   *
   * Key design: The mock returns DIFFERENT values based on input to ensure tests catch
   * incorrect parameter passing. If version is empty, returns latestVersion; if version is
   * specified, returns that exact version. This means tests will fail if:
   * - loadLatestSnapshot passes a specific version instead of empty
   * - loadSnapshotAt passes empty instead of the specific version
   * - loadSnapshotAt passes a wrong version number
   */
  class TestUCCatalogManagedClient(latestVersion: Long)
      extends UCCatalogManagedClient(new InMemoryUCClient("test-metastore")) {

    // Track calls for verification - allows asserting exact parameters
    var lastLoadSnapshotVersionOpt: Optional[java.lang.Long] = _
    var lastLoadSnapshotTimestampOpt: Optional[java.lang.Long] = _
    var lastCommitRangeStartVersion: Optional[java.lang.Long] = _
    var lastCommitRangeEndVersion: Optional[java.lang.Long] = _
    var loadSnapshotCallCount: Int = 0
    var loadCommitRangeCallCount: Int = 0

    override def loadSnapshot(
        engine: Engine,
        ucTableId: String,
        tablePath: String,
        versionOpt: Optional[java.lang.Long],
        timestampOpt: Optional[java.lang.Long]): Snapshot = {
      loadSnapshotCallCount += 1
      lastLoadSnapshotVersionOpt = versionOpt
      lastLoadSnapshotTimestampOpt = timestampOpt

      // Return different values based on input to catch incorrect parameter passing
      val version = if (versionOpt.isPresent) {
        versionOpt.get().longValue()
      } else {
        latestVersion
      }
      new MockSnapshot(version, tablePath)
    }

    override def loadCommitRange(
        engine: Engine,
        ucTableId: String,
        tablePath: String,
        startVersionOpt: Optional[java.lang.Long],
        startTimestampOpt: Optional[java.lang.Long],
        endVersionOpt: Optional[java.lang.Long],
        endTimestampOpt: Optional[java.lang.Long]): CommitRange = {
      loadCommitRangeCallCount += 1
      lastCommitRangeStartVersion = startVersionOpt
      lastCommitRangeEndVersion = endVersionOpt

      val startVersion = startVersionOpt.orElse(0L).longValue()
      val endVersion = endVersionOpt.orElse(latestVersion).longValue()
      new MockCommitRange(startVersion, endVersion)
    }
  }

  // ==================== Helper Methods ====================

  private def createTestTableInfo(): UCTableInfo = {
    new UCTableInfo(TEST_TABLE_ID, TEST_TABLE_PATH, TEST_UC_URI, TEST_UC_TOKEN)
  }

  private def createTestEngine(): Engine = {
    io.delta.kernel.defaults.engine.DefaultEngine.create(
      spark.sessionState.newHadoopConf())
  }

  private def createManager(
      client: UCCatalogManagedClient,
      engine: Engine = createTestEngine()): UCManagedTableSnapshotManager = {
    new UCManagedTableSnapshotManager(client, createTestTableInfo(), engine)
  }

  // ==================== loadLatestSnapshot Tests ====================

  test("loadLatestSnapshot returns snapshot at latest version") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    val snapshot = manager.loadLatestSnapshot()

    // The mock returns latestVersion only when versionOpt is empty.
    // If impl passed Optional.of(someOtherVersion), we'd get that version back.
    assert(snapshot.getVersion == TEST_LATEST_VERSION)
  }

  test("loadLatestSnapshot calls client with empty version and timestamp") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    manager.loadLatestSnapshot()

    // Verify exact parameters - this catches bugs where impl passes wrong values
    assert(
      !client.lastLoadSnapshotVersionOpt.isPresent,
      "loadLatestSnapshot should pass empty versionOpt, not a specific version")
    assert(
      !client.lastLoadSnapshotTimestampOpt.isPresent,
      "loadLatestSnapshot should pass empty timestampOpt")
    assert(client.loadSnapshotCallCount == 1, "loadSnapshot should be called exactly once")
  }

  test("loadLatestSnapshot would return wrong version if wrong parameters passed") {
    // This test verifies our mock behavior is correct - it returns different values
    // based on input, which is crucial for valid testing
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)

    // When version is specified, mock returns that version (not latest)
    val snapshotAt5 = client.loadSnapshot(
      createTestEngine(),
      TEST_TABLE_ID,
      TEST_TABLE_PATH,
      Optional.of(5L: java.lang.Long),
      Optional.empty())
    assert(snapshotAt5.getVersion == 5L, "Mock should return requested version")

    // When version is empty, mock returns latest
    val snapshotLatest = client.loadSnapshot(
      createTestEngine(),
      TEST_TABLE_ID,
      TEST_TABLE_PATH,
      Optional.empty(),
      Optional.empty())
    assert(snapshotLatest.getVersion == TEST_LATEST_VERSION, "Mock should return latest when empty")
  }

  // ==================== loadSnapshotAt Tests ====================

  test("loadSnapshotAt returns snapshot at specific version") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    val snapshot = manager.loadSnapshotAt(5L)

    assert(snapshot.getVersion == 5L)
  }

  test("loadSnapshotAt at version zero returns correct snapshot") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    val snapshot = manager.loadSnapshotAt(0L)

    assert(snapshot.getVersion == 0L)
  }

  test("loadSnapshotAt passes version correctly to client") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    manager.loadSnapshotAt(7L)

    assert(client.lastLoadSnapshotVersionOpt.isPresent)
    assert(client.lastLoadSnapshotVersionOpt.get() == 7L)
    assert(!client.lastLoadSnapshotTimestampOpt.isPresent)
  }

  // ==================== checkVersionExists Tests ====================

  test("checkVersionExists: version within valid range does not throw") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    // Should not throw - version 5 is within [0, 10]
    manager.checkVersionExists(5L, true /* mustBeRecreatable */, false /* allowOutOfRange */ )

    // Verify the implementation actually called loadLatestSnapshot to get bounds
    assert(
      client.loadSnapshotCallCount == 1,
      "checkVersionExists should call loadLatestSnapshot to determine version bounds")
  }

  test("checkVersionExists: version at boundary zero does not throw") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    // Version 0 is the earliest version for UC tables (always 0)
    manager.checkVersionExists(0L, true /* mustBeRecreatable */, false /* allowOutOfRange */ )

    // Verify the check was actually performed
    assert(client.loadSnapshotCallCount == 1)
  }

  test("checkVersionExists: version at boundary latest does not throw") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    // Version 10 (TEST_LATEST_VERSION) should be valid
    manager.checkVersionExists(
      TEST_LATEST_VERSION,
      true /* mustBeRecreatable */,
      false /* allowOutOfRange */ )

    // Verify the check was actually performed
    assert(client.loadSnapshotCallCount == 1)
  }

  test("checkVersionExists: version below zero throws VersionNotFoundException") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    val exception = intercept[VersionNotFoundException] {
      manager.checkVersionExists(-1L, true /* mustBeRecreatable */, false /* allowOutOfRange */ )
    }
    assert(exception.getUserVersion == -1L)
    assert(exception.getEarliest == 0L)
    assert(exception.getLatest == TEST_LATEST_VERSION)
  }

  test("checkVersionExists: version above latest with allowOutOfRange=false throws") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    val exception = intercept[VersionNotFoundException] {
      manager.checkVersionExists(15L, true /* mustBeRecreatable */, false /* allowOutOfRange */ )
    }
    assert(exception.getUserVersion == 15L)
    assert(exception.getEarliest == 0L)
    assert(exception.getLatest == TEST_LATEST_VERSION)
  }

  test("checkVersionExists: version above latest with allowOutOfRange=true does not throw") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    // Should not throw
    manager.checkVersionExists(15L, true /* mustBeRecreatable */, true /* allowOutOfRange */ )
  }

  // ==================== getTableChanges Tests ====================

  test("getTableChanges with explicit end version returns correct commit range") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val engine = createTestEngine()
    val manager = createManager(client, engine)

    val commitRange = manager.getTableChanges(engine, 3L, Optional.of(7L))

    // Mock returns the exact start/end versions passed, so this verifies correct parameter passing
    assert(
      commitRange.getStartVersion == 3L,
      "Start version should match what was passed to getTableChanges")
    assert(
      commitRange.getEndVersion == 7L,
      "End version should match what was passed to getTableChanges")
    assert(client.loadCommitRangeCallCount == 1, "loadCommitRange should be called exactly once")
  }

  test("getTableChanges without end version returns range to latest") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val engine = createTestEngine()
    val manager = createManager(client, engine)

    val commitRange = manager.getTableChanges(engine, 5L, Optional.empty())

    // When endVersion is empty, mock returns latestVersion
    assert(commitRange.getStartVersion == 5L)
    assert(
      commitRange.getEndVersion == TEST_LATEST_VERSION,
      "When endVersion is empty, should extend to latest")
  }

  test("getTableChanges passes parameters correctly to loadCommitRange") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val engine = createTestEngine()
    val manager = createManager(client, engine)

    manager.getTableChanges(engine, 2L, Optional.of(8L))

    // Verify exact parameters passed to underlying client
    assert(client.lastCommitRangeStartVersion.isPresent, "startVersionOpt should be present")
    assert(
      client.lastCommitRangeStartVersion.get() == 2L,
      s"startVersion should be 2, got ${client.lastCommitRangeStartVersion.get()}")
    assert(
      client.lastCommitRangeEndVersion.isPresent,
      "endVersionOpt should be present when explicit end provided")
    assert(
      client.lastCommitRangeEndVersion.get() == 8L,
      s"endVersion should be 8, got ${client.lastCommitRangeEndVersion.get()}")
  }

  test("getTableChanges with empty end version passes empty to loadCommitRange") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val engine = createTestEngine()
    val manager = createManager(client, engine)

    manager.getTableChanges(engine, 1L, Optional.empty())

    assert(client.lastCommitRangeStartVersion.isPresent)
    assert(client.lastCommitRangeStartVersion.get() == 1L)
    assert(
      !client.lastCommitRangeEndVersion.isPresent,
      "When endVersion is Optional.empty, should pass empty to client")
  }

  // ==================== Additional Edge Case Tests (from adversarial review) ====================

  test(
    "checkVersionExists: mustBeRecreatable flag is accepted but behavior unchanged for UC tables") {
    // For UC-managed tables, all ratified commits are available (earliest = 0)
    // mustBeRecreatable doesn't change behavior since UC guarantees all versions are recreatable
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    // Both should succeed - mustBeRecreatable doesn't affect UC tables
    manager.checkVersionExists(5L, true /* mustBeRecreatable */, false /* allowOutOfRange */ )
    manager.checkVersionExists(5L, false /* mustBeRecreatable */, false /* allowOutOfRange */ )
  }

  test("checkVersionExists: Long.MAX_VALUE with allowOutOfRange=false throws") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    val exception = intercept[VersionNotFoundException] {
      manager.checkVersionExists(
        Long.MaxValue,
        true /* mustBeRecreatable */,
        false /* allowOutOfRange */ )
    }
    assert(exception.getUserVersion == Long.MaxValue)
  }

  test("checkVersionExists: Long.MAX_VALUE with allowOutOfRange=true does not throw") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val manager = createManager(client)

    // Should not throw
    manager.checkVersionExists(
      Long.MaxValue,
      true /* mustBeRecreatable */,
      true /* allowOutOfRange */ )
  }

  test("getTableChanges with start version 0 returns range from beginning") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val engine = createTestEngine()
    val manager = createManager(client, engine)

    val commitRange = manager.getTableChanges(engine, 0L, Optional.empty())

    assert(commitRange.getStartVersion == 0L)
    assert(commitRange.getEndVersion == TEST_LATEST_VERSION)
  }

  test("getTableChanges with start equals end returns single-version range") {
    val client = new TestUCCatalogManagedClient(TEST_LATEST_VERSION)
    val engine = createTestEngine()
    val manager = createManager(client, engine)

    val commitRange = manager.getTableChanges(engine, 5L, Optional.of(5L))

    assert(commitRange.getStartVersion == 5L)
    assert(commitRange.getEndVersion == 5L)
  }

  // ==================== Exception Propagation Tests ====================

  /**
   * Client that throws exceptions to test error handling.
   */
  class ThrowingUCCatalogManagedClient(exceptionToThrow: RuntimeException)
      extends UCCatalogManagedClient(new InMemoryUCClient("test-metastore")) {

    override def loadSnapshot(
        engine: Engine,
        ucTableId: String,
        tablePath: String,
        versionOpt: Optional[java.lang.Long],
        timestampOpt: Optional[java.lang.Long]): Snapshot = {
      throw exceptionToThrow
    }

    override def loadCommitRange(
        engine: Engine,
        ucTableId: String,
        tablePath: String,
        startVersionOpt: Optional[java.lang.Long],
        startTimestampOpt: Optional[java.lang.Long],
        endVersionOpt: Optional[java.lang.Long],
        endTimestampOpt: Optional[java.lang.Long]): CommitRange = {
      throw exceptionToThrow
    }
  }

  test("loadLatestSnapshot propagates exceptions from client") {
    val expectedException = new RuntimeException("UC connection failed")
    val client = new ThrowingUCCatalogManagedClient(expectedException)
    val manager = createManager(client)

    val thrown = intercept[RuntimeException] {
      manager.loadLatestSnapshot()
    }
    assert(thrown eq expectedException)
  }

  test("loadSnapshotAt propagates exceptions from client") {
    val expectedException = new IllegalArgumentException("Invalid version")
    val client = new ThrowingUCCatalogManagedClient(expectedException)
    val manager = createManager(client)

    val thrown = intercept[IllegalArgumentException] {
      manager.loadSnapshotAt(5L)
    }
    assert(thrown eq expectedException)
  }

  test("checkVersionExists propagates exceptions from loadLatestSnapshot") {
    val expectedException = new RuntimeException("Network error")
    val client = new ThrowingUCCatalogManagedClient(expectedException)
    val manager = createManager(client)

    val thrown = intercept[RuntimeException] {
      manager.checkVersionExists(5L, true /* mustBeRecreatable */, false /* allowOutOfRange */ )
    }
    assert(thrown eq expectedException)
  }

  test("getTableChanges propagates exceptions from client") {
    val expectedException = new RuntimeException("Commit range not available")
    val client = new ThrowingUCCatalogManagedClient(expectedException)
    val engine = createTestEngine()
    val manager = createManager(client, engine)

    val thrown = intercept[RuntimeException] {
      manager.getTableChanges(engine, 1L, Optional.of(5L))
    }
    assert(thrown eq expectedException)
  }
}
