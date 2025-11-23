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

package io.delta.kernel.spark.catalog

import io.delta.kernel.Snapshot
import io.delta.kernel.engine.Engine
import io.delta.kernel.unitycatalog.UCCatalogManagedClient
import io.delta.storage.commit.{Commit, GetCommitsResponse}
import io.delta.storage.commit.uccommitcoordinator.UCClient
import java.io.IOException
import java.net.URI
import java.util.Optional
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for [[UCManagedCommitClientAdapter]].
 *
 * Uses simple test doubles instead of mocking frameworks to validate
 * adapter behavior including error handling and edge cases.
 */
class UCManagedCommitClientAdapterSuite extends AnyFunSuite {

  // Test double for UCClient
  class TestUCClient extends UCClient {
    var closeCalled = false
    var getCommitsCalled = 0
    var latestVersionToReturn: Long = 5
    var shouldThrowOnGetCommits = false
    var commitsToReturn: java.util.List[Commit] = java.util.Collections.emptyList()

    override def getMetastoreId(): String = "test-metastore"

    override def commit(
        tableId: String,
        tableUri: URI,
        commit: Optional[Commit],
        lastKnownBackfilledVersion: Optional[java.lang.Long],
        disown: Boolean,
        newMetadata: Optional[io.delta.storage.commit.actions.AbstractMetadata],
        newProtocol: Optional[io.delta.storage.commit.actions.AbstractProtocol]): Unit = {
      throw new UnsupportedOperationException("Not used in adapter tests")
    }

    override def getCommits(
        tableId: String,
        tableUri: URI,
        startVersion: Optional[java.lang.Long],
        endVersion: Optional[java.lang.Long]): GetCommitsResponse = {
      getCommitsCalled += 1
      if (shouldThrowOnGetCommits) {
        throw new IOException("Test exception")
      }
      new GetCommitsResponse(commitsToReturn, latestVersionToReturn)
    }

    override def close(): Unit = {
      closeCalled = true
    }
  }

  // Test double for UCCatalogManagedClient
  class TestUCCatalogManagedClient(client: UCClient) extends UCCatalogManagedClient(client) {
    var loadSnapshotCalled = 0
    var lastEnginePassedIn: Engine = _
    var lastTableIdPassedIn: String = _
    var lastTablePathPassedIn: String = _
    var lastVersionPassedIn: Optional[java.lang.Long] = _
    var lastTimestampPassedIn: Optional[java.lang.Long] = _
    var snapshotToReturn: Snapshot = _

    override def loadSnapshot(
        engine: Engine,
        ucTableId: String,
        tablePath: String,
        versionOpt: Optional[java.lang.Long],
        timestampOpt: Optional[java.lang.Long]): Snapshot = {
      loadSnapshotCalled += 1
      lastEnginePassedIn = engine
      lastTableIdPassedIn = ucTableId
      lastTablePathPassedIn = tablePath
      lastVersionPassedIn = versionOpt
      lastTimestampPassedIn = timestampOpt
      snapshotToReturn
    }
  }


  test("constructor validates non-null ucClient") {
    val testUCClient = new TestUCClient()
    assertThrows[NullPointerException] {
      new UCManagedCommitClientAdapter(null, testUCClient, "/tmp/test")
    }
  }

  test("constructor validates non-null rawUCClient") {
    val testUCClient = new TestUCClient()
    val testUCCatalogClient = new TestUCCatalogManagedClient(testUCClient)
    assertThrows[NullPointerException] {
      new UCManagedCommitClientAdapter(testUCCatalogClient, null, "/tmp/test")
    }
  }

  test("constructor validates non-null tablePath") {
    val testUCClient = new TestUCClient()
    val testUCCatalogClient = new TestUCCatalogManagedClient(testUCClient)
    assertThrows[NullPointerException] {
      new UCManagedCommitClientAdapter(testUCCatalogClient, testUCClient, null)
    }
  }

  test("getSnapshot delegates to UCCatalogManagedClient with correct parameters") {
    val testUCClient = new TestUCClient()
    val testUCCatalogClient = new TestUCCatalogManagedClient(testUCClient)
    // Use null as snapshot - we're only testing delegation, not snapshot behavior
    testUCCatalogClient.snapshotToReturn = null

    val adapter = new UCManagedCommitClientAdapter(
      testUCCatalogClient,
      testUCClient,
      "/tmp/test/table")

    val mockEngine = null.asInstanceOf[Engine]  // Not used by test double
    adapter.getSnapshot(
      mockEngine,
      "test-table-id",
      "/tmp/test/table",
      Optional.empty(),
      Optional.empty())

    // Verify delegation happened with correct parameters
    assert(testUCCatalogClient.loadSnapshotCalled == 1)
    assert(testUCCatalogClient.lastTableIdPassedIn == "test-table-id")
    assert(testUCCatalogClient.lastTablePathPassedIn == "/tmp/test/table")
    assert(!testUCCatalogClient.lastVersionPassedIn.isPresent)
    assert(!testUCCatalogClient.lastTimestampPassedIn.isPresent)
  }

  test("versionExists returns true when version exists") {
    val testUCClient = new TestUCClient()
    testUCClient.latestVersionToReturn = 10
    testUCClient.commitsToReturn = java.util.Collections.emptyList()

    val testUCCatalogClient = new TestUCCatalogManagedClient(testUCClient)
    val adapter = new UCManagedCommitClientAdapter(
      testUCCatalogClient,
      testUCClient,
      "/tmp/test/table")

    assert(adapter.versionExists("test-table-id", 5))
    assert(testUCClient.getCommitsCalled == 1)
  }

  test("versionExists returns false when UCClient throws exception") {
    val testUCClient = new TestUCClient()
    testUCClient.shouldThrowOnGetCommits = true

    val testUCCatalogClient = new TestUCCatalogManagedClient(testUCClient)
    val adapter = new UCManagedCommitClientAdapter(
      testUCCatalogClient,
      testUCClient,
      "/tmp/test/table")

    assert(!adapter.versionExists("test-table-id", 5))
    assert(testUCClient.getCommitsCalled == 1)
  }

  test("getLatestVersion returns latestTableVersion from UC") {
    val testUCClient = new TestUCClient()
    testUCClient.latestVersionToReturn = 42

    val testUCCatalogClient = new TestUCCatalogManagedClient(testUCClient)
    val adapter = new UCManagedCommitClientAdapter(
      testUCCatalogClient,
      testUCClient,
      "/tmp/test/table")

    val result = adapter.getLatestVersion("test-table-id")
    assert(result == 42)
    assert(testUCClient.getCommitsCalled == 1)
  }

  test("getLatestVersion converts -1 to 0 for newly created tables") {
    val testUCClient = new TestUCClient()
    testUCClient.latestVersionToReturn = -1  // UC returns -1 when only 0.json exists

    val testUCCatalogClient = new TestUCCatalogManagedClient(testUCClient)
    val adapter = new UCManagedCommitClientAdapter(
      testUCCatalogClient,
      testUCClient,
      "/tmp/test/table")

    val result = adapter.getLatestVersion("test-table-id")
    assert(result == 0)  // Should convert -1 to 0
  }

  test("getLatestVersion throws RuntimeException when UCClient fails") {
    val testUCClient = new TestUCClient()
    testUCClient.shouldThrowOnGetCommits = true

    val testUCCatalogClient = new TestUCCatalogManagedClient(testUCClient)
    val adapter = new UCManagedCommitClientAdapter(
      testUCCatalogClient,
      testUCClient,
      "/tmp/test/table")

    val exception = intercept[RuntimeException] {
      adapter.getLatestVersion("test-table-id")
    }
    assert(exception.getMessage.contains("Failed to get latest version"))
  }

  test("close delegates to UCClient") {
    val testUCClient = new TestUCClient()
    val testUCCatalogClient = new TestUCCatalogManagedClient(testUCClient)
    val adapter = new UCManagedCommitClientAdapter(
      testUCCatalogClient,
      testUCClient,
      "/tmp/test/table")

    assert(!testUCClient.closeCalled)
    adapter.close()
    assert(testUCClient.closeCalled)
  }
}
