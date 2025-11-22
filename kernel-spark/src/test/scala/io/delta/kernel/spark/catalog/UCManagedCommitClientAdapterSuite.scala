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

import java.util.Optional

import io.delta.kernel.unitycatalog.{InMemoryUCClient, UCCatalogManagedClient, UCCatalogManagedTestUtils}

import org.scalatest.funsuite.AnyFunSuite

/** Unit tests for [[UCManagedCommitClientAdapter]]. */
class UCManagedCommitClientAdapterSuite extends AnyFunSuite with UCCatalogManagedTestUtils {

  private val testUcTableId = "testUcTableId"

  test("constructor throws on null input") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    assertThrows[NullPointerException] {
      new UCManagedCommitClientAdapter(null, ucClient, "/tmp/test")
    }

    assertThrows[NullPointerException] {
      new UCManagedCommitClientAdapter(ucCatalogManagedClient, null, "/tmp/test")
    }

    assertThrows[NullPointerException] {
      new UCManagedCommitClientAdapter(ucCatalogManagedClient, ucClient, null)
    }
  }

  test("getSnapshot delegates to UCCatalogManagedClient") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
      val adapter = new UCManagedCommitClientAdapter(
        ucCatalogManagedClient,
        ucClient,
        tablePath)

      // Load snapshot using adapter
      val snapshot = adapter.getSnapshot(
        engine,
        testUcTableId,
        tablePath,
        Optional.empty(),
        Optional.empty())

      // Verify snapshot was loaded correctly
      assert(snapshot.getVersion == maxRatifiedVersion)
      assert(ucClient.getNumGetCommitCalls == 1)
    }
  }

  test("getSnapshot with specific version") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
      val adapter = new UCManagedCommitClientAdapter(
        ucCatalogManagedClient,
        ucClient,
        tablePath)

      // Load specific version
      val versionToLoad = 1L
      val snapshot = adapter.getSnapshot(
        engine,
        testUcTableId,
        tablePath,
        Optional.of(versionToLoad),
        Optional.empty())

      // Verify correct version was loaded
      assert(snapshot.getVersion == versionToLoad)
      assert(ucClient.getNumGetCommitCalls == 1)
    }
  }

  test("versionExists returns true for existing version") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
      val adapter = new UCManagedCommitClientAdapter(
        ucCatalogManagedClient,
        ucClient,
        tablePath)

      // Check existing versions
      assert(adapter.versionExists(testUcTableId, 0))
      assert(adapter.versionExists(testUcTableId, 1))
      assert(adapter.versionExists(testUcTableId, maxRatifiedVersion))
    }
  }

  test("versionExists returns false for non-existing version") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
      val adapter = new UCManagedCommitClientAdapter(
        ucCatalogManagedClient,
        ucClient,
        tablePath)

      // Check non-existing version (beyond max ratified version)
      assert(!adapter.versionExists(testUcTableId, maxRatifiedVersion + 1))
      assert(!adapter.versionExists(testUcTableId, 999))
    }
  }

  test("getLatestVersion returns correct version") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
      val adapter = new UCManagedCommitClientAdapter(
        ucCatalogManagedClient,
        ucClient,
        tablePath)

      val latestVersion = adapter.getLatestVersion(testUcTableId)
      assert(latestVersion == maxRatifiedVersion)
    }
  }

  test("close delegates to UCClient") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
    val adapter = new UCManagedCommitClientAdapter(
      ucCatalogManagedClient,
      ucClient,
      "/tmp/test")

    // Should not throw
    adapter.close()
  }
}
