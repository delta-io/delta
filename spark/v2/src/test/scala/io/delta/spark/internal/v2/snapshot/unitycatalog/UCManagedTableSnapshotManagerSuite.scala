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
package io.delta.spark.internal.v2.snapshot.unitycatalog

import java.util.Optional

import scala.jdk.CollectionConverters._

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.unitycatalog.{InMemoryUCClient, UCCatalogManagedClient, UCCatalogManagedTestUtils}
import io.delta.spark.internal.v2.exception.VersionNotFoundException
import io.delta.storage.commit.uccommitcoordinator.InvalidTargetTableException

import org.scalatest.funsuite.AnyFunSuite

/** Integration tests for [[UCManagedTableSnapshotManager]]. */
class UCManagedTableSnapshotManagerSuite
    extends AnyFunSuite
    with UCCatalogManagedTestUtils {

  private val testUcTableId = "testUcTableId"
  private val testUcUri = "https://test-uc.example.com"
  private val testUcToken = "test-token"
  private val testUcAuthConfig = Map("token" -> testUcToken).asJava

  private def createManager(
      ucClient: InMemoryUCClient,
      tablePath: String) = {
    val client = new UCCatalogManagedClient(ucClient)
    val tableInfo = new UCTableInfo(testUcTableId, tablePath, testUcUri, testUcAuthConfig)
    new UCManagedTableSnapshotManager(client, tableInfo, defaultEngine)
  }

  // ==================== Constructor ====================

  test("constructor rejects null arguments") {
    val ucClient = new InMemoryUCClient("testMetastore")
    val client = new UCCatalogManagedClient(ucClient)
    val tableInfo = new UCTableInfo(testUcTableId, "/test/path", testUcUri, testUcAuthConfig)

    val ex1 = intercept[NullPointerException] {
      new UCManagedTableSnapshotManager(null, tableInfo, defaultEngine)
    }
    assert(ex1.getMessage == "ucCatalogManagedClient is null")

    val ex2 = intercept[NullPointerException] {
      new UCManagedTableSnapshotManager(client, null, defaultEngine)
    }
    assert(ex2.getMessage == "tableInfo is null")

    val ex3 = intercept[NullPointerException] {
      new UCManagedTableSnapshotManager(client, tableInfo, null)
    }
    assert(ex3.getMessage == "engine is null")
  }

  // ==================== loadLatestSnapshot ====================

  test("loadLatestSnapshot: returns snapshot at max ratified version") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      val snapshot = manager.loadLatestSnapshot()

      assert(snapshot.getVersion == maxRatifiedVersion)
    }
  }

  test("loadLatestSnapshot: throws when table does not exist in catalog") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val tableInfo = new UCTableInfo("nonExistentTableId", "/fake/path", testUcUri, testUcAuthConfig)
    val client = new UCCatalogManagedClient(ucClient)
    val manager = new UCManagedTableSnapshotManager(client, tableInfo, defaultEngine)

    val ex = intercept[RuntimeException] {
      manager.loadLatestSnapshot()
    }
    assert(ex.getCause.isInstanceOf[InvalidTargetTableException])
  }

  // ==================== loadSnapshotAt ====================

  test("loadSnapshotAt: valid versions including v0 succeed, invalid versions throw") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      assert(manager.loadSnapshotAt(0L).getVersion == 0L)
      assert(manager.loadSnapshotAt(1L).getVersion == 1L)

      intercept[IllegalArgumentException] { manager.loadSnapshotAt(-1L) }
      intercept[IllegalArgumentException] { manager.loadSnapshotAt(maxRatifiedVersion + 10) }
    }
  }

  // ==================== checkVersionExists ====================

  test("checkVersionExists: validates version bounds and allowOutOfRange flag") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      // Valid versions including v0 do not throw
      manager.checkVersionExists(0L, true /* mustBeRecreatable */, false /* allowOutOfRange */ )
      manager.checkVersionExists(
        maxRatifiedVersion,
        true /* mustBeRecreatable */,
        false /* allowOutOfRange */ )
      manager.checkVersionExists(
        maxRatifiedVersion - 1,
        true /* mustBeRecreatable */,
        false /* allowOutOfRange */ )
      manager.checkVersionExists(1L, true /* mustBeRecreatable */, false /* allowOutOfRange */ )
      manager.checkVersionExists(1L, false /* mustBeRecreatable */, false /* allowOutOfRange */ )

      // Out-of-bounds versions throw
      val belowLowerBound = intercept[VersionNotFoundException] {
        manager.checkVersionExists(-1L, true /* mustBeRecreatable */, false /* allowOutOfRange */ )
      }
      assert(belowLowerBound.getUserVersion == -1L)
      assert(belowLowerBound.getEarliest == 0L)
      assert(belowLowerBound.getLatest == maxRatifiedVersion)

      val aboveUpperBound = intercept[VersionNotFoundException] {
        manager.checkVersionExists(
          maxRatifiedVersion + 10,
          true /* mustBeRecreatable */,
          false /* allowOutOfRange */ )
      }
      assert(aboveUpperBound.getUserVersion == maxRatifiedVersion + 10)
      assert(aboveUpperBound.getEarliest == 0L)
      assert(aboveUpperBound.getLatest == maxRatifiedVersion)

      // allowOutOfRange=true bypasses upper bound check
      manager.checkVersionExists(
        maxRatifiedVersion + 10,
        true /* mustBeRecreatable */,
        true /* allowOutOfRange */ )
    }
  }

  // ==================== getActiveCommitAtTime ====================

  test("getActiveCommitAtTime: resolves timestamps across all boundaries") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      // Before first commit (v0) - throws without canReturnEarliestCommit
      intercept[KernelException] {
        manager.getActiveCommitAtTime(
          v0Ts - 1,
          false /* canReturnLastCommit */,
          true /* mustBeRecreatable */,
          false /* canReturnEarliestCommit */ )
      }
      intercept[KernelException] {
        manager.getActiveCommitAtTime(
          -100L,
          false /* canReturnLastCommit */,
          true /* mustBeRecreatable */,
          false /* canReturnEarliestCommit */ )
      }
      // With canReturnEarliestCommit, returns v0
      val earliestCommit = manager.getActiveCommitAtTime(
        v0Ts - 1,
        false /* canReturnLastCommit */,
        true /* mustBeRecreatable */,
        true /* canReturnEarliestCommit */ )
      assert(earliestCommit.getVersion == 0L)

      // Exact and between-commit timestamps
      def activeVersion(ts: Long): Long =
        manager
          .getActiveCommitAtTime(
            ts,
            false /* canReturnLastCommit */,
            true /* mustBeRecreatable */,
            false /* canReturnEarliestCommit */ )
          .getVersion

      assert(activeVersion(v0Ts) == 0L)
      assert(activeVersion(v0Ts + 1) == 0L)
      assert(activeVersion(v1Ts) == 1L)
      assert(activeVersion(v1Ts + 1) == 1L)
      assert(activeVersion(v2Ts) == 2L)

      // After last commit (v2) - throws without canReturnLastCommit
      intercept[KernelException] {
        manager.getActiveCommitAtTime(
          v2Ts + 1,
          false /* canReturnLastCommit */,
          true /* mustBeRecreatable */,
          false /* canReturnEarliestCommit */ )
      }
      intercept[KernelException] {
        manager.getActiveCommitAtTime(
          Long.MaxValue,
          false /* canReturnLastCommit */,
          true /* mustBeRecreatable */,
          false /* canReturnEarliestCommit */ )
      }
      // With canReturnLastCommit, returns v2
      val lastCommit = manager.getActiveCommitAtTime(
        v2Ts + 1,
        true /* canReturnLastCommit */,
        true /* mustBeRecreatable */,
        false /* canReturnEarliestCommit */ )
      assert(lastCommit.getVersion == 2L)
    }
  }

  test("getActiveCommitAtTime: non-recreatable path returns earliest delta file") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      val active = manager.getActiveCommitAtTime(
        v0Ts - 1,
        false /* canReturnLastCommit */,
        false /* mustBeRecreatable */,
        true /* canReturnEarliestCommit */ )

      assert(active.getVersion == 0L)
    }
  }

  // ==================== getTableChanges ====================

  test("getTableChanges: returns valid ranges and rejects invalid arguments") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      // Valid ranges including v0 and latest boundaries
      val fullRange = manager.getTableChanges(defaultEngine, 0L, Optional.of(maxRatifiedVersion))
      assert(fullRange.getStartVersion == 0L)
      assert(fullRange.getEndVersion == maxRatifiedVersion)

      val toLatest = manager.getTableChanges(defaultEngine, 1L, Optional.empty())
      assert(toLatest.getStartVersion == 1L)
      assert(toLatest.getEndVersion == maxRatifiedVersion)

      val single = manager.getTableChanges(defaultEngine, 1L, Optional.of(1L))
      assert(single.getStartVersion == 1L)
      assert(single.getEndVersion == 1L)

      val first = manager.getTableChanges(defaultEngine, 0L, Optional.of(0L))
      assert(first.getStartVersion == 0L)
      assert(first.getEndVersion == 0L)

      val last = manager.getTableChanges(
        defaultEngine,
        maxRatifiedVersion,
        Optional.of(maxRatifiedVersion))
      assert(last.getStartVersion == maxRatifiedVersion)
      assert(last.getEndVersion == maxRatifiedVersion)

      // Invalid ranges throw
      intercept[IllegalArgumentException] {
        manager.getTableChanges(
          defaultEngine,
          maxRatifiedVersion,
          Optional.of(maxRatifiedVersion - 1))
      }

      intercept[IllegalArgumentException] {
        manager.getTableChanges(defaultEngine, maxRatifiedVersion + 5, Optional.empty())
      }
    }
  }

  // ==================== Exception Propagation ====================

  test("operations propagate InvalidTargetTableException from client") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val tableInfo = new UCTableInfo("nonExistentTableId", "/fake/path", testUcUri, testUcAuthConfig)
    val client = new UCCatalogManagedClient(ucClient)
    val manager = new UCManagedTableSnapshotManager(client, tableInfo, defaultEngine)

    val ex1 = intercept[RuntimeException] { manager.loadLatestSnapshot() }
    assert(ex1.getCause.isInstanceOf[InvalidTargetTableException])

    val ex2 = intercept[RuntimeException] { manager.loadSnapshotAt(0L) }
    assert(ex2.getCause.isInstanceOf[InvalidTargetTableException])

    val ex3 = intercept[RuntimeException] { manager.checkVersionExists(0L, true, false) }
    assert(ex3.getCause.isInstanceOf[InvalidTargetTableException])

    val ex4 = intercept[RuntimeException] {
      manager.getTableChanges(defaultEngine, 0L, Optional.empty())
    }
    assert(ex4.getCause.isInstanceOf[InvalidTargetTableException])
  }
}
