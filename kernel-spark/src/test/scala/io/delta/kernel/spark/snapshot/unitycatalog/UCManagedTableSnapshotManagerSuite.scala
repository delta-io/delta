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

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.spark.exception.VersionNotFoundException
import io.delta.kernel.unitycatalog.{InMemoryUCClient, UCCatalogManagedClient, UCCatalogManagedTestUtils}
import io.delta.storage.commit.uccommitcoordinator.InvalidTargetTableException

import org.scalatest.funsuite.AnyFunSuite

/** Integration tests for [[UCManagedTableSnapshotManager]]. */
class UCManagedTableSnapshotManagerSuite
    extends AnyFunSuite
    with UCCatalogManagedTestUtils {

  private val testUcTableId = "testUcTableId"
  private val testUcUri = "https://test-uc.example.com"
  private val testUcToken = "test-token"

  private def createManager(
      ucClient: InMemoryUCClient,
      tablePath: String) = {
    val client = new UCCatalogManagedClient(ucClient)
    val tableInfo = new UCTableInfo(testUcTableId, tablePath, testUcUri, testUcToken)
    new UCManagedTableSnapshotManager(client, tableInfo, defaultEngine)
  }

  // ==================== loadLatestSnapshot ====================

  test("loadLatestSnapshot returns snapshot at max ratified version") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      val snapshot = manager.loadLatestSnapshot()

      assert(snapshot.getVersion == maxRatifiedVersion)
    }
  }

  test("loadLatestSnapshot throws when table does not exist in catalog") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val tableInfo = new UCTableInfo("nonExistentTableId", "/fake/path", testUcUri, testUcToken)
    val client = new UCCatalogManagedClient(ucClient)
    val manager = new UCManagedTableSnapshotManager(client, tableInfo, defaultEngine)

    val ex = intercept[RuntimeException] {
      manager.loadLatestSnapshot()
    }
    assert(ex.getCause.isInstanceOf[InvalidTargetTableException])
  }

  // ==================== loadSnapshotAt ====================

  test("loadSnapshotAt returns snapshot at specified version") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      val snapshot = manager.loadSnapshotAt(1L)

      assert(snapshot.getVersion == 1L)
    }
  }

  test("loadSnapshotAt at version zero returns correct snapshot") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      val snapshot = manager.loadSnapshotAt(0L)

      assert(snapshot.getVersion == 0L)
    }
  }

  test("loadSnapshotAt with negative version throws IllegalArgumentException") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      val ex = intercept[IllegalArgumentException] {
        manager.loadSnapshotAt(-1L)
      }
    }
  }

  test("loadSnapshotAt with version beyond max throws IllegalArgumentException") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      val ex = intercept[IllegalArgumentException] {
        manager.loadSnapshotAt(maxRatifiedVersion + 10)
      }
    }
  }

  // ==================== checkVersionExists ====================

  test("checkVersionExists: version within valid range does not throw") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      manager.checkVersionExists(
        maxRatifiedVersion - 1,
        true /* mustBeRecreatable */,
        false /* allowOutOfRange */ )
    }
  }

  test("checkVersionExists: version at boundary zero does not throw") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      manager.checkVersionExists(0L, true /* mustBeRecreatable */, false /* allowOutOfRange */ )
    }
  }

  test("checkVersionExists: version at boundary latest does not throw") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      manager.checkVersionExists(
        maxRatifiedVersion,
        true /* mustBeRecreatable */,
        false /* allowOutOfRange */ )
    }
  }

  test("checkVersionExists: version below zero throws VersionNotFoundException") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      val exception = intercept[VersionNotFoundException] {
        manager.checkVersionExists(-1L, true /* mustBeRecreatable */, false /* allowOutOfRange */ )
      }
      assert(exception.getUserVersion == -1L)
      assert(exception.getEarliest == 0L)
      assert(exception.getLatest == maxRatifiedVersion)
    }
  }

  test("checkVersionExists: version above latest with allowOutOfRange=false throws") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      val exception = intercept[VersionNotFoundException] {
        manager.checkVersionExists(
          maxRatifiedVersion + 10,
          true /* mustBeRecreatable */,
          false /* allowOutOfRange */ )
      }
      assert(exception.getUserVersion == maxRatifiedVersion + 10)
      assert(exception.getEarliest == 0L)
      assert(exception.getLatest == maxRatifiedVersion)
    }
  }

  test("checkVersionExists: version above latest with allowOutOfRange=true does not throw") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      manager.checkVersionExists(
        maxRatifiedVersion + 10,
        true /* mustBeRecreatable */,
        true /* allowOutOfRange */ )
    }
  }

  test("checkVersionExists: mustBeRecreatable flag is accepted (UC tables always recreatable)") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      manager.checkVersionExists(1L, true /* mustBeRecreatable */, false /* allowOutOfRange */ )
      manager.checkVersionExists(1L, false /* mustBeRecreatable */, false /* allowOutOfRange */ )
    }
  }

  // ==================== getActiveCommitAtTime ====================

  test("getActiveCommitAtTime: before earliest without earliest flag throws") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      val ex = intercept[KernelException] {
        manager.getActiveCommitAtTime(
          v0Ts - 1,
          false /* canReturnLastCommit */,
          true /* mustBeRecreatable */,
          false /* canReturnEarliestCommit */ )
      }
    }
  }

  test("getActiveCommitAtTime: before earliest with earliest flag returns v0") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      val active = manager.getActiveCommitAtTime(
        v0Ts - 1,
        false /* canReturnLastCommit */,
        true /* mustBeRecreatable */,
        true /* canReturnEarliestCommit */ )

      assert(active.getVersion == 0L)
    }
  }

  test("getActiveCommitAtTime: exact boundaries and between commits") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      def activeVersion(ts: Long): Long = {
        manager
          .getActiveCommitAtTime(
            ts,
            false /* canReturnLastCommit */,
            true /* mustBeRecreatable */,
            false /* canReturnEarliestCommit */ )
          .getVersion
      }

      assert(activeVersion(v0Ts) == 0L)
      assert(activeVersion(v0Ts + 1) == 0L)
      assert(activeVersion(v1Ts) == 1L)
      assert(activeVersion(v1Ts + 1) == 1L)
      assert(activeVersion(v2Ts) == 2L)
    }
  }

  test("getActiveCommitAtTime: after latest without last-commit flag throws") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      val ex = intercept[KernelException] {
        manager.getActiveCommitAtTime(
          v2Ts + 1,
          false /* canReturnLastCommit */,
          true /* mustBeRecreatable */,
          false /* canReturnEarliestCommit */ )
      }
    }
  }

  test("getActiveCommitAtTime: after latest with last-commit flag returns last version") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      val active = manager.getActiveCommitAtTime(
        v2Ts + 1,
        true /* canReturnLastCommit */,
        true /* mustBeRecreatable */,
        false /* canReturnEarliestCommit */ )

      assert(active.getVersion == 2L)
    }
  }

  test("getActiveCommitAtTime: negative and very large timestamps are handled") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      val negativeTsEx = intercept[KernelException] {
        manager.getActiveCommitAtTime(
          -100L,
          false /* canReturnLastCommit */,
          true /* mustBeRecreatable */,
          false /* canReturnEarliestCommit */ )
      }

      val largeTsEx = intercept[KernelException] {
        manager.getActiveCommitAtTime(
          Long.MaxValue,
          false /* canReturnLastCommit */,
          true /* mustBeRecreatable */,
          false /* canReturnEarliestCommit */ )
      }
    }
  }

  // ==================== getTableChanges ====================

  test("getTableChanges with explicit end version returns correct commit range") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      val commitRange = manager.getTableChanges(defaultEngine, 0L, Optional.of(maxRatifiedVersion))

      assert(commitRange.getStartVersion == 0L)
      assert(commitRange.getEndVersion == maxRatifiedVersion)
    }
  }

  test("getTableChanges without end version returns range to latest") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      val commitRange = manager.getTableChanges(defaultEngine, 1L, Optional.empty())

      assert(commitRange.getStartVersion == 1L)
      assert(commitRange.getEndVersion == maxRatifiedVersion)
    }
  }

  test("getTableChanges with start equals end returns single-version range") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      val commitRange = manager.getTableChanges(defaultEngine, 1L, Optional.of(1L))

      assert(commitRange.getStartVersion == 1L)
      assert(commitRange.getEndVersion == 1L)
    }
  }

  test("getTableChanges with inverted range (start > end) throws IllegalArgumentException") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      val ex = intercept[IllegalArgumentException] {
        manager.getTableChanges(defaultEngine, 2L, Optional.of(1L))
      }
    }
  }

  test("getTableChanges with start beyond latest throws") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      val ex = intercept[IllegalArgumentException] {
        manager.getTableChanges(defaultEngine, maxRatifiedVersion + 5, Optional.empty())
      }
    }
  }

  test("getTableChanges with start=end=0 returns first commit") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val manager = createManager(ucClient, tablePath)

      val commitRange = manager.getTableChanges(defaultEngine, 0L, Optional.of(0L))

      assert(commitRange.getStartVersion == 0L)
      assert(commitRange.getEndVersion == 0L)
    }
  }

  test("getTableChanges with start=end=max returns latest commit") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val manager = createManager(ucClient, tablePath)

      val commitRange = manager.getTableChanges(
        defaultEngine,
        maxRatifiedVersion,
        Optional.of(maxRatifiedVersion))

      assert(commitRange.getStartVersion == maxRatifiedVersion)
      assert(commitRange.getEndVersion == maxRatifiedVersion)
    }
  }

  // ==================== Exception Propagation ====================

  test("operations propagate InvalidTargetTableException from client") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val tableInfo = new UCTableInfo("nonExistentTableId", "/fake/path", testUcUri, testUcToken)
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
