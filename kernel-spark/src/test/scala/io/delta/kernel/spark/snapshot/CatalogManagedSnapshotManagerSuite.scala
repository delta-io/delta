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
package io.delta.kernel.spark.snapshot

import java.util.Optional

import io.delta.kernel.spark.exception.VersionNotFoundException
import io.delta.kernel.spark.snapshot.unitycatalog.UnityCatalogAdapter
import io.delta.kernel.unitycatalog.{InMemoryUCClient, UCCatalogManagedTestUtils}

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

/** Tests for [[CatalogManagedSnapshotManager]]. */
class CatalogManagedSnapshotManagerSuite extends AnyFunSuite with UCCatalogManagedTestUtils {

  private val testUcTableId = "testUcTableId"

  test("constructor throws on null hadoopConf") {
    val adapter = new UnityCatalogAdapter(
      testUcTableId,
      "/tmp/path",
      new InMemoryUCClient("ucMetastoreId"))

    assertThrows[NullPointerException] {
      new CatalogManagedSnapshotManager(adapter, testUcTableId, "/tmp/path", null)
    }
  }

  test("constructor throws on null catalogAdapter") {
    assertThrows[NullPointerException] {
      new CatalogManagedSnapshotManager(null, testUcTableId, "/tmp/path", new Configuration())
    }
  }

  test("constructor throws on null tableId") {
    val adapter = new UnityCatalogAdapter(
      testUcTableId,
      "/tmp/path",
      new InMemoryUCClient("ucMetastoreId"))

    assertThrows[NullPointerException] {
      new CatalogManagedSnapshotManager(adapter, null, "/tmp/path", new Configuration())
    }
  }

  test("constructor throws on null tablePath") {
    val adapter = new UnityCatalogAdapter(
      testUcTableId,
      "/tmp/path",
      new InMemoryUCClient("ucMetastoreId"))

    assertThrows[NullPointerException] {
      new CatalogManagedSnapshotManager(adapter, testUcTableId, null, new Configuration())
    }
  }

  test("loadLatestSnapshot returns snapshot at max ratified version") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val adapter = new UnityCatalogAdapter(testUcTableId, tablePath, ucClient)
      val manager = new CatalogManagedSnapshotManager(
        adapter,
        testUcTableId,
        tablePath,
        new Configuration())

      try {
        val snapshot = manager.loadLatestSnapshot()

        assert(snapshot != null, "Snapshot should not be null")
        assert(snapshot.getVersion == maxRatifiedVersion, "Should load max ratified version")
      } finally {
        manager.close()
      }
    }
  }

  test("loadSnapshotAt loads specified version") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val adapter = new UnityCatalogAdapter(testUcTableId, tablePath, ucClient)
      val manager = new CatalogManagedSnapshotManager(
        adapter,
        testUcTableId,
        tablePath,
        new Configuration())

      try {
        val snapshot = manager.loadSnapshotAt(1L)

        assert(snapshot != null, "Snapshot should not be null")
        assert(snapshot.getVersion == 1L, "Should load version 1")
      } finally {
        manager.close()
      }
    }
  }

  test("loadSnapshotAt throws on negative version") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val adapter = new UnityCatalogAdapter(testUcTableId, tablePath, ucClient)
      val manager = new CatalogManagedSnapshotManager(
        adapter,
        testUcTableId,
        tablePath,
        new Configuration())

      try {
        assertThrows[IllegalArgumentException] {
          manager.loadSnapshotAt(-1L)
        }
      } finally {
        manager.close()
      }
    }
  }

  test("checkVersionExists validates version range") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val adapter = new UnityCatalogAdapter(testUcTableId, tablePath, ucClient)
      val manager = new CatalogManagedSnapshotManager(
        adapter,
        testUcTableId,
        tablePath,
        new Configuration())

      try {
        // Versions 0, 1, 2 should exist
        manager.checkVersionExists(
          0L,
          /* mustBeRecreatable = */ true,
          /* allowOutOfRange = */ false)
        manager.checkVersionExists(
          1L,
          /* mustBeRecreatable = */ true,
          /* allowOutOfRange = */ false)
        manager.checkVersionExists(
          maxRatifiedVersion,
          /* mustBeRecreatable = */ true,
          /* allowOutOfRange = */ false)

        // Version beyond latest should throw
        assertThrows[VersionNotFoundException] {
          manager.checkVersionExists(
            maxRatifiedVersion + 1,
            /* mustBeRecreatable = */ true,
            /* allowOutOfRange = */ false)
        }
      } finally {
        manager.close()
      }
    }
  }

  test("checkVersionExists allows out of range when specified") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val adapter = new UnityCatalogAdapter(testUcTableId, tablePath, ucClient)
      val manager = new CatalogManagedSnapshotManager(
        adapter,
        testUcTableId,
        tablePath,
        new Configuration())

      try {
        // Should not throw when allowOutOfRange = true
        manager.checkVersionExists(
          maxRatifiedVersion + 10,
          /* mustBeRecreatable = */ true,
          /* allowOutOfRange = */ true)
      } finally {
        manager.close()
      }
    }
  }

  test("checkVersionExists throws on negative version") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val adapter = new UnityCatalogAdapter(testUcTableId, tablePath, ucClient)
      val manager = new CatalogManagedSnapshotManager(
        adapter,
        testUcTableId,
        tablePath,
        new Configuration())

      try {
        assertThrows[IllegalArgumentException] {
          manager.checkVersionExists(
            -1L,
            /* mustBeRecreatable = */ true,
            /* allowOutOfRange = */ false)
        }
      } finally {
        manager.close()
      }
    }
  }

  test("getTableChanges returns commit range") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val adapter = new UnityCatalogAdapter(testUcTableId, tablePath, ucClient)
      val manager = new CatalogManagedSnapshotManager(
        adapter,
        testUcTableId,
        tablePath,
        new Configuration())

      try {
        val commitRange = manager.getTableChanges(
          defaultEngine,
          /* startVersion = */ 1L,
          /* endVersion = */ Optional.of(2L))

        assert(commitRange != null, "CommitRange should not be null")
      } finally {
        manager.close()
      }
    }
  }

  test("close releases resources") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val adapter = new UnityCatalogAdapter(testUcTableId, tablePath, ucClient)
      val manager = new CatalogManagedSnapshotManager(
        adapter,
        testUcTableId,
        tablePath,
        new Configuration())

      // Should not throw
      manager.close()
    }
  }

  // Time-travel tests for getActiveCommitAtTime

  test("getActiveCommitAtTime returns commit at exact timestamp") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val adapter = new UnityCatalogAdapter(testUcTableId, tablePath, ucClient)
      val manager = new CatalogManagedSnapshotManager(
        adapter,
        testUcTableId,
        tablePath,
        new Configuration())

      try {
        // Timestamp at v1 should return version 1
        val commit = manager.getActiveCommitAtTime(
          v1Ts,
          /* canReturnLastCommit = */ false,
          /* mustBeRecreatable = */ true,
          /* canReturnEarliestCommit = */ false)

        assert(commit != null, "Commit should not be null")
        assert(commit.getVersion == 1L, s"Expected version 1, got ${commit.getVersion}")
      } finally {
        manager.close()
      }
    }
  }

  test("getActiveCommitAtTime returns latest when canReturnLastCommit is true") {
    withUCClientAndTestTable { (ucClient, tablePath, maxRatifiedVersion) =>
      val adapter = new UnityCatalogAdapter(testUcTableId, tablePath, ucClient)
      val manager = new CatalogManagedSnapshotManager(
        adapter,
        testUcTableId,
        tablePath,
        new Configuration())

      try {
        // Timestamp after all commits with canReturnLastCommit=true
        val futureTs = v2Ts + 1000000L
        val commit = manager.getActiveCommitAtTime(
          futureTs,
          /* canReturnLastCommit = */ true,
          /* mustBeRecreatable = */ true,
          /* canReturnEarliestCommit = */ false)

        assert(commit != null, "Commit should not be null")
        assert(commit.getVersion == maxRatifiedVersion,
          s"Expected version $maxRatifiedVersion, got ${commit.getVersion}")
      } finally {
        manager.close()
      }
    }
  }

  test("getActiveCommitAtTime returns earliest when canReturnEarliestCommit is true") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val adapter = new UnityCatalogAdapter(testUcTableId, tablePath, ucClient)
      val manager = new CatalogManagedSnapshotManager(
        adapter,
        testUcTableId,
        tablePath,
        new Configuration())

      try {
        // Timestamp before all commits with canReturnEarliestCommit=true
        val pastTs = v0Ts - 1000000L
        val commit = manager.getActiveCommitAtTime(
          pastTs,
          /* canReturnLastCommit = */ false,
          /* mustBeRecreatable = */ true,
          /* canReturnEarliestCommit = */ true)

        assert(commit != null, "Commit should not be null")
        // Should return the earliest available version
        assert(commit.getVersion >= 0L, s"Expected version >= 0, got ${commit.getVersion}")
      } finally {
        manager.close()
      }
    }
  }

  test("getActiveCommitAtTime returns commit between versions") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val adapter = new UnityCatalogAdapter(testUcTableId, tablePath, ucClient)
      val manager = new CatalogManagedSnapshotManager(
        adapter,
        testUcTableId,
        tablePath,
        new Configuration())

      try {
        // Timestamp between v1 and v2 should return v1
        val betweenTs = (v1Ts + v2Ts) / 2
        val commit = manager.getActiveCommitAtTime(
          betweenTs,
          /* canReturnLastCommit = */ false,
          /* mustBeRecreatable = */ true,
          /* canReturnEarliestCommit = */ false)

        assert(commit != null, "Commit should not be null")
        assert(commit.getVersion == 1L, s"Expected version 1, got ${commit.getVersion}")
      } finally {
        manager.close()
      }
    }
  }

  test("loadSnapshotAt loads version 0") {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val adapter = new UnityCatalogAdapter(testUcTableId, tablePath, ucClient)
      val manager = new CatalogManagedSnapshotManager(
        adapter,
        testUcTableId,
        tablePath,
        new Configuration())

      try {
        val snapshot = manager.loadSnapshotAt(0L)

        assert(snapshot != null, "Snapshot should not be null")
        assert(snapshot.getVersion == 0L, "Should load version 0")
      } finally {
        manager.close()
      }
    }
  }
}
