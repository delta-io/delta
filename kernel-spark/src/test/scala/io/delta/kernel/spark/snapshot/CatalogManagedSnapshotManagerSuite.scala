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

import io.delta.kernel.CommitRange
import io.delta.kernel.Snapshot
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.files.ParsedLogData
import io.delta.kernel.spark.snapshot.unitycatalog.UnityCatalogAdapter
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

/** Tests for [[CatalogManagedSnapshotManager]] wireframe. */
class CatalogManagedSnapshotManagerSuite extends AnyFunSuite {

  private val tableId = "testTable"
  private val tablePath = "/tmp/path"

  private def stubAdapter: ManagedCatalogAdapter = new ManagedCatalogAdapter {
    override def loadSnapshot(
        engine: Engine,
        versionOpt: Optional[java.lang.Long],
        timestampOpt: Optional[java.lang.Long]): Snapshot =
      throw new UnsupportedOperationException("stub")

    override def loadCommitRange(
        engine: Engine,
        startVersionOpt: Optional[java.lang.Long],
        startTimestampOpt: Optional[java.lang.Long],
        endVersionOpt: Optional[java.lang.Long],
        endTimestampOpt: Optional[java.lang.Long]): CommitRange =
      throw new UnsupportedOperationException("stub")

    override def getRatifiedCommits(endVersionOpt: Optional[java.lang.Long]): java.util.List[ParsedLogData] =
      throw new UnsupportedOperationException("stub")

    override def getLatestRatifiedVersion: Long =
      throw new UnsupportedOperationException("stub")

    override def close(): Unit = {}
  }

  test("constructor throws on nulls") {
    assertThrows[NullPointerException] {
      new CatalogManagedSnapshotManager(null, tableId, tablePath, new Configuration())
    }
    assertThrows[NullPointerException] {
      new CatalogManagedSnapshotManager(stubAdapter, null, tablePath, new Configuration())
    }
    assertThrows[NullPointerException] {
      new CatalogManagedSnapshotManager(stubAdapter, tableId, null, new Configuration())
    }
    assertThrows[NullPointerException] {
      new CatalogManagedSnapshotManager(stubAdapter, tableId, tablePath, null)
    }
  }

  test("operations are unsupported in wireframe") {
    val manager = new CatalogManagedSnapshotManager(stubAdapter, tableId, tablePath, new Configuration())

    assertThrows[UnsupportedOperationException] {
      manager.loadLatestSnapshot()
    }
    assertThrows[UnsupportedOperationException] {
      manager.loadSnapshotAt(0L)
    }
    assertThrows[UnsupportedOperationException] {
      manager.getTableChanges(null, 0L, Optional.empty())
    }
    assertThrows[UnsupportedOperationException] {
      manager.checkVersionExists(0L, true, false)
    }
    assertThrows[UnsupportedOperationException] {
      manager.getActiveCommitAtTime(0L, true, true, true)
    }
  }

  test("loadSnapshotAt validates version") {
    val manager = new CatalogManagedSnapshotManager(stubAdapter, tableId, tablePath, new Configuration())
    assertThrows[IllegalArgumentException] {
      manager.loadSnapshotAt(-1L)
    }
  }

  test("checkVersionExists validates version") {
    val manager = new CatalogManagedSnapshotManager(stubAdapter, tableId, tablePath, new Configuration())
    assertThrows[IllegalArgumentException] {
      manager.checkVersionExists(-1L, true, false)
    }
  }

  test("close is tolerant") {
    val manager = new CatalogManagedSnapshotManager(stubAdapter, tableId, tablePath, new Configuration())
    manager.close() // should not throw
  }
}
