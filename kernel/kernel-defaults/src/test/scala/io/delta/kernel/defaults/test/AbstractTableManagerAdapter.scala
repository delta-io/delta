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

package io.delta.kernel.defaults.test

import io.delta.kernel.{Table, TableManager}
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.{SnapshotImpl, TableImpl}
import io.delta.kernel.internal.table.SnapshotBuilderImpl

/**
 * Test framework adapter that provides a unified interface for **loading** Delta tables.
 *
 * This trait enables test suites to be parameterized over different Kernel APIs via the
 * [[LegacyTableManagerAdapter]] and [[TableManagerAdapter]] child classes.
 *
 * By using this adapter pattern, the same test suite can verify both APIs work correctly, without
 * duplicating test logic.
 *
 * Tests can switch between implementations by mixing in either
 * [[io.delta.kernel.defaults.utils.TestUtilsWithLegacyKernelAPIs]] or
 * [[io.delta.kernel.defaults.utils.TestUtilsWithTableManagerAPIs]].
 */
trait AbstractTableManagerAdapter {

  /** Does this adapter support resolving a timestamp to a version? */
  def supportsTimestampResolution: Boolean

  def getSnapshotAtLatest(engine: Engine, path: String): SnapshotImpl

  def getSnapshotAtVersion(engine: Engine, path: String, version: Long): SnapshotImpl

  def getSnapshotAtTimestamp(engine: Engine, path: String, timestamp: Long): SnapshotImpl
}

/**
 * Legacy implementation using the [[Table.forPath]] API.
 */
class LegacyTableManagerAdapter extends AbstractTableManagerAdapter {
  override def supportsTimestampResolution: Boolean = true

  override def getSnapshotAtLatest(
      engine: Engine,
      path: String): SnapshotImpl = {
    Table.forPath(engine, path).asInstanceOf[TableImpl].getLatestSnapshot(engine)
  }

  override def getSnapshotAtVersion(
      engine: Engine,
      path: String,
      version: Long): SnapshotImpl = {
    Table.forPath(engine, path).asInstanceOf[TableImpl].getSnapshotAsOfVersion(engine, version)
  }

  override def getSnapshotAtTimestamp(
      engine: Engine,
      path: String,
      timestamp: Long): SnapshotImpl = {
    Table.forPath(engine, path).asInstanceOf[TableImpl].getSnapshotAsOfTimestamp(engine, timestamp)
  }
}

/**
 * New implementation using the [[TableManager.loadSnapshot]] API.
 */
class TableManagerAdapter extends AbstractTableManagerAdapter {
  override def supportsTimestampResolution: Boolean = false

  override def getSnapshotAtLatest(
      engine: Engine,
      path: String): SnapshotImpl = {
    TableManager.loadSnapshot(path).asInstanceOf[SnapshotBuilderImpl].build(engine)
  }

  override def getSnapshotAtVersion(
      engine: Engine,
      path: String,
      version: Long): SnapshotImpl = {
    TableManager
      .loadSnapshot(path).asInstanceOf[SnapshotBuilderImpl].atVersion(version).build(engine)
  }

  override def getSnapshotAtTimestamp(
      engine: Engine,
      path: String,
      timestamp: Long): SnapshotImpl = {
    throw new UnsupportedOperationException("not implemented")
  }
}
