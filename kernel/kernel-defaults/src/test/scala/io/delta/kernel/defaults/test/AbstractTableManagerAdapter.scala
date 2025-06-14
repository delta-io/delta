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
import io.delta.kernel.defaults.test.ResolvedTableAdapterImplicits._
import io.delta.kernel.engine.Engine

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
  def getResolvedTableAdapterAtLatest(engine: Engine, path: String): AbstractResolvedTableAdapter

  def getResolvedTableAdapterAtVersion(
      engine: Engine,
      path: String,
      version: Long): AbstractResolvedTableAdapter

  def getResolvedTableAdapterAtTimestamp(
      engine: Engine,
      path: String,
      timestamp: Long): AbstractResolvedTableAdapter
}

/**
 * Legacy implementation using the [[Table.forPath]] API which then wraps a [[Snapshot]].
 */
class LegacyTableManagerAdapter extends AbstractTableManagerAdapter {
  override def getResolvedTableAdapterAtLatest(
      engine: Engine,
      path: String): AbstractResolvedTableAdapter = {
    Table.forPath(engine, path).getLatestSnapshot(engine).toTestAdapter
  }

  override def getResolvedTableAdapterAtVersion(
      engine: Engine,
      path: String,
      version: Long): AbstractResolvedTableAdapter = {
    Table.forPath(engine, path).getSnapshotAsOfVersion(engine, version).toTestAdapter
  }

  override def getResolvedTableAdapterAtTimestamp(
      engine: Engine,
      path: String,
      timestamp: Long): AbstractResolvedTableAdapter = {
    Table.forPath(engine, path).getSnapshotAsOfTimestamp(engine, timestamp).toTestAdapter
  }
}

/**
 * Current implementation using the [[TableManager.loadTable]] API, which then wraps a
 * [[ResolvedTable]].
 */
class TableManagerAdapter extends AbstractTableManagerAdapter {
  override def getResolvedTableAdapterAtLatest(
      engine: Engine,
      path: String): AbstractResolvedTableAdapter = {
    TableManager.loadTable(path).build(engine).toTestAdapter
  }

  override def getResolvedTableAdapterAtVersion(
      engine: Engine,
      path: String,
      version: Long): AbstractResolvedTableAdapter = {
    TableManager.loadTable(path).atVersion(version).build(engine).toTestAdapter
  }

  override def getResolvedTableAdapterAtTimestamp(
      engine: Engine,
      path: String,
      timestamp: Long): AbstractResolvedTableAdapter = {
    throw new UnsupportedOperationException("not implemented")
  }
}
