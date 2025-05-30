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
import io.delta.kernel.internal.SnapshotImpl

import TestAdapterImplicits._

trait AbstractTestTableManager {
  def getTestResolvedTableAtLatest(engine: Engine, path: String): AbstractTestResolvedTable

  def getTestResolvedTableAtVersion(
      engine: Engine,
      path: String,
      version: Long): AbstractTestResolvedTable

  def getTestResolvedTableAtTimestamp(
      engine: Engine,
      path: String,
      timestamp: Long): AbstractTestResolvedTable
}

class LegacyTestTableManagerAdapter extends AbstractTestTableManager {
  override def getTestResolvedTableAtLatest(
      engine: Engine,
      path: String): AbstractTestResolvedTable = {
    Table.forPath(engine, path).getLatestSnapshot(engine).toTestAdapter
  }

  override def getTestResolvedTableAtVersion(
      engine: Engine,
      path: String,
      version: Long): AbstractTestResolvedTable = {
    Table.forPath(engine, path).getSnapshotAsOfVersion(engine, version).toTestAdapter
  }

  override def getTestResolvedTableAtTimestamp(
      engine: Engine,
      path: String,
      timestamp: Long): AbstractTestResolvedTable = {
    Table.forPath(engine, path).getSnapshotAsOfTimestamp(engine, timestamp).toTestAdapter
  }
}

class NewTestTableManagerAdapter extends AbstractTestTableManager {
  override def getTestResolvedTableAtLatest(
      engine: Engine,
      path: String): AbstractTestResolvedTable = {
    TableManager.loadTable(path).build(engine).toTestAdapter
  }

  override def getTestResolvedTableAtVersion(
      engine: Engine,
      path: String,
      version: Long): AbstractTestResolvedTable = {
    TableManager.loadTable(path).atVersion(version).build(engine).toTestAdapter
  }

  override def getTestResolvedTableAtTimestamp(
      engine: Engine,
      path: String,
      timestamp: Long): AbstractTestResolvedTable = {
    throw new UnsupportedOperationException("not implemented")
  }
}
