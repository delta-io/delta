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

trait TableManagerAdapter {
  def getResolvedTableAdapterAtLatest(engine: Engine, path: String): ResolvedTableAdapter

  def getResolvedTableAdapterAtVersion(
      engine: Engine,
      path: String,
      version: Long): ResolvedTableAdapter

  def getResolvedTableAdapterAtTimestamp(
      engine: Engine,
      path: String,
      timestamp: Long): ResolvedTableAdapter
}

class LegacyTableManagerAdapter extends TableManagerAdapter {
  override def getResolvedTableAdapterAtLatest(
      engine: Engine,
      path: String): ResolvedTableAdapter = {
    Table.forPath(engine, path).getLatestSnapshot(engine).toTestAdapter
  }

  override def getResolvedTableAdapterAtVersion(
      engine: Engine,
      path: String,
      version: Long): ResolvedTableAdapter = {
    Table.forPath(engine, path).getSnapshotAsOfVersion(engine, version).toTestAdapter
  }

  override def getResolvedTableAdapterAtTimestamp(
      engine: Engine,
      path: String,
      timestamp: Long): ResolvedTableAdapter = {
    Table.forPath(engine, path).getSnapshotAsOfTimestamp(engine, timestamp).toTestAdapter
  }
}

class NewTableManagerAdapter extends TableManagerAdapter {
  override def getResolvedTableAdapterAtLatest(
      engine: Engine,
      path: String): ResolvedTableAdapter = {
    TableManager.loadTable(path).build(engine).toTestAdapter
  }

  override def getResolvedTableAdapterAtVersion(
      engine: Engine,
      path: String,
      version: Long): ResolvedTableAdapter = {
    TableManager.loadTable(path).atVersion(version).build(engine).toTestAdapter
  }

  override def getResolvedTableAdapterAtTimestamp(
      engine: Engine,
      path: String,
      timestamp: Long): ResolvedTableAdapter = {
    throw new UnsupportedOperationException("not implemented")
  }
}
