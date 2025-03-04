/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.coordinatedcommits

import java.lang.{Long => JLong}
import java.net.URI
import java.util.Optional

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import io.delta.storage.commit.{Commit => JCommit, GetCommitsResponse => JGetCommitsResponse}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uccommitcoordinator.UCClient

/**
 * An in-memory implementation of [[UCClient]] for testing purposes.
 * This implementation simulates Unity Catalog operations without actually connecting to a remote
 * service. It maintains all state in memory in [[InMemoryUCCommitCoordinator]]
 *
 * This class provides a lightweight way to test Delta table operations that would
 * normally require interaction with the Unity Catalog.
 *
 * Example usage:
 * {{{
 * val metastoreId = "test-metastore"
 * val ucCommitCoordinator = new InMemoryUCCommitCoordinator()
 * val client = new InMemoryUCClient(metastoreId, ucCommitCoordinator)
 *
 * // Use the client for testing
 * val getCommitsResponse = client.getCommits(
 *     "tableId",
 *     new URI("tableUri"),
 *     Optional.empty(),
 *     Optional.empty())
 * }}}
 *
 * @param metastoreId The identifier for the simulated metastore
 * @param ucCommitCoordinator The in-memory coordinator to handle commit operations
 */
class InMemoryUCClient(
    metastoreId: String,
    ucCommitCoordinator: InMemoryUCCommitCoordinator) extends UCClient {

  override def getMetastoreId: String = metastoreId

  override def commit(
      tableId: String,
      tableUri: URI,
      commit: Optional[JCommit],
      lastKnownBackfilledVersion: Optional[JLong],
      disown: Boolean,
      newMetadata: Optional[AbstractMetadata],
      newProtocol: Optional[AbstractProtocol]): Unit = {
    ucCommitCoordinator.commitToCoordinator(
      tableId,
      tableUri,
      Option(commit.orElse(null)).map(_.getFileStatus.getPath.getName),
      Option(commit.orElse(null)).map(_.getVersion),
      Option(commit.orElse(null)).map(_.getFileStatus.getLen),
      Option(commit.orElse(null)).map(_.getFileStatus.getModificationTime),
      Option(commit.orElse(null)).map(_.getCommitTimestamp),
      Option(lastKnownBackfilledVersion.orElse(null)).map(_.toLong),
      disown,
      Option(newProtocol.orElse(null)).map(_.asInstanceOf[Protocol]),
      Option(newMetadata.orElse(null)).map(_.asInstanceOf[Metadata]))
  }

  override def getCommits(
      tableId: String,
      tableUri: URI,
      startVersion: Optional[JLong],
      endVersion: Optional[JLong]): JGetCommitsResponse = {
    ucCommitCoordinator.getCommitsFromCoordinator(
      tableId,
      tableUri,
      Option(startVersion.orElse(null)).map(_.toLong),
      Option(endVersion.orElse(null)).map(_.toLong))
  }

  override def close(): Unit = {}
}
