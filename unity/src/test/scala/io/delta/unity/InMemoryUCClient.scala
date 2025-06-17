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

package io.delta.unity

import java.lang.{Long => JLong}
import java.net.URI
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.storage.commit.{Commit, CommitFailedException, GetCommitsResponse}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uccommitcoordinator.{InvalidTargetTableException, UCClient}

object InMemoryUCClient {
  class TableData {
    private var maxRatifiedVersion = -1L
    private val commits: ArrayBuffer[Commit] = ArrayBuffer.empty

    def getMaxRatifiedVersion: Long = maxRatifiedVersion

    def getCommits: List[Commit] = commits.toList

    def appendCommit(commit: Commit): Unit = {
      val expectedCommitVersion = maxRatifiedVersion + 1

      if (commit.getVersion != expectedCommitVersion) {
        throw new CommitFailedException(
          false, /* retryable */
          false, /* conflict */
          s"Expected commit version $expectedCommitVersion but got ${commit.getVersion}")
      }

      commits += commit
      maxRatifiedVersion += 1
    }
  }
}

class InMemoryUCClient(ucMetastoreId: String) extends UCClient {

  import InMemoryUCClient._

  /** Map from UC_TABLE_ID to TABLE_DATA */
  val tables = new ConcurrentHashMap[String, TableData]()

  override def getMetastoreId: String = ucMetastoreId

  override def commit(
      tableId: String,
      tableUri: URI,
      commit: Optional[Commit],
      lastKnownBackfilledVersion: Optional[JLong],
      disown: Boolean,
      newMetadata: Optional[AbstractMetadata],
      newProtocol: Optional[AbstractProtocol]): Unit = {
    throw new UnsupportedOperationException("Not implemented yet")
  }

  override def getCommits(
      tableId: String,
      tableUri: URI,
      startVersion: Optional[JLong],
      endVersion: Optional[JLong]): GetCommitsResponse = {
    val tableData = getTableDataElseThrow(tableId)

    val commits = tableData
      .getCommits
      .filter { x =>
        startVersion.orElse(0) <= x.getVersion &&
        x.getVersion <= endVersion.orElse(Long.MaxValue)
      }

    new GetCommitsResponse(commits.asJava, tableData.getMaxRatifiedVersion)
  }

  override def close(): Unit = {}

  private def getTableDataElseThrow(tableId: String): TableData = {
    if (!tables.containsKey(tableId)) {
      throw new InvalidTargetTableException(s"Table not found: $tableId")
    }

    tables.get(tableId)
  }
}
