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

  /**
   * Internal data structure to track table state including commits and version information.
   *
   * Thread Safety: All public methods are synchronized to ensure thread-safe access to the
   * internal mutable state. This class is designed to be safely accessed by multiple threads
   * concurrently.
   */
  class TableData {
    private var maxRatifiedVersion = -1L
    private val commits: ArrayBuffer[Commit] = ArrayBuffer.empty

    /** @return the maximum ratified version, or -1 if no commits have been made. */
    def getMaxRatifiedVersion: Long = synchronized {
      maxRatifiedVersion
    }

    /** @return An immutable list of all commits. */
    def getCommits: List[Commit] = synchronized { commits.toList }

    /** @return commits filtered by version range. */
    def getCommitsInRange(
        startVersion: Optional[JLong],
        endVersion: Optional[JLong]): List[Commit] = synchronized {
      commits
        .filter { commit =>
          startVersion.orElse(0L) <= commit.getVersion &&
          commit.getVersion <= endVersion.orElse(Long.MaxValue)
        }
        .toList
    }

    /** Appends a new commit to this table. */
    def appendCommit(commit: Commit): Unit = synchronized {
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

/**
 * In-memory Unity Catalog client implementation for testing.
 *
 * Provides a mock implementation of UCClient that stores all table data in memory. This is useful
 * for unit tests that need to simulate Unity Catalog operations without connecting to an actual UC
 * service.
 *
 * Thread Safety: This implementation is thread-safe for concurrent access. Multiple threads can
 * safely perform operations on different tables simultaneously. Operations on the same table are
 * internally synchronized by the [[TableData]] class.
 */
class InMemoryUCClient(ucMetastoreId: String) extends UCClient {

  import InMemoryUCClient._

  /** Map from UC_TABLE_ID to TABLE_DATA */
  private val tables = new ConcurrentHashMap[String, TableData]()

  override def getMetastoreId: String = ucMetastoreId

  /** Convenience method for tests to commit with default parameters. */
  def commitWithDefaults(
      tableId: String,
      tableUri: URI,
      commit: Optional[Commit],
      lastKnownBackfilledVersion: Optional[JLong] = Optional.empty(),
      disown: Boolean = false,
      newMetadata: Optional[AbstractMetadata] = Optional.empty(),
      newProtocol: Optional[AbstractProtocol] = Optional.empty()): Unit = {
    this.commit(
      tableId,
      tableUri,
      commit,
      lastKnownBackfilledVersion,
      disown,
      newMetadata,
      newProtocol)
  }

  override def commit(
      tableId: String,
      tableUri: URI,
      commit: Optional[Commit] = Optional.empty(),
      lastKnownBackfilledVersion: Optional[JLong],
      disown: Boolean,
      newMetadata: Optional[AbstractMetadata],
      newProtocol: Optional[AbstractProtocol]): Unit = {
    Seq(
      (lastKnownBackfilledVersion.isPresent, "lastKnownBackfilledVersion"),
      (disown, "disown"),
      (newMetadata.isPresent, "newMetadata"),
      (newProtocol.isPresent, "newProtocol")).foreach { case (isUnsupported, name) =>
      if (isUnsupported) {
        throw new UnsupportedOperationException(s"$name not supported yet in InMemoryUCClient")
      }
    }

    if (!commit.isPresent) return

    getOrCreateTableIfNotExists(tableId).appendCommit(commit.get())
  }

  override def getCommits(
      tableId: String,
      tableUri: URI,
      startVersion: Optional[JLong],
      endVersion: Optional[JLong]): GetCommitsResponse = {
    val tableData = getTableDataElseThrow(tableId)
    val filteredCommits = tableData.getCommitsInRange(startVersion, endVersion)
    new GetCommitsResponse(filteredCommits.asJava, tableData.getMaxRatifiedVersion)
  }

  override def close(): Unit = {}

  private[unity] def getTablesCopy: Map[String, TableData] = {
    tables.asScala.toMap
  }

  /** Retrieves the table data for the given table ID, creating it if it does not exist. */
  private def getOrCreateTableIfNotExists(tableId: String): TableData = {
    tables.computeIfAbsent(tableId, _ => new TableData)
  }

  /** Retrieves table data for the given table ID or throws an exception if not found. */
  private def getTableDataElseThrow(tableId: String): TableData = {
    Option(tables.get(tableId))
      .getOrElse(throw new InvalidTargetTableException(s"Table not found: $tableId"))
  }
}
