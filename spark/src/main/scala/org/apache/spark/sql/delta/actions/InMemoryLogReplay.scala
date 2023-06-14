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

package org.apache.spark.sql.delta.actions

import java.net.URI

/**
 * Replays a history of actions, resolving them to produce the current state
 * of the table. The protocol for resolution is as follows:
 *  - The most recent [[AddFile]] and accompanying metadata for any `(path, dv id)` tuple wins.
 *  - [[RemoveFile]] deletes a corresponding [[AddFile]] and is retained as a
 *    tombstone until `minFileRetentionTimestamp` has passed.
 *    A [[RemoveFile]] "corresponds" to the [[AddFile]] that matches both the parquet file URI
 *    *and* the deletion vector's URI (if any).
 *  - The most recent version for any `appId` in a [[SetTransaction]] wins.
 *  - The most recent [[Metadata]] wins.
 *  - The most recent [[Protocol]] version wins.
 *  - For each `(path, dv id)` tuple, this class should always output only one [[FileAction]]
 *    (either [[AddFile]] or [[RemoveFile]])
 *
 * This class is not thread safe.
 */
class InMemoryLogReplay(
    minFileRetentionTimestamp: Long,
    minSetTransactionRetentionTimestamp: Option[Long]) extends LogReplay {

  import InMemoryLogReplay._

  private var currentProtocolVersion: Protocol = null
  private var currentVersion: Long = -1
  private var currentMetaData: Metadata = null
  private val transactions = new scala.collection.mutable.HashMap[String, SetTransaction]()
  private val domainMetadatas = collection.mutable.Map.empty[String, DomainMetadata]
  private val activeFiles = new scala.collection.mutable.HashMap[UniqueFileActionTuple, AddFile]()
  private val tombstones = new scala.collection.mutable.HashMap[UniqueFileActionTuple, RemoveFile]()

  override def append(version: Long, actions: Iterator[Action]): Unit = {
    assert(currentVersion == -1 || version == currentVersion + 1,
      s"Attempted to replay version $version, but state is at $currentVersion")
    currentVersion = version
    actions.foreach {
      case a: SetTransaction =>
        transactions(a.appId) = a
      case a: DomainMetadata if a.removed =>
        domainMetadatas.remove(a.domain)
      case a: DomainMetadata if !a.removed =>
        domainMetadatas(a.domain) = a
      case a: Metadata =>
        currentMetaData = a
      case a: Protocol =>
        currentProtocolVersion = a
      case add: AddFile =>
        val uniquePath = UniqueFileActionTuple(add.pathAsUri, add.getDeletionVectorUniqueId)
        activeFiles(uniquePath) = add.copy(dataChange = false)
        // Remove the tombstone to make sure we only output one `FileAction`.
        tombstones.remove(uniquePath)
      case remove: RemoveFile =>
        val uniquePath = UniqueFileActionTuple(remove.pathAsUri, remove.getDeletionVectorUniqueId)
        activeFiles.remove(uniquePath)
        tombstones(uniquePath) = remove.copy(dataChange = false)
      case _: CommitInfo => // do nothing
      case _: AddCDCFile => // do nothing
      case null => // Some crazy future feature. Ignore
    }
  }

  private def getTombstones: Iterable[FileAction] = {
    tombstones.values.filter(_.delTimestamp > minFileRetentionTimestamp)
  }

  private[delta] def getTransactions: Iterable[SetTransaction] = {
    if (minSetTransactionRetentionTimestamp.isEmpty) {
      transactions.values
    } else {
      transactions.values.filter { txn =>
        txn.lastUpdated.exists(_ > minSetTransactionRetentionTimestamp.get)
      }
    }
  }

  private[delta] def getDomainMetadatas: Iterable[DomainMetadata] = domainMetadatas.values

  /** Returns the current state of the Table as an iterator of actions. */
  override def checkpoint: Iterator[Action] = {
    Option(currentProtocolVersion).toIterator ++
    Option(currentMetaData).toIterator ++
    getDomainMetadatas ++
    getTransactions ++
    (activeFiles.values ++ getTombstones).toSeq.sortBy(_.path).iterator
  }

  /** Returns all [[AddFile]] actions after the Log Replay */
  private[delta] def allFiles: Seq[AddFile] = activeFiles.values.toSeq
}

object InMemoryLogReplay{
  /** The unit of path uniqueness in delta log actions is the tuple `(parquet file, dv)`. */
  final case class UniqueFileActionTuple(fileURI: URI, deletionVectorURI: Option[String])
}
