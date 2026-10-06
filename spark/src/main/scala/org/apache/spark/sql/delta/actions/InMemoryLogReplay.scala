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

import org.apache.spark.sql.delta.actions.FileAction.UniqueFileActionTuple
import org.apache.hadoop.fs.Path


/**
 * Replays a history of actions, resolving them to produce the current state
 * of the table. The protocol for resolution is as follows:
 *  - The most recent [[AddFile]] and accompanying metadata for any `(path, dv id)` tuple wins.
 *  - [[RemoveFile]] deletes a corresponding [[AddFile]] and is retained as a
 *    tombstone until `minFileRetentionTimestamp` has passed. If `minFileRetentionTimestamp` is
 *    None, all [[RemoveFile]] actions are retained.
 *    A [[RemoveFile]] "corresponds" to the [[AddFile]] that matches both the parquet file URI
 *    *and* the deletion vector id (if any).
 *  - The most recent version for any `appId` in a [[SetTransaction]] wins.
 *  - The most recent [[Metadata]] wins.
 *  - The most recent [[Protocol]] version wins.
 *  - For each `(path, dv id)` tuple, this class should always output only one [[FileAction]]
 *    (either [[AddFile]] or [[RemoveFile]])
 *
 * This class is not thread safe.
 *
 * @param retainFirstBackreference Whether to retain the first non-empty back reference for each
 *                                 UniqueFileActionTuple or let the last action win.
 *
 */
class InMemoryLogReplay(
    minFileRetentionTimestamp: Option[Long],
    minSetTransactionRetentionTimestamp: Option[Long],
    tableRoot: Path,
    useDeletionVectorObjectIdentity: Boolean,
    retainFirstBackreference: Boolean = false) extends LogReplay {

  private var currentProtocolVersion: Protocol = null
  private var currentVersion: Long = -1
  private var currentMetaData: Metadata = null
  private val transactions = new scala.collection.mutable.HashMap[String, SetTransaction]()
  private val domainMetadatas = collection.mutable.Map.empty[String, DomainMetadata]
  private val activeFiles = new scala.collection.mutable.HashMap[UniqueFileActionTuple, AddFile]()
  // RemoveFiles that had cancelled AddFile during replay
  private val cancelledRemoveFiles =
    new scala.collection.mutable.HashMap[UniqueFileActionTuple, RemoveFile]()
  // RemoveFiles that had NOT cancelled any AddFile during replay
  private val activeRemoveFiles =
    new scala.collection.mutable.HashMap[UniqueFileActionTuple, RemoveFile]()
  // The first seen non-empty AMT BackReference for each unique file action.
  private lazy val firstBackReferences =
    new scala.collection.mutable.HashMap[UniqueFileActionTuple, BackReference]()

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
      case _: CheckpointOnlyAction => // Ignore this while doing LogReplay
      case a: Metadata =>
        currentMetaData = a
      case a: Protocol =>
        currentProtocolVersion = a
      case add: AddFile =>
        val uniquePath = add.toUniqueFileActionTuple(tableRoot, useDeletionVectorObjectIdentity)
        activeFiles(uniquePath) = add.copy(dataChange = false)
        // Remove the tombstone to make sure we only output one `FileAction`.
        cancelledRemoveFiles.remove(uniquePath)
        // Remove from activeRemoveFiles to handle commits that add a previously-removed file
        activeRemoveFiles.remove(uniquePath)
        if (retainFirstBackreference) {
          add.backReference.foreach(br => firstBackReferences.getOrElseUpdate(uniquePath, br))
        }
      case remove: RemoveFile =>
        val uniquePath =
          remove.toUniqueFileActionTuple(tableRoot, useDeletionVectorObjectIdentity)
        activeFiles.remove(uniquePath) match {
          case Some(_) => cancelledRemoveFiles(uniquePath) = remove
          case None => activeRemoveFiles(uniquePath) = remove
        }
        if (retainFirstBackreference) {
          remove.backReference.foreach(br => firstBackReferences.getOrElseUpdate(uniquePath, br))
        }
      case _: CommitInfo => // do nothing
      case _: AddCDCFile => // do nothing
      case _: Checkpoint => // AMT pointer; the manifest tree is loaded separately.
      case null => // Some crazy future feature. Ignore
    }
  }

  private def getLiveFiles: Iterable[AddFile] = {
    if (retainFirstBackreference) {
      activeFiles.map { case (uniquePath, add) =>
        firstBackReferences.get(uniquePath)
          .map(preservedBackReference => add.copy(backReference = Some(preservedBackReference)))
          .getOrElse(add)
      }
    } else {
      activeFiles.values
    }
  }

  private def getTombstones: Iterable[FileAction] = {
    val allRemovedFiles = cancelledRemoveFiles.toSeq ++ activeRemoveFiles.toSeq
    val filteredRemovedFiles = minFileRetentionTimestamp match {
      case None => allRemovedFiles
      case Some(timestamp) => allRemovedFiles.filter(_._2.delTimestamp > timestamp)
    }
    filteredRemovedFiles.map { case (uniquePath, remove) =>
      if (retainFirstBackreference) {
        remove.copy(dataChange = false, backReference = firstBackReferences.get(uniquePath))
      } else {
        remove.copy(dataChange = false)
      }
    }
  }

  private[delta] def getTransactions: Iterable[SetTransaction] = {
    minSetTransactionRetentionTimestamp match {
      case None => transactions.values
      case Some(timestamp) =>
        transactions.values.filter { txn => txn.lastUpdated.exists(_ > timestamp) }
    }
  }

  private[delta] def getDomainMetadatas: Iterable[DomainMetadata] = domainMetadatas.values

  /**
   * Returns the most recent [[Protocol]] seen during replay, or None if no Protocol action was
   * seen during the replay.
   */
  private[delta] def getProtocol: Option[Protocol] = Option(currentProtocolVersion)
  /**
   * Returns the most recent [[Metadata]] seen during replay, or None if no Metadata action was
   * seen during the replay.
   */
  private[delta] def getMetadata: Option[Metadata] = Option(currentMetaData)

  /** Returns the current state of the Table as an iterator of actions. */
  override def checkpoint: Iterator[Action] = {
    val fileActions = (getLiveFiles ++ getTombstones).toSeq.sortBy(_.path)

    Option(currentProtocolVersion).toIterator ++
    Option(currentMetaData).toIterator ++
    getDomainMetadatas ++
    getTransactions ++
    fileActions.toIterator
  }

  /** Returns all [[AddFile]] actions after the Log Replay */
  private[delta] def allFiles: Seq[AddFile] = activeFiles.values.toSeq
}
