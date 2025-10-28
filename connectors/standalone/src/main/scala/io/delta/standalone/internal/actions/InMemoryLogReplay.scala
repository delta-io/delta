/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.actions

import java.net.URI

import org.apache.hadoop.conf.Configuration

import io.delta.standalone.internal.SnapshotImpl.canonicalizePath

/**
 * Replays a history of action, resolving them to produce the current state
 * of the table. The protocol for resolution is as follows:
 *  - The most recent [[AddFile]] and accompanying metadata for any `path` wins.
 *  - [[RemoveFile]] deletes a corresponding [[AddFile]] and is retained as a
 *    tombstone until `minFileRetentionTimestamp` has passed.
 *  - The most recent [[Metadata]] wins.
 *  - The most recent [[Protocol]] version wins.
 *  - For each path, this class should always output only one [[FileAction]] (either [[AddFile]] or
 *    [[RemoveFile]])
 *
 * This class is not thread safe.
 */
private[internal] class InMemoryLogReplay(
     hadoopConf: Configuration,
     minFileRetentionTimestamp: Long,
     minSetTransactionRetentionTimestamp: Option[Long]) {
  var currentProtocolVersion: Protocol = null
  var currentVersion: Long = -1
  var currentMetaData: Metadata = null
  var sizeInBytes: Long = 0
  var numMetadata: Long = 0
  var numProtocol: Long = 0
  private val transactions = new scala.collection.mutable.HashMap[String, SetTransaction]()
  private val activeFiles = new scala.collection.mutable.HashMap[URI, AddFile]()
  private val tombstones = new scala.collection.mutable.HashMap[URI, RemoveFile]()

  def append(version: Long, actions: Iterator[Action]): Unit = {
    assert(currentVersion == -1 || version == currentVersion + 1,
      s"Attempted to replay version $version, but state is at $currentVersion")
    currentVersion = version
    actions.foreach {
      case a: SetTransaction =>
        transactions(a.appId) = a
      case a: Metadata =>
        currentMetaData = a
        numMetadata += 1
      case a: Protocol =>
        currentProtocolVersion = a
        numProtocol += 1
      case add: AddFile =>
        val canonicalizeAdd = add.copy(
          dataChange = false,
          path = canonicalizePath(add.path, hadoopConf))
        activeFiles(canonicalizeAdd.pathAsUri) = canonicalizeAdd
        // Remove the tombstone to make sure we only output one `FileAction`.
        tombstones.remove(canonicalizeAdd.pathAsUri)
        sizeInBytes += canonicalizeAdd.size
      case remove: RemoveFile =>
        val canonicaleRemove = remove.copy(
          dataChange = false,
          path = canonicalizePath(remove.path, hadoopConf))
        val removedFile = activeFiles.remove(canonicaleRemove.pathAsUri)
        tombstones(canonicaleRemove.pathAsUri) = canonicaleRemove

        if (removedFile.isDefined) {
          sizeInBytes -= removedFile.get.size
        }
      case _ => // do nothing
    }
  }

  def getSetTransactions: Seq[SetTransaction] = {
    if (minSetTransactionRetentionTimestamp.isEmpty) {
      transactions.values.toSeq
    } else {
      transactions.values.filter { txn =>
        txn.lastUpdated.exists(_ > minSetTransactionRetentionTimestamp.get)
      }.toSeq
    }
  }

  def getActiveFiles: Iterable[AddFile] = activeFiles.values

  def getTombstones: Iterable[RemoveFile] = {
    tombstones.values.filter(_.delTimestamp > minFileRetentionTimestamp)
  }

}
