/*
 * Copyright (2020) The Delta Lake Project Authors.
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
 * Replays a history of action, resolving them to produce the current state
 * of the table. The protocol for resolution is as follows:
 *  - The most recent [[AddFile]] and accompanying metadata for any `path` wins.
 *  - [[RemoveFile]] deletes a corresponding [[AddFile]] and is retained as a
 *    tombstone until `minFileRetentionTimestamp` has passed.
 *  - The most recent version for any `appId` in a [[SetTransaction]] wins.
 *  - The most recent [[Metadata]] wins.
 *  - The most recent [[Protocol]] version wins.
 *  - For each path, this class should always output only one [[FileAction]] (either [[AddFile]] or
 *    [[RemoveFile]])
 *
 * This class is not thread safe.
 */
class InMemoryLogReplay(minFileRetentionTimestamp: Long) {
  var currentProtocolVersion: Protocol = null
  var currentVersion: Long = -1
  var currentMetaData: Metadata = null
  val transactions = new scala.collection.mutable.HashMap[String, SetTransaction]()
  val activeFiles = new scala.collection.mutable.HashMap[URI, AddFile]()
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
      case a: Protocol =>
        currentProtocolVersion = a
      case add: AddFile =>
        activeFiles(add.pathAsUri) = add.copy(dataChange = false)
        // Remove the tombstone to make sure we only output one `FileAction`.
        tombstones.remove(add.pathAsUri)
      case remove: RemoveFile =>
        activeFiles.remove(remove.pathAsUri)
        tombstones(remove.pathAsUri) = remove.copy(dataChange = false)
      case ci: CommitInfo => // do nothing
      case cdc: AddCDCFile => // do nothing
      case null => // Some crazy future feature. Ignore
    }
  }

  private def getTombstones: Iterable[FileAction] = {
    tombstones.values.filter(_.delTimestamp > minFileRetentionTimestamp)
  }

  /** Returns the current state of the Table as an iterator of actions. */
  def checkpoint: Iterator[Action] = {
    Option(currentProtocolVersion).toIterator ++
    Option(currentMetaData).toIterator ++
    transactions.values.toIterator ++
    (activeFiles.values ++ getTombstones).toSeq.sortBy(_.path).iterator
  }
}
