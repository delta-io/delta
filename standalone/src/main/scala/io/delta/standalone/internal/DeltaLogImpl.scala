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

package io.delta.standalone.internal

import java.io.File
import java.util.concurrent.locks.ReentrantLock

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import io.delta.standalone.DeltaLog
import io.delta.standalone.actions.{CommitInfo => CommitInfoJ}
import io.delta.standalone.internal.storage.HDFSReadOnlyLogStore
import io.delta.standalone.internal.util.ConversionUtils

/**
 * Scala implementation of Java interface [[DeltaLog]].
 */
private[internal] class DeltaLogImpl private(
    val hadoopConf: Configuration,
    val logPath: Path,
    val dataPath: Path)
  extends DeltaLog
  with Checkpoints
  with SnapshotManagement {

  /** Used to read (not write) physical log files and checkpoints. */
  lazy val store = new HDFSReadOnlyLogStore(hadoopConf)

  /** Use ReentrantLock to allow us to call `lockInterruptibly`. */
  private val deltaLogLock = new ReentrantLock()

  /** Delta History Manager containing version and commit history. */
  protected lazy val history = DeltaHistoryManager(this)

  override def getLogPath: Path = logPath

  override def getDataPath: Path = dataPath

  override def getCommitInfoAt(version: Long): CommitInfoJ = {
    history.checkVersionExists(version)
    ConversionUtils.convertCommitInfo(history.getCommitInfo(version))
  }

  /**
   * Run `body` inside `deltaLogLock` lock using `lockInterruptibly` so that the thread can be
   * interrupted when waiting for the lock.
   */
  protected def lockInterruptibly[T](body: => T): T = {
    deltaLogLock.lockInterruptibly()
    try {
      body
    } finally {
      deltaLogLock.unlock()
    }
  }
}

private[standalone] object DeltaLogImpl {
  def forTable(hadoopConf: Configuration, dataPath: String): DeltaLogImpl = {
    apply(hadoopConf, new Path(dataPath, "_delta_log"))
  }

  def forTable(hadoopConf: Configuration, dataPath: File): DeltaLogImpl = {
    apply(hadoopConf, new Path(dataPath.getAbsolutePath, "_delta_log"))
  }

  def forTable(hadoopConf: Configuration, dataPath: Path): DeltaLogImpl = {
    apply(hadoopConf, new Path(dataPath, "_delta_log"))
  }

  def apply(hadoopConf: Configuration, rawPath: Path): DeltaLogImpl = {
    val fs = rawPath.getFileSystem(hadoopConf)
    val path = fs.makeQualified(rawPath)

    new DeltaLogImpl(hadoopConf, path, path.getParent)
  }
}
