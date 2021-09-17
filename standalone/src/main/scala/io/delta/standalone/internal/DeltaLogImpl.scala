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

import java.io.IOException
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import io.delta.standalone.{DeltaLog, OptimisticTransaction, VersionLog}
import io.delta.standalone.actions.{CommitInfo => CommitInfoJ}
import io.delta.standalone.expressions.{And, Expression, Literal}
import io.delta.standalone.internal.actions.{Action, AddFile, Metadata, Protocol}
import io.delta.standalone.internal.data.PartitionRowRecord
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.storage.HDFSReadOnlyLogStore
import io.delta.standalone.internal.util.{ConversionUtils, FileNames}
import io.delta.standalone.types.StructType

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

  /** Direct access to the underlying storage system. */
  private lazy val fs = logPath.getFileSystem(hadoopConf)

  // TODO: There is a race here where files could get dropped when increasing the
  // retention interval...
  private def metadata = if (snapshot == null) Metadata() else snapshot.metadataScala

  /** Use ReentrantLock to allow us to call `lockInterruptibly`. */
  private val deltaLogLock = new ReentrantLock()

  /** Delta History Manager containing version and commit history. */
  protected lazy val history = DeltaHistoryManager(this)

  override def getPath: Path = dataPath

  override def getCommitInfoAt(version: Long): CommitInfoJ = {
    history.checkVersionExists(version)
    ConversionUtils.convertCommitInfo(history.getCommitInfo(version))
  }

  override def getChanges(
      startVersion: Long,
      failOnDataLoss: Boolean): java.util.Iterator[VersionLog] = {
    if (startVersion < 0) throw new IllegalArgumentException(s"Invalid startVersion: $startVersion")

    val deltaPaths = store.listFrom(FileNames.deltaFile(logPath, startVersion))
      .filter(f => FileNames.isDeltaFile(f.getPath))

    // Subtract 1 to ensure that we have the same check for the inclusive startVersion
    var lastSeenVersion = startVersion - 1
    deltaPaths.map { status =>
      val p = status.getPath
      val version = FileNames.deltaVersion(p)
      if (failOnDataLoss && version > lastSeenVersion + 1) {
        throw DeltaErrors.failOnDataLossException(lastSeenVersion + 1, version)
      }
      lastSeenVersion = version

      new VersionLog(version,
        store.read(p).map(x => ConversionUtils.convertAction(Action.fromJson(x))).toList.asJava)
    }.asJava
  }

  override def startTransaction(): OptimisticTransaction = {
    update()
    new OptimisticTransactionImpl(this, snapshot)
  }

  /**
   * Run `body` inside `deltaLogLock` lock using `lockInterruptibly` so that the thread can be
   * interrupted when waiting for the lock.
   */
  def lockInterruptibly[T](body: => T): T = {
    deltaLogLock.lockInterruptibly()
    try {
      body
    } finally {
      deltaLogLock.unlock()
    }
  }

  /** Creates the log directory if it does not exist. */
  def ensureLogDirectoryExist(): Unit = {
    if (!fs.exists(logPath)) {
      if (!fs.mkdirs(logPath)) {
        throw new IOException(s"Cannot create $logPath")
      }
    }
  }

  /**
   * Asserts that the client is up to date with the protocol and
   * allowed to write to the table that is using the given `protocol`.
   */
  def assertProtocolWrite(protocol: Protocol): Unit = {
    if (protocol != null && Action.writerVersion < protocol.minWriterVersion) {
      throw new DeltaErrors.InvalidProtocolVersionException(Action.protocolVersion, protocol)
    }
  }

  /**
   * Checks whether this table only accepts appends. If so it will throw an error in operations that
   * can remove data such as DELETE/UPDATE/MERGE.
   */
  def assertRemovable(): Unit = {
    if (metadata.configuration.getOrElse("appendOnly", "false").toBoolean) {
      throw DeltaErrors.modifyAppendOnlyTableException
    }
  }
}

private[standalone] object DeltaLogImpl {
  def forTable(hadoopConf: Configuration, dataPath: String): DeltaLogImpl = {
    apply(hadoopConf, new Path(dataPath, "_delta_log"))
  }

  def forTable(hadoopConf: Configuration, dataPath: Path): DeltaLogImpl = {
    apply(hadoopConf, new Path(dataPath, "_delta_log"))
  }

  def apply(hadoopConf: Configuration, rawPath: Path): DeltaLogImpl = {
    val fs = rawPath.getFileSystem(hadoopConf)
    val path = fs.makeQualified(rawPath)

    new DeltaLogImpl(hadoopConf, path, path.getParent)
  }

  /**
   * Filters the given [[AddFile]]s by the given `partitionFilters`, returning those that match.
   * @param files The active files in the DeltaLog state, which contains the partition value
   *              information
   * @param partitionFilters Filters on the partition columns
   */
  def filterFileList(
      partitionSchema: StructType,
      files: Seq[AddFile],
      partitionFilters: Seq[Expression]): Seq[AddFile] = {
    val expr = partitionFilters.reduceLeftOption(new And(_, _)).getOrElse(Literal.True)
    // TODO: compressedExpr = ...

    files.filter { addFile =>
      val partitionRowRecord = new PartitionRowRecord(partitionSchema, addFile.partitionValues)
      val result = expr.eval(partitionRowRecord)
      result == true
    }
  }
}
