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

package io.delta.standalone.internal

import java.io.IOException
import java.sql.Timestamp
import java.util.TimeZone
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.delta.standalone.{DeltaLog, OptimisticTransaction, VersionLog}
import io.delta.standalone.actions.{CommitInfo => CommitInfoJ}

import io.delta.standalone.internal.actions.{Action, Metadata, Protocol}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.logging.Logging
import io.delta.standalone.internal.sources.StandaloneHadoopConf
import io.delta.standalone.internal.storage.LogStoreProvider
import io.delta.standalone.internal.util.{Clock, ConversionUtils, FileNames, SystemClock}

/**
 * Scala implementation of Java interface [[DeltaLog]].
 */
private[internal] class DeltaLogImpl private[internal](
    val hadoopConf: Configuration,
    val logPath: Path,
    val dataPath: Path,
    val clock: Clock)
  extends DeltaLog
  with Checkpoints
  with MetadataCleanup
  with LogStoreProvider
  with SnapshotManagement
  with Logging {

  /** Used to read and write physical log files and checkpoints. */
  lazy val store = createLogStore(hadoopConf)

  /** Direct access to the underlying storage system. */
  lazy val fs = logPath.getFileSystem(hadoopConf)

  // TODO: There is a race here where files could get dropped when increasing the
  // retention interval...
  protected def metadata = if (snapshot == null) Metadata() else snapshot.metadataScala

  /** How long to keep around logically deleted files before physically deleting them. */
  def tombstoneRetentionMillis: Long =
    DeltaConfigs.getMilliSeconds(DeltaConfigs.TOMBSTONE_RETENTION.fromMetadata(metadata))

  /**
   * Tombstones before this timestamp will be dropped from the state and the files can be
   * garbage collected.
   */
  def minFileRetentionTimestamp: Long = clock.getTimeMillis() - tombstoneRetentionMillis

  /** The unique identifier for this table. */
  def tableId: String = metadata.id

  /** Use ReentrantLock to allow us to call `lockInterruptibly`. */
  private val deltaLogLock = new ReentrantLock()

  /** Delta History Manager containing version and commit history. */
  protected lazy val history = DeltaHistoryManager(this)

  /** Returns the checkpoint interval for this log. Not transactional. */
  def checkpointInterval: Int = DeltaConfigs.CHECKPOINT_INTERVAL.fromMetadata(metadata)

  /** Convert the timeZoneId to an actual timeZone that can be used for decoding. */
  def timezone: TimeZone = {
    if (hadoopConf.get(StandaloneHadoopConf.PARQUET_DATA_TIME_ZONE_ID) == null) {
      TimeZone.getDefault
    } else {
      TimeZone.getTimeZone(hadoopConf.get(StandaloneHadoopConf.PARQUET_DATA_TIME_ZONE_ID))
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Public Java API Methods
  ///////////////////////////////////////////////////////////////////////////

  override def getPath: Path = dataPath

  override def getCommitInfoAt(version: Long): CommitInfoJ = {
    history.checkVersionExists(version)
    ConversionUtils.convertCommitInfo(history.getCommitInfo(version))
  }

  override def getChanges(
      startVersion: Long,
      failOnDataLoss: Boolean): java.util.Iterator[VersionLog] = {

    if (startVersion < 0) throw new IllegalArgumentException(s"Invalid startVersion: $startVersion")

    val deltaPaths = store.listFrom(FileNames.deltaFile(logPath, startVersion), hadoopConf)
      .asScala
      .filter(f => FileNames.isDeltaFile(f.getPath))

    // Subtract 1 to ensure that we have the same check for the inclusive startVersion
    var lastSeenVersion = startVersion - 1
    deltaPaths.map[VersionLog] { status =>
      val p = status.getPath
      val version = FileNames.deltaVersion(p)
      if (failOnDataLoss && version > lastSeenVersion + 1) {
        throw DeltaErrors.failOnDataLossException(lastSeenVersion + 1, version)
      }
      lastSeenVersion = version

      new MemoryOptimizedVersionLog(
        version,
        () => store.read(p, hadoopConf))
    }.asJava
  }

  override def getVersionBeforeOrAtTimestamp(timestamp: Long): Long = {
    if (!tableExists) return -1

    // Note: if the provided timestamp is earlier than any committed version, then
    // `getActiveCommitAtTime` will throw IllegalArgumentException (specifically,
    // `DeltaErrors.timestampEarlierThanTableFirstCommit`).
    history.getActiveCommitAtTime(
      new Timestamp(timestamp),
      // e.g. if we give time T+2 and last commit has time T, then we DO want that last commit
      canReturnLastCommit = true,
      mustBeRecreatable = false,
      // e.g. we give time T-1 and first commit has time T, then do NOT want that earliest commit
      canReturnEarliestCommit = false
    ).version
  }

  override def getVersionAtOrAfterTimestamp(timestamp: Long): Long = {
    if (!tableExists) return -1

    // Note: if the provided timestamp is later than any committed version, then
    // `getActiveCommitAtTime` will throw IllegalArgumentException (specifically,
    // `DeltaErrors.timestampLaterThanTableLastCommit`).
    val commit = history.getActiveCommitAtTime(
      new Timestamp(timestamp),
      // e.g. if we give time T+2 and last commit has time T, then we do NOT want that last commit
      canReturnLastCommit = false,
      mustBeRecreatable = false,
      // e.g. we give time T-1 and first commit has time T, then we DO want that earliest commit
      canReturnEarliestCommit = true
    )

    if (commit.timestamp >= timestamp) {
      commit.version
    } else {
      // this commit.timestamp is before the input timestamp. if this is the last commit, then the
      // input timestamp is after the last commit and `getActiveCommitAtTime` would have thrown
      // an IllegalArgumentException. So, clearly, this can't be the last commit, so we can safely
      // return commit.version + 1 as the version that is at or after the input timestamp.
      commit.version + 1
    }
  }

  override def startTransaction(): OptimisticTransaction = {
    update()
    new OptimisticTransactionImpl(this, snapshot)
  }

  /** Whether a Delta table exists at this directory. */
  override def tableExists: Boolean = snapshot.version >= 0

  ///////////////////////////////////////////////////////////////////////////
  // Internal Methods
  ///////////////////////////////////////////////////////////////////////////

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
   * allowed to read the table that is using the given `protocol`.
   */
  def assertProtocolRead(protocol: Protocol): Unit = {
    if (protocol != null && Action.readerVersion < protocol.minReaderVersion) {
      throw new DeltaErrors.InvalidProtocolVersionException(Action.protocolVersion, protocol)
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
    if (DeltaConfigs.IS_APPEND_ONLY.fromMetadata(metadata)) {
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

  def forTable(hadoopConf: Configuration, dataPath: String, clock: Clock): DeltaLogImpl = {
    apply(hadoopConf, new Path(dataPath, "_delta_log"), clock)
  }

  def forTable(hadoopConf: Configuration, dataPath: Path, clock: Clock): DeltaLogImpl = {
    apply(hadoopConf, new Path(dataPath, "_delta_log"), clock)
  }

  private def apply(
      hadoopConf: Configuration,
      rawPath: Path,
      clock: Clock = new SystemClock): DeltaLogImpl = {
    val fs = rawPath.getFileSystem(hadoopConf)
    val path = fs.makeQualified(rawPath)

    new DeltaLogImpl(hadoopConf, path, path.getParent, clock)
  }
}
