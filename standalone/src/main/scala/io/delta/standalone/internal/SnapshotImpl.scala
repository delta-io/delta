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

import java.net.URI

import scala.collection.JavaConverters._

import com.github.mjakubowski84.parquet4s.ParquetReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import io.delta.standalone.{DeltaScan, Snapshot}
import io.delta.standalone.actions.{AddFile => AddFileJ, Metadata => MetadataJ, Protocol => ProtocolJ, RemoveFile => RemoveFileJ, SetTransaction => SetTransactionJ}
import io.delta.standalone.data.{CloseableIterator, RowRecord => RowParquetRecordJ}
import io.delta.standalone.expressions.Expression

import io.delta.standalone.internal.actions.{AddFile, InMemoryLogReplay, Metadata, Parquet4sSingleActionWrapper, Protocol, RemoveFile, SetTransaction, SingleAction}
import io.delta.standalone.internal.data.CloseableParquetDataIterator
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.logging.Logging
import io.delta.standalone.internal.scan.{DeltaScanImpl, FilteredDeltaScanImpl}
import io.delta.standalone.internal.util.{ConversionUtils, FileNames, JsonUtils}

/**
 * Scala implementation of Java interface [[Snapshot]].
 *
 * @param timestamp The timestamp of the latest commit in milliseconds. Can also be set to -1 if the
 *                  timestamp of the commit is unknown or the table has not been initialized, i.e.
 *                  `version = -1`.
 */
private[internal] class SnapshotImpl(
    val hadoopConf: Configuration,
    val path: Path,
    val version: Long,
    val logSegment: LogSegment,
    val minFileRetentionTimestamp: Long,
    val deltaLog: DeltaLogImpl,
    val timestamp: Long) extends Snapshot with Logging {

  import SnapshotImpl._

  ///////////////////////////////////////////////////////////////////////////
  // Public API Methods
  ///////////////////////////////////////////////////////////////////////////

  override def scan(): DeltaScan = new DeltaScanImpl(allFilesScala)

  override def scan(predicate: Expression): DeltaScan =
    new FilteredDeltaScanImpl(
      allFilesScala,
      predicate,
      metadataScala.partitionSchema)

  override def getAllFiles: java.util.List[AddFileJ] = activeFilesJ

  override def getMetadata: MetadataJ = ConversionUtils.convertMetadata(state.metadata)

  override def getVersion: Long = version

  override def open(): CloseableIterator[RowParquetRecordJ] =
    CloseableParquetDataIterator(
      allFilesScala
        .map(_.path)
        .map(FileNames.absolutePath(deltaLog.dataPath, _).toString),
      getMetadata.getSchema,
      // the time zone ID if it exists, else null
      deltaLog.timezone,
      hadoopConf)

  ///////////////////////////////////////////////////////////////////////////
  // Internal-Only Methods
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Returns an implementation that provides an accessor to the files as internal Scala
   * [[AddFile]]s. This prevents us from having to replay the log internally, generate Scala
   * actions, convert them to Java actions (as per the [[DeltaScan]] interface), and then
   * convert them back to Scala actions.
   */
  def scanScala(): DeltaScanImpl = new DeltaScanImpl(allFilesScala)

  def scanScala(predicate: Expression): DeltaScanImpl =
    new FilteredDeltaScanImpl(
      allFilesScala,
      predicate,
      metadataScala.partitionSchema)

  def tombstones: Seq[RemoveFileJ] = state.tombstones.toSeq.map(ConversionUtils.convertRemoveFile)
  def setTransactions: Seq[SetTransactionJ] =
    state.setTransactions.map(ConversionUtils.convertSetTransaction)
  def protocol: ProtocolJ = ConversionUtils.convertProtocol(state.protocol)

  def allFilesScala: Seq[AddFile] = state.activeFiles.toSeq
  def tombstonesScala: Seq[RemoveFile] = state.tombstones.toSeq
  def setTransactionsScala: Seq[SetTransaction] = state.setTransactions
  def protocolScala: Protocol = state.protocol
  def metadataScala: Metadata = state.metadata
  def numOfFiles: Long = state.numOfFiles

  private def load(paths: Seq[Path]): Seq[SingleAction] = {
    paths.map(_.toString).sortWith(_ < _).par.flatMap { path =>
      if (path.endsWith("json")) {
        import io.delta.standalone.internal.util.Implicits._
        deltaLog.store
          .read(new Path(path), hadoopConf)
          .toArray
          .map { line => JsonUtils.mapper.readValue[SingleAction](line) }
      } else if (path.endsWith("parquet")) {
        ParquetReader.read[Parquet4sSingleActionWrapper](
          path,
          ParquetReader.Options(
            timeZone = deltaLog.timezone,
            hadoopConf = hadoopConf)
        ).toSeq.map(_.unwrap)
      } else Seq.empty[SingleAction]
    }.toList
  }

  /**
   * Reconstruct the state by applying deltas in order to the checkpoint.
   */
  protected lazy val state: State = {
    val logPathURI = path.toUri
    val replay = new InMemoryLogReplay(hadoopConf, minFileRetentionTimestamp)
    val files = (logSegment.deltas ++ logSegment.checkpoints).map(_.getPath)

    // assert that the log belongs to table
    files.foreach { f =>
      if (f.toString.isEmpty || f.getParent != new Path(logPathURI)) {
        // scalastyle:off throwerror
        throw new AssertionError(
          s"File (${f.toString}) doesn't belong in the transaction log at $logPathURI.")
        // scalastyle:on throwerror
      }
    }

    val actions = load(files).map(_.unwrap)

    replay.append(0, actions.iterator)

    if (null == replay.currentProtocolVersion) {
      throw DeltaErrors.actionNotFoundException("protocol", version)
    }
    if (null == replay.currentMetaData) {
      throw DeltaErrors.actionNotFoundException("metadata", version)
    }

    State(
      replay.currentProtocolVersion,
      replay.currentMetaData,
      replay.getSetTransactions,
      replay.getActiveFiles,
      replay.getTombstones,
      replay.sizeInBytes,
      replay.getActiveFiles.size,
      replay.numMetadata,
      replay.numProtocol,
      replay.getTombstones.size,
      replay.getSetTransactions.size
    )
  }

  private lazy val activeFilesJ =
    state.activeFiles.map(ConversionUtils.convertAddFile).toList.asJava

  /** A map to look up transaction version by appId. */
  lazy val transactions: Map[String, Long] =
    setTransactionsScala.map(t => t.appId -> t.version).toMap

  logInfo(s"[tableId=${deltaLog.tableId}] Created snapshot $this")

  /** Complete initialization by checking protocol version. */
  deltaLog.assertProtocolRead(protocolScala)
}

private[internal] object SnapshotImpl {
  /** Canonicalize the paths for Actions. */
  def canonicalizePath(path: String, hadoopConf: Configuration): String = {
    val hadoopPath = new Path(new URI(path))
    if (hadoopPath.isAbsoluteAndSchemeAuthorityNull) {
      val fs = FileSystem.get(hadoopConf)
      fs.makeQualified(hadoopPath).toUri.toString
    } else {
      // return untouched if it is a relative path or is already fully qualified
      hadoopPath.toUri.toString
    }
  }

  /**
   * Metrics and metadata computed around the Delta table.
   *
   * @param protocol The protocol version of the Delta table
   * @param metadata The metadata of the table
   * @param setTransactions The streaming queries writing to this table
   * @param activeFiles The files in this table
   * @param tombstones The unexpired tombstones
   * @param sizeInBytes The total size of the table (of active files, not including tombstones)
   * @param numOfFiles The number of files in this table
   * @param numOfMetadata The number of metadata actions in the state. Should be 1
   * @param numOfProtocol The number of protocol actions in the state. Should be 1
   * @param numOfRemoves The number of tombstones in the state
   * @param numOfSetTransactions Number of streams writing to this table
   */
  case class State(
      protocol: Protocol,
      metadata: Metadata,
      setTransactions: Seq[SetTransaction],
      activeFiles: Iterable[AddFile],
      tombstones: Iterable[RemoveFile],
      sizeInBytes: Long,
      numOfFiles: Long,
      numOfMetadata: Long,
      numOfProtocol: Long,
      numOfRemoves: Long,
      numOfSetTransactions: Long)
}

/**
 * An initial snapshot. Uses default Protocol and Metadata.
 *
 * @param hadoopConf the hadoop configuration for the table
 * @param logPath the path to transaction log
 * @param deltaLog the delta log object
 */
private class InitialSnapshotImpl(
    override val hadoopConf: Configuration,
    val logPath: Path,
    override val deltaLog: DeltaLogImpl)
  extends SnapshotImpl(hadoopConf, logPath, -1, LogSegment.empty(logPath), -1, deltaLog, -1) {

  override lazy val state: SnapshotImpl.State = {
    SnapshotImpl.State(Protocol(), Metadata(), Nil, Nil, Nil, 0L, 0L, 1L, 1L, 0L, 0L)
  }

  override def scan(): DeltaScan = new DeltaScanImpl(Nil)

  override def scan(predicate: Expression): DeltaScan =
    new FilteredDeltaScanImpl(Nil, predicate, metadataScala.partitionSchema)
}
