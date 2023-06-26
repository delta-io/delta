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

package org.apache.spark.sql.delta

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.SnapshotManagement.checkpointV2ThreadPool
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.FileNames._
import org.apache.spark.sql.delta.util.threads.NonFateSharingFuture
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

/**
 * Represents basic information about a checkpoint.
 * This is the info we always can know about a checkpoint, without doing any additional I/O.
 */
trait UninitializedCheckpointProvider {

  /** True if the checkpoint provider is empty (does not refer to a valid checkpoint) */
  def isEmpty: Boolean = version < 0

  /** Checkpoint version */
  def version: Long

  /**
   * Top level files that represents this checkpoint.
   * These files could be reused again to initialize the [[CheckpointProvider]].
   */
  def topLevelFiles: Seq[FileStatus]

  /**
   * File index which could help derive actions stored in top level files
   * for the checkpoint.
   * This could be used to get [[Protocol]], [[Metadata]] etc from a checkpoint.
   * This could also be used if we want to shallow copy a checkpoint.
   */
  def topLevelFileIndex: Option[DeltaLogFileIndex]
}

/**
 * A trait which provides information about a checkpoint to the Snapshot.
 */
trait CheckpointProvider extends UninitializedCheckpointProvider {

  /** Effective size of checkpoint across all files */
  def effectiveCheckpointSizeInBytes(): Long

  /**
   * List of different file indexes which could help derive full state-reconstruction
   * for the checkpoint.
   */
  def allActionsFileIndexes(): Seq[DeltaLogFileIndex]
}

object CheckpointProvider extends DeltaLogging {

  /** Helper method to convert non-empty checkpoint files to DeltaLogFileIndex */
  def checkpointFileIndex(checkpointFiles: Seq[FileStatus]): DeltaLogFileIndex = {
    assert(checkpointFiles.nonEmpty, "checkpointFiles must not be empty")
    DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, checkpointFiles).get
  }

  /** Converts an [[UninitializedCheckpointProvider]] into a [[CheckpointProvider]] */
  def apply(
      spark: SparkSession,
      snapshotDescriptor: SnapshotDescriptor,
      checksumOpt: Option[VersionChecksum],
      uninitializedCheckpointProvider: UninitializedCheckpointProvider)
      : CheckpointProvider = uninitializedCheckpointProvider match {
    // Note: snapshotDescriptor.protocol should be accessed as late as possible inside the futures
    // as it might need I/O.
    case uninitializedV2CheckpointProvider: UninitializedV2CheckpointProvider =>
      new LazyCompleteCheckpointProvider(uninitializedV2CheckpointProvider) {
        override def createCheckpointProvider(): CheckpointProvider = {
          val (checkpointMetadataOpt, sidecarFiles) =
            uninitializedV2CheckpointProvider.nonFateSharingCheckpointReadFuture.get(Duration.Inf)
          // This must be a v2 checkpoint, so checkpointMetadataOpt must be non empty.
          val checkpointMetadata = checkpointMetadataOpt.getOrElse {
            val checkpointFile = uninitializedV2CheckpointProvider.topLevelFiles.head
            throw new IllegalStateException(s"V2 Checkpoint ${checkpointFile.getPath} " +
              s"has no CheckpointMetadata action")
          }
          require(isV2CheckpointEnabled(snapshotDescriptor.protocol))
          V2CheckpointProvider(uninitializedV2CheckpointProvider, checkpointMetadata, sidecarFiles)
        }
      }
    case provider: UninitializedV1OrV2ParquetCheckpointProvider
        if isV2CheckpointEnabled(checksumOpt).contains(false) =>
      // V2 checkpoints are specifically disabled, so it must be V1
      PreloadedCheckpointProvider(provider.topLevelFiles, provider.lastCheckpointInfoOpt)
    case provider: UninitializedV1OrV2ParquetCheckpointProvider =>
      // Either v2 checkpoints are explicitly enabled, or we lack a Protocol to prove otherwise.
      // We can't tell immediately whether it's V1 or V2, just by looking at the file name.

      // Start a future to start reading the v2 actions from the parquet checkpoint and return
      // a lazy checkpoint provider wrapping the future. we won't wait on the future unless/until
      // somebody calls a complete checkpoint provider method.
      val future = checkpointV2ThreadPool.submitNonFateSharing { spark: SparkSession =>
        readV2ActionsFromParquetCheckpoint(
          spark, provider.logPath, provider.fileStatus, snapshotDescriptor.deltaLog.options)
      }
      new LazyCompleteCheckpointProvider(provider) {
        override def createCheckpointProvider(): CheckpointProvider = {
          val (checkpointMetadataOpt, sidecarFiles) = future.get(Duration.Inf)
          checkpointMetadataOpt match {
            case Some(cm) =>
              require(isV2CheckpointEnabled(snapshotDescriptor))
              V2CheckpointProvider(provider, cm, sidecarFiles)
            case None =>
              PreloadedCheckpointProvider(provider.topLevelFiles, provider.lastCheckpointInfoOpt)
          }
        }
      }
  }

  private[delta] def isV2CheckpointEnabled(protocol: Protocol): Boolean =
    protocol.isFeatureSupported(V2CheckpointTableFeature)

  /**
   * Returns whether V2 Checkpoints are enabled or not.
   * This means an underlying checkpoint in this table could be a V2Checkpoint with sidecar files.
   */
  def isV2CheckpointEnabled(snapshotDescriptor: SnapshotDescriptor): Boolean =
    isV2CheckpointEnabled(snapshotDescriptor.protocol)

  /**
   * Returns:
   * - Some(true) if V2 Checkpoints are enabled for the snapshot corresponding to the given
   *   `checksumOpt`.
   * - Some(false) if V2 Checkpoints are disabled for the snapshot
   * - None if the given checksumOpt is not sufficient to identify if v2 checkpoints are enabled or
   *   not.
   */
  def isV2CheckpointEnabled(checksumOpt: Option[VersionChecksum]): Option[Boolean] = {
    checksumOpt.flatMap(checksum => Option(checksum.protocol)).map(isV2CheckpointEnabled)
  }

  private def sendEventForV2CheckpointRead(
      startTimeMs: Long,
      fileStatus: FileStatus,
      fileType: String,
      logPath: Path,
      exception: Option[Throwable]): Unit = {
    recordDeltaEvent(
      deltaLog = null,
      opType = "delta.checkpointV2.readV2ActionsFromCheckpoint",
      data = Map(
        "timeTakenMs" -> (System.currentTimeMillis() - startTimeMs),
        "v2CheckpointPath" -> fileStatus.getPath.toString,
        "v2CheckpointSize" -> fileStatus.getLen,
        "errorMessage" -> exception.map(_.toString).getOrElse(""),
        "fileType" -> fileType
      ),
      path = Some(logPath.getParent)
    )
  }

  /** Reads and returns the [[CheckpointMetadata]] and [[SidecarFile]]s from a json v2 checkpoint */
  private[delta] def readV2ActionsFromJsonCheckpoint(
      logStore: LogStore,
      logPath: Path,
      fileStatus: FileStatus,
      hadoopConf: Configuration): (CheckpointMetadata, Seq[SidecarFile]) = {
    val startTimeMs = System.currentTimeMillis()
    try {
      var checkpointMetadataOpt: Option[CheckpointMetadata] = None
      val sidecarFileActions: ArrayBuffer[SidecarFile] = ArrayBuffer.empty
      logStore.readAsIterator(fileStatus, hadoopConf).processAndClose { _
        .map(Action.fromJson)
        .foreach {
          case cm: CheckpointMetadata if checkpointMetadataOpt.isEmpty =>
            checkpointMetadataOpt = Some(cm)
          case cm: CheckpointMetadata =>
            throw new IllegalStateException(
              "More than 1 CheckpointMetadata actions found in the checkpoint file")
          case sidecarFile: SidecarFile =>
            sidecarFileActions.append(sidecarFile)
          case _ => ()
        }
      }
      val checkpointMetadata = checkpointMetadataOpt.getOrElse {
        throw new IllegalStateException("Json V2 Checkpoint has no CheckpointMetadata action")
      }
      sendEventForV2CheckpointRead(startTimeMs, fileStatus, "json", logPath, exception = None)
      (checkpointMetadata, sidecarFileActions.toSeq)
    } catch {
      case NonFatal(e) =>
        sendEventForV2CheckpointRead(startTimeMs, fileStatus, "json", logPath, exception = Some(e))
        throw e
    }
  }

  /**
   * Reads and returns the optional [[CheckpointMetadata]], [[SidecarFile]]s from a parquet
   * checkpoint.
   * The checkpoint metadata returned might be None if the underlying parquet file is not a v2
   * checkpoint.
   */
  private[delta] def readV2ActionsFromParquetCheckpoint(
      spark: SparkSession,
      logPath: Path,
      fileStatus: FileStatus,
      deltaLogOptions: Map[String, String]): (Option[CheckpointMetadata], Seq[SidecarFile]) = {
    val startTimeMs = System.currentTimeMillis()
    try {
      val relation = DeltaLog.indexToRelation(
        spark, checkpointFileIndex(Seq(fileStatus)), deltaLogOptions, Action.logSchema)
      import implicits._
      val rows = Dataset.ofRows(spark, relation)
        .select("checkpointMetadata", "sidecar")
        .where("checkpointMetadata.version is not null or sidecar.path is not null")
        .as[(CheckpointMetadata, SidecarFile)]
        .collect()

      var checkpointMetadata: Option[CheckpointMetadata] = None
      val checkpointSidecarFiles = ArrayBuffer.empty[SidecarFile]
      rows.foreach {
        case (cm: CheckpointMetadata, _) if checkpointMetadata.isEmpty =>
          checkpointMetadata = Some(cm)
        case (cm: CheckpointMetadata, _) =>
          throw new IllegalStateException(
            "More than 1 CheckpointMetadata actions found in the checkpoint file")
        case (_, sf: SidecarFile) =>
          checkpointSidecarFiles.append(sf)
      }
      if (checkpointMetadata.isEmpty && checkpointSidecarFiles.nonEmpty) {
        throw new IllegalStateException(
          "sidecar files present in checkpoint even when checkpoint metadata is missing")
      }
      sendEventForV2CheckpointRead(startTimeMs, fileStatus, "parquet", logPath, exception = None)
      (checkpointMetadata, checkpointSidecarFiles.toSeq)
    } catch {
      case NonFatal(e) =>
        sendEventForV2CheckpointRead(startTimeMs, fileStatus, "parquet", logPath, Some(e))
        throw e
    }
  }
}

/**
 * An implementation of [[CheckpointProvider]] where the information about checkpoint files
 * (i.e. Seq[FileStatus]) is already known in advance.
 *
 * @param topLevelFiles - file statuses that describes the checkpoint
 * @param lastCheckpointInfoOpt - optional [[LastCheckpointInfo]] corresponding to this checkpoint.
 *                                This comes from _last_checkpoint file
 */
case class PreloadedCheckpointProvider(
    override val topLevelFiles: Seq[FileStatus],
    lastCheckpointInfoOpt: Option[LastCheckpointInfo])
  extends CheckpointProvider
  with DeltaLogging {

  require(topLevelFiles.nonEmpty, "There should be atleast 1 checkpoint file")
  private lazy val fileIndex = CheckpointProvider.checkpointFileIndex(topLevelFiles)

  override def version: Long = checkpointVersion(topLevelFiles.head)

  override def effectiveCheckpointSizeInBytes(): Long = fileIndex.sizeInBytes

  override def allActionsFileIndexes(): Seq[DeltaLogFileIndex] = Seq(fileIndex)

  override lazy val topLevelFileIndex: Option[DeltaLogFileIndex] = Some(fileIndex)
}

/**
 * An implementation for [[CheckpointProvider]] which could be used to represent a scenario when
 * checkpoint doesn't exist. This helps us simplify the code by making
 * [[LogSegment.checkpointProvider]] as non-optional.
 *
 * The [[CheckpointProvider.isEmpty]] method returns true for [[EmptyCheckpointProvider]]. Also
 * version is returned as -1.
 * For a real checkpoint, this will be returned true and version will be >= 0.
 */
object EmptyCheckpointProvider extends CheckpointProvider {
  override def version: Long = -1
  override def topLevelFiles: Seq[FileStatus] = Nil
  override def effectiveCheckpointSizeInBytes(): Long = 0L
  override def allActionsFileIndexes(): Seq[DeltaLogFileIndex] = Nil
  override def topLevelFileIndex: Option[DeltaLogFileIndex] = None
}

/** A trait representing a v2 [[UninitializedCheckpointProvider]] */
trait UninitializedV2LikeCheckpointProvider extends UninitializedCheckpointProvider {
  def fileStatus: FileStatus
  def logPath: Path
  def lastCheckpointInfoOpt: Option[LastCheckpointInfo]
  def v2CheckpointFormat: V2Checkpoint.Format

  override lazy val topLevelFiles: Seq[FileStatus] = Seq(fileStatus)
  override lazy val topLevelFileIndex: Option[DeltaLogFileIndex] =
    DeltaLogFileIndex(v2CheckpointFormat.fileFormat, topLevelFiles)
}

/**
 * An implementation of [[UninitializedCheckpointProvider]] to represent a parquet checkpoint
 * which could be either a v1 checkpoint or v2 checkpoint.
 * This needs to be resolved into a [[PreloadedCheckpointProvider]] or a [[V2CheckpointProvider]]
 * depending on whether the [[CheckpointMetadata]] action is present or not in the underlying
 * parquet file.
 */
case class UninitializedV1OrV2ParquetCheckpointProvider(
    override val version: Long,
    override val fileStatus: FileStatus,
    override val logPath: Path,
    override val lastCheckpointInfoOpt: Option[LastCheckpointInfo]
) extends UninitializedV2LikeCheckpointProvider {

  override val v2CheckpointFormat: V2Checkpoint.Format = V2Checkpoint.Format.PARQUET
}

/**
 * An implementation of [[UninitializedCheckpointProvider]] to for v2 checkpoints.
 * This needs to be resolved into a [[V2CheckpointProvider]].
 * This class starts an I/O to fetch the V2 actions ([[CheckpointMetadata]], [[SidecarFile]]) as
 * soon as the class is initialized so that the extra overhead could be parallelized with other
 * operations like reading CRC.
 */
case class UninitializedV2CheckpointProvider(
    override val version: Long,
    override val fileStatus: FileStatus,
    override val logPath: Path,
    hadoopConf: Configuration,
    deltaLogOptions: Map[String, String],
    logStore: LogStore,
    override val lastCheckpointInfoOpt: Option[LastCheckpointInfo]
) extends UninitializedV2LikeCheckpointProvider {

  override val v2CheckpointFormat: V2Checkpoint.Format =
    V2Checkpoint.toFormat(fileStatus.getPath.getName)

  // Try to get the required actions from LastCheckpointInfo
  private val v2ActionsFromLastCheckpointOpt: Option[(CheckpointMetadata, Seq[SidecarFile])] = {
    lastCheckpointInfoOpt
      .flatMap(_.v2Checkpoint)
      .map(v2 => (v2.checkpointMetadataOpt, v2.sidecarFiles))
      .collect {
        case (Some(checkpointMetadata), Some(sidecarFiles)) =>
          (checkpointMetadata, sidecarFiles)
      }
  }

  /** Helper method to do I/O and read v2 actions from the underlying v2 checkpoint file */
  private def readV2Actions(spark: SparkSession): (Option[CheckpointMetadata], Seq[SidecarFile]) = {
    v2CheckpointFormat match {
      case V2Checkpoint.Format.JSON =>
        val (checkpointMetadata, sidecars) = CheckpointProvider.readV2ActionsFromJsonCheckpoint(
            logStore, logPath, fileStatus, hadoopConf)
        (Some(checkpointMetadata), sidecars)
      case V2Checkpoint.Format.PARQUET =>
        CheckpointProvider.readV2ActionsFromParquetCheckpoint(
            spark, logPath, fileStatus, deltaLogOptions)
    }
  }

  val nonFateSharingCheckpointReadFuture
      : NonFateSharingFuture[(Option[CheckpointMetadata], Seq[SidecarFile])] = {
    checkpointV2ThreadPool.submitNonFateSharing { spark: SparkSession =>
      v2ActionsFromLastCheckpointOpt match {
        case Some((cm, sidecars)) => Some(cm) -> sidecars
        case None => readV2Actions(spark)
      }
    }
  }
}

/**
 * A wrapper implementation of [[CheckpointProvider]] which wraps
 * `underlyingCheckpointProviderFuture` and `uninitializedCheckpointProvider` for implementing all
 * the [[UninitializedCheckpointProvider]] and [[CheckpointProvider]] APIs.
 *
 * @param uninitializedCheckpointProvider the underlying [[UninitializedCheckpointProvider]]
 */
abstract class LazyCompleteCheckpointProvider(
    uninitializedCheckpointProvider: UninitializedCheckpointProvider)
  extends CheckpointProvider {

  override def version: Long = uninitializedCheckpointProvider.version
  override def topLevelFiles: Seq[FileStatus] = uninitializedCheckpointProvider.topLevelFiles
  override def topLevelFileIndex: Option[DeltaLogFileIndex] =
    uninitializedCheckpointProvider.topLevelFileIndex

  protected def createCheckpointProvider(): CheckpointProvider

  lazy val underlyingCheckpointProvider: CheckpointProvider = createCheckpointProvider()

  override def effectiveCheckpointSizeInBytes(): Long =
    underlyingCheckpointProvider.effectiveCheckpointSizeInBytes()

  override def allActionsFileIndexes(): Seq[DeltaLogFileIndex] =
    underlyingCheckpointProvider.allActionsFileIndexes()
}

/**
 * [[CheckpointProvider]] implementation for Json/Parquet V2 checkpoints.
 *
 * @param version               checkpoint version for the underlying checkpoint
 * @param v2CheckpointFile      [[FileStatus]] for the json/parquet v2 checkpoint file
 * @param v2CheckpointFormat    format (json/parquet) for the v2 checkpoint
 * @param checkpointMetadata    [[CheckpointMetadata]] for the v2 checkpoint
 * @param sidecarFiles          seq of [[SidecarFile]] for the v2 checkpoint
 * @param lastCheckpointInfoOpt optional last checkpoint info for the v2 checkpoint
 * @param logPath               delta log path for the underlying delta table
 */
case class V2CheckpointProvider(
    override val version: Long,
    v2CheckpointFile: FileStatus,
    v2CheckpointFormat: V2Checkpoint.Format,
    checkpointMetadata: CheckpointMetadata,
    sidecarFiles: Seq[SidecarFile],
    lastCheckpointInfoOpt: Option[LastCheckpointInfo],
    logPath: Path
  ) extends CheckpointProvider with DeltaLogging {

  private[delta] def sidecarFileStatuses: Seq[FileStatus] =
    sidecarFiles.map(_.toFileStatus(logPath))

  protected lazy val fileIndexesForSidecarFiles: Seq[DeltaLogFileIndex] = {
    // V2 checkpoints without sidecars are legal.
    if (sidecarFileStatuses.isEmpty) {
      Seq.empty
    } else {
      Seq(CheckpointProvider.checkpointFileIndex(sidecarFileStatuses))
    }
  }

  protected lazy val fileIndexForV2Checkpoint: DeltaLogFileIndex =
    DeltaLogFileIndex(v2CheckpointFormat.fileFormat, Seq(v2CheckpointFile)).head

  override lazy val topLevelFiles: Seq[FileStatus] = Seq(v2CheckpointFile)
  override lazy val topLevelFileIndex: Option[DeltaLogFileIndex] = Some(fileIndexForV2Checkpoint)
  override def effectiveCheckpointSizeInBytes(): Long =
    sidecarFiles.map(_.sizeInBytes).sum + v2CheckpointFile.getLen
  override def allActionsFileIndexes(): Seq[DeltaLogFileIndex] =
    topLevelFileIndex ++: fileIndexesForSidecarFiles

}

object V2CheckpointProvider {

  /** Alternate constructor which uses [[UninitializedV2LikeCheckpointProvider]] */
  def apply(
      uninitializedV2LikeCheckpointProvider: UninitializedV2LikeCheckpointProvider,
      checkpointMetadata: CheckpointMetadata,
      sidecarFiles: Seq[SidecarFile]): V2CheckpointProvider = {
    V2CheckpointProvider(
      uninitializedV2LikeCheckpointProvider.version,
      uninitializedV2LikeCheckpointProvider.fileStatus,
      uninitializedV2LikeCheckpointProvider.v2CheckpointFormat,
      checkpointMetadata,
      sidecarFiles,
      uninitializedV2LikeCheckpointProvider.lastCheckpointInfoOpt,
      uninitializedV2LikeCheckpointProvider.logPath)
  }
}
