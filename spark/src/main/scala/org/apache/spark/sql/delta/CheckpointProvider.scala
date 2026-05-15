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

import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DataFrameUtils
import org.apache.spark.sql.delta.DeltaLogFileIndex.COMMIT_VERSION_COLUMN
import org.apache.spark.sql.delta.SnapshotManagement.checkpointV2ThreadPool
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.expressions.EncodeNestedVariantAsZ85String
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.FileNames._
import org.apache.spark.sql.delta.util.threads.NonFateSharingFuture
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, to_json}
import org.apache.spark.sql.types.{StructField, StructType}

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
   * The checkpoint's contribution to the protocol/metadata/in-commit-timestamp fast path, as a
   * DataFrame over [[Snapshot.pAndMQuerySchema]] tagged with [[COMMIT_VERSION_COLUMN]] (None when
   * the checkpoint is empty). Callers read their delta side with the same schema so the two
   * DataFrames can be unioned.
   */
  def loadProtocolMetadataActions(
      spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame]
}

/**
 * A trait which provides information about a checkpoint to the Snapshot.
 */
trait CheckpointProvider extends UninitializedCheckpointProvider {

  /** Effective size of checkpoint across all files */
  def effectiveCheckpointSizeInBytes(): Long

  /**
   * The type of checkpoint (V2 vs Classic). This will be None when no checkpoint is available.
   * This is only intended to be used for logging and metrics.
   */
  def checkpointPolicyForLogging: Option[CheckpointPolicy.Policy]

  /**
   * Writes a compatibility classic single-file checkpoint for this checkpoint at `logPath`, so a
   * legacy reader that does not understand newer kind of checkpoints can read it and fail
   * gracefully with a Protocol requirement failure. Only checkpoint kinds that need a compat
   * checkpoint (i.e. V2 checkpoints) implement this; the default throws.
   */
  def createCompatibilityCheckpoint(
      spark: SparkSession,
      deltaLog: DeltaLog,
      logPath: Path,
      hadoopConf: Configuration,
      tableProperties: Map[String, String]): Unit =
    throw new IllegalStateException(
      s"createCompatibilityCheckpoint is not supported for ${this.getClass.getName}.")

  /**
   * The checkpoint's full action set for state reconstruction as a single DataFrame (None when the
   * checkpoint is empty), already carrying [[COMMIT_VERSION_COLUMN]] and
   * [[Snapshot.ADD_STATS_TO_USE_COL_NAME]] and unioned across all underlying files. Consumers just
   * union this with the delta DataFrames; the physical layout stays behind this method.
   */
  def loadActionsForStateReconstruction(
      spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame]
}

/**
 * A trait representing [[UninitializedCheckpointProvider]] corresponding to checkpoints whose file
 * actions are stored as flat set of parquet files.
 */
trait FileBasedUninitializedCheckpointProvider extends UninitializedCheckpointProvider {

  /**
   * File index which could help derive non-fileactions of a checkpoint. Note that the underlying
   * files may contain fileactions which needs to be filtered out by the caller.
   */
  def topLevelFileIndex: Option[DeltaLogFileIndex]

  override def loadProtocolMetadataActions(
      spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame] = {
    topLevelFileIndex.map { index =>
      deltaLog.loadIndex(index, Snapshot.pAndMQuerySchema)
        .withColumn(COMMIT_VERSION_COLUMN, lit(version))
    }
  }
}

/**
 * A [[CheckpointProvider]] whose file actions are stored as flat set of parquet files.
 */
trait FileBasedCheckpointProvider
  extends CheckpointProvider with FileBasedUninitializedCheckpointProvider {

  /**
   * List of different file indexes and corresponding schemas which could help derive full
   * state-reconstruction for the checkpoint.
   * Different FileIndexes could have different schemas depending on `stats_parsed` / `stats`
   * columns in the underlying file(s).
   */
  def allActionsFileIndexesAndSchemas(
    spark: SparkSession, deltaLog: DeltaLog): Seq[(DeltaLogFileIndex, StructType)]

  override def loadActionsForStateReconstruction(
      spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame] = {
    val jsonStatsCol = col("add.stats")
    val checkpointDataframes = allActionsFileIndexesAndSchemas(spark, deltaLog)
      .map { case (index, schema) =>
        val addSchema = schema("add").dataType.asInstanceOf[StructType]
        val (checkpointSchemaToUse, checkpointStatsColToUse) =
          if (addSchema.exists(_.name == "stats_parsed") && !addSchema.exists(_.name == "stats")) {
            val statsParsedSchema = addSchema("stats_parsed").dataType.asInstanceOf[StructType]
            val checkpointSchemaToUse =
              Action.logSchemaWithAddStatsParsed(addSchema("stats_parsed"))
            val statsCol = col("add.stats_parsed")
            // Only use EncodeNestedVariantAsZ85String if the schema contains VariantType.
            // This avoids performance overhead for tables without variant columns.
            val encodedStatsCol =
              if (SchemaUtils.checkForVariantTypeColumnsRecursively(statsParsedSchema)) {
                Column(EncodeNestedVariantAsZ85String(statsCol.expr))
              } else {
                statsCol
              }
            (
              checkpointSchemaToUse,
              to_json(encodedStatsCol)
            )
          } else {
            // Normal (JSON-like) schema suffices
            (Action.logSchema, jsonStatsCol)
          }

        // For schema compat, make sure to discard add.stats_parsed (if present)
        deltaLog.loadIndex(index, checkpointSchemaToUse)
          .withColumn(COMMIT_VERSION_COLUMN, lit(version))
          .withColumn(Snapshot.ADD_STATS_TO_USE_COL_NAME, checkpointStatsColToUse)
          .withColumn("add", col("add").dropFields("stats_parsed"))
      }
    checkpointDataframes.reduceOption(_.union(_))
  }
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
        override def createCheckpointProvider(): FileBasedCheckpointProvider = {
          val (checkpointMetadataOpt, sidecarFiles) =
            uninitializedV2CheckpointProvider.nonFateSharingCheckpointReadFuture.get(Duration.Inf)
          // This must be a v2 checkpoint, so checkpointMetadataOpt must be non empty.
          val checkpointMetadata = checkpointMetadataOpt.getOrElse {
            val checkpointFile = uninitializedV2CheckpointProvider.topLevelFiles.head
            throw new IllegalStateException(s"V2 Checkpoint ${checkpointFile.getPath} " +
              s"has no CheckpointMetadata action")
          }
          require(isV2CheckpointEnabled(snapshotDescriptor.protocol))
          V2CheckpointProvider(
            uninitializedV2CheckpointProvider,
            checkpointMetadata,
            sidecarFiles,
            snapshotDescriptor.deltaLog)
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
        override def createCheckpointProvider(): FileBasedCheckpointProvider = {
          val (checkpointMetadataOpt, sidecarFiles) = future.get(Duration.Inf)
          checkpointMetadataOpt match {
            case Some(cm) =>
              require(isV2CheckpointEnabled(snapshotDescriptor))
              V2CheckpointProvider(provider, cm, sidecarFiles, snapshotDescriptor.deltaLog)
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

  private[delta] def getParquetSchema(
      spark: SparkSession,
      deltaLog: DeltaLog,
      parquetFile: FileStatus,
      schemaFromLastCheckpoint: Option[StructType]): StructType = {
    // Try to get the checkpoint schema from the last_checkpoint.
    // If it is not there then get it from filesystem by doing I/O.
    val fetchChkSchemaFromLastCheckpoint = spark.sessionState.conf.getConf(
      DeltaSQLConf.USE_CHECKPOINT_SCHEMA_FROM_CHECKPOINT_METADATA)
    schemaFromLastCheckpoint match {
      case Some(schema) if fetchChkSchemaFromLastCheckpoint => schema
      case _ =>
        recordDeltaOperation(deltaLog, "snapshot.checkpointSchema.fromFileSystem") {
          Snapshot.getParquetFileSchemaAndRowCount(spark, deltaLog, parquetFile)._1
        }
    }
  }

  private def sendEventForV2CheckpointRead(
      startTimeMs: Long,
      fileStatus: FileStatus,
      fileType: String,
      logPath: Path,
      exception: Option[Throwable]): Unit = {
    recordDeltaEvent(
      provider = null,
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
      val rows = DataFrameUtils.ofRows(spark, relation)
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
  extends FileBasedCheckpointProvider
  with DeltaLogging {

  require(topLevelFiles.nonEmpty, "There should be atleast 1 checkpoint file")
  private lazy val fileIndex = CheckpointProvider.checkpointFileIndex(topLevelFiles)

  override def version: Long = checkpointVersion(topLevelFiles.head)

  override def effectiveCheckpointSizeInBytes(): Long = fileIndex.sizeInBytes

  override lazy val topLevelFileIndex: Option[DeltaLogFileIndex] = Some(fileIndex)

  override def checkpointPolicyForLogging: Option[CheckpointPolicy.Policy] =
    Some(CheckpointPolicy.Classic)

  override def allActionsFileIndexesAndSchemas(
      spark: SparkSession, deltaLog: DeltaLog): Seq[(DeltaLogFileIndex, StructType)] = {
    Seq((fileIndex, checkpointSchema(spark, deltaLog)))
  }

  private val checkpointSchemaWithCaching = new LazyCheckpointSchemaGetter {
    override def fileStatus: FileStatus = topLevelFiles.head
    override def schemaFromLastCheckpoint: Option[StructType] =
      lastCheckpointInfoOpt.flatMap(_.checkpointSchema)
  }
  private def checkpointSchema(spark: SparkSession, deltaLog: DeltaLog): StructType =
    checkpointSchemaWithCaching.get(spark, deltaLog)

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
  override def checkpointPolicyForLogging: Option[CheckpointPolicy.Policy] = None
  override def loadProtocolMetadataActions(
    spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame] = None
  override def loadActionsForStateReconstruction(
    spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame] = None
}

/** A trait representing a v2 [[UninitializedCheckpointProvider]] */
trait UninitializedV2LikeCheckpointProvider extends FileBasedUninitializedCheckpointProvider {
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
    uninitializedCheckpointProvider: FileBasedUninitializedCheckpointProvider)
  extends FileBasedCheckpointProvider {

  override def version: Long = uninitializedCheckpointProvider.version
  override def topLevelFiles: Seq[FileStatus] = uninitializedCheckpointProvider.topLevelFiles
  override def topLevelFileIndex: Option[DeltaLogFileIndex] =
    uninitializedCheckpointProvider.topLevelFileIndex

  protected def createCheckpointProvider(): FileBasedCheckpointProvider

  lazy val underlyingCheckpointProvider: FileBasedCheckpointProvider = createCheckpointProvider()

  override def effectiveCheckpointSizeInBytes(): Long =
    underlyingCheckpointProvider.effectiveCheckpointSizeInBytes()

  override def checkpointPolicyForLogging: Option[CheckpointPolicy.Policy] =
    underlyingCheckpointProvider.checkpointPolicyForLogging

  override def createCompatibilityCheckpoint(
      spark: SparkSession,
      deltaLog: DeltaLog,
      logPath: Path,
      hadoopConf: Configuration,
      tableProperties: Map[String, String]): Unit =
    underlyingCheckpointProvider.createCompatibilityCheckpoint(
      spark, deltaLog, logPath, hadoopConf, tableProperties)

  override def allActionsFileIndexesAndSchemas(
      spark: SparkSession, deltaLog: DeltaLog): Seq[(DeltaLogFileIndex, StructType)] = {
    underlyingCheckpointProvider.allActionsFileIndexesAndSchemas(spark, deltaLog)
  }
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
 * @param sidecarSchemaFetcher     function to fetch sidecar schema.
 *                              Returns None if there are no sidecar files.
 */
case class V2CheckpointProvider(
    override val version: Long,
    v2CheckpointFile: FileStatus,
    v2CheckpointFormat: V2Checkpoint.Format,
    checkpointMetadata: CheckpointMetadata,
    sidecarFiles: Seq[SidecarFile],
    lastCheckpointInfoOpt: Option[LastCheckpointInfo],
    logPath: Path,
    sidecarSchemaFetcher: () => Option[StructType]
  ) extends FileBasedCheckpointProvider with DeltaLogging {

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

  override def checkpointPolicyForLogging: Option[CheckpointPolicy.Policy] =
    Some(CheckpointPolicy.V2)

  override def createCompatibilityCheckpoint(
      spark: SparkSession,
      deltaLog: DeltaLog,
      logPath: Path,
      hadoopConf: Configuration,
      tableProperties: Map[String, String]): Unit = {
    // topLevelFileIndex is non-empty for V2CheckpointProvider and
    // represents the v2 manifest file
    val shallowCopyDf = deltaLog.loadIndex(topLevelFileIndex.get, Action.logSchema)
    val finalPath = checkpointFileSingular(logPath, version)
    Checkpoints.createCheckpointV2ParquetFile(
      spark,
      shallowCopyDf,
      finalPath,
      hadoopConf,
      useRename = false,
      tableProperties = tableProperties)
  }

  private val v2SchemaWithCaching = new LazyCheckpointSchemaGetter {
    override def fileStatus: FileStatus = v2CheckpointFile
    override def schemaFromLastCheckpoint: Option[StructType] =
      lastCheckpointInfoOpt.flatMap(_.checkpointSchema)
  }

  protected def schemaForV2Checkpoint(
      spark: SparkSession, deltaLog: DeltaLog): StructType = {
    if (v2CheckpointFormat != V2Checkpoint.Format.PARQUET) {
      return Action.logSchema
    }
    v2SchemaWithCaching.get(spark, deltaLog)
  }

  protected def schemaForSidecarFile(spark: SparkSession, deltaLog: DeltaLog): StructType = {
    sidecarSchemaFetcher()
      .getOrElse {
        throw DeltaErrors.assertionFailedError("Sidecar schema asked without any sidecar files")
      }
  }

  override def allActionsFileIndexesAndSchemas(
      spark: SparkSession, deltaLog: DeltaLog): Seq[(DeltaLogFileIndex, StructType)] = {
    (fileIndexForV2Checkpoint, schemaForV2Checkpoint(spark, deltaLog)) +:
      fileIndexesForSidecarFiles.map((_, schemaForSidecarFile(spark, deltaLog)))
  }
}

object V2CheckpointProvider {
  /** Alternate constructor which uses [[UninitializedV2LikeCheckpointProvider]] */
  def apply(
      uninitializedV2LikeCheckpointProvider: UninitializedV2LikeCheckpointProvider,
      checkpointMetadata: CheckpointMetadata,
      sidecarFiles: Seq[SidecarFile],
      deltaLog: DeltaLog): V2CheckpointProvider = {
    def getSidecarSchemaFetcher: () => Option[StructType] = {
      val sidecarSchemaFromMetadata = checkpointMetadata.sidecarFileSchema
      val nonFateSharingSidecarSchemaFuture: NonFateSharingFuture[Option[StructType]] = {
        checkpointV2ThreadPool.submitNonFateSharing { spark: SparkSession =>
          sidecarFiles.headOption.map { sidecarFile =>
            val sidecarFileStatus =
              sidecarFile.toFileStatus(uninitializedV2LikeCheckpointProvider.logPath)
            CheckpointProvider.getParquetSchema(
              spark,
              deltaLog,
              sidecarFileStatus,
              schemaFromLastCheckpoint = sidecarSchemaFromMetadata)
          }
        }
      }
      () => nonFateSharingSidecarSchemaFuture.get(Duration.Inf)
    }
    V2CheckpointProvider(
      uninitializedV2LikeCheckpointProvider.version,
      uninitializedV2LikeCheckpointProvider.fileStatus,
      uninitializedV2LikeCheckpointProvider.v2CheckpointFormat,
      checkpointMetadata,
      sidecarFiles,
      uninitializedV2LikeCheckpointProvider.lastCheckpointInfoOpt,
      uninitializedV2LikeCheckpointProvider.logPath,
      getSidecarSchemaFetcher
    )
  }
}

abstract class LazyCheckpointSchemaGetter {
  protected def fileStatus: FileStatus
  protected def schemaFromLastCheckpoint: Option[StructType]

  private var lazySchema = Option.empty[StructType]

  def get(spark: SparkSession, deltaLog: DeltaLog): StructType = {
    lazySchema.getOrElse {
      this.synchronized {
        // re-check with lock held, in case of races with other initializers
        if (lazySchema.isEmpty) {
          lazySchema = Some(CheckpointProvider.getParquetSchema(
            spark, deltaLog, fileStatus, schemaFromLastCheckpoint))
        }
        lazySchema.get
      }
    }
  }

  def getIfKnown: Option[StructType] = lazySchema
}
