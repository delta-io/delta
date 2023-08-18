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

import java.io.FileNotFoundException
import java.util.UUID

import scala.collection.mutable
import scala.math.Ordering.Implicits._
import scala.util.control.NonFatal

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.actions.{Metadata, SingleAction}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.delta.util.FileNames._
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapred.{JobConf, TaskAttemptContextImpl, TaskAttemptID}
import org.apache.hadoop.mapreduce.{Job, TaskType}

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Cast, ElementAt, Literal}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.{coalesce, col, struct, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.Utils

/**
 * A class to help with comparing checkpoints with each other, where we may have had concurrent
 * writers that checkpoint with different number of parts.
 */
case class CheckpointInstance(
    version: Long,
    format: CheckpointInstance.Format,
    numParts: Option[Int] = None) extends Ordered[CheckpointInstance] {

  // Assert that numParts are present when checkpoint format is Format.WITH_PARTS.
  // For other formats, numParts must be None.
  require((format == CheckpointInstance.Format.WITH_PARTS) == numParts.isDefined,
    s"numParts ($numParts) must be present for checkpoint format" +
      s" ${CheckpointInstance.Format.WITH_PARTS.name}")

  /**
   * Returns a [[CheckpointProvider]] which can tell the files corresponding to this
   * checkpoint.
   * The `lastCheckpointInfoHint` might be passed to [[CheckpointProvider]] so that underlying
   * [[CheckpointProvider]] provides more precise info.
   */
  def getCheckpointProvider(
      deltaLog: DeltaLog,
      filesForCheckpointConstruction: Seq[FileStatus],
      lastCheckpointInfoHint: Option[LastCheckpointInfo] = None)
      : UninitializedCheckpointProvider = {
    val logPath = deltaLog.logPath
    val lastCheckpointInfo = lastCheckpointInfoHint.filter(cm => CheckpointInstance(cm) == this)
    val cpFiles = filterFiles(deltaLog, filesForCheckpointConstruction)
    format match {
      case CheckpointInstance.Format.WITH_PARTS | CheckpointInstance.Format.SINGLE =>
        PreloadedCheckpointProvider(cpFiles, lastCheckpointInfo)
      case CheckpointInstance.Format.SENTINEL =>
        throw DeltaErrors.assertionFailedError(
          s"invalid checkpoint format ${CheckpointInstance.Format.SENTINEL}")
    }
  }

  def filterFiles(deltaLog: DeltaLog,
                  filesForCheckpointConstruction: Seq[FileStatus]) : Seq[FileStatus] = {
    val logPath = deltaLog.logPath
    format match {
      case CheckpointInstance.Format.WITH_PARTS | CheckpointInstance.Format.SINGLE =>
        val filePaths = if (format == CheckpointInstance.Format.WITH_PARTS) {
          checkpointFileWithParts(logPath, version, numParts.get).toSet
        } else {
          Set(checkpointFileSingular(logPath, version))
        }
        val newCheckpointFileArray =
          filesForCheckpointConstruction.filter(f => filePaths.contains(f.getPath))
        assert(newCheckpointFileArray.length == filePaths.size,
          "Failed in getting the file information for:\n" +
            filePaths.mkString(" -", "\n -", "") + "\namong\n" +
            filesForCheckpointConstruction.map(_.getPath).mkString(" -", "\n -", ""))
        newCheckpointFileArray
      case CheckpointInstance.Format.SENTINEL =>
        throw DeltaErrors.assertionFailedError(
          s"invalid checkpoint format ${CheckpointInstance.Format.SENTINEL}")
    }
  }

  /**
   * Comparison rules:
   * 1. A [[CheckpointInstance]] with higher version is greater than the one with lower version.
   * 2. For [[CheckpointInstance]]s with same version, a Multi-part checkpoint is greater than a
   *    Single part checkpoint.
   * 3. For Multi-part [[CheckpointInstance]]s corresponding to same version, the one with more
   *    parts is greater than the one with less parts.
   */
  override def compare(other: CheckpointInstance): Int = {
    (version, format, numParts) compare (other.version, other.format, other.numParts)
  }
}

object CheckpointInstance {
  sealed abstract class Format(val ordinal: Int, val name: String) extends Ordered[Format] {
    override def compare(other: Format): Int = ordinal compare other.ordinal
  }

  object Format {
    def unapply(name: String): Option[Format] = name match {
      case SINGLE.name => Some(SINGLE)
      case WITH_PARTS.name => Some(WITH_PARTS)
      case _ => None
    }

    /** single-file checkpoint format */
    object SINGLE extends Format(0, "SINGLE")
    /** multi-file checkpoint format */
    object WITH_PARTS extends Format(1, "WITH_PARTS")
    /** Sentinel, for internal use only */
    object SENTINEL extends Format(Int.MaxValue, "SENTINEL")
  }

  def apply(path: Path): CheckpointInstance = {
    // Three formats to worry about:
    // * <version>.checkpoint.parquet
    // * <version>.checkpoint.<i>.<n>.parquet
    path.getName.split("\\.") match {
      case Array(v, "checkpoint", "parquet") =>
        CheckpointInstance(v.toLong, Format.SINGLE, numParts = None)
      case Array(v, "checkpoint", _, n, "parquet") =>
        CheckpointInstance(v.toLong, Format.WITH_PARTS, numParts = Some(n.toInt))
      case _ =>
        throw DeltaErrors.assertionFailedError(s"Unrecognized checkpoint path format: $path")
    }
  }

  def apply(version: Long): CheckpointInstance = {
    CheckpointInstance(version, Format.SINGLE, numParts = None)
  }

  def apply(metadata: LastCheckpointInfo): CheckpointInstance = {
    CheckpointInstance(
      version = metadata.version,
      format = metadata.getFormatEnum(),
      numParts = metadata.parts)
  }

  val MaxValue: CheckpointInstance = sentinelValue(versionOpt = None)

  def sentinelValue(versionOpt: Option[Long]): CheckpointInstance = {
    val version = versionOpt.getOrElse(Long.MaxValue)
    CheckpointInstance(version, Format.SENTINEL, numParts = None)
  }
}

trait Checkpoints extends DeltaLogging {
  self: DeltaLog =>

  def logPath: Path
  def dataPath: Path
  protected def store: LogStore

  /** Used to clean up stale log files. */
  protected def doLogCleanup(snapshotToCleanup: Snapshot): Unit

  /** Returns the checkpoint interval for this log. Not transactional. */
  def checkpointInterval(metadata: Metadata): Int =
    DeltaConfigs.CHECKPOINT_INTERVAL.fromMetaData(metadata)

  /** The path to the file that holds metadata about the most recent checkpoint. */
  val LAST_CHECKPOINT = new Path(logPath, Checkpoints.LAST_CHECKPOINT_FILE_NAME)

  /**
   * Catch non-fatal exceptions related to checkpointing, since the checkpoint is written
   * after the commit has completed. From the perspective of the user, the commit has
   * completed successfully. However, throw if this is in a testing environment -
   * that way any breaking changes can be caught in unit tests.
   */
  protected def withCheckpointExceptionHandling(
      deltaLog: DeltaLog, opType: String)(thunk: => Unit): Unit = {
    try {
      thunk
    } catch {
      case NonFatal(e) =>
        recordDeltaEvent(
          deltaLog,
          opType,
          data = Map("exception" -> e.getMessage(), "stackTrace" -> e.getStackTrace())
        )
        logWarning(s"Error when writing checkpoint-related files", e)
        val throwError = Utils.isTesting ||
          spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CHECKPOINT_THROW_EXCEPTION_WHEN_FAILED)
        if (throwError) throw e
    }
  }

  /**
   * Creates a checkpoint using the default snapshot.
   *
   * WARNING: This API is being deprecated, and will be removed in future versions.
   * Please use the checkpoint(Snapshot) function below to write checkpoints to the delta log.
   */
  @deprecated("This method is deprecated and will be removed in future versions.", "12.0")
  def checkpoint(): Unit = checkpoint(unsafeVolatileSnapshot)

  /**
   * Creates a checkpoint using snapshotToCheckpoint. By default it uses the current log version.
   * Note that this function captures and logs all exceptions, since the checkpoint shouldn't fail
   * the overall commit operation.
   */
  def checkpoint(snapshotToCheckpoint: Snapshot): Unit = recordDeltaOperation(
      this, "delta.checkpoint") {
    withCheckpointExceptionHandling(snapshotToCheckpoint.deltaLog, "delta.checkpoint.sync.error") {
      if (snapshotToCheckpoint.version < 0) {
        throw DeltaErrors.checkpointNonExistTable(dataPath)
      }
      checkpointAndCleanUpDeltaLog(snapshotToCheckpoint)
    }
  }

  protected def checkpointAndCleanUpDeltaLog(
      snapshotToCheckpoint: Snapshot): Unit = {
    val lastCheckpointInfo = writeCheckpointFiles(snapshotToCheckpoint)
    writeLastCheckpointFile(
      snapshotToCheckpoint.deltaLog, lastCheckpointInfo, LastCheckpointInfo.checksumEnabled(spark))
    doLogCleanup(snapshotToCheckpoint)
  }

  protected[delta] def writeLastCheckpointFile(
      deltaLog: DeltaLog,
      lastCheckpointInfo: LastCheckpointInfo,
      addChecksum: Boolean): Unit = {
    withCheckpointExceptionHandling(deltaLog, "delta.lastCheckpoint.write.error") {
      val suppressOptionalFields = spark.sessionState.conf.getConf(
        DeltaSQLConf.SUPPRESS_OPTIONAL_LAST_CHECKPOINT_FIELDS)
      val lastCheckpointInfoToWrite = lastCheckpointInfo
      val json = LastCheckpointInfo.serializeToJson(
        lastCheckpointInfoToWrite,
        addChecksum,
        suppressOptionalFields)
      store.write(LAST_CHECKPOINT, Iterator(json), overwrite = true, newDeltaHadoopConf())
    }
  }

  protected def writeCheckpointFiles(
      snapshotToCheckpoint: Snapshot): LastCheckpointInfo = {
    Checkpoints.writeCheckpoint(spark, this, snapshotToCheckpoint)
  }

  /** Returns information about the most recent checkpoint. */
  private[delta] def readLastCheckpointFile(): Option[LastCheckpointInfo] = {
    loadMetadataFromFile(0)
  }

  /** Loads the checkpoint metadata from the _last_checkpoint file. */
  private def loadMetadataFromFile(tries: Int): Option[LastCheckpointInfo] =
    recordDeltaOperation(self, "delta.deltaLog.loadMetadataFromFile") {
      try {
        val lastCheckpointInfoJson = store.read(LAST_CHECKPOINT, newDeltaHadoopConf())
        val validate = LastCheckpointInfo.checksumEnabled(spark)
        Some(LastCheckpointInfo.deserializeFromJson(lastCheckpointInfoJson.head, validate))
      } catch {
        case _: FileNotFoundException =>
          None
        case NonFatal(e) if tries < 3 =>
          logWarning(s"Failed to parse $LAST_CHECKPOINT. This may happen if there was an error " +
            "during read operation, or a file appears to be partial. Sleeping and trying again.", e)
          Thread.sleep(1000)
          loadMetadataFromFile(tries + 1)
        case NonFatal(e) =>
          recordDeltaEvent(
            self,
            "delta.lastCheckpoint.read.corruptedJson",
            data = Map("exception" -> Utils.exceptionString(e))
          )

          logWarning(s"$LAST_CHECKPOINT is corrupted. Will search the checkpoint files directly", e)
          // Hit a partial file. This could happen on Azure as overwriting _last_checkpoint file is
          // not atomic. We will try to list all files to find the latest checkpoint and restore
          // LastCheckpointInfo from it.
          val verifiedCheckpoint = findLastCompleteCheckpointBefore(checkpointInstance = None)
          verifiedCheckpoint.map(manuallyLoadCheckpoint)
      }
    }

  /** Loads the given checkpoint manually to come up with the [[LastCheckpointInfo]] */
  protected def manuallyLoadCheckpoint(cv: CheckpointInstance): LastCheckpointInfo = {
    LastCheckpointInfo(
      version = cv.version,
      size = -1,
      parts = cv.numParts,
      sizeInBytes = None,
      numOfAddFiles = None,
      checkpointSchema = None
    )
  }

  /**
   * Finds the first verified, complete checkpoint before the given version.
   * Note that the returned checkpoint will always be < `version`.
   * @param version The checkpoint version to compare against
   */
  protected def findLastCompleteCheckpointBefore(version: Long): Option[CheckpointInstance] = {
    val upperBound = CheckpointInstance(version, CheckpointInstance.Format.SINGLE, numParts = None)
    findLastCompleteCheckpointBefore(Some(upperBound))
  }

  /**
   * Finds the first verified, complete checkpoint before the given [[CheckpointInstance]].
   * If `checkpointInstance` is passed as None, then we return the last complete checkpoint in the
   * deltalog directory.
   * @param checkpointInstance The checkpoint instance to compare against
   */
  protected def findLastCompleteCheckpointBefore(
      checkpointInstance: Option[CheckpointInstance] = None): Option[CheckpointInstance] = {
    val (upperBoundCv, startVersion) = checkpointInstance
      .collect { case cv if cv.version >= 0 => (cv, cv.version) }
      .getOrElse((CheckpointInstance.sentinelValue(versionOpt = None), 0L))
    var cur = startVersion
    val hadoopConf = newDeltaHadoopConf()

    logInfo(s"Try to find Delta last complete checkpoint before version $startVersion")
    while (cur >= 0) {
      val checkpoints = store.listFrom(
            listingPrefix(logPath, math.max(0, cur - 1000)),
            hadoopConf)
          // Checkpoint files of 0 size are invalid but Spark will ignore them silently when reading
          // such files, hence we drop them so that we never pick up such checkpoints.
          .filter { file => isCheckpointFile(file) && file.getLen != 0 }
          .map{ file => CheckpointInstance(file.getPath) }
          .takeWhile(tv => (cur == 0 || tv.version <= cur) && tv < upperBoundCv)
          .toArray
      val lastCheckpoint =
        getLatestCompleteCheckpointFromList(checkpoints, Some(upperBoundCv.version))
      if (lastCheckpoint.isDefined) {
        logInfo(s"Delta checkpoint is found at version ${lastCheckpoint.get.version}")
        return lastCheckpoint
      } else {
        cur -= 1000
      }
    }
    logInfo(s"No checkpoint found for Delta table before version $startVersion")
    None
  }

  /**
   * Given a list of checkpoint files, pick the latest complete checkpoint instance which is not
   * later than `notLaterThan`.
   */
  protected[delta] def getLatestCompleteCheckpointFromList(
      instances: Array[CheckpointInstance],
      notLaterThanVersion: Option[Long] = None): Option[CheckpointInstance] = {
    val sentinelCv = CheckpointInstance.sentinelValue(notLaterThanVersion)
    val complete = instances.filter(_ <= sentinelCv).groupBy(identity).filter {
      case (ci, matchingCheckpointInstances) =>
       ci.format match {
         case CheckpointInstance.Format.SINGLE =>
           matchingCheckpointInstances.length == 1
         case CheckpointInstance.Format.WITH_PARTS =>
           assert(ci.numParts.nonEmpty, "Multi-Part Checkpoint must have non empty numParts")
           matchingCheckpointInstances.length == ci.numParts.get
         case CheckpointInstance.Format.SENTINEL =>
           false
       }
    }
    if (complete.isEmpty) None else Some(complete.keys.max)
  }
}

object Checkpoints extends DeltaLogging {

  /** The name of the last checkpoint file */
  val LAST_CHECKPOINT_FILE_NAME = "_last_checkpoint"

  /**
   * Returns the checkpoint schema that should be written to the last checkpoint file based on
   * [[DeltaSQLConf.CHECKPOINT_SCHEMA_WRITE_THRESHOLD_LENGTH]] conf.
   */
  private[delta] def checkpointSchemaToWriteInLastCheckpointFile(
      spark: SparkSession,
      schema: StructType): Option[StructType] = {
    val checkpointSchemaSizeThreshold = spark.sessionState.conf.getConf(
      DeltaSQLConf.CHECKPOINT_SCHEMA_WRITE_THRESHOLD_LENGTH)
    Some(schema).filter(s => JsonUtils.toJson(s).length <= checkpointSchemaSizeThreshold)
  }

  /**
   * Writes out the contents of a [[Snapshot]] into a checkpoint file that
   * can be used to short-circuit future replays of the log.
   *
   * Returns the checkpoint metadata to be committed to a file. We will use the value
   * in this file as the source of truth of the last valid checkpoint.
   */
  private[delta] def writeCheckpoint(
      spark: SparkSession,
      deltaLog: DeltaLog,
      snapshot: Snapshot): LastCheckpointInfo = recordFrameProfile(
      "Delta", "Checkpoints.writeCheckpoint") {
    val hadoopConf = deltaLog.newDeltaHadoopConf()

    // The writing of checkpoints doesn't go through log store, so we need to check with the
    // log store and decide whether to use rename.
    val useRename = deltaLog.store.isPartialWriteVisible(deltaLog.logPath, hadoopConf)

    val checkpointRowCount = spark.sparkContext.longAccumulator("checkpointRowCount")
    val numOfFiles = spark.sparkContext.longAccumulator("numOfFiles")

    val sessionConf = spark.sessionState.conf
    val checkpointPartSize =
        sessionConf.getConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE)

    val numParts = checkpointPartSize.map { partSize =>
      math.ceil((snapshot.numOfFiles + snapshot.numOfRemoves).toDouble / partSize).toLong
    }.getOrElse(1L)

    val checkpointPaths = if (numParts > 1) {
      checkpointFileWithParts(snapshot.path, snapshot.version, numParts.toInt)
    } else {
      checkpointFileSingular(snapshot.path, snapshot.version) :: Nil
    }

    val numPartsOption = if (numParts > 1) {
      Some(checkpointPaths.length)
    } else {
      None
    }

    // Use the string in the closure as Path is not Serializable.
    val paths = checkpointPaths.map(_.toString)
    val base = snapshot.stateDS
      .repartition(paths.length, coalesce(col("add.path"), col("remove.path")))
      .map { action =>
        if (action.add != null) {
          numOfFiles.add(1)
        }
        action
      }
      // commitInfo, cdc, remove.tags and remove.stats are not included in the checkpoint
      // TODO: Add support for V2 Checkpoints here.
      .drop("commitInfo", "cdc", "checkpointMetadata", "sidecar")
      .withColumn("remove", col("remove").dropFields("tags", "stats"))

    val chk = buildCheckpoint(base, snapshot)
    val schema = chk.schema.asNullable

    val (factory, serConf) = {
      val format = new ParquetFileFormat()
      val job = Job.getInstance(hadoopConf)
      (format.prepareWrite(spark, job, Map.empty, schema),
        new SerializableConfiguration(job.getConfiguration))
    }

    // This is a hack to get spark to write directly to a file.
    val qe = chk.queryExecution
    def executeFinalCheckpointFiles(): Array[SerializableFileStatus] = qe
      .executedPlan
      .execute()
      .mapPartitionsWithIndex { case (index, iter) =>
        val finalPath = new Path(paths(index))
        val writtenPath =
          if (useRename) {
            // Two instances of the same task may run at the same time in some cases (e.g.,
            // speculation, stage retry), so generate the temp path here to avoid two tasks
            // using the same path.
            val tempPath =
              new Path(finalPath.getParent, s".${finalPath.getName}.${UUID.randomUUID}.tmp")
            DeltaFileOperations.registerTempFileDeletionTaskFailureListener(serConf.value, tempPath)
            tempPath
          } else {
            finalPath
          }
        val fs = writtenPath.getFileSystem(serConf.value)
        val writeAction = () => {
          try {
            val writer = factory.newInstance(
              writtenPath.toString,
              schema,
              new TaskAttemptContextImpl(
                new JobConf(serConf.value),
                new TaskAttemptID("", 0, TaskType.REDUCE, 0, 0)))

            iter.foreach { row =>
              checkpointRowCount.add(1)
              writer.write(row)
            }
            // Note: `writer.close()` is not put in a `finally` clause because we don't want to
            // close it when an exception happens. Closing the file would flush the content to the
            // storage and create an incomplete file. A concurrent reader might see it and fail.
            // This would leak resources but we don't have a way to abort the storage request here.
            writer.close()
          } catch {
            case e: org.apache.hadoop.fs.FileAlreadyExistsException if !useRename =>
              if (fs.exists(writtenPath)) {
                // The file has been written by a zombie task. We can just use this checkpoint file
                // rather than failing a Delta commit.
              } else {
                throw e
              }
          }
        }
        if (isGCSPath(serConf.value, writtenPath)) {
          // GCS may upload an incomplete file when the current thread is interrupted, hence we move
          // the write to a new thread so that the write cannot be interrupted.
          // TODO Remove this hack when the GCS Hadoop connector fixes the issue.
          DeltaFileOperations.runInNewThread("delta-gcs-checkpoint-write") {
            writeAction()
          }
        } else {
          writeAction()
        }
        if (useRename) {
          renameAndCleanupTempPartFile(writtenPath, finalPath, fs)
        }
        val finalPathFileStatus = try {
          fs.getFileStatus(finalPath)
        } catch {
          case _: FileNotFoundException if useRename =>
            throw DeltaErrors.failOnCheckpointRename(writtenPath, finalPath)
        }

        Iterator(SerializableFileStatus.fromStatus(finalPathFileStatus))
      }.collect()

    val finalCheckpointFiles = SQLExecution.withNewExecutionId(qe, Some("Delta checkpoint")) {
      executeFinalCheckpointFiles()
    }

    val checkpointSizeInBytes = finalCheckpointFiles.map(_.length).sum
    if (numOfFiles.value != snapshot.numOfFiles) {
      throw DeltaErrors.checkpointMismatchWithSnapshot
    }

    // Attempting to write empty checkpoint
    if (checkpointRowCount.value == 0) {
      logWarning(DeltaErrors.EmptyCheckpointErrorMessage)
    }
    LastCheckpointInfo(
      version = snapshot.version,
      size = checkpointRowCount.value,
      parts = numPartsOption,
      sizeInBytes = Some(checkpointSizeInBytes),
      numOfAddFiles = Some(snapshot.numOfFiles),
      checkpointSchema = checkpointSchemaToWriteInLastCheckpointFile(spark, schema)
    )
  }

  /**
   * Helper method to rename a `tempPath` checkpoint part file to `finalPath` checkpoint part file.
   * This also tries to handle any race conditions with Zombie tasks.
   */
  private[delta] def renameAndCleanupTempPartFile(
      tempPath: Path, finalPath: Path, fs: FileSystem): Unit = {
    // If rename fails because the final path already exists, it's ok -- some zombie
    // task probably got there first.
    // We rely on the fact that all checkpoint writers write the same content to any given
    // checkpoint part file. So it shouldn't matter which writer wins the race.
    val renameSuccessful = try {
      // Note that the fs.exists check here is redundant as fs.rename should fail if destination
      // file already exists as per File System spec. But the LocalFS doesn't follow this and it
      // overrides the final path even if it already exists. So we use exists here to handle that
      // case.
      // TODO: Remove isTesting and fs.exists check after fixing LocalFS
      if (Utils.isTesting && fs.exists(finalPath)) {
        false
      } else {
        fs.rename(tempPath, finalPath)
      }
    } catch {
      case _: org.apache.hadoop.fs.FileAlreadyExistsException => false
    }
    if (!renameSuccessful) {
      try {
        fs.delete(tempPath, false)
      } catch { case NonFatal(e) =>
        logWarning(s"Error while deleting the temporary checkpoint part file $tempPath", e)
      }
    }
  }

  // scalastyle:off line.size.limit
  /**
   * All GCS paths can only have the scheme of "gs". Note: the scheme checking is case insensitive.
   * See:
   * - https://github.com/databricks/hadoop-connectors/blob/master/gcs/src/main/java/com/google/cloud/hadoop/fs/gcs/GoogleHadoopFileSystemBase.java#L493
   * - https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/v2.2.3/gcsio/src/main/java/com/google/cloud/hadoop/gcsio/GoogleCloudStorageFileSystem.java#L88
   */
  // scalastyle:on line.size.limit
  private[delta] def isGCSPath(hadoopConf: Configuration, path: Path): Boolean = {
    val scheme = path.toUri.getScheme
    if (scheme != null) {
      scheme.equalsIgnoreCase("gs")
    } else {
      // When the schema is not available in the path, we check the file system scheme resolved from
      // the path.
      path.getFileSystem(hadoopConf).getScheme.equalsIgnoreCase("gs")
    }
  }

  /**
   * Modify the contents of the add column based on the table properties
   */
  private[delta] def buildCheckpoint(state: DataFrame, snapshot: Snapshot): DataFrame = {
    val additionalCols = new mutable.ArrayBuffer[Column]()
    val sessionConf = state.sparkSession.sessionState.conf
    if (Checkpoints.shouldWriteStatsAsJson(snapshot)) {
      additionalCols += col("add.stats").as("stats")
    }
    // We provide fine grained control using the session conf for now, until users explicitly
    // opt in our out of the struct conf.
    val includeStructColumns = shouldWriteStatsAsStruct(sessionConf, snapshot)
    if (includeStructColumns) {
      val partitionValues = Checkpoints.extractPartitionValues(
        snapshot.metadata.partitionSchema, "add.partitionValues")
      additionalCols ++= partitionValues
    }
    state.withColumn("add",
      when(col("add").isNotNull, struct(Seq(
        col("add.path"),
        col("add.partitionValues"),
        col("add.size"),
        col("add.modificationTime"),
        col("add.dataChange"), // actually not really useful here
        col("add.tags"),
        col("add.deletionVector"),
        col("add.baseRowId"),
        col("add.defaultRowCommitVersion")) ++
        additionalCols: _*
      ))
    )
  }

  def shouldWriteStatsAsStruct(conf: SQLConf, snapshot: Snapshot): Boolean = {
    DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.fromMetaData(snapshot.metadata)
  }

  def shouldWriteStatsAsJson(snapshot: Snapshot): Boolean = {
    DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_JSON.fromMetaData(snapshot.metadata)
  }

  val STRUCT_PARTITIONS_COL_NAME = "partitionValues_parsed"
  val STRUCT_STATS_COL_NAME = "stats_parsed"

  /**
   * Creates a nested struct column of partition values that extract the partition values
   * from the original MapType.
   */
  def extractPartitionValues(partitionSchema: StructType, partitionValuesColName: String):
      Option[Column] = {
    val partitionValues = partitionSchema.map { field =>
      val physicalName = DeltaColumnMapping.getPhysicalName(field)
      val attribute = UnresolvedAttribute.quotedString(partitionValuesColName)
      new Column(Cast(
        ElementAt(
          attribute,
          Literal(physicalName),
          failOnError = false),
        field.dataType,
        ansiEnabled = false)
      ).as(physicalName)
    }
    if (partitionValues.isEmpty) {
      None
    } else Some(struct(partitionValues: _*).as(STRUCT_PARTITIONS_COL_NAME))
  }
}

object V2Checkpoint {
  /** Format for V2 Checkpoints */
  sealed abstract class Format(val name: String) {
    def fileFormat: FileFormat
  }

  def toFormat(fileName: String): Format = fileName match {
    case _ if fileName.endsWith(Format.JSON.name) => Format.JSON
    case _ if fileName.endsWith(Format.PARQUET.name) => Format.PARQUET
    case _ => throw new IllegalStateException(s"Unknown v2 checkpoint file format: ${fileName}")
  }

  object Format {
    /** json v2 checkpoint */
    object JSON extends Format("json") {
      override def fileFormat: FileFormat = DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_JSON
    }

    /** parquet v2 checkpoint */
    object PARQUET extends Format("parquet") {
      override def fileFormat: FileFormat = DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET
    }

    /** All valid formats for the top level file of v2 checkpoints. */
    val ALL: Set[Format] = Set(Format.JSON, Format.PARQUET)

    /** The string representations of all the valid formats. */
    val ALL_AS_STRINGS: Set[String] = ALL.map(_.name)
  }
}

object CheckpointPolicy {

  sealed abstract class Policy(val name: String) {
    override def toString: String = name
    def needsV2CheckpointSupport: Boolean = true
  }

  /**
   * Write classic single file/multi-part checkpoints when this policy is enabled.
   * Note that [[V2CheckpointTableFeature]] is not required for this checkpoint policy.
   */
  case object Classic extends Policy("classic") {
    override def needsV2CheckpointSupport: Boolean = false
  }

  /**
   * Write V2 checkpoints when this policy is enabled.
   * This needs [[V2CheckpointTableFeature]] to be enabled on the table.
   */
  case object V2 extends Policy("v2")

  /** ALl checkpoint policies */
  val ALL: Seq[Policy] = Seq(Classic, V2)

  /** Converts a `name` String into a [[Policy]] */
  def fromName(name: String): Policy = ALL.find(_.name == name).getOrElse {
    throw new IllegalArgumentException(s"Invalid policy $name")
  }
}
