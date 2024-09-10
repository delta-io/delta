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
import org.apache.spark.sql.delta.actions.{Action, CheckpointMetadata, Metadata, SidecarFile, SingleAction}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.{DeltaFileOperations, DeltaLogGroupingIterator, FileNames}
import org.apache.spark.sql.delta.util.FileNames._
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapred.{JobConf, TaskAttemptContextImpl, TaskAttemptID}
import org.apache.hadoop.mapreduce.{Job, TaskType}

import org.apache.spark.TaskContext
import org.apache.spark.internal.MDC
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Cast, ElementAt, Literal}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.{coalesce, col, struct, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.Utils

/**
 * A class to help with comparing checkpoints with each other, where we may have had concurrent
 * writers that checkpoint with different number of parts.
 * The `numParts` field will be present only for multipart checkpoints (represented by
 * Format.WITH_PARTS).
 * The `fileName` field is present only for V2 Checkpoints (represented by Format.V2)
 * These additional fields are used as a tie breaker when comparing multiple checkpoint
 * instance of same Format for the same `version`.
 */
case class CheckpointInstance(
    version: Long,
    format: CheckpointInstance.Format,
    fileName: Option[String] = None,
    numParts: Option[Int] = None) extends Ordered[CheckpointInstance] {

  // Assert that numParts are present when checkpoint format is Format.WITH_PARTS.
  // For other formats, numParts must be None.
  require((format == CheckpointInstance.Format.WITH_PARTS) == numParts.isDefined,
    s"numParts ($numParts) must be present for checkpoint format" +
      s" ${CheckpointInstance.Format.WITH_PARTS.name}")
  // Assert that filePath is present only when checkpoint format is Format.V2.
  // For other formats, filePath must be None.
  require((format == CheckpointInstance.Format.V2) == fileName.isDefined,
    s"fileName ($fileName) must be present for checkpoint format" +
      s" ${CheckpointInstance.Format.V2.name}")

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
      // Treat single file checkpoints also as V2 Checkpoints because we don't know if it is
      // actually a V2 checkpoint until we read it.
      case CheckpointInstance.Format.V2 | CheckpointInstance.Format.SINGLE =>
        assert(cpFiles.size == 1)
        val fileStatus = cpFiles.head
        if (format == CheckpointInstance.Format.V2) {
          val hadoopConf = deltaLog.newDeltaHadoopConf()
          UninitializedV2CheckpointProvider(
            version,
            fileStatus,
            logPath,
            hadoopConf,
            deltaLog.options,
            deltaLog.store,
            lastCheckpointInfo)
        } else {
          UninitializedV1OrV2ParquetCheckpointProvider(
            version, fileStatus, logPath, lastCheckpointInfo)
        }
      case CheckpointInstance.Format.WITH_PARTS =>
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
      // Treat Single File checkpoints also as V2 Checkpoints because we don't know if it is
      // actually a V2 checkpoint until we read it.
      case format if format.usesSidecars =>
        val checkpointFileName = format match {
          case CheckpointInstance.Format.V2 => fileName.get
          case CheckpointInstance.Format.SINGLE => checkpointFileSingular(logPath, version).getName
          case other =>
            throw new IllegalStateException(s"Unknown checkpoint format $other supporting sidecars")
        }
        val fileStatus = filesForCheckpointConstruction
          .find(_.getPath.getName == checkpointFileName)
          .getOrElse {
            throw new IllegalStateException("Failed in getting the file information for:\n" +
              fileName.get + "\namong\n" +
              filesForCheckpointConstruction.map(_.getPath.getName).mkString(" -", "\n -", ""))
          }
        Seq(fileStatus)
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
   * 4. For V2 Checkpoints corresponding to same version, we use the fileName as tie breaker.
   */
  override def compare(other: CheckpointInstance): Int = {
      (version, format, numParts, fileName) compare
        (other.version, other.format, other.numParts, other.fileName)
  }
}

object CheckpointInstance {
  sealed abstract class Format(val ordinal: Int, val name: String) extends Ordered[Format] {
    override def compare(other: Format): Int = ordinal compare other.ordinal
    def usesSidecars: Boolean = this.isInstanceOf[FormatUsesSidecars]
  }
  trait FormatUsesSidecars

  object Format {
    def unapply(name: String): Option[Format] = name match {
      case SINGLE.name => Some(SINGLE)
      case WITH_PARTS.name => Some(WITH_PARTS)
      case V2.name => Some(V2)
      case _ => None
    }

    /** single-file checkpoint format */
    object SINGLE extends Format(0, "SINGLE") with FormatUsesSidecars
    /** multi-file checkpoint format */
    object WITH_PARTS extends Format(1, "WITH_PARTS")
    /** V2 Checkpoint format */
    object V2 extends Format(2, "V2") with FormatUsesSidecars
    /** Sentinel, for internal use only */
    object SENTINEL extends Format(Int.MaxValue, "SENTINEL")
  }

  def apply(path: Path): CheckpointInstance = {
    // Three formats to worry about:
    // * <version>.checkpoint.parquet
    // * <version>.checkpoint.<i>.<n>.parquet
    // * <version>.checkpoint.<u>.parquet where u is a unique string
    path.getName.split("\\.") match {
      case Array(v, "checkpoint", uniqueStr, format) if Seq("json", "parquet").contains(format) =>
        CheckpointInstance(
          version = v.toLong,
          format = Format.V2,
          numParts = None,
          fileName = Some(path.getName))
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
      fileName = metadata.v2Checkpoint.map(_.path),
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
        logWarning("Error when writing checkpoint-related files", e)
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

  /**
   * Creates a checkpoint at given version. Does not invoke metadata cleanup as part of it.
   * @param version - version at which we want to create a checkpoint.
   */
  def createCheckpointAtVersion(version: Long): Unit =
    recordDeltaOperation(this, "delta.createCheckpointAtVersion") {
      val snapshot = getSnapshotAt(version)
      withCheckpointExceptionHandling(this, "delta.checkpoint.sync.error") {
        if (snapshot.version < 0) {
          throw DeltaErrors.checkpointNonExistTable(dataPath)
        }
        writeCheckpointFiles(snapshot)
      }
    }

  def checkpointAndCleanUpDeltaLog(
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

  protected def writeCheckpointFiles(snapshotToCheckpoint: Snapshot): LastCheckpointInfo = {
    // With Coordinated-Commits, commit files are not guaranteed to be backfilled immediately in the
    // _delta_log dir. While it is possible to compute a checkpoint file without backfilling,
    // writing the checkpoint file in the log directory before backfilling the relevant commits
    // will leave gaps in the dir structure. This can cause issues for readers that are not
    // communicating with the commit-coordinator.
    //
    // Sample directory structure with a gap if we don't backfill commit files:
    // _delta_log/
    //   _commits/
    //     00017.$uuid.json
    //     00018.$uuid.json
    //   00015.json
    //   00016.json
    //   00018.checkpoint.parquet
    // TODO(table-identifier-plumbing): Plumb the right tableIdentifier from the Checkpoint Hook
    //  and pass it to `ensureCommitFilesBackfilled`.
    snapshotToCheckpoint.ensureCommitFilesBackfilled(tableIdentifierOpt = None)
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
          logWarning(log"Failed to parse ${MDC(DeltaLogKeys.PATH, LAST_CHECKPOINT)}. " +
            log"This may happen if there was an error during read operation, " +
            log"or a file appears to be partial. Sleeping and trying again.", e)
          Thread.sleep(1000)
          loadMetadataFromFile(tries + 1)
        case NonFatal(e) =>
          recordDeltaEvent(
            self,
            "delta.lastCheckpoint.read.corruptedJson",
            data = Map("exception" -> Utils.exceptionString(e))
          )

          logWarning(log"${MDC(DeltaLogKeys.PATH, LAST_CHECKPOINT)} is corrupted. " +
            log"Will search the checkpoint files directly", e)
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
  private[delta] def findLastCompleteCheckpointBefore(version: Long): Option[CheckpointInstance] = {
    val upperBound = CheckpointInstance(version, CheckpointInstance.Format.SINGLE, numParts = None)
    findLastCompleteCheckpointBefore(Some(upperBound))
  }

  /**
   * Finds the first verified, complete checkpoint before the given [[CheckpointInstance]].
   * If `checkpointInstance` is passed as None, then we return the last complete checkpoint in the
   * deltalog directory.
   * @param checkpointInstance The checkpoint instance to compare against
   */
  private[delta] def findLastCompleteCheckpointBefore(
      checkpointInstance: Option[CheckpointInstance] = None): Option[CheckpointInstance] = {
    val eventData = mutable.Map[String, String]()
    val startTimeMs = System.currentTimeMillis()
    def sendUsageLog(): Unit = {
      eventData("totalTimeTakenMs") = (System.currentTimeMillis() - startTimeMs).toString
      recordDeltaEvent(
        self, opType = "delta.findLastCompleteCheckpointBefore", data = eventData.toMap)
    }
    try {
      val resultOpt = findLastCompleteCheckpointBeforeInternal(eventData, checkpointInstance)
      eventData("resultantCheckpointVersion") = resultOpt.map(_.version).getOrElse(-1L).toString
      sendUsageLog()
      resultOpt
    } catch {
      case e@(NonFatal(_) | _: InterruptedException | _: java.io.InterruptedIOException |
              _: java.nio.channels.ClosedByInterruptException) =>
        eventData("exception") = Utils.exceptionString(e)
        sendUsageLog()
        throw e
    }
  }

  private def findLastCompleteCheckpointBeforeInternal(
      eventData: mutable.Map[String, String],
      checkpointInstance: Option[CheckpointInstance]): Option[CheckpointInstance] = {
    val upperBoundCv =
      checkpointInstance
        // If someone passes the upperBound as 0 or sentinel value, we should not do backward
        // listing. Instead we should list the entire directory from 0 and return the latest
        // available checkpoint.
        .filterNot(cv => cv.version < 0 || cv.version == CheckpointInstance.MaxValue.version)
        .getOrElse {
          logInfo("Try to find Delta last complete checkpoint")
          eventData("listingFromZero") = true.toString
          return findLastCompleteCheckpoint()
        }
    eventData("efficientBackwardListingEnabled") = true.toString
    eventData("upperBoundVersion") = upperBoundCv.version.toString
    eventData("upperBoundCheckpointType") = upperBoundCv.format.name
    var iterations: Long = 0L
    var numFilesScanned: Long = 0L
    logInfo(log"Try to find Delta last complete checkpoint before version " +
      log"${MDC(DeltaLogKeys.VERSION, upperBoundCv.version)}")
    var listingEndVersion = upperBoundCv.version

    // Do a backward listing from the upperBoundCv version. We list in chunks of 1000 versions.
    // ...........................................................................................
    //                                                                        |
    //                                                               upper bound cv's version
    //                                          [ iter-1 looks in this window ]
    //                          [ iter-2 window ]
    //         [ iter-3 window  ]
    //              |
    //        latest checkpoint
    while (listingEndVersion >= 0) {
      iterations += 1
      eventData("iterations") = iterations.toString
      val listingStartVersion = math.max(0, listingEndVersion - 1000)
      val checkpoints = store
        .listFrom(listingPrefix(logPath, listingStartVersion), newDeltaHadoopConf())
        .map { file => numFilesScanned += 1 ; file }
        .collect {
          // Also collect delta files from the listing result so that the next takeWhile helps us
          // terminate iterator early if no checkpoint exists upto the `listingEndVersion`
          // version.
          case DeltaFile(file, version) => (file, FileType.DELTA, version)
          case CheckpointFile(file, version) => (file, FileType.CHECKPOINT, version)
        }
        .takeWhile { case (_, _, currentFileVersion) => currentFileVersion <= listingEndVersion }
        // Checkpoint files of 0 size are invalid but Spark will ignore them silently when
        // reading such files, hence we drop them so that we never pick up such checkpoints.
        .collect { case (file, FileType.CHECKPOINT, _) if file.getLen > 0 =>
          CheckpointInstance(file.getPath)
        }
        // We still need to filter on `upperBoundCv` to eliminate checkpoint files which are
        // same version as `upperBoundCv` but have higher [[CheckpointInstance.Format]]. e.g.
        // upperBoundCv is a V2_Checkpoint and we have a Single part checkpoint and a v2
        // checkpoint at the same version. In such a scenario, we should not consider the
        // v2 checkpoint as it is nor lower than the upperBoundCv.
        .filter(_ < upperBoundCv)
        .toArray
      val lastCheckpoint =
        getLatestCompleteCheckpointFromList(checkpoints, Some(upperBoundCv.version))
      eventData("numFilesScanned") = numFilesScanned.toString
      if (lastCheckpoint.isDefined) {
        logInfo(log"Delta checkpoint is found at version " +
          log"${MDC(DeltaLogKeys.VERSION, lastCheckpoint.get.version)}")
        return lastCheckpoint
      }
      listingEndVersion = listingEndVersion - 1000
    }
    logInfo(log"No checkpoint found for Delta table before version " +
      log"${MDC(DeltaLogKeys.VERSION, upperBoundCv.version)}")
    None
  }

  /** Returns the last complete checkpoint in the delta log directory (if any) */
  private def findLastCompleteCheckpoint(): Option[CheckpointInstance] = {
    val hadoopConf = newDeltaHadoopConf()
    val listingResult = store
      .listFrom(listingPrefix(logPath, 0L), hadoopConf)
      // Checkpoint files of 0 size are invalid but Spark will ignore them silently when
      // reading such files, hence we drop them so that we never pick up such checkpoints.
      .collect { case CheckpointFile(file, _) if file.getLen != 0 => file }
    new DeltaLogGroupingIterator(listingResult)
      .flatMap { case (_, files) =>
        getLatestCompleteCheckpointFromList(files.map(f => CheckpointInstance(f.getPath)).toArray)
      }.foldLeft(Option.empty[CheckpointInstance])((_, right) => Some(right))
    // ^The foldLeft here emulates the non-existing Iterator.tailOption method.
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
         case CheckpointInstance.Format.V2 =>
           matchingCheckpointInstances.length == 1
         case CheckpointInstance.Format.SENTINEL =>
           false
       }
    }
    if (complete.isEmpty) None else Some(complete.keys.max)
  }
}

object Checkpoints
  extends DeltaLogging
  {

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

    val v2CheckpointFormatOpt = {
      val policy = DeltaConfigs.CHECKPOINT_POLICY.fromMetaData(snapshot.metadata)
      if (policy.needsV2CheckpointSupport) {
        assert(CheckpointProvider.isV2CheckpointEnabled(snapshot))
        val v2Format = spark.conf.get(DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT)
        // The format of the top level file in V2 checkpoints can be configured through
        // the optional config [[DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT]].
        // If nothing is specified, we use the json format. In the future, we may
        // write json/parquet dynamically based on heuristics.
        v2Format match {
          case Some(V2Checkpoint.Format.JSON.name) | None => Some(V2Checkpoint.Format.JSON)
          case Some(V2Checkpoint.Format.PARQUET.name) => Some(V2Checkpoint.Format.PARQUET)
          case _ => throw new IllegalStateException("unknown checkpoint format")
        }
      } else {
        None
      }
    }
    val v2CheckpointEnabled = v2CheckpointFormatOpt.nonEmpty

    val checkpointRowCount = spark.sparkContext.longAccumulator("checkpointRowCount")
    val numOfFiles = spark.sparkContext.longAccumulator("numOfFiles")

    val sessionConf = spark.sessionState.conf
    val checkpointPartSize =
        sessionConf.getConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE)

    val numParts = checkpointPartSize.map { partSize =>
      math.ceil((snapshot.numOfFiles + snapshot.numOfRemoves).toDouble / partSize).toLong
    }.getOrElse(1L).toInt
    val legacyMultiPartCheckpoint = !v2CheckpointEnabled && numParts > 1

    val base = {
      val repartitioned = snapshot.stateDS
        .repartition(numParts, coalesce(col("add.path"), col("remove.path")))
        .map { action =>
          if (action.add != null) {
            numOfFiles.add(1)
          }
          action
        }
      // commitInfo, cdc and remove.tags are not included in both classic and V2 checkpoints.
      if (v2CheckpointEnabled) {
        // When V2 Checkpoint is enabled, the baseCheckpoint refers to the sidecar files which will
        // only have AddFile and RemoveFile actions. The other non-file actions will be written
        // separately after sidecar files are written.
        repartitioned
          .select("add", "remove")
          .withColumn("remove", col("remove").dropFields("tags", "stats"))
          .where("add is not null or remove is not null")
      } else {
        // When V2 Checkpoint is disabled, the baseCheckpoint refers to the main classic checkpoint
        // which has all actions except "commitInfo", "cdc", "checkpointMetadata", "sidecar".
        repartitioned
          .drop("commitInfo", "cdc", "checkpointMetadata", "sidecar")
          .withColumn("remove", col("remove").dropFields("tags", "stats"))
      }
    }

    val chk = buildCheckpoint(base, snapshot)
    val schema = chk.schema.asNullable

    val (factory, serConf) = {
      val format = new ParquetFileFormat()
      val job = Job.getInstance(hadoopConf)
      (format.prepareWrite(spark, job, Map.empty, schema),
        new SerializableConfiguration(job.getConfiguration))
    }

    // Use the SparkPath in the closure as Path is not Serializable.
    val logSparkPath = SparkPath.fromPath(snapshot.path)
    val version = snapshot.version

    // This is a hack to get spark to write directly to a file.
    val qe = chk.queryExecution
    def executeFinalCheckpointFiles(): Array[SerializableFileStatus] = qe
      .executedPlan
      .execute()
      .mapPartitions { case iter =>
        val actualNumParts = Option(TaskContext.get()).map(_.numPartitions())
          .getOrElse(numParts)
        val partition = TaskContext.getPartitionId()
        val (writtenPath, finalPath) = Checkpoints.getCheckpointWritePath(
          serConf.value,
          logSparkPath.toPath,
          version,
          actualNumParts,
          partition,
          useRename,
          v2CheckpointEnabled)
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

    if (numOfFiles.value != snapshot.numOfFiles) {
      throw DeltaErrors.checkpointMismatchWithSnapshot
    }

    val parquetFilesSizeInBytes = finalCheckpointFiles.map(_.length).sum
    var overallCheckpointSizeInBytes = parquetFilesSizeInBytes
    var overallNumCheckpointActions: Long = checkpointRowCount.value
    var checkpointSchemaToWriteInLastCheckpoint: Option[StructType] =
      Checkpoints.checkpointSchemaToWriteInLastCheckpointFile(spark, schema)

    val v2Checkpoint = if (v2CheckpointEnabled) {
      val (v2CheckpointFileStatus, nonFileActionsWriten, v2Checkpoint, checkpointSchema) =
        Checkpoints.writeTopLevelV2Checkpoint(
          v2CheckpointFormatOpt.get,
          finalCheckpointFiles,
          spark,
          schema,
          snapshot,
          deltaLog,
          overallNumCheckpointActions,
          parquetFilesSizeInBytes,
          hadoopConf,
          useRename
        )
      overallCheckpointSizeInBytes += v2CheckpointFileStatus.getLen
      overallNumCheckpointActions += nonFileActionsWriten.size
      checkpointSchemaToWriteInLastCheckpoint = checkpointSchema

      Some(v2Checkpoint)
    } else {
      None
    }

    if (!v2CheckpointEnabled && checkpointRowCount.value == 0) {
      // In case of V2 Checkpoints, zero row count is possible.
      logWarning(DeltaErrors.EmptyCheckpointErrorMessage)
    }

    // If we don't parallelize, we use None for backwards compatibility
    val checkpointParts = if (legacyMultiPartCheckpoint) Some(numParts) else None

    LastCheckpointInfo(
      version = snapshot.version,
      size = overallNumCheckpointActions,
      parts = checkpointParts,
      sizeInBytes = Some(overallCheckpointSizeInBytes),
      numOfAddFiles = Some(snapshot.numOfFiles),
      v2Checkpoint = v2Checkpoint,
      checkpointSchema = checkpointSchemaToWriteInLastCheckpoint
    )
  }

  /**
   * Generate a tuple of the file to write the checkpoint and where it may later need
   * to be copied. Should be used within a task, so that task or stage retries don't
   * create the same files.
   */
  def getCheckpointWritePath(
      conf: Configuration,
      logPath: Path,
      version: Long,
      numParts: Int,
      part: Int,
      useRename: Boolean,
      v2CheckpointEnabled: Boolean): (Path, Path) = {
    def getCheckpointWritePath(path: Path): Path = {
      if (useRename) {
        val tempPath =
          new Path(path.getParent, s".${path.getName}.${UUID.randomUUID}.tmp")
        DeltaFileOperations.registerTempFileDeletionTaskFailureListener(conf, tempPath)
        tempPath
      } else {
        path
      }
    }
    val destinationName: Path = if (v2CheckpointEnabled) {
      newV2CheckpointSidecarFile(logPath, version, numParts, part + 1)
    } else {
      if (numParts > 1) {
        assert(part < numParts, s"Asked to create part: $part of max $numParts in checkpoint.")
        checkpointFileWithParts(logPath, version, numParts)(part)
      } else {
        checkpointFileSingular(logPath, version)
      }
    }

    getCheckpointWritePath(destinationName) -> destinationName
  }

  /**
   * Writes a top-level V2 Checkpoint file which may point to multiple
   * sidecar files.
   *
   * @param v2CheckpointFormat The format in which the top-level file should be
   *                           written. Currently, json and parquet are supported.
   * @param sidecarCheckpointFiles The list of sidecar files that have already been
   *                               written. The top-level file will store this list.
   * @param spark The current spark session
   * @param sidecarSchema The schema of the sidecar parquet files.
   * @param snapshot The snapshot for which the checkpoint is being written.
   * @param deltaLog The deltaLog instance pointing to our tables deltaLog.
   * @param rowsWrittenInCheckpointJob The number of rows that were written in total
   *                                   to the sidecar files.
   * @param parquetFilesSizeInBytes The combined size of all sidecar files in bytes.
   * @param hadoopConf The hadoopConf to use for the filesystem operation.
   * @param useRename Whether we should first write to a temporary file and then
   *                  rename it to the target file name during the write.
   * @return A tuple containing
   *          1. [[FileStatus]] of the newly created top-level V2Checkpoint.
   *          2. The sequence of actions that were written to the top-level file.
   *          3. An instance of the LastCheckpointV2 containing V2-checkpoint related
   *           metadata which can later be written to LAST_CHECKPOINT
   *          4. Schema of the newly written top-level file (only for parquet files)
   */
  protected[delta] def writeTopLevelV2Checkpoint(
      v2CheckpointFormat: V2Checkpoint.Format,
      sidecarCheckpointFiles: Array[SerializableFileStatus],
      spark: SparkSession,
      sidecarSchema: StructType,
      snapshot: Snapshot,
      deltaLog: DeltaLog,
      rowsWrittenInCheckpointJob: Long,
      parquetFilesSizeInBytes: Long,
      hadoopConf: Configuration,
      useRename: Boolean) : (FileStatus, Seq[Action], LastCheckpointV2, Option[StructType]) = {
    // Write the main v2 checkpoint file.
    val sidecarFilesWritten = sidecarCheckpointFiles.map(SidecarFile(_)).toSeq
    // Filter out the sidecar schema if it is too large.
    val sidecarFileSchemaOpt =
      Checkpoints.checkpointSchemaToWriteInLastCheckpointFile(spark, sidecarSchema)
    val checkpointMetadata = CheckpointMetadata(snapshot.version)

    val nonFileActionsToWrite =
      (checkpointMetadata +: sidecarFilesWritten) ++ snapshot.nonFileActions
    val (v2CheckpointPath, checkpointSchemaToWriteInLastCheckpoint) =
      if (v2CheckpointFormat == V2Checkpoint.Format.JSON) {
        val v2CheckpointPath = newV2CheckpointJsonFile(deltaLog.logPath, snapshot.version)
        // We don't need a putIfAbsent for this write, so we set overwrite to true.
        // However, this can be dangerous if the cloud makes partial writes visible.
        val isPartialWriteVisible =
          deltaLog.store.isPartialWriteVisible(v2CheckpointPath, hadoopConf)
        deltaLog.store.write(
          v2CheckpointPath,
          nonFileActionsToWrite.map(_.json).toIterator,
          overwrite = !isPartialWriteVisible,
          hadoopConf = hadoopConf
        )
        (v2CheckpointPath, None)
      } else if (v2CheckpointFormat == V2Checkpoint.Format.PARQUET) {
        val sparkSession = spark
        // scalastyle:off sparkimplicits
        import sparkSession.implicits._
        // scalastyle:on sparkimplicits
        val dfToWrite = nonFileActionsToWrite.map(_.wrap).toDF()
        val v2CheckpointPath = newV2CheckpointParquetFile(deltaLog.logPath, snapshot.version)
        val schemaOfDfWritten = createCheckpointV2ParquetFile(
          spark, dfToWrite, v2CheckpointPath, hadoopConf, useRename)
        (v2CheckpointPath, Some(schemaOfDfWritten))
      } else {
        throw DeltaErrors.assertionFailedError(
          s"Unrecognized checkpoint V2 format: $v2CheckpointFormat")
      }
    // Main Checkpoint V2 File written successfully. Now create the last checkpoint v2 blob so
    // that we can persist it in _last_checkpoint file.
    val v2CheckpointFileStatus =
      v2CheckpointPath.getFileSystem(hadoopConf).getFileStatus(v2CheckpointPath)
    val unfilteredV2Checkpoint = LastCheckpointV2(
      fileStatus = v2CheckpointFileStatus,
      nonFileActions = Some((snapshot.nonFileActions :+ checkpointMetadata).map(_.wrap)),
      sidecarFiles = Some(sidecarFilesWritten)
    )
    (
      v2CheckpointFileStatus,
      nonFileActionsToWrite,
      trimLastCheckpointV2(unfilteredV2Checkpoint, spark),
      checkpointSchemaToWriteInLastCheckpoint
    )
  }

  /**
   * Helper method to create a V2 Checkpoint parquet file or the V2 Checkpoint Compat file.
   * V2 Checkpoint Compat files follow the same naming convention as classic checkpoints
   * and they are needed so that V2Checkpoint-unaware readers can read them to understand
   * that they don't have the capability to read table for which they were created.
   * This is needed in cases where commit 0 has been cleaned up and the reader needs to
   * read a checkpoint to read the [[Protocol]].
   */
  def createCheckpointV2ParquetFile(
      spark: SparkSession,
      ds: Dataset[Row],
      finalPath: Path,
      hadoopConf: Configuration,
      useRename: Boolean): StructType = recordFrameProfile(
        "Checkpoints", "createCheckpointV2ParquetFile") {
    val df = ds.select(
      "txn", "add", "remove", "metaData", "protocol", "domainMetadata",
      "checkpointMetadata", "sidecar")
    val schema = df.schema.asNullable
    val format = new ParquetFileFormat()
    val job = Job.getInstance(hadoopConf)
    val factory = format.prepareWrite(spark, job, Map.empty, schema)
    val serConf = new SerializableConfiguration(job.getConfiguration)
    val finalSparkPath = SparkPath.fromPath(finalPath)

    df.repartition(1)
      .queryExecution
      .executedPlan
      .execute()
      .mapPartitions { iter =>
        val actualNumParts = Option(TaskContext.get()).map(_.numPartitions()).getOrElse(1)
        require(actualNumParts == 1, "The parquet V2 checkpoint must be written in 1 file")
        val partition = TaskContext.getPartitionId()
        val finalPath = finalSparkPath.toPath
        val writePath = if (useRename) {
          val tempPath =
            new Path(finalPath.getParent, s".${finalPath.getName}.${UUID.randomUUID}.tmp")
          DeltaFileOperations.registerTempFileDeletionTaskFailureListener(serConf.value, tempPath)
          tempPath
        } else {
          finalPath
        }

        val fs = writePath.getFileSystem(serConf.value)

        val attemptId = 0
        val taskAttemptContext = new TaskAttemptContextImpl(
          new JobConf(serConf.value),
          new TaskAttemptID("", 0, TaskType.REDUCE, partition, attemptId))

        var writerOpt: Option[OutputWriter] = None

        try {
          writerOpt = Some(factory.newInstance(
            writePath.toString,
            schema,
            taskAttemptContext))

          val writer = writerOpt.get
          iter.foreach { row =>
            writer.write(row)
          }
          // Note: `writer.close()` is not put in a `finally` clause because we don't want to
          // close it when an exception happens. Closing the file would flush the content to the
          // storage and create an incomplete file. A concurrent reader might see it and fail.
          // This would leak resources but we don't have a way to abort the storage request here.
          writer.close()
        } catch {
          case _: org.apache.hadoop.fs.FileAlreadyExistsException
            if !useRename && fs.exists(writePath) =>
          // The file has been written by a zombie task. We can just use this checkpoint file
          // rather than failing a Delta commit.
          case t: Throwable =>
            throw t
        }
        if (useRename) {
          renameAndCleanupTempPartFile(writePath, finalPath, fs)
        }
        val finalPathFileStatus = try {
          fs.getFileStatus(finalPath)
        } catch {
          case _: FileNotFoundException if useRename =>
            throw DeltaErrors.failOnCheckpointRename(writePath, finalPath)
        }
        Iterator(SerializableFileStatus.fromStatus(finalPathFileStatus))
      }.collect()
    schema
  }

  /** Bounds the size of a [[LastCheckpointV2]] by removing any oversized optional fields */
  def trimLastCheckpointV2(
      lastCheckpointV2: LastCheckpointV2,
      spark: SparkSession): LastCheckpointV2 = {
    val nonFileActionThreshold =
      spark.sessionState.conf.getConf(DeltaSQLConf.LAST_CHECKPOINT_NON_FILE_ACTIONS_THRESHOLD)
    val sidecarThreshold =
      spark.sessionState.conf.getConf(DeltaSQLConf.LAST_CHECKPOINT_SIDECARS_THRESHOLD)
    lastCheckpointV2.copy(
      sidecarFiles = lastCheckpointV2.sidecarFiles.filter(_.size <= sidecarThreshold),
      nonFileActions = lastCheckpointV2.nonFileActions.filter(_.size <= nonFileActionThreshold))
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
        logWarning(log"Error while deleting the temporary checkpoint part file " +
          log"${MDC(DeltaLogKeys.PATH, tempPath)}", e)
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
        col("add.defaultRowCommitVersion"),
        col("add.clusteringProvider")) ++
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
      Column(Cast(
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
