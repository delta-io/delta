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

package org.apache.spark.sql.delta

import java.io.FileNotFoundException
import java.util.UUID

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.actions.{Metadata, SingleAction}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.delta.util.FileNames._
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{JobConf, TaskAttemptContextImpl, TaskAttemptID}
import org.apache.hadoop.mapreduce.{Job, TaskType}

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.{col, struct, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * Records information about a checkpoint.
 *
 * @param version the version of this checkpoint
 * @param size the number of actions in the checkpoint
 * @param parts the number of parts when the checkpoint has multiple parts. None if this is a
 *              singular checkpoint
 */
case class CheckpointMetaData(
    version: Long,
    size: Long,
    parts: Option[Int])

/**
 * A class to help with comparing checkpoints with each other, where we may have had concurrent
 * writers that checkpoint with different number of parts.
 */
case class CheckpointInstance(
    version: Long,
    numParts: Option[Int]) extends Ordered[CheckpointInstance] {
  /**
   * Due to lexicographic sorting, a version with more parts will appear after a version with
   * less parts during file listing. We use that logic here as well.
   */
  def isEarlierThan(other: CheckpointInstance): Boolean = {
    if (other == CheckpointInstance.MaxValue) return true
    version < other.version ||
        (version == other.version && numParts.forall(_ < other.numParts.getOrElse(1)))
  }

  def isNotLaterThan(other: CheckpointInstance): Boolean = {
    if (other == CheckpointInstance.MaxValue) return true
    version <= other.version
  }

  def getCorrespondingFiles(path: Path): Seq[Path] = {
    assert(this != CheckpointInstance.MaxValue, "Can't get files for CheckpointVersion.MaxValue.")
    numParts match {
      case None => checkpointFileSingular(path, version) :: Nil
      case Some(parts) => checkpointFileWithParts(path, version, parts)
    }
  }

  override def compare(that: CheckpointInstance): Int = {
    if (version == that.version) {
      numParts.getOrElse(1) - that.numParts.getOrElse(1)
    } else {
      // we need to guard against overflow. We just can't return (this - that).toInt
      if (version - that.version < 0) -1 else 1
    }
  }
}

object CheckpointInstance {
  def apply(path: Path): CheckpointInstance = {
    CheckpointInstance(checkpointVersion(path), numCheckpointParts(path))
  }

  def apply(metadata: CheckpointMetaData): CheckpointInstance = {
    CheckpointInstance(metadata.version, metadata.parts)
  }

  val MaxValue: CheckpointInstance = CheckpointInstance(-1, None)
}

trait Checkpoints extends DeltaLogging {
  self: DeltaLog =>

  def logPath: Path
  def dataPath: Path
  def snapshot: Snapshot
  protected def store: LogStore
  protected def metadata: Metadata

  /** Used to clean up stale log files. */
  protected def doLogCleanup(): Unit

  /** The path to the file that holds metadata about the most recent checkpoint. */
  val LAST_CHECKPOINT = new Path(logPath, "_last_checkpoint")

  /**
   * Creates a checkpoint using the default snapshot.
   */
  def checkpoint(): Unit = checkpoint(snapshot)

  /**
   * Creates a checkpoint using snapshotToCheckpoint. By default it uses the current log version.
   */
  def checkpoint(snapshotToCheckpoint: Snapshot): Unit =
    recordDeltaOperation(this, "delta.checkpoint") {
      if (snapshotToCheckpoint.version < 0) {
        throw DeltaErrors.checkpointNonExistTable(dataPath)
      }
      val checkpointMetaData = writeCheckpointFiles(snapshotToCheckpoint)
      val json = JsonUtils.toJson(checkpointMetaData)
      store.write(LAST_CHECKPOINT, Iterator(json), overwrite = true)

      doLogCleanup()
    }

  protected def writeCheckpointFiles(snapshotToCheckpoint: Snapshot): CheckpointMetaData = {
    Checkpoints.writeCheckpoint(spark, this, snapshotToCheckpoint)
  }

  /** Returns information about the most recent checkpoint. */
  private[delta] def lastCheckpoint: Option[CheckpointMetaData] = {
    loadMetadataFromFile(0)
  }

  /** Loads the checkpoint metadata from the _last_checkpoint file. */
  private def loadMetadataFromFile(tries: Int): Option[CheckpointMetaData] = {
    try {
      val checkpointMetadataJson = store.read(LAST_CHECKPOINT)
      val checkpointMetadata =
        JsonUtils.mapper.readValue[CheckpointMetaData](checkpointMetadataJson.head)
      Some(checkpointMetadata)
    } catch {
      case _: FileNotFoundException =>
        None
      case NonFatal(e) if tries < 3 =>
        logWarning(s"Failed to parse $LAST_CHECKPOINT. This may happen if there was an error " +
          "during read operation, or a file appears to be partial. Sleeping and trying again.", e)
        Thread.sleep(1000)
        loadMetadataFromFile(tries + 1)
      case NonFatal(e) =>
        logWarning(s"$LAST_CHECKPOINT is corrupted. Will search the checkpoint files directly", e)
        // Hit a partial file. This could happen on Azure as overwriting _last_checkpoint file is
        // not atomic. We will try to list all files to find the latest checkpoint and restore
        // CheckpointMetaData from it.
        val verifiedCheckpoint = findLastCompleteCheckpoint(CheckpointInstance(-1L, None))
        verifiedCheckpoint.map(manuallyLoadCheckpoint)
    }
  }

  /** Loads the given checkpoint manually to come up with the CheckpointMetaData */
  protected def manuallyLoadCheckpoint(cv: CheckpointInstance): CheckpointMetaData = {
    CheckpointMetaData(cv.version, -1L, cv.numParts)
  }

  /**
   * Finds the first verified, complete checkpoint before the given version.
   *
   * @param cv The CheckpointVersion to compare against
   */
  protected def findLastCompleteCheckpoint(cv: CheckpointInstance): Option[CheckpointInstance] = {
    var cur = math.max(cv.version, 0L)
    while (cur >= 0) {
      val checkpoints = store.listFrom(checkpointPrefix(logPath, math.max(0, cur - 1000)))
          .map(_.getPath)
          .filter(isCheckpointFile)
          .map(CheckpointInstance(_))
          .takeWhile(tv => (cur == 0 || tv.version <= cur) && tv.isEarlierThan(cv))
          .toArray
      val lastCheckpoint = getLatestCompleteCheckpointFromList(checkpoints, cv)
      if (lastCheckpoint.isDefined) {
        return lastCheckpoint
      } else {
        cur -= 1000
      }
    }
    None
  }

  /**
   * Given a list of checkpoint files, pick the latest complete checkpoint instance which is not
   * later than `notLaterThan`.
   */
  protected def getLatestCompleteCheckpointFromList(
      instances: Array[CheckpointInstance],
      notLaterThan: CheckpointInstance): Option[CheckpointInstance] = {
    val complete = instances.filter(_.isNotLaterThan(notLaterThan)).groupBy(identity).filter {
      case (CheckpointInstance(_, None), inst) => inst.length == 1
      case (CheckpointInstance(_, Some(parts)), inst) => inst.length == parts
    }
    complete.keys.toArray.sorted.lastOption
  }
}

object Checkpoints extends DeltaLogging {
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
      snapshot: Snapshot): CheckpointMetaData = {
    import SingleAction._

    // The writing of checkpoints doesn't go through log store, so we need to check with the
    // log store and decide whether to use rename.
    val useRename = deltaLog.store.isPartialWriteVisible(deltaLog.logPath)

    val checkpointSize = spark.sparkContext.longAccumulator("checkpointSize")
    val numOfFiles = spark.sparkContext.longAccumulator("numOfFiles")
    // Use the string in the closure as Path is not Serializable.
    val path = checkpointFileSingular(snapshot.path, snapshot.version).toString
    val base = snapshot.state
      .repartition(1)
      .map { action =>
        if (action.add != null) {
          numOfFiles.add(1)
        }
        action
      }.drop("commitInfo", "cdc")

    val chk = buildCheckpoint(base, snapshot)
    val schema = chk.schema.asNullable

    val (factory, serConf) = {
      val format = new ParquetFileFormat()
      val job = Job.getInstance()
      (format.prepareWrite(spark, job, Map.empty, schema),
        new SerializableConfiguration(job.getConfiguration))
    }

    val writtenPath = chk
      .queryExecution // This is a hack to get spark to write directly to a file.
      .executedPlan
      .execute()
      .mapPartitions { iter =>
        val writtenPath =
          if (useRename) {
            val p = new Path(path)
            // Two instances of the same task may run at the same time in some cases (e.g.,
            // speculation, stage retry), so generate the temp path here to avoid two tasks
            // using the same path.
            val tempPath = new Path(p.getParent, s".${p.getName}.${UUID.randomUUID}.tmp")
            DeltaFileOperations.registerTempFileDeletionTaskFailureListener(serConf.value, tempPath)
            tempPath.toString
          } else {
            path
          }
        try {
          val writer = factory.newInstance(
            writtenPath,
            schema,
            new TaskAttemptContextImpl(
              new JobConf(serConf.value),
              new TaskAttemptID("", 0, TaskType.REDUCE, 0, 0)))

          iter.foreach { row =>
            checkpointSize.add(1)
            writer.write(row)
          }
          writer.close()
        } catch {
          case e: org.apache.hadoop.fs.FileAlreadyExistsException if !useRename =>
            val p = new Path(writtenPath)
            if (p.getFileSystem(serConf.value).exists(p)) {
              // The file has been written by a zombie task. We can just use this checkpoint file
              // rather than failing a Delta commit.
            } else {
              throw e
            }
        }
        Iterator(writtenPath)
      }.collect().head

    if (useRename) {
      val src = new Path(writtenPath)
      val dest = new Path(path)
      val fs = dest.getFileSystem(spark.sessionState.newHadoopConf)
      var renameDone = false
      try {
        if (fs.rename(src, dest)) {
          renameDone = true
        } else {
          // There should be only one writer writing the checkpoint file, so there must be
          // something wrong here.
          throw new IllegalStateException(s"Cannot rename $src to $dest")
        }
      } finally {
        if (!renameDone) {
          fs.delete(src, false)
        }
      }
    }

    if (numOfFiles.value != snapshot.numOfFiles) {
      throw new IllegalStateException(
        "State of the checkpoint doesn't match that of the snapshot.")
    }

    // Attempting to write empty checkpoint
    if (checkpointSize.value == 0) {
      logWarning(DeltaErrors.EmptyCheckpointErrorMessage)
    }
    CheckpointMetaData(snapshot.version, checkpointSize.value, None)
  }

  /**
   * Modify the contents of the add column based on the table properties
   */
  private[delta] def buildCheckpoint(state: DataFrame, snapshot: Snapshot): DataFrame = {
    val additionalCols = new mutable.ArrayBuffer[Column]()
    if (DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_JSON.fromMetaData(snapshot.metadata)) {
      additionalCols += col("add.stats").as("stats")
    }
    val sessionConf = state.sparkSession.sessionState.conf
    // We provide fine grained control using the session conf for now, until users explicitly
    // opt in our out of the struct conf.
    val includeStructColumns = getWriteStatsAsStructConf(sessionConf, snapshot)
    if (includeStructColumns) {
      additionalCols ++= CheckpointV2.extractPartitionValues(snapshot.metadata.partitionSchema)
    }
    state.withColumn("add",
      when(col("add").isNotNull, struct(Seq(
        col("add.path"),
        col("add.partitionValues"),
        col("add.size"),
        col("add.modificationTime"),
        col("add.dataChange"), // actually not really useful here
        col("add.tags")) ++
        additionalCols: _*
      ))
    )
  }

  def getWriteStatsAsStructConf(conf: SQLConf, snapshot: Snapshot): Boolean = {
    DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT
      .fromMetaData(snapshot.metadata)
      .getOrElse(conf.getConf(DeltaSQLConf.DELTA_CHECKPOINT_V2_ENABLED))
  }
}

/**
 * Utility methods for generating and using V2 checkpoints. V2 checkpoints have partition values and
 * statistics as struct fields of the `add` column.
 */
object CheckpointV2 {
  val PARTITIONS_COL_NAME = "partitionValues_parsed"
  val STATS_COL_NAME = "stats_parsed"

  /**
   * Creates a nested struct column of partition values that extract the partition values
   * from the original MapType.
   */
  def extractPartitionValues(partitionSchema: StructType): Option[Column] = {
    val partitionValues = partitionSchema.map { field =>
      val colName = field.name
      new Column(UnresolvedAttribute("add" :: "partitionValues" :: colName :: Nil).name)
        .cast(field.dataType)
        .as(colName)
    }
    if (partitionValues.isEmpty) None else Some(struct(partitionValues: _*).as(PARTITIONS_COL_NAME))
  }
}
