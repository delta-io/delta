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

package org.apache.spark.sql.delta.files

// scalastyle:off import.ordering.noEmptyLine
import java.net.URI
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, FileAction}
import org.apache.spark.sql.delta.commands.cdc.CDCReader.{CDC_LOCATION, CDC_PARTITION_COL}
import org.apache.spark.sql.delta.util.{DateFormatter, PartitionUtils, TimestampFormatter, Utils => DeltaUtils}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types.StringType

/**
 * Writes out the files to `path` and returns a list of them in `addedStatuses`. Includes
 * special handling for partitioning on [[CDC_PARTITION_COL]] for
 * compatibility between enabled and disabled CDC; partitions with a value of false in this
 * column produce no corresponding partitioning directory.
 * @param path The base path files will be written
 * @param randomPrefixLength The length of random subdir name under 'path' that files been written
 * @param subdir The immediate subdir under path; If randomPrefixLength and subdir both exist, file
 *               path will be path/subdir/[rand str of randomPrefixLength]/file
 */
class DelayedCommitProtocol(
      jobId: String,
      path: String,
      randomPrefixLength: Option[Int],
      subdir: Option[String])
  extends FileCommitProtocol with Serializable with Logging {
  // Track the list of files added by a task, only used on the executors.
  @transient protected var addedFiles: ArrayBuffer[(Map[String, String], String)] = _

  // Track the change files added, only used on the driver. Files are sorted between this buffer
  // and addedStatuses based on the value of the [[CDC_TYPE_COLUMN_NAME]] partition column - a
  // file goes to addedStatuses if the value is CDC_TYPE_NOT_CDC and changeFiles otherwise.
  @transient val changeFiles = new ArrayBuffer[AddCDCFile]

  // Track the overall files added, only used on the driver.
  //
  // In rare cases, some of these AddFiles can be empty (i.e. contain no logical records).
  // If the caller wishes to have only non-empty AddFiles, they must collect stats and perform
  // the filter themselves. See TransactionalWrite::writeFiles. This filter will be best-effort,
  // since there's no guarantee the stats will exist.
  @transient val addedStatuses = new ArrayBuffer[AddFile]

  val timestampPartitionPattern = "yyyy-MM-dd HH:mm:ss[.S]"

  // Constants for CDC partition manipulation. Used only in newTaskTempFile(), but we define them
  // here to avoid building a new redundant regex for every file.
  protected val cdcPartitionFalse = s"${CDC_PARTITION_COL}=false"
  protected val cdcPartitionTrue = s"${CDC_PARTITION_COL}=true"
  protected val cdcPartitionTrueRegex = cdcPartitionTrue.r

  override def setupJob(jobContext: JobContext): Unit = {

  }

  /**
   * Commits a job after the writes succeed. Must be called on the driver. Partitions the written
   * files into [[AddFile]]s and [[AddCDCFile]]s as these metadata actions are treated differently
   * by [[TransactionalWrite]] (i.e. AddFile's may have additional statistics injected)
   */
  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    val (addFiles, changeFiles) = taskCommits.flatMap(_.obj.asInstanceOf[Seq[_]])
      .partition {
        case _: AddFile => true
        case _: AddCDCFile => false
        case other =>
          throw DeltaErrors.unrecognizedFileAction(s"$other", s"${other.getClass}")
      }

    // we cannot add type information above because of type erasure
    addedStatuses ++= addFiles.map(_.asInstanceOf[AddFile])
    this.changeFiles ++= changeFiles.map(_.asInstanceOf[AddCDCFile]).toArray[AddCDCFile]
  }

  override def abortJob(jobContext: JobContext): Unit = {
    // TODO: Best effort cleanup
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    addedFiles = new ArrayBuffer[(Map[String, String], String)]
  }

  protected def getFileName(
      taskContext: TaskAttemptContext,
      ext: String,
      partitionValues: Map[String, String]): String = {
    // The file name looks like part-r-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003.gz.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    val uuid = UUID.randomUUID.toString
    // CDC files (CDC_PARTITION_COL = true) are named with "cdc-..." instead of "part-...".
    if (partitionValues.get(CDC_PARTITION_COL).contains("true")) {
      f"cdc-$split%05d-$uuid$ext"
    } else {
      f"part-$split%05d-$uuid$ext"
    }
  }

  protected def parsePartitions(dir: String): Map[String, String] = {
    // TODO: timezones?
    // TODO: enable validatePartitionColumns?
    val dateFormatter = DateFormatter()
    val timestampFormatter =
      TimestampFormatter(timestampPartitionPattern, java.util.TimeZone.getDefault)
    val parsedPartition =
      PartitionUtils
        .parsePartition(
          new Path(dir),
          typeInference = false,
          Set.empty,
          Map.empty,
          validatePartitionColumns = false,
          java.util.TimeZone.getDefault,
          dateFormatter,
          timestampFormatter)
        ._1
        .get
    parsedPartition
        .columnNames
        .zip(
          parsedPartition
            .literals
            .map(l => Cast(l, StringType).eval())
            .map(Option(_).map(_.toString).orNull))
        .toMap
  }

  /**
   * Notifies the commit protocol to add a new file, and gets back the full path that should be
   * used.
   *
   * Includes special logic for CDC files and paths. Specifically, if the directory `dir` contains
   * the CDC partition `__is_cdc=true` then
   * - the file name begins with `cdc-` instead of `part-`
   * - the directory has the `__is_cdc=true` partition removed and is placed in the `_changed_data`
   *   folder
   */
  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val partitionValues = dir.map(parsePartitions).getOrElse(Map.empty[String, String])
    val filename = getFileName(taskContext, ext, partitionValues)
    val relativePath = randomPrefixLength.map { prefixLength =>
      DeltaUtils.getRandomPrefix(prefixLength) // Generate a random prefix as a first choice
    }.orElse {
      dir // or else write into the partition directory if it is partitioned
    }.map { subDir =>
      // Do some surgery on the paths we write out to eliminate the CDC_PARTITION_COL. Non-CDC
      // data is written to the base location, while CDC data is written to a special folder
      // _change_data.
      // The code here gets a bit complicated to accommodate two corner cases: an empty subdir
      // can't be passed to new Path() at all, and a single-level subdir won't have a trailing
      // slash.
      if (subDir == cdcPartitionFalse) {
        new Path(filename)
      } else if (subDir.startsWith(cdcPartitionTrue)) {
        val cleanedSubDir = cdcPartitionTrueRegex.replaceFirstIn(subDir, CDC_LOCATION)
        new Path(cleanedSubDir, filename)
      } else if (subDir.startsWith(cdcPartitionFalse)) {
        // We need to remove the trailing slash in addition to the directory - otherwise
        // it'll be interpreted as an absolute path and fail.
        val cleanedSubDir = subDir.stripPrefix(cdcPartitionFalse + "/")
        new Path(cleanedSubDir, filename)
      } else {
        new Path(subDir, filename)
      }
    }.getOrElse(new Path(filename)) // or directly write out to the output path

    val relativePathWithSubdir = subdir.map(new Path(_, relativePath)).getOrElse(relativePath)
    addedFiles.append((partitionValues, relativePathWithSubdir.toUri.toString))
    new Path(path, relativePathWithSubdir).toString
  }

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    throw DeltaErrors.unsupportedAbsPathAddFile(s"$this")
  }

  protected def buildActionFromAddedFile(
      f: (Map[String, String], String),
      stat: FileStatus,
      taskContext: TaskAttemptContext): FileAction = {
    // The partitioning in the Delta log action will be read back as part of the data, so our
    // virtual CDC_PARTITION_COL needs to be stripped out.
    val partitioning = f._1.filter { case (k, v) => k != CDC_PARTITION_COL }
    f._1.get(CDC_PARTITION_COL) match {
      case Some("true") =>
        val partitioning = f._1.filter { case (k, v) => k != CDC_PARTITION_COL }
        AddCDCFile(f._2, partitioning, stat.getLen)
      case _ =>
        val addFile = AddFile(f._2, partitioning, stat.getLen, stat.getModificationTime, true)
        addFile
    }
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    if (addedFiles.nonEmpty) {
      val fs = new Path(path, addedFiles.head._2).getFileSystem(taskContext.getConfiguration)
      val statuses: Seq[FileAction] = addedFiles.map { f =>
        // scalastyle:off pathfromuri
        val filePath = new Path(path, new Path(new URI(f._2)))
        // scalastyle:on pathfromuri
        val stat = fs.getFileStatus(filePath)

        buildActionFromAddedFile(f, stat, taskContext)
      }.toSeq

      new TaskCommitMessage(statuses)
    } else {
      new TaskCommitMessage(Nil)
    }
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    // TODO: we can also try delete the addedFiles as a best-effort cleanup.
  }
}
