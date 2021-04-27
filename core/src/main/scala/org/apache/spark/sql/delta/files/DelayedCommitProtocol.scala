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

package org.apache.spark.sql.delta.files

// scalastyle:off import.ordering.noEmptyLine
import java.net.URI
import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.delta.util.{DateFormatter, PartitionUtils, TimestampFormatter}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType

/**
 * Writes out the files to `path` and returns a list of them in `addedStatuses`.
 */
class DelayedCommitProtocol(
      jobId: String,
      path: String,
      randomPrefixLength: Option[Int])
  extends FileCommitProtocol with Serializable with Logging {
  // Track the list of files added by a task, only used on the executors.
  @transient protected var addedFiles: ArrayBuffer[(Map[String, String], String)] = _
  // Track the overall files added, only used on the driver.
  @transient val addedStatuses = new ArrayBuffer[AddFile]

  val timestampPartitionPattern = "yyyy-MM-dd HH:mm:ss[.S]"

  override def setupJob(jobContext: JobContext): Unit = {

  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    val fileStatuses = taskCommits.flatMap(_.obj.asInstanceOf[Seq[AddFile]]).toArray
    addedStatuses ++= fileStatuses
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
    f"part-$split%05d-$uuid$ext"
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

  /** Generates a string created of `randomPrefixLength` alphanumeric characters. */
  protected def getRandomPrefix(numChars: Int): String = {
    Random.alphanumeric.take(numChars).mkString
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val partitionValues = dir.map(parsePartitions).getOrElse(Map.empty[String, String])
    val filename = getFileName(taskContext, ext, partitionValues)
    val relativePath = randomPrefixLength.map { prefixLength =>
      getRandomPrefix(prefixLength) // Generate a random prefix as a first choice
    }.orElse {
      dir // or else write into the partition directory if it is partitioned
    }.map { subDir =>
      new Path(subDir, filename)
    }.getOrElse(new Path(filename)) // or directly write out to the output path

    addedFiles.append((partitionValues, relativePath.toUri.toString))
    new Path(path, relativePath).toString
  }

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    throw new UnsupportedOperationException(
      s"$this does not support adding files with an absolute path")
  }

  protected def buildActionFromAddedFile(
      f: (Map[String, String], String),
      stat: FileStatus,
      taskContext: TaskAttemptContext): FileAction = {
    AddFile(f._2, f._1, stat.getLen, stat.getModificationTime, true)
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    if (addedFiles.nonEmpty) {
      val fs = new Path(path, addedFiles.head._2).getFileSystem(taskContext.getConfiguration)
      val statuses: Seq[FileAction] = addedFiles.map { f =>
        val filePath = new Path(path, new Path(new URI(f._2)))
        val stat = fs.getFileStatus(filePath)

        buildActionFromAddedFile(f, stat, taskContext)
      }

      new TaskCommitMessage(statuses)
    } else {
      new TaskCommitMessage(Nil)
    }
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    // TODO: we can also try delete the addedFiles as a best-effort cleanup.
  }
}
