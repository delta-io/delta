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

import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{LongType, StructField, StructType}

/**
 * A specialized file index for files found in the _delta_log directory. By using this file index,
 * we avoid any additional file listing, partitioning inference, and file existence checks when
 * computing the state of a Delta table.
 *
 * @param format The file format of the log files. Currently "parquet" or "json"
 * @param files The files to read
 */
case class DeltaLogFileIndex private (
    format: FileFormat,
    files: Array[FileStatus])
  extends FileIndex
  with Logging {

  import DeltaLogFileIndex._

  override lazy val rootPaths: Seq[Path] = files.map(_.getPath)

  def listAllFiles(): Seq[PartitionDirectory] = {
    files
      .groupBy(f => FileNames.getFileVersionOpt(f.getPath).getOrElse(-1L))
      .map { case (version, files) => PartitionDirectory(InternalRow(version), files) }
      .toSeq
  }

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (partitionFilters.isEmpty) {
      listAllFiles()
    } else {
      val predicate = partitionFilters.reduce(And)
      val boundPredicate = predicate.transform {
        case a: AttributeReference =>
          val index = partitionSchema.indexWhere(a.name == _.name)
          BoundReference(index, partitionSchema(index).dataType, partitionSchema(index).nullable)
      }
      val predicateEvaluator = Predicate.create(boundPredicate, Nil)
      listAllFiles().filter(d => predicateEvaluator.eval(d.values))
    }
  }

  override val inputFiles: Array[String] = files.map(_.getPath.toString)

  override def refresh(): Unit = {}

  override val sizeInBytes: Long = files.map(_.getLen).sum

  override val partitionSchema: StructType =
    new StructType().add(COMMIT_VERSION_COLUMN, LongType, nullable = false)

  override def toString: String =
    s"DeltaLogFileIndex($format, numFilesInSegment: ${files.size}, totalFileSize: $sizeInBytes)"

  logInfo(s"Created $this")
}

object DeltaLogFileIndex {
  val COMMIT_VERSION_COLUMN = "version"

  lazy val COMMIT_FILE_FORMAT = new JsonFileFormat
  lazy val CHECKPOINT_FILE_FORMAT_PARQUET = new ParquetFileFormat
  lazy val CHECKPOINT_FILE_FORMAT_JSON = new JsonFileFormat

  def apply(format: FileFormat, fs: FileSystem, paths: Seq[Path]): DeltaLogFileIndex = {
    DeltaLogFileIndex(format, paths.map(fs.getFileStatus).toArray)
  }

  def apply(format: FileFormat, files: Seq[FileStatus]): Option[DeltaLogFileIndex] = {
    if (files.isEmpty) None else Some(DeltaLogFileIndex(format, files.toArray))
  }

  def apply(format: FileFormat, filesOpt: Option[Seq[FileStatus]]): Option[DeltaLogFileIndex] = {
    filesOpt.flatMap(DeltaLogFileIndex(format, _))
  }
}
