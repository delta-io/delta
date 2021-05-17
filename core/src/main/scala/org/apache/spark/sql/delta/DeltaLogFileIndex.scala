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

import org.apache.hadoop.fs._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

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

  override lazy val rootPaths: Seq[Path] = files.map(_.getPath)

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    PartitionDirectory(InternalRow(), files) :: Nil
  }

  override val inputFiles: Array[String] = files.map(_.getPath).map(_.toString)

  override def refresh(): Unit = {}

  override val sizeInBytes: Long = files.map(_.getLen).sum

  override def partitionSchema: StructType = new StructType()

  override def toString: String =
    s"DeltaLogFileIndex($format, numFilesInSegment: ${files.size}, totalFileSize: $sizeInBytes)"

  logInfo(s"Created $this")
}

object DeltaLogFileIndex {

  lazy val COMMIT_FILE_FORMAT = new JsonFileFormat
  lazy val CHECKPOINT_FILE_FORMAT = new ParquetFileFormat

  def apply(format: FileFormat, fs: FileSystem, paths: Seq[Path]): DeltaLogFileIndex = {
    DeltaLogFileIndex(format, paths.map(fs.getFileStatus).toArray)
  }

  def apply(format: FileFormat, files: Seq[FileStatus]): Option[DeltaLogFileIndex] = {
    if (files.isEmpty) None else Some(DeltaLogFileIndex(format, files.toArray))
  }
}
