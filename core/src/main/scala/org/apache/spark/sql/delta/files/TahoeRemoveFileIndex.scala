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

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.commands.cdc.CDCReader._
import org.apache.spark.sql.delta.implicits._
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType

/**
 * A [[TahoeFileIndex]] for scanning a sequence of removed files as CDC. Similar to
 * [[TahoeBatchFileIndex]], the equivalent for reading [[AddFile]] actions.
 * @param spark The Spark session.
 * @param filesByVersion Grouped FileActions, one per table version.
 * @param deltaLog The delta log instance.
 * @param path The table's data path.
 * @param snapshot The snapshot where we read CDC from.
 * @param rowIndexFilters Map from <b>URI-encoded</b> file path to a row index filter type.
 */
class TahoeRemoveFileIndex(
    spark: SparkSession,
    val filesByVersion: Seq[CDCDataSpec[RemoveFile]],
    deltaLog: DeltaLog,
    path: Path,
    snapshot: SnapshotDescriptor,
    override val rowIndexFilters: Option[Map[String, RowIndexFilterType]] = None
  ) extends TahoeFileIndexWithSnapshotDescriptor(spark, deltaLog, path, snapshot) {

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {
    // Make some fake AddFiles to satisfy the interface.
    val addFiles = filesByVersion.flatMap {
      case CDCDataSpec(version, ts, files, ci) =>
        files.map { r =>
          if (!r.extendedFileMetadata.getOrElse(false)) {
            // This shouldn't happen in user queries - the CDC flag was added at the same time as
            // extended metadata, so all removes in a table with CDC enabled should have it. (The
            // only exception is FSCK removes, which we screen out separately because they have
            // dataChange set to false.)
            throw DeltaErrors.removeFileCDCMissingExtendedMetadata(r.toString)
          }
          // We add the metadata as faked partition columns in order to attach it on a per-file
          // basis.
          val newPartitionVals = r.partitionValues +
            (CDC_COMMIT_VERSION -> version.toString) +
            (CDC_COMMIT_TIMESTAMP -> Option(ts).map(_.toString).orNull) +
            (CDC_TYPE_COLUMN_NAME -> CDC_TYPE_DELETE_STRING)
          AddFile(
            path = r.path,
            partitionValues = newPartitionVals,
            size = r.size.getOrElse(0L),
            modificationTime = 0,
            dataChange = r.dataChange,
            tags = r.tags,
            deletionVector = r.deletionVector
          )
        }
    }
    DeltaLog.filterFileList(partitionSchema, addFiles.toDF(spark), partitionFilters)
      .as[AddFile]
      .collect()
  }

  override def inputFiles: Array[String] = {
    filesByVersion.flatMap(_.actions).map(f => absolutePath(f.path).toString).toArray
  }

  override def partitionSchema: StructType = CDCReader.cdcReadSchema(super.partitionSchema)

  override def refresh(): Unit = {}

  override val sizeInBytes: Long = filesByVersion.flatMap(_.actions).map(_.size.getOrElse(0L)).sum
}
