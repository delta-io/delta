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

import java.text.SimpleDateFormat

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.commands.cdc.CDCReader._
import org.apache.spark.sql.delta.implicits._
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType

/**
 * A [[TahoeFileIndex]] for scanning a sequence of added files as CDC. Similar to
 * [[TahoeBatchFileIndex]], with a bit of special handling to attach the log version
 * and CDC type on a per-file basis.
 * @param spark The Spark session.
 * @param filesByVersion Grouped FileActions, one per table version.
 * @param deltaLog The delta log instance.
 * @param path The table's data path.
 * @param snapshot The snapshot where we read CDC from.
 * @param rowIndexFilters Map from <b>URI-encoded</b> file path to a row index filter type.
 */
class CdcAddFileIndex(
    spark: SparkSession,
    filesByVersion: Seq[CDCDataSpec[AddFile]],
    deltaLog: DeltaLog,
    path: Path,
    snapshot: SnapshotDescriptor,
    override val rowIndexFilters: Option[Map[String, RowIndexFilterType]] = None
  ) extends TahoeBatchFileIndex(
    spark, "cdcRead", filesByVersion.flatMap(_.actions), deltaLog, path, snapshot) {

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {
    val addFiles = filesByVersion.flatMap {
      case CDCDataSpec(version, ts, files, ci) =>
        files.map { f =>
          // We add the metadata as faked partition columns in order to attach it on a per-file
          // basis.
          val tsOpt = Option(ts)
            .map(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z").format(_)).orNull
          val newPartitionVals = f.partitionValues +
            (CDC_COMMIT_VERSION -> version.toString) +
            (CDC_COMMIT_TIMESTAMP -> tsOpt) +
            (CDC_TYPE_COLUMN_NAME -> CDC_TYPE_INSERT)
          f.copy(partitionValues = newPartitionVals)
        }
    }
    DeltaLog.filterFileList(partitionSchema, addFiles.toDF(spark), partitionFilters)
      .as[AddFile]
      .collect()
  }

  override def inputFiles: Array[String] = {
    filesByVersion.flatMap(_.actions).map(f => absolutePath(f.path).toString).toArray
  }

  override val partitionSchema: StructType =
    CDCReader.cdcReadSchema(snapshot.metadata.partitionSchema)

}
