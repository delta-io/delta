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

import org.apache.spark.sql.delta.{DeltaLog, Snapshot, SnapshotDescriptor}
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile}
import org.apache.spark.sql.delta.commands.cdc.CDCReader.{CDC_COMMIT_TIMESTAMP, CDC_COMMIT_VERSION, CDCDataSpec}
import org.apache.spark.sql.delta.implicits._
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{LongType, StructType, TimestampType}

/**
 * A [[TahoeFileIndex]] for scanning a sequence of CDC files. Similar to [[TahoeBatchFileIndex]],
 * the equivalent for reading [[AddFile]] actions.
 */
class TahoeChangeFileIndex(
    spark: SparkSession,
    val filesByVersion: Seq[CDCDataSpec[AddCDCFile]],
    deltaLog: DeltaLog,
    path: Path,
    snapshot: SnapshotDescriptor)
  extends TahoeFileIndexWithSnapshotDescriptor(spark, deltaLog, path, snapshot) {

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {
    // Make some fake AddFiles to satisfy the interface.
    val addFiles = filesByVersion.flatMap {
      case CDCDataSpec(version, ts, files, ci) =>
        files.map { f =>
          // We add the metadata as faked partition columns in order to attach it on a per-file
          // basis.
          val newPartitionVals = f.partitionValues +
            (CDC_COMMIT_VERSION -> version.toString) +
            (CDC_COMMIT_TIMESTAMP -> Option(ts).map(_.toString).orNull)
          AddFile(f.path, newPartitionVals, f.size, 0, dataChange = false, tags = f.tags)
        }
    }
    DeltaLog.filterFileList(partitionSchema, addFiles.toDF(spark), partitionFilters)
      .as[AddFile]
      .collect()
  }

  override def inputFiles: Array[String] = {
    filesByVersion.flatMap(_.actions).map(f => absolutePath(f.path).toString).toArray
  }

  override val partitionSchema: StructType = super.partitionSchema
    .add(CDC_COMMIT_VERSION, LongType)
    .add(CDC_COMMIT_TIMESTAMP, TimestampType)

  override def refresh(): Unit = {}

  override val sizeInBytes: Long = filesByVersion.flatMap(_.actions).map(_.size).sum
}
