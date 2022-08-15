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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile}
import org.apache.spark.sql.delta.commands.cdc.CDCReader.{CDCDataSpec, CDC_COMMIT_TIMESTAMP, CDC_COMMIT_VERSION}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.types.{LongType, StructType, TimestampType}

/**
 * A [[TahoeFileIndex]] for scanning a sequence of CDC files. Similar to [[TahoeBatchFileIndex]],
 * the equivalent for reading [[AddFile]] actions.
 */
class TahoeChangeFileIndex(
    spark: SparkSession,
    filesByVersion: Seq[CDCDataSpec[AddCDCFile]],
    deltaLog: DeltaLog,
    path: Path,
    snapshot: Snapshot)
  extends TahoeCDCBaseFileIndex(spark, filesByVersion, deltaLog, path, snapshot) {

  override val partitionSchema: StructType = snapshot.metadata.partitionSchema
    .add(CDC_COMMIT_VERSION, LongType)
    .add(CDC_COMMIT_TIMESTAMP, TimestampType)

  // Data already has cdc partition values
  override def cdcPartitionValues(): Map[String, String] = Map.empty

  override protected def extractActionParameters(addCDCFile: AddCDCFile): ActionParameters =
    ActionParameters(addCDCFile.partitionValues, addCDCFile.size, 0, false, addCDCFile.tags)
}
