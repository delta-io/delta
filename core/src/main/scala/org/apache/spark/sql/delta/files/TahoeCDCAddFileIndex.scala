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
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.commands.cdc.CDCReader._
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.types.StructType

/**
 * A [[TahoeFileIndex]] for scanning a sequence of added files as CDC. Similar to
 * [[TahoeBatchFileIndex]], with a bit of special handling to attach the log version
 * and CDC type on a per-file basis.
 */
class TahoeCDCAddFileIndex(
  override val spark: SparkSession,
  val actionType: String = "cdcRead",
  filesByVersion: Seq[CDCDataSpec[AddFile]],
  deltaLog: DeltaLog,
  path: Path,
  snapshot: Snapshot)
  extends TahoeCDCBaseFileIndex(spark, filesByVersion, deltaLog, path, snapshot) {

  // We add the metadata as faked partition columns in order to attach it on a per-file
  // basis.
  override def cdcPartitionValues(): Map[String, String] =
    Map(CDC_TYPE_COLUMN_NAME -> CDC_TYPE_INSERT)

  override def partitionSchema: StructType =
    CDCReader.cdcReadSchema(snapshot.metadata.partitionSchema)
}
