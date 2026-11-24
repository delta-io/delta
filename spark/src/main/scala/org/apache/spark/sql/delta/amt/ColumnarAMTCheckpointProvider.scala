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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, Checkpoint, SingleAction}
import org.apache.hadoop.fs.Path

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

/**
 * An [[AMTCheckpointProvider]] whose state reconstruction builds each `AddFile` field as a column
 * over the persisted manifest rows (see `AMTManifestReaderImpl.addFileColumn`).
 *
 * @param rootInfo The root's leaf pointers and optional prefetched rows.
 */
final class ColumnarAMTCheckpointProvider(
    manifestCommitVersion: Long,
    checkpointAction: Checkpoint,
    tableRoot: Path,
    private val rootInfo: AMTRootInfo)
  extends AMTCheckpointProvider(
    manifestCommitVersion, checkpointAction, rootInfo.leaves, tableRoot)
{

  override protected def rootLiveAddSingleActions(
      spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction] =
    toAddSingleActions(
      manifestReader.withStatuses(rootEntries(spark, deltaLog), Tracking.Status.liveEntryStatuses),
      baseRowId = col("tracking.first_row_id"))

  /**
   * Like `AMTManifestReaderImpl.rootEntries`, but reuses the prefetched root if possible, with
   * the same columns, so that the rows union with leaf entry rows.
   */
  private[amt] def rootEntries(spark: SparkSession, deltaLog: DeltaLog): DataFrame =
    rootInfo.toDF(spark).map { root =>
      val persistedSchema =
        AMTSingleAction.persistedSchema(checkpointAction.metaData, checkpointAction.protocol)
      val columns = persistedSchema.fieldNames.toIndexedSeq.map(col) :+
        lit(SparkPath.fromPath(rootFile.getPath).urlEncoded).as("leafPath") :+
        // The shared back-reference expression requires a non-null pos, unused for root entries.
        lit(0L).as("pos")
      manifestReader.withRootEntryContext(manifestReader.dataEntries(root.select(columns: _*)))
    }.getOrElse(manifestReader.rootEntries(deltaLog, Seq(rootFile)))

  override protected def leafLiveAddSingleActions(
      spark: SparkSession,
      deltaLog: DeltaLog): Dataset[SingleAction] =
    toAddSingleActions(
      manifestReader.withStatuses(
        manifestReader.leafEntriesWithRowIdPrefix(spark, deltaLog, liveLeavesWithManifestFilters),
        Tracking.Status.liveEntryStatuses),
      baseRowId = manifestReader.inheritedBaseRowId)

  /** Reuses prefetched root for incremental writes as well as snapshot state reconstruction. */
  override private[amt] def readLiveRootDataEntries(
      spark: SparkSession, deltaLog: DeltaLog): Seq[AddFile] =
    rootLiveAddSingleActions(spark, deltaLog).collect().toSeq.map(_.add)
}
