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

import org.apache.spark.sql.delta.{DeltaLog, DeltaLogFileIndex}
import org.apache.spark.sql.delta.actions.{AddFile, Checkpoint, SingleAction}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * An [[AMTCheckpointProvider]] whose state reconstruction decodes every manifest entry into an
 * [[AMTCheckpointProvider.AMTDataEntryExtended]] and converts it to an `AddFile` on the executors.
 * The default, kept as the fallback for [[ColumnarAMTCheckpointProvider]].
 */
final class PerRowAMTCheckpointProvider(
    manifestCommitVersion: Long,
    checkpointAction: Checkpoint,
    leaves: Seq[DataManifestEntry],
    tableRoot: Path)
  extends AMTCheckpointProvider(manifestCommitVersion, checkpointAction, leaves, tableRoot) {

  override protected def rootLiveAddSingleActions(
      spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction] = {
    val index = DeltaLogFileIndex(
      DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, Array(rootFile))
    val entries = liveDataEntries(deltaLog, index)
    toAddSingleActions(entries, dataEntryResolverProvider(parentLeaves = Seq.empty))
  }

  override protected def leafLiveAddSingleActions(deltaLog: DeltaLog): Dataset[SingleAction] =
    toAddSingleActions(
      loadLeafDataEntriesWithRowIdPrefix(deltaLog, liveLeavesWithManifestFilters),
      dataEntryResolverProvider(parentLeaves = liveLeaves))

  override private[amt] def readLiveRootDataEntries(
      spark: SparkSession, deltaLog: DeltaLog): Seq[AddFile] =
    AMTCheckpointProvider.readLiveRootDataEntries(deltaLog, checkpointAction)
}
