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

import org.apache.spark.sql.delta.actions.{Action}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames._

import org.apache.spark.sql.test.SharedSparkSession

class CheckpointProviderSuite
  extends SharedSparkSession
  with DeltaSQLCommandTest {

  for (v2CheckpointFormat <- Seq("json", "parquet"))
  test(s"V2 Checkpoint compat file equivalency to normal V2 Checkpoint" +
      s" [v2CheckpointFormat: $v2CheckpointFormat]") {
    withSQLConf(
      DeltaConfigs.CHECKPOINT_POLICY.defaultTablePropertyKey -> CheckpointPolicy.V2.name,
      DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> v2CheckpointFormat
    ) {
      withTempDir { tempDir =>
        spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
        val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)

        spark.range(10).write.mode("append").format("delta").save(tempDir.getAbsolutePath)

        deltaLog.checkpoint() // Checkpoint 1
        val snapshot = deltaLog.update()

        deltaLog.createSinglePartCheckpointForBackwardCompat(
          snapshot, new deltaLog.V2CompatCheckpointMetrics) // Compatibility Checkpoint 1

        val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())
        val v2CompatCheckpoint = fs.getFileStatus(
          checkpointFileSingular(deltaLog.logPath, snapshot.checkpointProvider.version))

        val origCheckpoint = snapshot.checkpointProvider
          .asInstanceOf[LazyCompleteCheckpointProvider]
          .underlyingCheckpointProvider
          .asInstanceOf[V2CheckpointProvider]
        val compatCheckpoint = CheckpointProvider(
          spark,
          deltaLog.snapshot,
          None,
          UninitializedV2CheckpointProvider(
            2L,
            v2CompatCheckpoint,
            deltaLog.logPath,
            deltaLog.newDeltaHadoopConf(),
            deltaLog.options,
            deltaLog.store,
            None))
          .asInstanceOf[LazyCompleteCheckpointProvider]
          .underlyingCheckpointProvider
          .asInstanceOf[V2CheckpointProvider]

        // Check whether these checkpoints are equivalent after being loaded
        assert(compatCheckpoint.sidecarFiles.toSet === origCheckpoint.sidecarFiles.toSet)
        assert(compatCheckpoint.checkpointMetadata === origCheckpoint.checkpointMetadata)

        val compatDf =
            deltaLog.loadIndex(compatCheckpoint.topLevelFileIndex.get, Action.logSchema)
          // Check whether the manifest content is same or not
        val originalDf =
            deltaLog.loadIndex(origCheckpoint.topLevelFileIndex.get, Action.logSchema)
            assert(originalDf.sort().collect() === compatDf.sort().collect())
      }
    }
  }
}
