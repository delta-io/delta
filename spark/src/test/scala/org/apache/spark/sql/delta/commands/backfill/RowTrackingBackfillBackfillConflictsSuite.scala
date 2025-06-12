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

package org.apache.spark.sql.delta.commands.backfill

import java.util.concurrent.ExecutionException

import org.apache.spark.sql.delta.{DeltaOperations, RowId}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

class RowTrackingBackfillBackfillConflictsSuite extends RowTrackingBackfillConflictsTestBase {

  /**
   * Concurrent backfill starts after the main backfill enabled the table feature and tries to
   * commit its only batch and the metadata update after the main backfill is finished.
   */
  test("Two Concurrent backfills") {
    withTestTable {
      withTrackedBackfillCommits {
        // Start main backfill and set table feature.
        val mainBackfillFuture = launchBackFillAndBlockAfterFeatureIsCommitted()
        prepareSingleBackfillBatchCommit()

        // Start concurrent backfill. Table feature is already enabled.
        val concurrentBackfillFuture = launchBackFillAndBlockAfterFeatureIsCommitted()
        prepareSingleBackfillBatchCommit()

        // Finish the main backfill which only requires one commit.
        commitPreparedBackfillBatchCommit()
        mainBackfillFuture.get()

        // Commit the batch of the concurrent backfill.
        commitPreparedBackfillBatchCommit()

        // Finish the concurrent backfill. It will commit 0 file.
        concurrentBackfillFuture.get()

        // Concurrent backfill does not upgrade the protocol again.
        assert(deltaLog.history.getHistory(None).count(_.operation == "UPGRADE PROTOCOL") === 1)
        validateResult(() => tableCreationDF)
      }
    }
  }

  test("A second backfill after a failed backfill") {
    withTestTable {
      withTrackedBackfillCommits {
        // Start a first backfill and set table feature.
        val firstBackfillFuture = launchBackFillAndBlockAfterFeatureIsCommitted()
        prepareSingleBackfillBatchCommit()

        // Concurrenty update the metadata.
        val updatedMetadata = latestSnapshot.metadata
          .copy(configuration = Map("foo" -> "bar"))
        deltaLog.startTransaction().commit(Seq(updatedMetadata), DeltaOperations.ManualUpdate)

        // Committing the batch and completing the command will fail because of the metadata update.
        commitPreparedBackfillBatchCommit()
        val e = intercept[ExecutionException] {
          firstBackfillFuture.get()
        }
        assertAbortedBecauseOfMetadataChange(e)
        assert(!RowId.isEnabled(latestSnapshot.protocol, latestSnapshot.metadata))

        // Launch a second backfill to finish the aborted backfill.
        val secondBackfillFuture = launchBackFillAndBlockAfterFeatureIsCommitted()
        commitSingleBackfillBatch()
        secondBackfillFuture.get()

        validateResult(() => tableCreationDF)
      }
    }
  }

  test("Concurrent commits from one backfill command") {
    val maxNumThreads = BackfillExecutor.getOrCreateThreadPool().getMaximumPoolSize
    require(maxNumThreads >= numFiles,
      s"Max thread pool size $maxNumThreads smaller than number of files $numFiles.")

    withTestTable {
      withTrackedBackfillCommits {
        withSQLConf(
          DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_BATCHES_IN_PARALLEL.key ->
            maxNumThreads.toString,
          DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT.key -> "1"
        ) {
          val backfillFuture = launchBackFillAndBlockAfterFeatureIsCommitted()

          commitBatchesSimultaneously(numFiles)

          backfillFuture.get()
        }

        validateResult(() => tableCreationDF)
      }
    }
  }
}
