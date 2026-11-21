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

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.util.ThreadUtils

/**
 * Tests for a backfill that plans its batches from a single scan of the table, with transactions
 * that commit after the scan and touch the files of a later batch.
 */
class RowTrackingBackfillSingleScanConflictsSuite extends RowTrackingBackfillConflictsTestBase {

  /**
   * With 3 files per commit, the backfill plans three batches in path order: the files of
   * partitions 0 and 1 except one, the remaining file of partition 1 and partition 2, and
   * partition 3. A DELETE in partition 3 commits while the second batch is prepared. The second
   * batch is conflict checked against the DELETE, but has no file in common with it. The third
   * batch reads the snapshot after the DELETE, so it is not conflict checked against it: the
   * backfill must drop the file that the DELETE removed (or re-added with a deletion vector)
   * from the third batch itself, or it would resurrect the deleted row.
   */
  test("concurrent DELETE of a file planned for a later batch") {
    withTrackedBackfillCommits {
      withTestTable {
        withSQLConf(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT.key -> "3") {
          validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 3) {
            val backfillFuture = launchBackFillAndBlockAfterFeatureIsCommitted()

            // First batch: partitions 0 and 1.
            commitSingleBackfillBatch()

            // Prepare the second batch, then delete a row of partition 3.
            prepareSingleBackfillBatchCommit()
            sql(s"DELETE FROM $testTableName WHERE $colName = 3").collect()
            commitPreparedBackfillBatchCommit()

            // Third batch: partition 3.
            commitSingleBackfillBatch()

            ThreadUtils.awaitResult(backfillFuture, timeout)
            validateResult(() => tableCreationDF.where(s"$colName != 3"))
          }
        }
      }
    }
  }
}

class RowTrackingBackfillSingleScanConflictsDVSuite
  extends RowTrackingBackfillSingleScanConflictsSuite {
  override val usePersistentDeletionVectors = true
}
