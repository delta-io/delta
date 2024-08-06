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

/**
 * Metrics for the BackfillCommand.
 *
 * @param transactionId: The transaction id associated with this BackfillCommand that
 *                       is the parent transaction for BackfillBatch commits.
 * @param nameOfTriggeringOperation: The name of the operation that triggered backfill. For now,
 *                                   this can be ALTER TABLE SET TBLPROPERTIES
 * @param maxNumBatchesInParallel The maximum number of batches that could have run in parallel.
 * @param totalExecutionTimeMs The total execution time in milliseconds.
 * @param numSuccessfulBatches The number of BackfillBatch's that was successfully committed.
 * @param numFailedBatches The number of BackfillBatch's that failed.
 * @param wasSuccessful Boolean indicating whether this BackfillCommand didn't have any error.
 */
case class BackfillCommandStats(
    transactionId: String,
    nameOfTriggeringOperation: String,
    maxNumBatchesInParallel: Int,
    var totalExecutionTimeMs: Long = 0,
    var numSuccessfulBatches: Int = 0,
    var numFailedBatches: Int = 0,
    var wasSuccessful: Boolean = false
)
