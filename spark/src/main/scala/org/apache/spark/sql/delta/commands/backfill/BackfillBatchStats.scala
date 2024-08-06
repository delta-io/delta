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
 * Metrics for each BackfillBatch.
 *
 * @param parentTransactionId The transaction id associated with the parent command.
 * @param transactionId The transaction id used in this batch.
 * @param batchId An integer identifier of the batch within a parent BackfillCommand.
 * @param initialNumFiles The number of files in BackfillBatch prior to conflict
 *                        resolution.
 * @param totalExecutionTimeInMs The total execution time in milliseconds.
 * @param wasSuccessful Boolean indicating whether the batch was successfully committed.
 */
case class BackfillBatchStats(
    parentTransactionId: String,
    transactionId: String,
    batchId: Int,
    initialNumFiles: Long,
    totalExecutionTimeInMs: Long,
    wasSuccessful: Boolean
)
