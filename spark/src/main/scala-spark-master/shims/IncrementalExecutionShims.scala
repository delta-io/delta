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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object IncrementalExecutionShims {

  /**
   * Handles a breaking change in the [[IncrementalExecution]] constructor between Spark 3.5 and
   * 4.0:
   * - Spark 3.5: no `isFirstBatch: Boolean` param
   * - Spark 4.0: adds `isFirstBatch: Boolean` param
   */
  def newInstance(
      sparkSession: SparkSession,
      logicalPlan: LogicalPlan,
      incrementalExecution: IncrementalExecution): IncrementalExecution = new IncrementalExecution(
    sparkSession,
    logicalPlan,
    incrementalExecution.outputMode,
    incrementalExecution.checkpointLocation,
    incrementalExecution.queryId,
    incrementalExecution.runId,
    incrementalExecution.currentBatchId,
    incrementalExecution.prevOffsetSeqMetadata,
    incrementalExecution.offsetSeqMetadata,
    incrementalExecution.watermarkPropagator,
    incrementalExecution.isFirstBatch // Spark 4.0 API
  )
}
