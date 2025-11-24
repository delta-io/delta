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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * A resolution rule that marks ReplaceTableAsSelect plans that originate from DataFrameWriter V1
 * by adding the IS_DATAFRAME_WRITER_V1 option.
 *
 * This is needed because in Spark 4.1 connect mode, planning and execution are decoupled,
 * so the stack trace no longer contains the original calling API when the command is executed.
 * By marking the plan during resolution (when it's still in the same stack as the API call),
 * we can propagate this information to the execution phase.
 */
case class MarkDataFrameWriterV1(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case rtas: ReplaceTableAsSelect if isFromDataFrameWriterV1 && !hasV1Marker(rtas) =>
      // Add the IS_DATAFRAME_WRITER_V1 option to mark this as coming from DFWV1.
      rtas.copy(
        writeOptions = rtas.writeOptions + (DeltaOptions.IS_DATAFRAME_WRITER_V1 -> "true")
      )
  }

  /**
   * Check if the current call stack contains DataFrameWriter V1.
   */
  private def isFromDataFrameWriterV1: Boolean = {
    Thread.currentThread().getStackTrace.exists(_.toString.contains(
      Relocated.dataFrameWriterClassName + "."))
  }

  /**
   * Check if the plan already has the V1 marker to avoid adding it multiple times.
   */
  private def hasV1Marker(rtas: ReplaceTableAsSelect): Boolean = {
    rtas.writeOptions.get(DeltaOptions.IS_DATAFRAME_WRITER_V1).contains("true")
  }
}
