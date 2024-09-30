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

package org.apache.spark.sql.delta.sources

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.{IncrementalExecution, IncrementalExecutionShims, StreamExecution}

object DeltaStreamUtils {

  /**
   * Select `cols` from a micro batch DataFrame. Directly calling `select` won't work because it
   * will create a `QueryExecution` rather than inheriting `IncrementalExecution` from
   * the micro batch DataFrame. A streaming micro batch DataFrame to execute should use
   * `IncrementalExecution`.
   */
  def selectFromStreamingDataFrame(
      incrementalExecution: IncrementalExecution,
      df: DataFrame,
      cols: Column*): DataFrame = {
    val newMicroBatch = df.select(cols: _*)
    val newIncrementalExecution = IncrementalExecutionShims.newInstance(
      newMicroBatch.sparkSession,
      newMicroBatch.queryExecution.logical,
      incrementalExecution)
    newIncrementalExecution.executedPlan // Force the lazy generation of execution plan


    // Use reflection to call the private constructor.
    val constructor =
      classOf[Dataset[_]].getConstructor(classOf[QueryExecution], classOf[Encoder[_]])
    constructor.newInstance(
      newIncrementalExecution,
      ExpressionEncoder(newIncrementalExecution.analyzed.schema)).asInstanceOf[DataFrame]
  }
}
