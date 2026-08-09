/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.test.shims

/**
 * Test shims for streaming classes that were relocated in Spark 4.1.
 * In Spark 4.0, these classes are in their original locations.
 */
object StreamingTestShims {
  // MemoryStream
  type MemoryStream[T] = org.apache.spark.sql.execution.streaming.MemoryStream[T]
  val MemoryStream: org.apache.spark.sql.execution.streaming.MemoryStream.type =
    org.apache.spark.sql.execution.streaming.MemoryStream

  // MicroBatchExecution (class only, no companion object)
  type MicroBatchExecution = org.apache.spark.sql.execution.streaming.MicroBatchExecution

  // StreamingQueryWrapper (class only, no companion object)
  type StreamingQueryWrapper = org.apache.spark.sql.execution.streaming.StreamingQueryWrapper

  // StreamingExecutionRelation (class only, no companion object)
  type StreamingExecutionRelation =
    org.apache.spark.sql.execution.streaming.StreamingExecutionRelation

  // OffsetSeqLog (class only, no companion object)
  type OffsetSeqLog = org.apache.spark.sql.execution.streaming.OffsetSeqLog
}

