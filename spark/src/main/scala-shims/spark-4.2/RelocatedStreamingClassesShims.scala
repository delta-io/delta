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
package org.apache.spark.sql.delta

import java.util.UUID

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.streaming.runtime.WatermarkPropagator
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import org.apache.spark.sql.execution.streaming.checkpointing.{
  CheckpointFileManager => CheckpointFileManagerShim,
  MetadataVersionUtil => MetadataVersionUtilShim,
  OffsetSeqMetadataBase
}
import org.apache.spark.sql.execution.streaming.runtime.{
  IncrementalExecution => IncrementalExecutionShim,
  MetadataLogFileIndex => MetadataLogFileIndexShim,
  StreamExecution => StreamExecutionShim,
  StreamingRelation => StreamingRelationShim
}
import org.apache.spark.sql.execution.streaming.sinks.{FileStreamSink => FileStreamSinkShim}

object Relocated {
  type CheckpointFileManager = CheckpointFileManagerShim
  val CheckpointFileManager: CheckpointFileManagerShim.type = CheckpointFileManagerShim

  type IncrementalExecution = IncrementalExecutionShim
  // scalastyle:off argcount
  def createIncrementalExecution(
      sparkSession: org.apache.spark.sql.classic.SparkSession,
      logicalPlan: LogicalPlan,
      outputMode: OutputMode,
      checkpointLocation: String,
      queryId: UUID,
      runId: UUID,
      currentBatchId: Long,
      prevOffsetSeqMetadata: Option[OffsetSeqMetadataBase],
      offsetSeqMetadata: OffsetSeqMetadataBase,
      watermarkPropagator: WatermarkPropagator,
      isFirstBatch: Boolean): IncrementalExecution = {
    // scalastyle:on argcount
    new IncrementalExecutionShim(
      sparkSession,
      logicalPlan,
      outputMode,
      checkpointLocation,
      queryId,
      runId,
      currentBatchId,
      prevOffsetSeqMetadata,
      offsetSeqMetadata,
      watermarkPropagator,
      isFirstBatch)
  }

  type StreamingRelation = StreamingRelationShim
  val StreamingRelation: StreamingRelationShim.type = StreamingRelationShim

  type MetadataLogFileIndex = MetadataLogFileIndexShim
  def createMetadataLogFileIndex(
      sparkSession: org.apache.spark.sql.SparkSession,
      path: Path,
      options: Map[String, String],
      userSpecifiedSchema: Option[StructType]): MetadataLogFileIndex = {
    new MetadataLogFileIndexShim(sparkSession, path, options, userSpecifiedSchema)
  }

  type FileStreamSink = FileStreamSinkShim
  val FileStreamSink: FileStreamSinkShim.type = FileStreamSinkShim

  type StreamExecution = StreamExecutionShim
  val StreamExecution: StreamExecutionShim.type = StreamExecutionShim

  val MetadataVersionUtil: MetadataVersionUtilShim.type = MetadataVersionUtilShim
}
