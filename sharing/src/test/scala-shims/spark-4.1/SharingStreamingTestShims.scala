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

package io.delta.sharing.spark.test.shims

import org.apache.spark.sql.execution.streaming.checkpointing.{
  CheckpointFileManager => CheckpointFileManagerShim,
  CommitMetadata => CommitMetadataShim,
  OffsetSeqLog => OffsetSeqLogShim
}
import org.apache.spark.sql.execution.streaming.runtime.{
  SerializedOffset => SerializedOffsetShim,
  StreamingCheckpointConstants => StreamingCheckpointConstantsShim,
  StreamMetadata => StreamMetadataShim
}

/**
 * Test shims for streaming classes that were relocated in Spark 4.1.
 * In Spark 4.1, these classes moved to checkpointing and runtime sub-packages.
 */
object SharingStreamingTestShims {
  val CheckpointFileManager: CheckpointFileManagerShim.type =
    CheckpointFileManagerShim
  val CommitMetadata: CommitMetadataShim.type = CommitMetadataShim
  type OffsetSeqLog = OffsetSeqLogShim
  val SerializedOffset: SerializedOffsetShim.type = SerializedOffsetShim
  val StreamMetadata: StreamMetadataShim.type = StreamMetadataShim
  val StreamingCheckpointConstants: StreamingCheckpointConstantsShim.type =
    StreamingCheckpointConstantsShim
}
