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

import org.apache.spark.sql.execution.streaming.{
  CheckpointFileManager => CheckpointFileManagerShim,
  CommitMetadata => CommitMetadataShim,
  OffsetSeqLog => OffsetSeqLogShim,
  SerializedOffset => SerializedOffsetShim,
  StreamMetadata => StreamMetadataShim
}

/**
 * Test shims for streaming classes that were relocated in Spark 4.1.
 * In Spark 4.0, these classes are in org.apache.spark.sql.execution.streaming.
 * StreamingCheckpointConstants does not exist in Spark 4.0, so we define
 * the constants directly.
 */
object SharingStreamingTestShims {
  type CheckpointFileManager = CheckpointFileManagerShim
  val CheckpointFileManager: CheckpointFileManagerShim.type =
    CheckpointFileManagerShim
  val CommitMetadata: CommitMetadataShim.type = CommitMetadataShim
  type OffsetSeqLog = OffsetSeqLogShim
  val SerializedOffset: SerializedOffsetShim.type = SerializedOffsetShim
  val StreamMetadata: StreamMetadataShim.type = StreamMetadataShim

  object StreamingCheckpointConstants {
    val DIR_NAME_COMMITS = "commits"
    val DIR_NAME_OFFSETS = "offsets"
    val DIR_NAME_METADATA = "metadata"
  }
}
