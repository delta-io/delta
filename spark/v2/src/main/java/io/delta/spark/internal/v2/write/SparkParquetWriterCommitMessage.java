/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.write;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/** Commit payload produced by each executor-side data writer task. */
public class SparkParquetWriterCommitMessage implements WriterCommitMessage, Serializable {
  private static final long serialVersionUID = 1L;

  private final int partitionId;
  private final long taskId;
  private final long numRowsWritten;
  private final String targetDirectory;

  public SparkParquetWriterCommitMessage(
      int partitionId, long taskId, long numRowsWritten, String targetDirectory) {
    if (numRowsWritten < 0L) {
      throw new IllegalArgumentException("numRowsWritten must be non-negative");
    }
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.numRowsWritten = numRowsWritten;
    this.targetDirectory = requireNonNull(targetDirectory, "target directory is null");
    if (this.targetDirectory.isEmpty()) {
      throw new IllegalArgumentException("target directory is empty");
    }
  }

  int getPartitionId() {
    return partitionId;
  }

  long getTaskId() {
    return taskId;
  }

  long getNumRowsWritten() {
    return numRowsWritten;
  }

  String getTargetDirectory() {
    return targetDirectory;
  }
}
