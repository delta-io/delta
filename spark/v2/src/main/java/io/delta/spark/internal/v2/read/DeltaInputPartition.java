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
package io.delta.spark.internal.v2.read;

import java.util.Objects;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.HasPartitionKey;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.execution.datasources.FilePartition;

/**
 * A Delta-specific InputPartition that wraps a FilePartition and implements HasPartitionKey. This
 * enables Spark to leverage partition information for optimizations like shuffle elimination in
 * joins and aggregations when using KeyGroupedPartitioning.
 *
 * <p>Each DeltaInputPartition represents files from a single logical partition (i.e., all files
 * share the same partition values). The partition key is the InternalRow containing the partition
 * column values.
 */
public class DeltaInputPartition implements InputPartition, HasPartitionKey {
  private final FilePartition filePartition;
  private final InternalRow partitionKey;

  /**
   * Creates a new DeltaInputPartition.
   *
   * @param filePartition The underlying FilePartition containing the files to read.
   * @param partitionKey The partition key (partition column values) for all files in this
   *     partition. Must not be null.
   */
  public DeltaInputPartition(FilePartition filePartition, InternalRow partitionKey) {
    this.filePartition = Objects.requireNonNull(filePartition, "filePartition is null");
    this.partitionKey = Objects.requireNonNull(partitionKey, "partitionKey is null");
  }

  /**
   * Returns the partition key (partition column values) associated with this partition. All files
   * in this partition have the same partition key.
   *
   * @return The partition key as an InternalRow.
   */
  @Override
  public InternalRow partitionKey() {
    return partitionKey;
  }

  /**
   * Returns the underlying FilePartition.
   *
   * @return The FilePartition containing the files to read.
   */
  public FilePartition getFilePartition() {
    return filePartition;
  }

  @Override
  public String[] preferredLocations() {
    return filePartition.preferredLocations();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeltaInputPartition that = (DeltaInputPartition) o;
    return Objects.equals(filePartition, that.filePartition)
        && Objects.equals(partitionKey, that.partitionKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filePartition, partitionKey);
  }

  @Override
  public String toString() {
    return String.format(
        "DeltaInputPartition(partitionKey=%s, files=%d)",
        partitionKey, filePartition.files().length);
  }
}
