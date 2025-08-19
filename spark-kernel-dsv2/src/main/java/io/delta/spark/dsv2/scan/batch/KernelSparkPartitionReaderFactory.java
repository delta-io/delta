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
package io.delta.spark.dsv2.scan.batch;

import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 * Factory for creating {@link KernelSparkPartitionReader} instances from Delta Kernel input
 * partitions.
 *
 * <p>This factory implements Spark's {@link PartitionReaderFactory} interface and provides a
 * stateless, serializable way to create partition readers on executor nodes. The factory can be
 * safely reused across multiple partitions and is designed to be distributed across a Spark
 * cluster.
 *
 * <p>The factory expects {@link KernelSparkInputPartition} instances as input and creates
 * corresponding {@link KernelSparkPartitionReader} instances that can read Delta Lake data using
 * the Kernel API.
 *
 * <p>Key characteristics:
 *
 * <ul>
 *   <li><strong>Stateless:</strong> The factory maintains no internal state and can be used
 *       concurrently
 *   <li><strong>Serializable:</strong> Can be distributed across Spark executor nodes
 *   <li><strong>Type-safe:</strong> Performs runtime type checking on input partitions
 *   <li><strong>Row-based:</strong> Currently supports only row-based reading (not columnar)
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create factory (typically done once per scan)
 * KernelSparkPartitionReaderFactory factory = new KernelSparkPartitionReaderFactory();
 *
 * // Create readers for multiple partitions
 * for (InputPartition partition : partitions) {
 *     PartitionReader<InternalRow> reader = factory.createReader(partition);
 *     // Use reader to read data...
 * }
 * }</pre>
 *
 * @see KernelSparkInputPartition
 * @see KernelSparkPartitionReader
 */
public class KernelSparkPartitionReaderFactory implements PartitionReaderFactory, Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new partition reader factory. This factory is stateless and can be reused across
   * multiple partitions.
   */
  public KernelSparkPartitionReaderFactory() {}

  /**
   * Creates a new partition reader for the specified input partition.
   *
   * <p>This method extracts the serialized scan state and file row information from the input
   * partition and uses them to create a new {@link KernelSparkPartitionReader} instance. The
   * created reader will handle the actual data reading using the Delta Kernel API.
   *
   * <p>The method performs runtime type checking to ensure the partition is a valid {@link
   * KernelSparkInputPartition} instance.
   *
   * @param partition the input partition containing serialized Delta Kernel scan information. Must
   *     be an instance of {@link KernelSparkInputPartition}.
   * @return a new {@link KernelSparkPartitionReader} configured to read data from the specified
   *     partition
   * @throws IllegalArgumentException if the partition is not a {@link KernelSparkInputPartition}
   * @throws NullPointerException if the partition is null or contains null serialized data
   */
  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    if (!(partition instanceof KernelSparkInputPartition)) {
      throw new IllegalArgumentException(
          "Expected KernelSparkInputPartition, got: " + partition.getClass().getName());
    }

    KernelSparkInputPartition kernelPartition = (KernelSparkInputPartition) partition;
    return new KernelSparkPartitionReader(
        kernelPartition.getSerializedScanState(), kernelPartition.getSerializedScanFileRow());
  }

  /**
   * This implementation does not support columnar reading.
   *
   * @return false, indicating columnar reading is not supported
   */
  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
