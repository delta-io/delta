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

/** Factory for creating partition readers from Delta Kernel input partitions. */
public class KernelSparkPartitionReaderFactory implements PartitionReaderFactory, Serializable {

  private static final long serialVersionUID = 1L;

  /** Creates a new partition reader factory. */
  public KernelSparkPartitionReaderFactory() {}

  /**
   * Creates a new partition reader for the specified input partition.
   *
   * @param partition the input partition
   * @return a new partition reader
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

  /** @return false, columnar reading is not supported */
  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
