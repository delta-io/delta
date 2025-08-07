package io.delta.spark.dsv2.scan;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 * A PartitionReaderFactory implementation for Delta Kernel scans. This creates partition readers
 * that can read data from Delta tables using Delta Kernel.
 */
public class DeltaKernelPartitionReaderFactory implements PartitionReaderFactory {

  @Override
  public PartitionReader<org.apache.spark.sql.catalyst.InternalRow> createReader(
      InputPartition partition) {
    return new DeltaKernelPartitionReader(partition);
  }
}
