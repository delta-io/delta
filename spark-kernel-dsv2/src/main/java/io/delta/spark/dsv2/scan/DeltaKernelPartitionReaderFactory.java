package io.delta.spark.dsv2.scan;

import io.delta.kernel.Scan;
import io.delta.kernel.engine.Engine;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 * A PartitionReaderFactory implementation for Delta Kernel scans.
 * This creates partition readers that can read data from Delta tables using Delta Kernel.
 */
public class DeltaKernelPartitionReaderFactory implements PartitionReaderFactory {

  private final Scan kernelScan;
  private final Engine engine;

  public DeltaKernelPartitionReaderFactory(Scan kernelScan, Engine engine) {
    this.kernelScan = kernelScan;
    this.engine = engine;
  }

  @Override
  public PartitionReader<org.apache.spark.sql.catalyst.InternalRow> createReader(
      InputPartition partition) {
    return new DeltaKernelPartitionReader(kernelScan, engine, partition);
  }
} 