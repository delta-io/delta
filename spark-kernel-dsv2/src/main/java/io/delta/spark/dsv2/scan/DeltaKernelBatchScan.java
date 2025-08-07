package io.delta.spark.dsv2.scan;

import io.delta.kernel.engine.Engine;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

public class DeltaKernelBatchScan implements Batch {
  private final Engine engine;
  private final StructType readSchema;
  private final InputPartition[] partitions;

  public DeltaKernelBatchScan(Engine engine, StructType readSchema, InputPartition[] partitions) {
    this.engine = engine;
    this.readSchema = readSchema;
    this.partitions = partitions;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new DeltaKernelPartitionReaderFactory();
  }
}
