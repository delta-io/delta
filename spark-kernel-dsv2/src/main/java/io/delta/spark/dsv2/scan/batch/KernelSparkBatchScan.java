package io.delta.spark.dsv2.scan.batch;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Scan;
import java.io.Serializable;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.internal.lang.Lazy;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

/**
 * Spark Batch implementation backed by Delta Kernel Scan. This is a minimal scaffolding that
 * returns a single empty partition. Follow-ups will wire real partitions/readers to Kernel.
 */
public class KernelSparkBatchScan implements Batch, Serializable {

  private final ScanBuilder kernelScanBuilder;
  private final StructType sparkReadSchema;

  public KernelSparkBatchScan(ScanBuilder kernelScanBuilder, StructType sparkReadSchema) {
    this.kernelScanBuilder = requireNonNull(kernelScanBuilder, "kernelScan is null");
    this.sparkReadSchema = requireNonNull(sparkReadSchema, "sparkReadSchema is null");
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return new InputPartition[] {new EmptyInputPartition()};
  }


  @Override
  public PartitionReaderFactory createReaderFactory() {
    throw new UnsupportedOperationException("reader factory is not implemented");
  }

  private class




}
