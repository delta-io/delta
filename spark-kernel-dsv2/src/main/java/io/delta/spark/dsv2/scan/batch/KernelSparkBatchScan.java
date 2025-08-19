package io.delta.spark.dsv2.scan.batch;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 * Spark Batch implementation backed by Delta Kernel Scan. Uses a shared KernelSparkScanContext that
 * is created and managed by KernelSparkScan.
 */
public class KernelSparkBatchScan implements Batch, Serializable {

  private final KernelSparkScanContext sharedContext;

  public KernelSparkBatchScan(KernelSparkScanContext sharedContext) {
    this.sharedContext = requireNonNull(sharedContext, "sharedContext is null");
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return sharedContext.planPartitions();
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    throw new UnsupportedOperationException("reader factory is not implemented");
  }
}
