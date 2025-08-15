package io.delta.spark.dsv2.scan.batch;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Scan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.dsv2.utils.ResettableIterator;
import java.io.Serializable;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

/**
 * Spark Batch implementation backed by Delta Kernel Scan. This is a minimal scaffolding that
 * returns a single empty partition. Follow-ups will wire real partitions/readers to Kernel.
 */
public class KernelSparkBatchScan implements Batch, Serializable {

  private final Scan kernelScan;
  private final StructType sparkReadSchema;
  private final Engine engine;
  private transient KernelSparkBatchScanContext context;

  public KernelSparkBatchScan(Scan kernelScan, StructType sparkReadSchema) {
    this.kernelScan = requireNonNull(kernelScan, "kernelScan is null");
    this.sparkReadSchema = requireNonNull(sparkReadSchema, "sparkReadSchema is null");
    this.engine =
        io.delta.kernel.defaults.engine.DefaultEngine.create(
            org.apache.spark.sql.SparkSession.active().sessionState().newHadoopConf());
  }

  @Override
  public InputPartition[] planInputPartitions() {
    ensureContext();
    return context.planPartitions();
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    throw new UnsupportedOperationException("reader factory is not implemented");
  }

  private void ensureContext() {
    if (context == null) {
      CloseableIterator<FilteredColumnarBatch> iter = kernelScan.getScanFiles(engine);
      ResettableIterator<FilteredColumnarBatch> resettable = new ResettableIterator<>(iter);
      context = new KernelSparkBatchScanContext(resettable, kernelScan.getScanState(engine));
    }
  }
}
