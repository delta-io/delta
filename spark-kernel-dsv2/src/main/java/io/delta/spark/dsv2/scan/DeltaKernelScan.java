package io.delta.spark.dsv2.scan;

import io.delta.kernel.Scan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

/**
 * A Spark Scan implementation that wraps Delta Kernel's Scan. This allows Spark to use Delta Kernel
 * for reading Delta tables.
 */
public class DeltaKernelScan implements org.apache.spark.sql.connector.read.Scan {

  private final Scan kernelScan;
  private final Engine engine;
  private final StructType readSchema;
  private InputPartition[] cachedPartitions;

  public DeltaKernelScan(Scan kernelScan, StructType readSchema, Engine engine) {
    this.kernelScan = kernelScan;
    this.engine = engine;
    this.readSchema = readSchema;
  }

  @Override
  public StructType readSchema() {
    return readSchema;
  }

  private synchronized InputPartition[] planPartitionsOnce() {
    if (cachedPartitions != null) return cachedPartitions;

    List<InputPartition> partitions = new ArrayList<>();
    final String serializedScanState = JsonUtils.rowToJson(kernelScan.getScanState(engine));
    Iterator<FilteredColumnarBatch> scanFiles = kernelScan.getScanFiles(engine);
    while (scanFiles.hasNext()) {
      FilteredColumnarBatch batch = scanFiles.next();
      Iterator<Row> rows = batch.getRows();
      while (rows.hasNext()) {
        Row row = rows.next();
        partitions.add(new DeltaKernelScanPartition(JsonUtils.rowToJson(row), serializedScanState));
      }
    }
    cachedPartitions = partitions.toArray(new InputPartition[0]);
    return cachedPartitions;
  }

  @Override
  public Batch toBatch() {
    return new DeltaKernelBatchScan(engine, readSchema, planPartitionsOnce());
  }
}
