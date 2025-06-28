package io.delta.dsv2.read;

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
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaScan implements org.apache.spark.sql.connector.read.Scan, Batch {

  private static final Logger logger = LoggerFactory.getLogger(DeltaScan.class);

  private final Scan kernelScan;
  private final Engine tableEngine;
  private final StructType sparkReadSchema;
  private final String serializedScanState;
  private InputPartition[] cachedPartitions;

  public DeltaScan(Scan kernelScan, Engine tableEngine, StructType sparkReadSchema) {
    this.kernelScan = kernelScan;
    this.tableEngine = tableEngine;
    this.sparkReadSchema = sparkReadSchema;
    this.serializedScanState = JsonUtils.rowToJson(kernelScan.getScanState(tableEngine));
  }

  /**
   * Get the Kernel ScanFiles ColumnarBatchIter and convert to {@link DeltaInputPartition} array.
   */
  private synchronized InputPartition[] planPartitions() {
    if (cachedPartitions != null) {
      return cachedPartitions;
    }

    List<DeltaInputPartition> scanFileAsInputPartitions = new ArrayList<>();

    Iterator<FilteredColumnarBatch> columnarBatchIterator = kernelScan.getScanFiles(tableEngine);
    while (columnarBatchIterator.hasNext()) {
      FilteredColumnarBatch columnarBatch = columnarBatchIterator.next();
      Iterator<Row> rowIterator = columnarBatch.getRows();

      while (rowIterator.hasNext()) {
        Row row = rowIterator.next();
        String serializedScanFileRow = JsonUtils.rowToJson(row);
        DeltaInputPartition inputPartition =
            new DeltaInputPartition(serializedScanFileRow, serializedScanState);
        scanFileAsInputPartitions.add(inputPartition);
      }
    }

    cachedPartitions = scanFileAsInputPartitions.toArray(new InputPartition[0]);
    return cachedPartitions;
  }

  /////////////////////////
  // SparkScan Overrides //
  /////////////////////////

  @Override
  public StructType readSchema() {
    return sparkReadSchema;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  /////////////////////
  // Batch Overrides //
  /////////////////////

  /**
   * Returns a list of input partitions. Each InputPartition represents a data split that can be
   * processed by one Spark task. The number of input partitions returned here is the same as the
   * number of RDD partitions this scan outputs.
   *
   * <p>If the Scan supports filter pushdown, this Batch is likely configured with a filter and is
   * responsible for creating splits for that filter, which is not a full scan.
   *
   * <p>This method will be called only once during a data source scan, to launch one Spark job.
   */
  @Override
  public InputPartition[] planInputPartitions() {
    return planPartitions();
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new DeltaReaderFactory();
  }
}
