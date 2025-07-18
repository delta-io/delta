package io.delta.dsv2.read;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.delta.kernel.utils.CloseableIterator;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DeltaScan implements org.apache.spark.sql.connector.read.Scan, Batch {
  private final Scan kernelScan;
  private final Engine tableEngine;
  private final StructType sparkReadSchema;
  private final String serializedScanState;
  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;
  private InputPartition[] cachedPartitions;

  // ====== Ada Add ===========
  private boolean isDistributedLogReplayEnabled = true;
  private final JavaSparkContext sparkContext;
  private final SparkSession spark;

  public DeltaScan(
      Scan kernelScan,
      Engine tableEngine,
      StructType sparkReadSchema,
      String accessKey,
      String secretKey,
      String sessionToken,
      SparkSession spark) {
    this.kernelScan = kernelScan;
    this.tableEngine = tableEngine;
    this.sparkReadSchema = sparkReadSchema;
    this.serializedScanState = JsonUtils.rowToJson(kernelScan.getScanState(tableEngine));
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.sessionToken = sessionToken;

    this.spark = spark;
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  /**
   * Get the Kernel ScanFiles ColumnarBatchIter and convert to {@link DeltaInputPartition} array.
   */
  private synchronized InputPartition[] planPartitions() {
    if (cachedPartitions != null) {
      return cachedPartitions;
    }

    List<DeltaInputPartition> scanFileAsInputPartitions = new ArrayList<>();

    CloseableIterator<FilteredColumnarBatch> columnarBatchIterator = null;
    if(!isDistributedLogReplayEnabled) columnarBatchIterator = kernelScan.getScanFiles(tableEngine);
    else {
      // get json iterator first
      columnarBatchIterator = kernelScan.getScanFilesFromJSON(tableEngine);

      ColumnarBatch tombstoneHashsets = kernelScan.getLogReplayStates();
      List<Row> checkpointFileList = kernelScan.getLogSegmentCheckpointFiles();

      boolean isSerial = true;
      // serial implementation, can be used to test Kernel API
      if(isSerial) {
        for(Row checkpointFile : checkpointFileList) {
          // for each executor
          CloseableIterator<FilteredColumnarBatch> parquetScanFilesIter = kernelScan.getScanFileFromCheckpoint(tombstoneHashsets, checkpointFile);
          columnarBatchIterator.combine(parquetScanFilesIter);
        }
      }
      else {
        // TODO: distributed implementation
        // Broadcast shared variables
        Broadcast<ColumnarBatch> tombstoneHashsetsBroadcast = sparkContext.broadcast(tombstoneHashsets);
        Broadcast<Scan> kernelScanBroadcast = sparkContext.broadcast(kernelScan);

        // Parallelize checkpoint files across executor
        JavaRDD<Row> checkpointRDD = sparkContext.parallelize(checkpointFileList, checkpointFileList.size());

        // For each checkpoint file, read its scan files using the kernel API
        JavaRDD<FilteredColumnarBatch> batchRDD = checkpointRDD.flatMap(checkpointFile -> {
          ColumnarBatch tombstones = tombstoneHashsetsBroadcast.value();
          Scan scan = kernelScanBroadcast.value();

          // Get iterator of scan files from this checkpoint
          try (CloseableIterator<FilteredColumnarBatch> iter =
                   scan.getScanFileFromCheckpoint(tombstones, checkpointFile)) {
            List<FilteredColumnarBatch> result = new ArrayList<>();
            while (iter.hasNext()) {
              result.add(iter.next());
            }
            return result.iterator(); // Safe: list iterator, not stream
          }
        });

        // Collect all batches back to the driver
        List<FilteredColumnarBatch> allBatches = batchRDD.collect();
      }

      // Ref: how iceberg send log replay tasks to spark executors
//      JavaRDD<DataFile> dataFileRDD =
//          sparkContext
//              .parallelize(toBeans(dataManifests), dataManifests.size())
//              .flatMap(new ReadDataManifest(tableBroadcast(), context(), withColumnStats));
//
//      List<List<DataFile>> dataFileGroups = collectPartitions(dataFileRDD);
    }

    while (columnarBatchIterator.hasNext()) {
      FilteredColumnarBatch columnarBatch = columnarBatchIterator.next();
      Iterator<Row> rowIterator = columnarBatch.getRows();

      while (rowIterator.hasNext()) {
        Row row = rowIterator.next();
        String serializedScanFileRow = JsonUtils.rowToJson(row);

        DeltaInputPartition inputPartition =
            new DeltaInputPartition(
                serializedScanFileRow, serializedScanState, accessKey, secretKey, sessionToken);
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
