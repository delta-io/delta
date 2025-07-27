package io.delta.dsv2.read;

import io.delta.kernel.Scan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.Tombstones;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

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
  private final String tablePath;

  public DeltaScan(
      Scan kernelScan,
      Engine tableEngine,
      String tablePath,
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
    this.tablePath = tablePath;
  }

  /**
   * Get the Kernel ScanFiles ColumnarBatchIter and convert to {@link DeltaInputPartition} array.
   */
  private synchronized InputPartition[] planPartitions() {
    if (cachedPartitions != null) {
      return cachedPartitions;
    }

    List<DeltaInputPartition> scanFileAsInputPartitions = new ArrayList<>();

    List<FilteredColumnarBatch> scanFiles;
    if (!isDistributedLogReplayEnabled) {
      scanFiles = kernelScan.getScanFiles(tableEngine).toInMemoryList();
    } else {
      // ======== Distributed log replay: Using SparkSession (like Iceberg) ===============
      Tuple2<List<FilteredColumnarBatch>, Tombstones> result =
          kernelScan.getScanFilesFromJSON(tableEngine);
      scanFiles = new ArrayList<>(result._1);
      Tombstones tombstoneHashsets = result._2;

      String serializedTombstones = tombstoneHashsets.toSerialize();
      Broadcast<String> tombstoneHashsetsBroadcast = sparkContext.broadcast(serializedTombstones);
      Broadcast<String> broadcastTablePath = sparkContext.broadcast(tablePath);

      List<FileStatus> checkpointFileList = kernelScan.getLogSegmentCheckpointFiles();
      //      JavaRDD<FileStatus> checkpointRDD =
      //          sparkContext.parallelize(checkpointFileList, checkpointFileList.size());
      //      JavaRDD<String> checkpointStringRDD =
      //          checkpointRDD.map(
      //              fileStatus -> {
      //                Row row = InternalScanFileUtils.generateScanFileRow(fileStatus);
      //                return JsonUtils.rowToJson(row);
      //              });
      List<String> serializedCheckpoints =
          checkpointFileList.stream()
              .map(
                  fileStatus -> {
                    Row row = InternalScanFileUtils.generateScanFileRow(fileStatus);
                    return JsonUtils.rowToJson(row);
                  })
              .collect(Collectors.toList());

      JavaRDD<String> checkpointStringRDD =
          sparkContext.parallelize(serializedCheckpoints, serializedCheckpoints.size());

      // TODO: FilteredColumnarBatch must be serializable
      JavaRDD<FilteredColumnarBatch> batchRDD =
          checkpointStringRDD.flatMap(
              new DistributedLogReplay(tombstoneHashsetsBroadcast, broadcastTablePath));

      // Collect all batches back to the driver
      scanFiles.addAll(batchRDD.collect());
      // todo: apply filter here
    }

    for (FilteredColumnarBatch columnarBatch : scanFiles) {
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
  // ============ Method two: using DSV2 API ================

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
