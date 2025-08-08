package io.delta.dsv2.read;

import io.delta.kernel.Scan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.Tombstones;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
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
  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;
  private InputPartition[] cachedPartitions;

  // ====== Ada Add ===========
  private boolean isDistributedLogReplayEnabled = false;
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

    long startTime = System.currentTimeMillis();
    long lastTime = startTime;
    logger.info("Starting getScanFiles process");

    List<DeltaInputPartition> scanFileAsInputPartitions = new ArrayList<>();

    List<FilteredColumnarBatch> scanFiles;
    if (!isDistributedLogReplayEnabled) {

      Tuple2<List<FilteredColumnarBatch>, Tombstones> result =
          kernelScan.getScanFilesFromJSON(tableEngine);
      scanFiles = new ArrayList<>(result._1);
      Tombstones tombstoneHashsets = result._2;
      List<FileStatus> checkpointFileList = kernelScan.getLogSegmentCheckpointFiles();

      for (FileStatus checkpointFile : checkpointFileList) {
        long t1 = System.currentTimeMillis();
        List<FilteredColumnarBatch> batchesFromCheckpoint =
            LogReplay.getAddFilesForOneCheckpoint(
                    tableEngine, tablePath, checkpointFile, tombstoneHashsets)
                .toInMemoryList();
        long durationForOneFile = System.currentTimeMillis() - t1;
        String checkpointFileName =
            checkpointFile.getPath().substring(checkpointFile.getPath().lastIndexOf('/') + 1);
        logger.info(
            "Non-distributed getScanFiles for {} in {} ms", checkpointFileName, durationForOneFile);
        scanFiles.addAll(batchesFromCheckpoint);
      }

      long currentTime = System.currentTimeMillis();
      logger.info("Non-distributed getScanFiles completed in {} ms", currentTime - lastTime);
    } else {

      // ======== Distributed log replay: Using SparkSession (like Iceberg) ===============
      logger.info("Starting distributed log replay code path");

      // Phase 1: Get scan files and tombstones from JSON
      Tuple2<List<FilteredColumnarBatch>, Tombstones> result =
          kernelScan.getScanFilesFromJSON(tableEngine);
      scanFiles = new ArrayList<>(result._1);
      Tombstones tombstoneHashsets = result._2;
      long currentTime = System.currentTimeMillis();
      logger.info("getScanFilesFromJSON completed in {} ms", currentTime - lastTime); // 8ms
      lastTime = currentTime;

      // Phase 2: Serialize tombstones
      String serializedTombstones = tombstoneHashsets.toSerialize();
      currentTime = System.currentTimeMillis();
      logger.info("Tombstones serialization completed in {} ms", currentTime - lastTime); // 2 ms
      lastTime = currentTime;

      // Phase 3: Broadcast tombstones and table path
      Broadcast<String> tombstoneHashsetsBroadcast = sparkContext.broadcast(serializedTombstones);
      Broadcast<String> broadcastTablePath = sparkContext.broadcast(tablePath);
      currentTime = System.currentTimeMillis();
      logger.info("Broadcast operations completed in {} ms", currentTime - lastTime); // 81 ms
      lastTime = currentTime;

      // Phase 4: Prepare checkpoint files
      List<FileStatus> checkpointFileList = kernelScan.getLogSegmentCheckpointFiles();
      List<String> serializedCheckpoints =
          checkpointFileList.stream()
              .map(
                  fileStatus -> {
                    Row row = InternalScanFileUtils.generateScanFileRow(fileStatus);
                    return JsonUtils.rowToJson(row);
                  })
              .collect(Collectors.toList());
      currentTime = System.currentTimeMillis();
      logger.info(
          "Checkpoint file serialization completed in {} ms", currentTime - lastTime); // 14 ms
      lastTime = currentTime;

      // Phase 5: Distribute log replay tasks
      JavaRDD<String> checkpointStringRDD =
          sparkContext.parallelize(
              serializedCheckpoints, serializedCheckpoints.size()); // print out this

      currentTime = System.currentTimeMillis();
      logger.info("Create Java RDD in {} ms", currentTime - lastTime); // 103 ms
      lastTime = currentTime;

      JavaRDD<String> flatMapped =
          checkpointStringRDD.flatMap(
              new DistributedLogReplay(tombstoneHashsetsBroadcast, broadcastTablePath));

      currentTime = System.currentTimeMillis();
      logger.info("Java RDD mapping completed in {} ms", currentTime - lastTime); // 28 ms
      lastTime = currentTime;

      List<String> serializedAddFilesInCheckpoints = flatMapped.collect();

      currentTime = System.currentTimeMillis();
      logger.info(
          "Distributed log replay collect phase completed in {} ms",
          currentTime - lastTime); // 886ms
      lastTime = currentTime;

      /**
       * executor 1: 5s to log replay (pure log replay) executor 2: 5s to log replay (pure log
       * replay) serial : 8000 ms "distributed": 15,000 ms
       */
      // Phase 6: Deserialize filtered columnar batches
      List<FilteredColumnarBatch> allAddFilesInCheckpoints = new ArrayList<>();
      long totalExecutorTime = 0;
      for (String s : serializedAddFilesInCheckpoints) {
        if (s.startsWith("__TIMING__:")) {
          totalExecutorTime += Long.parseLong(s.substring("__TIMING__:".length()));
        } else {
          allAddFilesInCheckpoints.add(
              JsonUtils.filteredColumnarBatchFromJson(s, InternalScanFileUtils.SCAN_FILE_SCHEMA));
        }
      }

      currentTime = System.currentTimeMillis();
      logger.info(
          "FilteredColumnarBatch deserialization completed in {} ms for {} batches",
          currentTime - lastTime,
          allAddFilesInCheckpoints.size()); // 15ms
      lastTime = currentTime;

      // Collect all batches back to the driver
      scanFiles.addAll(allAddFilesInCheckpoints);

      int fileCnt = 0;
      for (FilteredColumnarBatch filteredBatch : allAddFilesInCheckpoints) {
        fileCnt += filteredBatch.getData().getSize();
      }
      logger.info("Number of Batches: " + scanFiles.size());
      logger.info("Number of AddFiles: " + fileCnt);

      currentTime = System.currentTimeMillis();
      logger.info("Total getScanFiles process completed in {} ms", currentTime - startTime);
      logger.info(
          "Total executor-side log replay time across all partitions: {} ms", totalExecutorTime);
    }

//    if(true) {
//      throw new UnsupportedOperationException("stop here: finish get scan files");
//    }

    // Phase 7: Convert to input partitions
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

    // logger.info("Input partition creation completed in {} ms", currentTime - lastTime);

    cachedPartitions = scanFileAsInputPartitions.toArray(new InputPartition[0]);

    return cachedPartitions;
    //return new InputPartition[0];
  }
  // todo: benchmark; make big table: a lot of tiny files; big multi part checkpoint -> should be
  // terrible (serialization)
  // also read normal dsv2 (no distributed log replay)
  // log: keep track of time metrics: how long it take to do xxxx
  // todo: implement broadcast predicate -> executor side filtering -> should get better
  // todo: clean up design doc; code: add logs (which executor is on; which executor is process
  // what)

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
