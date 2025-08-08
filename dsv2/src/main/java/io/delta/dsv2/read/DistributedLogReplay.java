package io.delta.dsv2.read;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DataSkipping;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.Tombstones;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.util.*;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedLogReplay implements FlatMapFunction<String, String> {
  private static final Logger logger = LoggerFactory.getLogger(DistributedLogReplay.class);
  private final Broadcast<String> tombstoneBroadcast;
  private final Broadcast<String> broadcastTablePath;

  public DistributedLogReplay(
      Broadcast<String> tombstoneBroadcast, Broadcast<String> broadcastTablePath) {
    this.tombstoneBroadcast = tombstoneBroadcast;
    this.broadcastTablePath = broadcastTablePath;
  }

  @Override
  public Iterator<String> call(String checkpointFileJson) throws Exception {

    long startTime = System.currentTimeMillis();
    long lastTime = startTime;

    // Phase 1: Deserialize checkpoint file
    Row scanFileRow =
        JsonUtils.rowFromJson(checkpointFileJson, InternalScanFileUtils.SCAN_FILE_SCHEMA);
    FileStatus checkpointFile = InternalScanFileUtils.getAddFileStatus(scanFileRow);
    long currentTime = System.currentTimeMillis();
    logger.info("Checkpoint file deserialization completed in {} ms", currentTime - lastTime);
    lastTime = currentTime;

    // Phase 2: Create local engine
    Configuration conf = new Configuration();
    Engine tableEngine = DefaultEngine.create(conf);
    currentTime = System.currentTimeMillis();
    logger.info("Local engine creation completed in {} ms", currentTime - lastTime);
    lastTime = currentTime;

    // Phase 3: Deserialize tombstone set
    Tombstones deserializedHashset = Tombstones.deserializeTombstone(tombstoneBroadcast.value());
    currentTime = System.currentTimeMillis();
    logger.info("Tombstone deserialization completed in {} ms", currentTime - lastTime);
    lastTime = currentTime;

    String checkpointFileName =
        checkpointFile.getPath().substring(checkpointFile.getPath().lastIndexOf('/') + 1);
    // Phase 4: Execute log replay

    try (CloseableIterator<FilteredColumnarBatch> iter =
        LogReplay.getAddFilesForOneCheckpoint(
            tableEngine, broadcastTablePath.value(), checkpointFile, deserializedHashset)) {

      // todo: do data skipping here

      List<FilteredColumnarBatch> filteredBatches =
          iter.toInMemoryList(); // consume the iterator -> real log replay

      currentTime = System.currentTimeMillis();

      long pureLogReplayTime = currentTime - lastTime;

      lastTime = currentTime;

      List<String> result =
          filteredBatches.stream()
              .map(JsonUtils::filteredColumnarBatchToJson2)
              .collect(Collectors.toList());

      currentTime = System.currentTimeMillis();
      long serializationTime = currentTime - lastTime;
      long totalExecutorDuration = currentTime - startTime;

      logger.info(
          "Total read file & log replay for file {} time is {} ms",
          checkpointFileName,
          pureLogReplayTime);
      logger.info("Total FilteredColumnarBatch serialization time is {} ms", serializationTime);
      logger.info("Executor-side log replay completed in {} ms ", totalExecutorDuration);
      result.add("__TIMING__:" + totalExecutorDuration);
      return result.iterator();
    }
  }
}
