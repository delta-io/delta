package io.delta.dsv2.read;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.Tombstones;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.util.*;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

public class DistributedLogReplay implements FlatMapFunction<String, FilteredColumnarBatch> {
  private final Broadcast<String> tombstoneBroadcast;
  private final Broadcast<String> broadcastTablePath;

  public DistributedLogReplay(
      Broadcast<String> tombstoneBroadcast, Broadcast<String> broadcastTablePath) {
    this.tombstoneBroadcast = tombstoneBroadcast;
    this.broadcastTablePath = broadcastTablePath;
  }

  @Override
  public Iterator<FilteredColumnarBatch> call(String checkpointFileJson) throws Exception {
    Row scanFileRow =
        JsonUtils.rowFromJson(checkpointFileJson, InternalScanFileUtils.SCAN_FILE_SCHEMA);
    FileStatus checkpointFile = InternalScanFileUtils.getAddFileStatus(scanFileRow);

    Configuration conf = new Configuration();
    Engine tableEngine = DefaultEngine.create(conf);

    // TODO: deserialize tombstone set
    Tombstones deserializedHashset = Tombstones.deserializeTombstone(tombstoneBroadcast.value());

    try (CloseableIterator<FilteredColumnarBatch> iter =
        LogReplay.getAddFilesForOneCheckpoint(
            tableEngine, broadcastTablePath.value(), checkpointFile, deserializedHashset)) {
      List<FilteredColumnarBatch> result = new ArrayList<>();
      while (iter.hasNext()) {
        result.add(iter.next());
      }
      return result.iterator();
    }
  }
}
