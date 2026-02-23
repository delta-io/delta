/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.spark.internal.v2.utils;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.ScanMetrics;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.util.SerializableConfiguration;

/**
 * Helper that extracts serializable data from a Kernel {@link SnapshotImpl} so that a Spark RDD can
 * reconstruct a {@link LogReplay} on any executor (including in cluster mode) and stream scan files
 * without holding non-serializable Kernel objects in the RDD closure.
 */
public class SnapshotScanHelper {

  private SnapshotScanHelper() {}

  /**
   * Extracts a {@link SerializableScanData} from an existing snapshot. This is a pure in-memory
   * operation with no I/O -- it reads only the already-materialized LogSegment from the snapshot.
   */
  public static SerializableScanData extractScanData(
      SnapshotImpl snapshot, Configuration hadoopConf) {
    LogSegment logSegment = snapshot.getLogSegment();

    return new SerializableScanData(
        snapshot.getDataPath().toString(),
        logSegment.getLogPath().toString(),
        logSegment.getVersion(),
        SerializableFileStatus.fromList(logSegment.getDeltas()),
        SerializableFileStatus.fromList(logSegment.getCompactions()),
        SerializableFileStatus.fromList(logSegment.getCheckpoints()),
        SerializableFileStatus.from(logSegment.getDeltaFileAtEndVersion()),
        logSegment.getLastSeenChecksum().map(SerializableFileStatus::from).orElse(null),
        logSegment.getMaxPublishedDeltaVersion().orElse(null),
        new SerializableConfiguration(hadoopConf));
  }

  /**
   * Reconstructs a {@link LogReplay} from the serialized data and returns a lazily-streaming
   * iterator of scan file batches. The only I/O is the normal log replay (reading checkpoint and
   * delta files from storage), which is identical to what {@code Scan.getScanFiles()} does.
   */
  public static CloseableIterator<FilteredColumnarBatch> rebuildScanFilesIterator(
      SerializableScanData data) {
    Engine engine = DefaultEngine.create(data.hadoopConf.value());

    LogSegment logSegment =
        new LogSegment(
            new Path(data.logPath),
            data.version,
            SerializableFileStatus.toFileStatusList(data.deltas),
            SerializableFileStatus.toFileStatusList(data.compactions),
            SerializableFileStatus.toFileStatusList(data.checkpoints),
            data.deltaAtEndVersion.toFileStatus(),
            Optional.ofNullable(data.lastSeenChecksum).map(SerializableFileStatus::toFileStatus),
            Optional.ofNullable(data.maxPublishedDeltaVersion));

    Lazy<LogSegment> lazyLogSegment = new Lazy<>(() -> logSegment);
    Lazy<Optional<CRCInfo>> lazyCrcInfo = new Lazy<>(Optional::empty);

    LogReplay logReplay =
        new LogReplay(engine, new Path(data.dataPath), lazyLogSegment, lazyCrcInfo);

    return logReplay.getAddFilesAsColumnarBatches(
        engine,
        false /* shouldReadStats */,
        Optional.empty() /* checkpointPredicate */,
        new ScanMetrics(),
        Optional.empty() /* paginationContextOpt */);
  }
}
