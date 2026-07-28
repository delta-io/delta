/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import io.delta.kernel.Scan;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.metrics.SnapshotReportImpl;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.metrics.SnapshotReport;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.util.SerializableConfiguration;

/**
 * Serializable carrier for a Delta snapshot's state. Created on the driver from an existing {@link
 * SnapshotImpl} (zero I/O), and reconstructed on the executor as a read-only {@link Scan} via
 * {@link #toScan(Configuration)}. The returned {@code Scan} interface exposes only read operations,
 * preventing accidental misuse of write-path APIs (e.g. {@code Committer}).
 *
 * <p>TODO: This class relies on 10 {@code io.delta.kernel.internal.*} packages which have no
 * stability guarantees. Any Kernel refactor (constructor signature changes, package moves, class
 * renames) will break this code without warning. This is temporary until Kernel exposes a public
 * serialization API (e.g. {@code Snapshot.toSerializableReadOnlyState()}).
 */
public class SerializableReadOnlySnapshot implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String dataPath;
  private final String logPath;
  private final long version;

  private final ArrayList<SerializableKernelFileStatus> deltas;
  private final ArrayList<SerializableKernelFileStatus> compactions;
  private final ArrayList<SerializableKernelFileStatus> checkpoints;
  private final SerializableKernelFileStatus deltaAtEndVersion;
  private final SerializableKernelFileStatus lastSeenChecksum; // nullable
  private final Long maxPublishedDeltaVersion; // nullable

  private final Protocol protocol;
  private final Metadata metadata;
  private final SerializableConfiguration hadoopConf;

  private SerializableReadOnlySnapshot(
      String dataPath,
      String logPath,
      long version,
      ArrayList<SerializableKernelFileStatus> deltas,
      ArrayList<SerializableKernelFileStatus> compactions,
      ArrayList<SerializableKernelFileStatus> checkpoints,
      SerializableKernelFileStatus deltaAtEndVersion,
      SerializableKernelFileStatus lastSeenChecksum,
      Long maxPublishedDeltaVersion,
      Protocol protocol,
      Metadata metadata,
      SerializableConfiguration hadoopConf) {
    this.dataPath = dataPath;
    this.logPath = logPath;
    this.version = version;
    this.deltas = deltas;
    this.compactions = compactions;
    this.checkpoints = checkpoints;
    this.deltaAtEndVersion = deltaAtEndVersion;
    this.lastSeenChecksum = lastSeenChecksum;
    this.maxPublishedDeltaVersion = maxPublishedDeltaVersion;
    this.protocol = protocol;
    this.metadata = metadata;
    this.hadoopConf = hadoopConf;
  }

  /**
   * Driver-side: extract the snapshot state from an existing Kernel {@link SnapshotImpl}. This
   * performs zero I/O — all data is already in memory on the driver.
   */
  public static SerializableReadOnlySnapshot fromSnapshot(
      SnapshotImpl snapshot, Configuration hadoopConf) {
    LogSegment logSegment = snapshot.getLogSegment();
    return new SerializableReadOnlySnapshot(
        snapshot.getDataPath().toString(),
        logSegment.getLogPath().toString(),
        snapshot.getVersion(),
        new ArrayList<>(SerializableKernelFileStatus.fromList(logSegment.getDeltas())),
        new ArrayList<>(SerializableKernelFileStatus.fromList(logSegment.getCompactions())),
        new ArrayList<>(SerializableKernelFileStatus.fromList(logSegment.getCheckpoints())),
        SerializableKernelFileStatus.from(logSegment.getDeltaFileAtEndVersion()),
        logSegment.getLastSeenChecksum().map(SerializableKernelFileStatus::from).orElse(null),
        logSegment.getMaxPublishedDeltaVersion().orElse(null),
        snapshot.getProtocol(),
        snapshot.getMetadata(),
        new SerializableConfiguration(hadoopConf));
  }

  /**
   * Executor-side: reconstruct a read-only {@link Scan} from the serialized snapshot state. The
   * returned {@code Scan} only exposes {@code getScanFiles()} — no write-path or commit APIs.
   */
  public Scan toScan(Configuration hadoopConfOverride) {
    return buildScan(hadoopConfOverride);
  }

  /**
   * Executor-side: reconstruct using the serialized Hadoop configuration. Convenience overload when
   * no conf override is needed.
   */
  public Scan toScan() {
    return buildScan(hadoopConf.value());
  }

  public long getVersion() {
    return version;
  }

  /** Returns the serialized Hadoop configuration for creating an Engine on the executor. */
  public Configuration getHadoopConf() {
    return hadoopConf.value();
  }

  // ---- internal reconstruction ----

  private Scan buildScan(Configuration conf) {
    Engine engine = DefaultEngine.create(conf);
    io.delta.kernel.internal.fs.Path kernelDataPath =
        new io.delta.kernel.internal.fs.Path(dataPath);
    io.delta.kernel.internal.fs.Path kernelLogPath = new io.delta.kernel.internal.fs.Path(logPath);

    LogSegment logSegment =
        new LogSegment(
            kernelLogPath,
            version,
            SerializableKernelFileStatus.toFileStatusList(deltas),
            SerializableKernelFileStatus.toFileStatusList(compactions),
            SerializableKernelFileStatus.toFileStatusList(checkpoints),
            deltaAtEndVersion.toFileStatus(),
            Optional.ofNullable(lastSeenChecksum).map(SerializableKernelFileStatus::toFileStatus),
            Optional.ofNullable(maxPublishedDeltaVersion));

    Lazy<LogSegment> lazyLogSegment = new Lazy<>(() -> logSegment);
    // CRC is passed as empty because this Scan is only used for getScanFiles() which does not
    // consult CRC. CRC validation was already performed on the driver when snapshotAtSourceInit
    // was constructed. NOTE: if LogReplay.getActiveDomainMetadataMap() were ever called on this
    // reconstructed snapshot, it would fall back to a full log scan (Case 1 in
    // loadDomainMetadataMap) instead of the O(1) CRC shortcut — correct but expensive.
    Lazy<Optional<CRCInfo>> lazyCrcInfo = new Lazy<>(Optional::empty);

    LogReplay logReplay = new LogReplay(engine, kernelDataPath, lazyLogSegment, lazyCrcInfo);

    SnapshotQueryContext queryContext = SnapshotQueryContext.forVersionSnapshot(dataPath, version);
    queryContext.setResolvedVersion(version);
    SnapshotReport snapshotReport = SnapshotReportImpl.forSuccess(queryContext);

    return new ScanImpl(
        metadata.getSchema(),
        metadata.getSchema(),
        protocol,
        metadata,
        logReplay,
        Optional.empty(),
        kernelDataPath,
        snapshotReport);
  }
}
