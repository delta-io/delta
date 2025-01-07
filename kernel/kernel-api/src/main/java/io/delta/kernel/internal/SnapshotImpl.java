/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal;

import static io.delta.kernel.internal.TableConfig.*;
import static io.delta.kernel.internal.TableConfig.TOMBSTONE_RETENTION;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Implementation of {@link Snapshot}. */
public class SnapshotImpl implements Snapshot {
  private final Path logPath;
  private final Path dataPath;
  private final long version;
  private final LogReplay logReplay;
  private final Protocol protocol;
  private final Metadata metadata;
  private final LogSegment logSegment;
  private Optional<Long> inCommitTimestampOpt;

  public SnapshotImpl(
      Path dataPath,
      LogSegment logSegment,
      LogReplay logReplay,
      Protocol protocol,
      Metadata metadata) {
    this.logPath = new Path(dataPath, "_delta_log");
    this.dataPath = dataPath;
    this.version = logSegment.version;
    this.logSegment = logSegment;
    this.logReplay = logReplay;
    this.protocol = protocol;
    this.metadata = metadata;
    this.inCommitTimestampOpt = Optional.empty();
  }

  /////////////////
  // Public APIs //
  /////////////////

  @Override
  public long getVersion(Engine engine) {
    return version;
  }

  /**
   * Get the timestamp (in milliseconds since the Unix epoch) of the latest commit in this Snapshot.
   * If the table does not yet exist (i.e. this Snapshot is being used to create the new table),
   * this method returns -1. Note that this -1 value will never be exposed to users - either they
   * get a valid snapshot for a table or they get an exception.
   *
   * <p>When InCommitTimestampTableFeature is enabled, the timestamp is retrieved from the
   * CommitInfo of the latest commit in this Snapshot, which can result in an IO operation.
   *
   * <p>For non-ICT tables, this is the same as the file modification time of the latest commit in
   * this Snapshot.
   */
  @Override
  public long getTimestamp(Engine engine) {
    if (IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata)) {
      if (!inCommitTimestampOpt.isPresent()) {
        Optional<CommitInfo> commitInfoOpt =
            CommitInfo.getCommitInfoOpt(engine, logPath, logSegment.version);
        inCommitTimestampOpt =
            Optional.of(
                CommitInfo.getRequiredInCommitTimestamp(
                    commitInfoOpt, String.valueOf(logSegment.version), dataPath));
      }
      return inCommitTimestampOpt.get();
    } else {
      return logSegment.lastCommitTimestamp;
    }
  }

  @Override
  public StructType getSchema(Engine engine) {
    return getMetadata().getSchema();
  }

  @Override
  public ScanBuilder getScanBuilder(Engine engine) {
    return new ScanBuilderImpl(dataPath, protocol, metadata, getSchema(engine), logReplay, engine);
  }

  ///////////////////
  // Internal APIs //
  ///////////////////

  public Path getLogPath() {
    return logPath;
  }

  public Path getDataPath() {
    return dataPath;
  }

  public Protocol getProtocol() {
    return protocol;
  }

  public List<String> getPartitionColumnNames(Engine engine) {
    return VectorUtils.toJavaList(getMetadata().getPartitionColumns());
  }

  /**
   * Get the domain metadata map from the log replay, which lazily loads and replays a history of
   * domain metadata actions, resolving them to produce the current state of the domain metadata.
   *
   * @return A map where the keys are domain names and the values are {@link DomainMetadata}
   *     objects.
   */
  public Map<String, DomainMetadata> getDomainMetadataMap() {
    return logReplay.getDomainMetadataMap();
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public LogSegment getLogSegment() {
    return logSegment;
  }

  public CreateCheckpointIterator getCreateCheckpointIterator(Engine engine) {
    long minFileRetentionTimestampMillis =
        System.currentTimeMillis() - TOMBSTONE_RETENTION.fromMetadata(metadata);
    return new CreateCheckpointIterator(engine, logSegment, minFileRetentionTimestampMillis);
  }

  /**
   * Get the latest transaction version for given <i>applicationId</i>. This information comes from
   * the transactions identifiers stored in Delta transaction log. This API is not a public API. For
   * now keep this internal to enable Flink upgrade to use Kernel.
   *
   * @param applicationId Identifier of the application that put transaction identifiers in Delta
   *     transaction log
   * @return Last transaction version or {@link Optional#empty()} if no transaction identifier
   *     exists for this application.
   */
  public Optional<Long> getLatestTransactionVersion(Engine engine, String applicationId) {
    return logReplay.getLatestTransactionIdentifier(engine, applicationId);
  }
}
