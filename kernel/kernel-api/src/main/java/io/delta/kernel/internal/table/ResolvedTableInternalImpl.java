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

package io.delta.kernel.internal.table;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.ScanBuilderImpl;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.metrics.SnapshotReportImpl;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.metrics.SnapshotReport;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** An implementation of {@link ResolvedTableInternal}. */
public class ResolvedTableInternalImpl implements ResolvedTableInternal {
  private final String path;
  private final String logPath;
  private final long version;
  private final Protocol protocol;
  private final Metadata metadata;
  private final Lazy<LogSegment> lazyLogSegment;
  private final LogReplay logReplay;
  private final Clock clock;
  private final SnapshotReport snapshotReport;

  public ResolvedTableInternalImpl(
      String path,
      long version,
      Protocol protocol,
      Metadata metadata,
      Lazy<LogSegment> lazyLogSegment,
      LogReplay logReplay,
      Clock clock,
      SnapshotQueryContext snapshotCtx) {
    this.path = requireNonNull(path, "path is null");
    this.logPath = new Path(path, "_delta_log").toString();
    this.version = version;
    this.protocol = requireNonNull(protocol, "protocol is null");
    this.metadata = requireNonNull(metadata, "metadata is null");
    this.lazyLogSegment = requireNonNull(lazyLogSegment, "lazyLogSegment is null");
    this.logReplay = requireNonNull(logReplay, "logReplay is null");
    this.clock = requireNonNull(clock, "clock is null");
    this.snapshotReport = SnapshotReportImpl.forSuccess(snapshotCtx);
  }

  //////////////////////////////////
  // Public ResolvedTable Methods //
  //////////////////////////////////

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public long getTimestamp() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public List<Column> getPartitionColumns() {
    return VectorUtils.<String>toJavaList(getMetadata().getPartitionColumns()).stream()
        .map(Column::new)
        .collect(Collectors.toList());
  }

  @Override
  public Optional<String> getDomainMetadata(String domain) {
    return Optional.ofNullable(getActiveDomainMetadataMap().get(domain))
        .map(DomainMetadata::getConfiguration);
  }

  @Override
  public StructType getSchema() {
    return getMetadata().getSchema();
  }

  @Override
  public ScanBuilder getScanBuilder() {
    return new ScanBuilderImpl(
        new Path(getPath()), getProtocol(), getMetadata(), getSchema(), logReplay, snapshotReport);
  }

  @Override
  public Committer getCommitter() {
    throw new UnsupportedOperationException("not implemented");
  }

  ///////////////////////////////////////
  // ResolvedTableInternalImpl Methods //
  ///////////////////////////////////////

  @Override
  public String getLogPath() {
    return logPath;
  }

  @Override
  public Protocol getProtocol() {
    return protocol;
  }

  @Override
  public Metadata getMetadata() {
    return metadata;
  }

  @Override
  public Clock getClock() {
    return clock;
  }

  @Override
  public Map<String, DomainMetadata> getActiveDomainMetadataMap() {
    return logReplay.getActiveDomainMetadataMap();
  }

  @Override
  @VisibleForTesting
  public LogSegment getLogSegment() {
    return lazyLogSegment.get();
  }

  @Override
  @VisibleForTesting
  public Lazy<LogSegment> getLazyLogSegment() {
    return lazyLogSegment;
  }
}
