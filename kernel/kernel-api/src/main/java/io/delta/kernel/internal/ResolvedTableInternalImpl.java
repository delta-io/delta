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

package io.delta.kernel.internal;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Optional;

public class ResolvedTableInternalImpl implements ResolvedTableInternal {
  private final String path;
  private final String logPath;
  private final long version;
  private final Lazy<LogSegment> lazyLogSegment;
  private final Clock clock;

  public ResolvedTableInternalImpl(
      String path, long version, Lazy<LogSegment> lazyLogSegment, Clock clock) {
    this.path = requireNonNull(path, "path is null");
    this.logPath = new Path(path, "_delta_log").toString();
    this.version = version;
    this.lazyLogSegment = requireNonNull(lazyLogSegment, "lazyLogSegment is null");
    this.clock = requireNonNull(clock, "clock is null");
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
  public List<String> getPartitionColumns() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Optional<String> getDomainMetadata(String domain) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public StructType getSchema() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ScanBuilder getScanBuilder() {
    throw new UnsupportedOperationException("Not implemented");
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
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Metadata getMetadata() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Clock getClock() {
    return clock;
  }

  @Override
  public LogSegment getLogSegment() {
    return lazyLogSegment.get();
  }
}
