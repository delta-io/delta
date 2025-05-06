/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.metrics;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.metrics.SnapshotMetricsResult;
import io.delta.kernel.metrics.SnapshotReport;
import java.util.Optional;

/** A basic POJO implementation of {@link SnapshotReport} for creating them */
public class SnapshotReportImpl extends DeltaOperationReportImpl implements SnapshotReport {

  /**
   * Creates a {@link SnapshotReport} for a failed snapshot query.
   *
   * @param snapshotContext context/metadata about the snapshot query
   * @param e the exception that was thrown
   */
  public static SnapshotReport forError(SnapshotQueryContext snapshotContext, Exception e) {
    return new SnapshotReportImpl(
        snapshotContext.getTablePath(),
        snapshotContext.getSnapshotMetrics(),
        snapshotContext.getVersion(),
        snapshotContext.getProvidedTimestamp(),
        Optional.of(e));
  }

  /**
   * Creates a {@link SnapshotReport} for a successful snapshot query.
   *
   * @param snapshotContext context/metadata about the snapshot query
   */
  public static SnapshotReport forSuccess(SnapshotQueryContext snapshotContext) {
    return new SnapshotReportImpl(
        snapshotContext.getTablePath(),
        snapshotContext.getSnapshotMetrics(),
        snapshotContext.getVersion(),
        snapshotContext.getProvidedTimestamp(),
        Optional.empty() /* exception */);
  }

  private final SnapshotMetricsResult snapshotMetrics;
  private final Optional<Long> version;
  private final Optional<Long> providedTimestamp;

  private SnapshotReportImpl(
      String tablePath,
      SnapshotMetrics snapshotMetrics,
      Optional<Long> version,
      Optional<Long> providedTimestamp,
      Optional<Exception> exception) {
    super(tablePath, exception);
    this.snapshotMetrics = requireNonNull(snapshotMetrics).captureSnapshotMetricsResult();
    this.version = requireNonNull(version);
    this.providedTimestamp = requireNonNull(providedTimestamp);
  }

  @Override
  public SnapshotMetricsResult getSnapshotMetrics() {
    return snapshotMetrics;
  }

  @Override
  public Optional<Long> getVersion() {
    return version;
  }

  @Override
  public Optional<Long> getProvidedTimestamp() {
    return providedTimestamp;
  }
}
