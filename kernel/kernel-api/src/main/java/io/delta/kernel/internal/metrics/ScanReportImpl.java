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
package io.delta.kernel.internal.metrics;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.metrics.ScanMetricsResult;
import io.delta.kernel.metrics.ScanReport;
import io.delta.kernel.types.StructType;
import java.util.Optional;
import java.util.UUID;

/** A basic POJO implementation of {@link ScanReport} for creating them */
public class ScanReportImpl extends DeltaOperationReportImpl implements ScanReport {

  private final long tableVersion;
  private final StructType tableSchema;
  private final UUID snapshotReportUUID;
  private final Optional<Predicate> filter;
  private final StructType readSchema;
  private final Optional<Predicate> partitionPredicate;
  private final Optional<Predicate> dataSkippingFilter;
  private final boolean isFullyConsumed;
  private final ScanMetricsResult scanMetricsResult;

  public ScanReportImpl(
      String tablePath,
      long tableVersion,
      StructType tableSchema,
      UUID snapshotReportUUID,
      Optional<Predicate> filter,
      StructType readSchema,
      Optional<Predicate> partitionPredicate,
      Optional<Predicate> dataSkippingFilter,
      boolean isFullyConsumed,
      ScanMetrics scanMetrics,
      Optional<Exception> exception) {
    super(tablePath, exception);
    this.tableVersion = tableVersion;
    this.tableSchema = requireNonNull(tableSchema);
    this.snapshotReportUUID = requireNonNull(snapshotReportUUID);
    this.filter = requireNonNull(filter);
    this.readSchema = requireNonNull(readSchema);
    this.partitionPredicate = requireNonNull(partitionPredicate);
    this.dataSkippingFilter = requireNonNull(dataSkippingFilter);
    this.isFullyConsumed = isFullyConsumed;
    this.scanMetricsResult = requireNonNull(scanMetrics).captureScanMetricsResult();
  }

  @Override
  public long getTableVersion() {
    return tableVersion;
  }

  @Override
  public StructType getTableSchema() {
    return tableSchema;
  }

  @Override
  public UUID getSnapshotReportUUID() {
    return snapshotReportUUID;
  }

  @Override
  public Optional<Predicate> getFilter() {
    return filter;
  }

  @Override
  public StructType getReadSchema() {
    return readSchema;
  }

  @Override
  public Optional<Predicate> getPartitionPredicate() {
    return partitionPredicate;
  }

  @Override
  public Optional<Predicate> getDataSkippingFilter() {
    return dataSkippingFilter;
  }

  @Override
  public boolean getIsFullyConsumed() {
    return isFullyConsumed;
  }

  @Override
  public ScanMetricsResult getScanMetrics() {
    return scanMetricsResult;
  }
}
