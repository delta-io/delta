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
package io.delta.kernel.metrics;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import java.util.Optional;
import java.util.UUID;

/** Defines the metadata and metrics for a Scan {@link MetricsReport} */
@JsonSerialize(as = ScanReport.class)
@JsonPropertyOrder({
  "tablePath",
  "operationType",
  "reportUUID",
  "exception",
  "tableVersion",
  "tableSchema",
  "snapshotReportUUID",
  "filter",
  "readSchema",
  "partitionPredicate",
  "dataSkippingFilter",
  "isFullyConsumed",
  "scanMetrics"
})
public interface ScanReport extends DeltaOperationReport {

  /** @return the version of the table in this scan */
  long getTableVersion();

  /** @return the schema of the table for this scan */
  StructType getTableSchema();

  /**
   * @return the {@link SnapshotReport#getReportUUID} for the snapshot this scan was created from
   */
  UUID getSnapshotReportUUID();

  /** @return the filter provided when building the scan */
  Optional<Predicate> getFilter();

  /** @return the read schema provided when building the scan */
  StructType getReadSchema();

  /** @return the part of {@link ScanReport#getFilter()} that was used for partition pruning */
  Optional<Predicate> getPartitionPredicate();

  /** @return the filter used for data skipping using the file statistics */
  Optional<Predicate> getDataSkippingFilter();

  /**
   * Whether the scan file iterator had been fully consumed when it was closed. The iterator may be
   * closed early (before being fully consumed) either due to an exception originating within
   * connector code or intentionally (such as for a LIMIT query).
   *
   * @return whether the scan file iterator had been fully consumed when it was closed
   */
  boolean getIsFullyConsumed();

  /** @return the metrics for this scan */
  ScanMetricsResult getScanMetrics();

  @Override
  default String getOperationType() {
    return "Scan";
  }
}
