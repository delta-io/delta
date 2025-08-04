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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.metrics.SnapshotReport;
import io.delta.kernel.metrics.TransactionMetricsResult;
import io.delta.kernel.metrics.TransactionReport;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** A basic POJO implementation of {@link TransactionReport} for creating them */
public class TransactionReportImpl extends DeltaOperationReportImpl implements TransactionReport {

  private final String operation;
  private final String engineInfo;
  private final long snapshotVersion;
  private final Optional<UUID> snapshotReportUUID;
  private final Optional<Long> committedVersion;
  private final List<Column> clusteringColumns;
  private final TransactionMetricsResult transactionMetrics;

  /**
   * @param tablePath the path of the table for the transaction
   * @param operation the operation provided by the connector when the transaction was created
   * @param engineInfo the engineInfo provided by the connector when the transaction was created
   * @param committedVersion the version committed to the table. Empty for a failed transaction.
   * @param clusteringColumns the clustering columns for the table, if any. Empty if not set.
   * @param transactionMetrics the metrics for the transaction
   * @param snapshotReport the SnapshotReport for the base snapshot of this transaction. Note, in
   *     the case of a new table (when version = -1), this SnapshotReport is just a placeholder and
   *     was never emitted to the engine's metrics reporters.
   * @param exception the exception thrown. Empty for a successful transaction.
   */
  public TransactionReportImpl(
      String tablePath,
      String operation,
      String engineInfo,
      Optional<Long> committedVersion,
      Optional<List<Column>> clusteringColumns,
      TransactionMetrics transactionMetrics,
      Optional<SnapshotReport> snapshotReport,
      Optional<Exception> exception) {
    super(tablePath, exception);
    this.operation = requireNonNull(operation);
    this.engineInfo = requireNonNull(engineInfo);
    this.transactionMetrics = requireNonNull(transactionMetrics).captureTransactionMetricsResult();
    this.committedVersion = committedVersion;
    this.clusteringColumns = requireNonNull(clusteringColumns).orElse(Collections.emptyList());
    requireNonNull(snapshotReport);
    if (snapshotReport.isPresent()) {
      checkArgument(
          !snapshotReport.get().getException().isPresent(),
          "Expected a successful SnapshotReport provided report has exception");
      checkArgument(
          snapshotReport.get().getVersion().isPresent(),
          "Expected a successful SnapshotReport but missing version");
      this.snapshotVersion = snapshotReport.get().getVersion().get();
      this.snapshotReportUUID = Optional.of(snapshotReport.get().getReportUUID());
    } else {
      this.snapshotVersion = -1;
      this.snapshotReportUUID = Optional.empty();
    }
  }

  @Override
  public String getOperation() {
    return operation;
  }

  @Override
  public String getEngineInfo() {
    return engineInfo;
  }

  @Override
  public long getBaseSnapshotVersion() {
    return snapshotVersion;
  }

  @Override
  public List<Column> getClusteringColumns() {
    return clusteringColumns;
  }

  @Override
  public Optional<UUID> getSnapshotReportUUID() {
    return snapshotReportUUID;
  }

  @Override
  public Optional<Long> getCommittedVersion() {
    return committedVersion;
  }

  @Override
  public TransactionMetricsResult getTransactionMetrics() {
    return transactionMetrics;
  }
}
