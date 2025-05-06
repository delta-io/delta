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

import io.delta.kernel.metrics.SnapshotReport;
import io.delta.kernel.metrics.TransactionMetricsResult;
import io.delta.kernel.metrics.TransactionReport;
import java.util.Optional;
import java.util.UUID;

/** A basic POJO implementation of {@link TransactionReport} for creating them */
public class TransactionReportImpl extends DeltaOperationReportImpl implements TransactionReport {

  private final String operation;
  private final String engineInfo;
  private final long snapshotVersion;
  private final Optional<UUID> snapshotReportUUID;
  private final Optional<Long> committedVersion;
  private final TransactionMetricsResult transactionMetrics;

  /**
   * @param tablePath the path of the table for the transaction
   * @param operation the operation provided by the connector when the transaction was created
   * @param engineInfo the engineInfo provided by the connector when the transaction was created
   * @param committedVersion the version committed to the table. Empty for a failed transaction.
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
      TransactionMetrics transactionMetrics,
      SnapshotReport snapshotReport,
      Optional<Exception> exception) {
    super(tablePath, exception);
    this.operation = requireNonNull(operation);
    this.engineInfo = requireNonNull(engineInfo);
    this.transactionMetrics = requireNonNull(transactionMetrics).captureTransactionMetricsResult();
    this.committedVersion = committedVersion;
    requireNonNull(snapshotReport);
    checkArgument(
        !snapshotReport.getException().isPresent(),
        "Expected a successful SnapshotReport provided report has exception");
    checkArgument(
        snapshotReport.getVersion().isPresent(),
        "Expected a successful SnapshotReport but missing version");
    this.snapshotVersion = requireNonNull(snapshotReport).getVersion().get();
    if (snapshotVersion < 0) {
      // For a new table, no Snapshot is actually loaded and thus no SnapshotReport is emitted
      this.snapshotReportUUID = Optional.empty();
    } else {
      this.snapshotReportUUID = Optional.of(snapshotReport.getReportUUID());
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
