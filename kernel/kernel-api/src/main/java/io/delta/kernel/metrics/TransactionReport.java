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
package io.delta.kernel.metrics;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.delta.kernel.expressions.Column;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Defines the metadata and metrics for a transaction {@link MetricsReport} */
@JsonSerialize(as = TransactionReport.class)
@JsonPropertyOrder({
  "tablePath",
  "operationType",
  "reportUUID",
  "exception",
  "operation",
  "engineInfo",
  "baseSnapshotVersion",
  "snapshotReportUUID",
  "committedVersion",
  "clusteringColumns",
  "transactionMetrics"
})
public interface TransactionReport extends DeltaOperationReport {

  /**
   * @return The {@link io.delta.kernel.Operation} provided when the transaction was created using
   *     {@link io.delta.kernel.Table#createTransactionBuilder}.
   */
  String getOperation();

  /**
   * @return The engineInfo provided when the transaction was created using {@link
   *     io.delta.kernel.Table#createTransactionBuilder}.
   */
  String getEngineInfo();

  /**
   * The version of the table the transaction was created from. For example, if the latest table
   * version is 4 when the transaction is created, the transaction is based off of the snapshot of
   * the table at version 4. For a new table (e.g. a transaction that is creating a table) this is
   * -1.
   *
   * @return the table version of the snapshot the transaction was started from
   */
  long getBaseSnapshotVersion();

  /**
   * Get the list of clustering columns the table data is expected to be clustered by. This is
   * optional because clustering columns are not always defined for a table. Consumers of the
   * transaction report trigger clustering operations based on this list.
   *
   * @return list of clustering columns for the table. The columns are physical
   *  names of how the data is written in the data files. Each column is a array of strings
   *  representing the column names, e.g. for a table with two clustering columns
   *  "col1" and "col2.nestedCol2", the list will be [["col1"], ["col2", "nestedCol2"]].
   */
  List<String[]> getClusteringColumns();

  /**
   * @return the {@link SnapshotReport#getReportUUID} of the SnapshotReport for the transaction's
   *     snapshot construction. Empty for a new table transaction.
   */
  Optional<UUID> getSnapshotReportUUID();

  /**
   * @return the version committed to the table in this transaction. Empty for a failed transaction.
   */
  Optional<Long> getCommittedVersion();

  /** @return the metrics for this transaction */
  TransactionMetricsResult getTransactionMetrics();

  @Override
  default String getOperationType() {
    return "Transaction";
  }
}
