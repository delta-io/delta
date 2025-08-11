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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

import io.delta.kernel.Operation;
import io.delta.kernel.Transaction;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.SetTransaction;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import java.util.*;

public class UpdateTableTransactionBuilderImpl implements UpdateTableTransactionBuilder {

  /** Timestamp when this builder was created, used for populating any {@link SetTransaction} */
  private final long txnBuilderStartTime = System.currentTimeMillis();

  /* Class fields provided in the constructor */
  private final SnapshotImpl snapshot;
  private final String engineInfo;
  private final Operation operation;

  /* Optional metadata configured in this builder by the connector */
  private Optional<SetTransaction> setTxnOpt = Optional.empty();
  private Optional<Map<String, String>> tablePropertiesAddedOpt = Optional.empty();
  private Optional<Set<String>> tablePropertiesRemovedOpt = Optional.empty();
  private Optional<StructType> updatedSchemaOpt = Optional.empty();
  private Optional<List<Column>> inputLogicalClusteringColumnsOpt = Optional.empty();
  /**
   * Number of retries for concurrent write exceptions to resolve conflicts and retry commit. In
   * Delta-Spark, for historical reasons the number of retries is really high (10m). We are starting
   * with a lower number by default for now. If this is not sufficient we can update it.
   */
  private int maxRetries = 200;

  /** Number of commits between producing a log compaction file. */
  private int logCompactionInterval = 0;

  public UpdateTableTransactionBuilderImpl(
      SnapshotImpl snapshot, String engineInfo, Operation operation) {
    validateOperationParam(operation);
    this.snapshot = snapshot;
    this.engineInfo = engineInfo;
    this.operation = operation;
    TableFeatures.validateKernelCanWriteToTable(
        snapshot.getProtocol(), snapshot.getMetadata(), snapshot.getPath());
  }

  @Override
  public UpdateTableTransactionBuilder withUpdatedSchema(StructType schema) {
    this.updatedSchemaOpt = Optional.of(schema);
    return this;
  }

  @Override
  public UpdateTableTransactionBuilder withTablePropertiesAdded(Map<String, String> properties) {
    this.tablePropertiesAddedOpt =
        Optional.of(
            Collections.unmodifiableMap(
                TableConfig.validateAndNormalizeDeltaProperties(properties)));
    validateTablePropertiesAddedRemovedNoOverlap();
    return this;
  }

  @Override
  public UpdateTableTransactionBuilder withTablePropertiesRemoved(Set<String> propertyKeys) {
    checkArgument(
        propertyKeys.stream().noneMatch(key -> key.toLowerCase(Locale.ROOT).startsWith("delta.")),
        "Unsetting 'delta.' table properties is currently unsupported");
    this.tablePropertiesRemovedOpt = Optional.of(propertyKeys);
    validateTablePropertiesAddedRemovedNoOverlap();
    return this;
  }

  @Override
  public UpdateTableTransactionBuilder withClusteringColumns(List<Column> clusteringColumns) {
    if (snapshot.getPartitionColumnNames().size() > 0) {
      throw DeltaErrors.enablingClusteringOnPartitionedTableNotAllowed(
          snapshot.getPath(), snapshot.getMetadata().getPartitionColNames(), clusteringColumns);
    }
    this.inputLogicalClusteringColumnsOpt = Optional.of(clusteringColumns);
    return this;
  }

  @Override
  public UpdateTableTransactionBuilder withTransactionId(
      String applicationId, long transactionVersion) {
    SetTransaction txnId =
        new SetTransaction(
            requireNonNull(applicationId, "applicationId is null"),
            transactionVersion,
            Optional.of(txnBuilderStartTime));
    this.setTxnOpt = Optional.of(txnId);
    return this;
  }

  @Override
  public UpdateTableTransactionBuilder withMaxRetries(int maxRetries) {
    checkArgument(maxRetries >= 0, "maxRetries must be >= 0");
    this.maxRetries = maxRetries;
    return this;
  }

  @Override
  public UpdateTableTransactionBuilder withLogCompactionInterval(int logCompactionInterval) {
    checkArgument(logCompactionInterval >= 0, "logCompactionInterval must be >= 0");
    this.logCompactionInterval = logCompactionInterval;
    return this;
  }

  @Override
  public Transaction build(Engine engine) {
    setTxnOpt.ifPresent(
        txnId -> {
          Optional<Long> lastTxnVersion =
              snapshot.getLatestTransactionVersion(engine, txnId.getAppId());
          if (lastTxnVersion.isPresent() && lastTxnVersion.get() >= txnId.getVersion()) {
            throw DeltaErrors.concurrentTransaction(
                txnId.getAppId(), txnId.getVersion(), lastTxnVersion.get());
          }
        });

    TransactionMetadataFactory.Output txnMetadata =
        TransactionMetadataFactory.buildUpdateTableMetadata(
            snapshot.getPath(),
            snapshot,
            tablePropertiesAddedOpt,
            tablePropertiesRemovedOpt,
            updatedSchemaOpt,
            inputLogicalClusteringColumnsOpt);

    return new TransactionImpl(
        false /* isCreateOrReplace */,
        snapshot.getDataPath(),
        Optional.of(snapshot),
        engineInfo,
        operation,
        txnMetadata.newProtocol,
        txnMetadata.newMetadata,
        setTxnOpt,
        txnMetadata.physicalNewClusteringColumns,
        maxRetries,
        logCompactionInterval,
        // TODO: support configuring clock if needed
        System::currentTimeMillis);
  }

  private void validateTablePropertiesAddedRemovedNoOverlap() {
    if (tablePropertiesAddedOpt.isPresent() && tablePropertiesRemovedOpt.isPresent()) {
      Set<String> invalidPropertyKeys =
          tablePropertiesRemovedOpt.get().stream()
              .filter(tablePropertiesAddedOpt.get()::containsKey)
              .collect(toSet());
      if (!invalidPropertyKeys.isEmpty()) {
        throw DeltaErrors.overlappingTablePropertiesSetAndUnset(invalidPropertyKeys);
      }
    }
  }

  private void validateOperationParam(Operation operation) {
    if (operation == Operation.CREATE_TABLE || operation == Operation.REPLACE_TABLE) {
      throw new IllegalArgumentException(
          String.format(
              "Operation %s is not compatible with Snapshot::buildUpdateTableTransaction",
              operation));
    }
  }
}
