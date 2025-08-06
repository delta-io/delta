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

import static io.delta.kernel.internal.DeltaErrors.*;
import static io.delta.kernel.internal.util.ColumnMapping.isColumnMappingModeEnabled;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

import io.delta.kernel.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableAlreadyExistsException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.types.StructType;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionBuilderImpl implements TransactionBuilder {
  private static final Logger logger = LoggerFactory.getLogger(TransactionBuilderImpl.class);

  private final long currentTimeMillis = System.currentTimeMillis();
  private final String engineInfo;
  private final Operation operation;
  private Optional<List<String>> partitionColumns = Optional.empty();
  private Optional<SetTransaction> setTxnOpt = Optional.empty();
  private Optional<Map<String, String>> tableProperties = Optional.empty();
  private Optional<Set<String>> unsetTablePropertiesKeys = Optional.empty();
  private boolean needDomainMetadataSupport = false;

  // The original clustering columns provided by the user when building the transaction.
  // This represents logical column references before schema resolution is applied.
  // (e.g., case sensitivity, column mapping)
  private Optional<List<Column>> inputLogicalClusteringColumns = Optional.empty();
  // The resolved clustering columns that will be written into domain metadata in the txn. This
  // reflects case-preserved column names or physical column names if column mapping is enabled.
  // This is set during transaction building after the schema has been updated/resolved with any
  // column mapping info. These are the physical columns of `inputLogicalClusteringColumns`.
  private Optional<List<Column>> newResolvedClusteringColumns = Optional.empty();

  protected final TableImpl table;
  protected Optional<StructType> schema = Optional.empty();

  /**
   * Number of retries for concurrent write exceptions to resolve conflicts and retry commit. In
   * Delta-Spark, for historical reasons the number of retries is really high (10m). We are starting
   * with a lower number by default for now. If this is not sufficient we can update it.
   */
  private int maxRetries = 200;

  /** Number of commits between producing a log compaction file. */
  private int logCompactionInterval = 0;

  public TransactionBuilderImpl(TableImpl table, String engineInfo, Operation operation) {
    this.table = table;
    this.engineInfo = engineInfo;
    this.operation = operation;
  }

  @Override
  public TransactionBuilder withSchema(Engine engine, StructType newSchema) {
    this.schema = Optional.of(newSchema); // will be verified as part of the build() call
    return this;
  }

  @Override
  public TransactionBuilder withPartitionColumns(Engine engine, List<String> partitionColumns) {
    if (!partitionColumns.isEmpty()) {
      this.partitionColumns = Optional.of(partitionColumns);
    }
    return this;
  }

  /**
   * There are three possible cases when handling clustering columns via `withClusteringColumns`:
   *
   * <ul>
   *   <li>Clustering columns are not set (i.e., `withClusteringColumns` is not called):
   *       <ul>
   *         <li>No changes are made related to clustering.
   *         <li>For table creation, the table is initialized as a non-clustered table.
   *         <li>For table updates, the existing clustered or non-clustered state remains unchanged
   *             (i.e., no protocol or domain metadata updates).
   *       </ul>
   *   <li>Clustering columns are an empty list:
   *       <ul>
   *         <li>This is equivalent to executing `ALTER TABLE ... CLUSTER BY NONE` in Delta.
   *         <li>The table remains a clustered table, but its clustering domain metadata is updated
   *             to reflect an empty list of clustering columns.
   *       </ul>
   *   <li>Clustering columns are a non-empty list:
   *       <ul>
   *         <li>The table is treated as a clustered table.
   *         <li>We update the protocol (if needed) to include clustering writer support and set the
   *             clustering domain metadata accordingly.
   *       </ul>
   * </ul>
   */
  @Override
  public TransactionBuilder withClusteringColumns(Engine engine, List<Column> clusteringColumns) {
    this.inputLogicalClusteringColumns = Optional.of(clusteringColumns);
    return this;
  }

  @Override
  public TransactionBuilder withTransactionId(
      Engine engine, String applicationId, long transactionVersion) {
    SetTransaction txnId =
        new SetTransaction(
            requireNonNull(applicationId, "applicationId is null"),
            transactionVersion,
            Optional.of(currentTimeMillis));
    this.setTxnOpt = Optional.of(txnId);
    return this;
  }

  @Override
  public TransactionBuilder withTableProperties(Engine engine, Map<String, String> properties) {
    this.tableProperties =
        Optional.of(Collections.unmodifiableMap(TableConfig.validateDeltaProperties(properties)));
    return this;
  }

  @Override
  public TransactionBuilder withTablePropertiesRemoved(Set<String> propertyKeys) {
    checkArgument(
        propertyKeys.stream().noneMatch(key -> key.toLowerCase(Locale.ROOT).startsWith("delta.")),
        "Unsetting 'delta.' table properties is currently unsupported");
    this.unsetTablePropertiesKeys = Optional.of(Collections.unmodifiableSet(propertyKeys));
    return this;
  }

  @Override
  public TransactionBuilder withMaxRetries(int maxRetries) {
    checkArgument(maxRetries >= 0, "maxRetries must be >= 0");
    this.maxRetries = maxRetries;
    return this;
  }

  @Override
  public TransactionBuilder withLogCompactionInverval(int logCompactionInterval) {
    checkArgument(logCompactionInterval >= 0, "logCompactionInterval must be >= 0");
    this.logCompactionInterval = logCompactionInterval;
    return this;
  }

  @Override
  public TransactionBuilder withDomainMetadataSupported() {
    needDomainMetadataSupport = true;
    return this;
  }

  @Override
  public Transaction build(Engine engine) {
    if (operation == Operation.REPLACE_TABLE) {
      throw new UnsupportedOperationException("REPLACE TABLE is not yet supported");
    }
    SnapshotImpl snapshot;
    try {
      snapshot = table.getLatestSnapshot(engine);
      if (operation == Operation.CREATE_TABLE) {
        throw new TableAlreadyExistsException(table.getPath(engine), "Operation = CREATE_TABLE");
      }
      return buildTransactionInternal(engine, false /* isCreateOrReplace */, Optional.of(snapshot));
    } catch (TableNotFoundException tblf) {
      String tablePath = table.getPath(engine);
      logger.info("Table {} doesn't exist yet. Trying to create a new table.", tablePath);
      schema.orElseThrow(() -> requiresSchemaForNewTable(tablePath));
      return buildTransactionInternal(engine, true /* isNewTableDef */, Optional.empty());
    }
  }

  /**
   * Returns a built {@link Transaction} for this transaction builder (with the input provided by
   * the user) given the provided parameters. This includes validation and updates as defined in the
   * builder.
   *
   * @param isCreateOrReplace whether we are defining a new table definition or not. This determines
   *     what metadata to commit in the returned transaction, and what operations to allow or block.
   * @param latestSnapshot the latest snapshot of the table if it exists. For a new table this
   *     should be empty. For replace table, this should be the latest snapshot of the table. This
   *     is used to validate that we can write to the table, and to get the protocol/metadata when
   *     isCreateOrReplace=false.
   */
  protected TransactionImpl buildTransactionInternal(
      Engine engine, boolean isCreateOrReplace, Optional<SnapshotImpl> latestSnapshot) {
    checkArgument(
        isCreateOrReplace || latestSnapshot.isPresent(),
        "Existing snapshot must be provided if not defining a new table definition");
    latestSnapshot.ifPresent(
        snapshot -> validateWriteToExistingTable(engine, snapshot, isCreateOrReplace));
    validateTransactionInputs(engine, isCreateOrReplace);

    boolean enablesDomainMetadataSupport =
        needDomainMetadataSupport
            && latestSnapshot.isPresent()
            && !latestSnapshot
                .get()
                .getProtocol()
                .supportsFeature(TableFeatures.DOMAIN_METADATA_W_FEATURE);

    boolean needsMetadataOrProtocolUpdate =
        isCreateOrReplace
            || schema.isPresent() // schema evolution
            || tableProperties.isPresent() // table properties updated
            || unsetTablePropertiesKeys.isPresent() // table properties unset
            || inputLogicalClusteringColumns.isPresent() // clustering columns changed
            || enablesDomainMetadataSupport; // domain metadata support added

    if (!needsMetadataOrProtocolUpdate) {
      // TODO: fix this https://github.com/delta-io/delta/issues/4713
      // Return early if there is no metadata or protocol updates and isCreateOrReplace=false
      new TransactionImpl(
          false, // isCreateOrReplace
          table.getDataPath(),
          table.getLogPath(),
          latestSnapshot,
          engineInfo,
          operation,
          Optional.empty(), // newProtocol
          Optional.empty(), // newMetadata
          setTxnOpt,
          Optional.empty(), /* clustering cols=empty */
          maxRetries,
          logCompactionInterval,
          table.getClock());
    }

    // Instead of special casing enabling clustering or domain metadata, we should just add them
    // to the table properties which we already handle.
    Map<String, String> tablePropertiesWithFeatureEnablement =
        new HashMap<>(tableProperties.orElse(emptyMap()));
    boolean domainMetadataEnabled =
        !isCreateOrReplace
            && latestSnapshot
                .get()
                .getProtocol()
                .supportsFeature(TableFeatures.DOMAIN_METADATA_W_FEATURE);
    if (needDomainMetadataSupport && !domainMetadataEnabled) {
      tablePropertiesWithFeatureEnablement.put(
          TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX
              + TableFeatures.DOMAIN_METADATA_W_FEATURE.featureName(),
          "supported");
    }
    boolean clusteringEnabled =
        !isCreateOrReplace
            && latestSnapshot
                .get()
                .getProtocol()
                .supportsFeature(TableFeatures.CLUSTERING_W_FEATURE);
    if (inputLogicalClusteringColumns.isPresent() && !clusteringEnabled) {
      tablePropertiesWithFeatureEnablement.put(
          TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX
              + TableFeatures.CLUSTERING_W_FEATURE.featureName(),
          "supported");
    }
    if (tablePropertiesWithFeatureEnablement.size() > 0) {
      tableProperties = Optional.of(tablePropertiesWithFeatureEnablement);
    }

    TransactionMetadataFactory.Output outputMetadata;
    if (!isCreateOrReplace) {
      outputMetadata =
          TransactionMetadataFactory.buildUpdateTableMetadata(
              table.getPath(engine),
              latestSnapshot.get(),
              tableProperties,
              unsetTablePropertiesKeys,
              schema,
              inputLogicalClusteringColumns);
    } else if (latestSnapshot.isPresent()) { // is REPLACE
      outputMetadata =
          TransactionMetadataFactory.buildReplaceTableMetadata(
              table.getPath(engine),
              latestSnapshot.get(),
              // when isCreateOrReplace we know schema is present
              schema.get(),
              tableProperties.orElse(emptyMap()),
              partitionColumns,
              inputLogicalClusteringColumns);
    } else {
      outputMetadata =
          TransactionMetadataFactory.buildCreateTableMetadata(
              table.getPath(engine),
              // when isCreateOrReplace we know schema is present
              schema.get(),
              tableProperties.orElse(emptyMap()),
              partitionColumns,
              inputLogicalClusteringColumns);
    }

    return new TransactionImpl(
        isCreateOrReplace,
        table.getDataPath(),
        table.getLogPath(),
        latestSnapshot,
        engineInfo,
        operation,
        outputMetadata.newProtocol,
        outputMetadata.newMetadata,
        setTxnOpt,
        outputMetadata.resolvedNewClusteringColumns,
        maxRetries,
        logCompactionInterval,
        table.getClock());
  }

  /**
   * Validates that Kernel can write to the existing table with the latest snapshot as provided.
   * This means (1) Kernel supports the reader and writer protocol of the table (2) if a transaction
   * identifier has been provided in this txn builder, a concurrent write has not already committed
   * this transaction (3) Updating a partitioned table with clustering columns is not allowed.
   */
  protected void validateWriteToExistingTable(
      Engine engine, SnapshotImpl snapshot, boolean isCreateOrReplace) {
    // Validate the table has no features that Kernel doesn't yet support writing into it.
    TableFeatures.validateKernelCanWriteToTable(
        snapshot.getProtocol(), snapshot.getMetadata(), table.getPath(engine));
    setTxnOpt.ifPresent(
        txnId -> {
          Optional<Long> lastTxnVersion =
              snapshot.getLatestTransactionVersion(engine, txnId.getAppId());
          if (lastTxnVersion.isPresent() && lastTxnVersion.get() >= txnId.getVersion()) {
            throw DeltaErrors.concurrentTransaction(
                txnId.getAppId(), txnId.getVersion(), lastTxnVersion.get());
          }
        });
    if (!isCreateOrReplace
        && inputLogicalClusteringColumns.isPresent()
        && snapshot.getMetadata().getPartitionColumns().getSize() != 0) {
      throw DeltaErrors.enablingClusteringOnPartitionedTableNotAllowed(
          table.getPath(engine),
          snapshot.getMetadata().getPartitionColNames(),
          inputLogicalClusteringColumns.get());
    }
  }

  /**
   * Validates the inputs to this transaction builder. This includes
   *
   * <ul>
   *   <li>Partition columns are only set for a new table definition.
   *   <li>Partition columns and clustering columns are not set at the same time.
   *   <li>The provided schema is valid.
   *   <li>The provided partition columns are valid.
   *   <li>The provided table properties to set and unset do not overlap with each other.
   * </ul>
   */
  protected void validateTransactionInputs(Engine engine, boolean isCreateOrReplace) {
    String tablePath = table.getPath(engine);
    if (!isCreateOrReplace) {
      if (partitionColumns.isPresent()) {
        throw tableAlreadyExists(
            tablePath,
            "Table already exists, but provided new partition columns. "
                + "Partition columns can only be set on a new table.");
      }
    } else {
      checkArgument(
          !(partitionColumns.isPresent() && inputLogicalClusteringColumns.isPresent()),
          "Partition Columns and Clustering Columns cannot be set at the same time");

      // New table verify the given schema and partition columns
      ColumnMappingMode mappingMode =
          ColumnMapping.getColumnMappingMode(tableProperties.orElse(emptyMap()));

      SchemaUtils.validateSchema(schema.get(), isColumnMappingModeEnabled(mappingMode));
      SchemaUtils.validatePartitionColumns(schema.get(), partitionColumns.orElse(emptyList()));
    }

    if (unsetTablePropertiesKeys.isPresent() && tableProperties.isPresent()) {
      Set<String> invalidPropertyKeys =
          unsetTablePropertiesKeys.get().stream()
              .filter(key -> tableProperties.get().containsKey(key))
              .collect(toSet());
      if (!invalidPropertyKeys.isEmpty()) {
        throw DeltaErrors.overlappingTablePropertiesSetAndUnset(invalidPropertyKeys);
      }
    }
  }
}
