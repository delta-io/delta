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

import static io.delta.kernel.internal.DeltaErrors.requiresSchemaForNewTable;
import static io.delta.kernel.internal.DeltaErrors.tableAlreadyExists;
import static io.delta.kernel.internal.TransactionImpl.DEFAULT_READ_VERSION;
import static io.delta.kernel.internal.TransactionImpl.DEFAULT_WRITE_VERSION;
import static io.delta.kernel.internal.util.ColumnMapping.isColumnMappingModeEnabled;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.SchemaUtils.casePreservingPartitionColNames;
import static io.delta.kernel.internal.util.VectorUtils.buildArrayValue;
import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

import io.delta.kernel.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.DomainDoesNotExistException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.icebergcompat.IcebergCompatV2MetadataValidatorAndUpdater;
import io.delta.kernel.internal.metrics.SnapshotMetrics;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotHint;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionBuilderImpl implements TransactionBuilder {
  private static final Logger logger = LoggerFactory.getLogger(TransactionBuilderImpl.class);

  private final long currentTimeMillis = System.currentTimeMillis();
  private final TableImpl table;
  private final String engineInfo;
  private final Operation operation;
  private final Map<String, DomainMetadata> domainMetadatasAdded = new HashMap<>();
  private final Set<String> domainMetadatasRemoved = new HashSet<>();
  private Optional<StructType> schema = Optional.empty();
  private Optional<List<String>> partitionColumns = Optional.empty();
  private Optional<SetTransaction> setTxnOpt = Optional.empty();
  private Optional<Map<String, String>> tableProperties = Optional.empty();

  /**
   * Number of retries for concurrent write exceptions to resolve conflicts and retry commit. In
   * Delta-Spark, for historical reasons the number of retries is really high (10m). We are starting
   * with a lower number by default for now. If this is not sufficient we can update it.
   */
  private int maxRetries = 200;

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
    this.tableProperties = Optional.of(new HashMap<>(properties));
    return this;
  }

  @Override
  public TransactionBuilder withMaxRetries(int maxRetries) {
    checkArgument(maxRetries >= 0, "maxRetries must be >= 0");
    this.maxRetries = maxRetries;
    return this;
  }

  @Override
  public TransactionBuilder withDomainMetadata(String domain, String config) {
    checkArgument(
        DomainMetadata.isUserControlledDomain(domain),
        "Setting a system-controlled domain is not allowed: " + domain);
    checkArgument(
        !domainMetadatasRemoved.contains(domain),
        "Cannot add a domain that is removed in this transaction");
    // we override any existing value
    domainMetadatasAdded.put(domain, new DomainMetadata(domain, config, false /* removed */));
    return this;
  }

  @Override
  public TransactionBuilder withDomainMetadataRemoved(String domain) {
    checkArgument(
        DomainMetadata.isUserControlledDomain(domain),
        "Removing a system-controlled domain is not allowed: " + domain);
    checkArgument(
        !domainMetadatasAdded.containsKey(domain),
        "Cannot remove a domain that is added in this transaction");
    domainMetadatasRemoved.add(domain);
    return this;
  }

  @Override
  public Transaction build(Engine engine) {
    SnapshotImpl snapshot;
    try {
      snapshot = (SnapshotImpl) table.getLatestSnapshot(engine);
    } catch (TableNotFoundException tblf) {
      String tablePath = table.getPath(engine);
      logger.info("Table {} doesn't exist yet. Trying to create a new table.", tablePath);
      schema.orElseThrow(() -> requiresSchemaForNewTable(tablePath));
      // Table doesn't exist yet. Create an initial snapshot with the new schema.
      Metadata metadata = getInitialMetadata();
      Protocol protocol = getInitialProtocol();
      SnapshotQueryContext snapshotContext = SnapshotQueryContext.forVersionSnapshot(tablePath, -1);
      LogReplay logReplay =
          getEmptyLogReplay(engine, metadata, protocol, snapshotContext.getSnapshotMetrics());
      snapshot =
          new InitialSnapshot(table.getDataPath(), logReplay, metadata, protocol, snapshotContext);
    }

    boolean isNewTable = snapshot.getVersion() < 0;
    validate(engine, snapshot, isNewTable);

    boolean shouldUpdateMetadata = false;
    boolean shouldUpdateProtocol = false;
    Metadata metadata = snapshot.getMetadata();
    Protocol protocol = snapshot.getProtocol();
    Map<String, String> validatedProperties =
        TableConfig.validateDeltaProperties(tableProperties.orElse(Collections.emptyMap()));
    Map<String, String> newProperties = metadata.filterOutUnchangedProperties(validatedProperties);

    if (!newProperties.isEmpty()) {
      shouldUpdateMetadata = true;
      Map<String, String> oldConfiguration = metadata.getConfiguration();

      metadata = metadata.withNewConfiguration(newProperties);

      ColumnMapping.verifyColumnMappingChange(
          oldConfiguration, metadata.getConfiguration() /* new config */, isNewTable);
    }

    Optional<Tuple2<Protocol, Set<TableFeature>>> newProtocolAndFeatures =
        TableFeatures.autoUpgradeProtocolBasedOnMetadata(
            metadata, !domainMetadatasAdded.isEmpty(), protocol);
    if (newProtocolAndFeatures.isPresent()) {
      logger.info(
          "Automatically enabling table features: {}",
          newProtocolAndFeatures.get()._2.stream().map(TableFeature::featureName).collect(toSet()));

      shouldUpdateProtocol = true;
      protocol = newProtocolAndFeatures.get()._1;
      TableFeatures.validateKernelCanWriteToTable(protocol, metadata, table.getPath(engine));
    }

    Optional<Metadata> metadataWithCMInfo =
        ColumnMapping.updateColumnMappingMetadataIfNeeded(metadata, isNewTable);
    if (metadataWithCMInfo.isPresent()) {
      shouldUpdateMetadata = true;
      metadata = metadataWithCMInfo.get();
    }

    Optional<Metadata> metadataWithIcebergCompatInfo =
        IcebergCompatV2MetadataValidatorAndUpdater.validateAndUpdateIcebergCompatV2Metadata(
            isNewTable, metadata, protocol);
    if (metadataWithIcebergCompatInfo.isPresent()) {
      shouldUpdateMetadata = true;
      metadata = metadataWithIcebergCompatInfo.get();
    }

    return new TransactionImpl(
        isNewTable,
        table.getDataPath(),
        table.getLogPath(),
        snapshot,
        engineInfo,
        operation,
        protocol,
        metadata,
        setTxnOpt,
        shouldUpdateMetadata,
        shouldUpdateProtocol,
        maxRetries,
        table.getClock(),
        getDomainMetadatasToCommit(snapshot));
  }

  /** Validate the given parameters for the transaction. */
  private void validate(Engine engine, SnapshotImpl snapshot, boolean isNewTable) {
    String tablePath = table.getPath(engine);
    // Validate the table has no features that Kernel doesn't yet support writing into it.
    TableFeatures.validateKernelCanWriteToTable(
        snapshot.getProtocol(), snapshot.getMetadata(), tablePath);

    if (!isNewTable) {
      if (schema.isPresent()) {
        throw tableAlreadyExists(
            tablePath,
            "Table already exists, but provided a new schema. "
                + "Schema can only be set on a new table.");
      }
      if (partitionColumns.isPresent()) {
        throw tableAlreadyExists(
            tablePath,
            "Table already exists, but provided new partition columns. "
                + "Partition columns can only be set on a new table.");
      }
    } else {
      // New table verify the given schema and partition columns
      ColumnMappingMode mappingMode =
          ColumnMapping.getColumnMappingMode(tableProperties.orElse(Collections.emptyMap()));

      SchemaUtils.validateSchema(schema.get(), isColumnMappingModeEnabled(mappingMode));
      SchemaUtils.validatePartitionColumns(
          schema.get(), partitionColumns.orElse(Collections.emptyList()));
    }

    setTxnOpt.ifPresent(
        txnId -> {
          Optional<Long> lastTxnVersion =
              snapshot.getLatestTransactionVersion(engine, txnId.getAppId());
          if (lastTxnVersion.isPresent() && lastTxnVersion.get() >= txnId.getVersion()) {
            throw DeltaErrors.concurrentTransaction(
                txnId.getAppId(), txnId.getVersion(), lastTxnVersion.get());
          }
        });
  }

  private class InitialSnapshot extends SnapshotImpl {
    InitialSnapshot(
        Path dataPath,
        LogReplay logReplay,
        Metadata metadata,
        Protocol protocol,
        SnapshotQueryContext snapshotContext) {
      super(
          dataPath,
          LogSegment.empty(table.getLogPath()),
          logReplay,
          protocol,
          metadata,
          snapshotContext);
    }

    @Override
    public long getTimestamp(Engine engine) {
      return -1L;
    }
  }

  private LogReplay getEmptyLogReplay(
      Engine engine, Metadata metadata, Protocol protocol, SnapshotMetrics snapshotMetrics) {
    return new LogReplay(
        table.getLogPath(),
        table.getDataPath(),
        -1,
        engine,
        LogSegment.empty(table.getLogPath()),
        Optional.empty(),
        snapshotMetrics) {

      @Override
      protected Tuple2<Protocol, Metadata> loadTableProtocolAndMetadata(
          Engine engine,
          LogSegment logSegment,
          Optional<SnapshotHint> snapshotHint,
          long snapshotVersion) {
        return new Tuple2<>(protocol, metadata);
      }

      @Override
      public Optional<Long> getLatestTransactionIdentifier(Engine engine, String applicationId) {
        return Optional.empty();
      }
    };
  }

  private Metadata getInitialMetadata() {
    List<String> partitionColumnsCasePreserving =
        casePreservingPartitionColNames(
            schema.get(), partitionColumns.orElse(Collections.emptyList()));

    return new Metadata(
        java.util.UUID.randomUUID().toString(), /* id */
        Optional.empty(), /* name */
        Optional.empty(), /* description */
        new Format(), /* format */
        schema.get().toJson(), /* schemaString */
        schema.get(), /* schema */
        buildArrayValue(partitionColumnsCasePreserving, StringType.STRING), /* partitionColumns */
        Optional.of(currentTimeMillis), /* createdTime */
        stringStringMapValue(Collections.emptyMap()) /* configuration */);
  }

  private Protocol getInitialProtocol() {
    return new Protocol(DEFAULT_READ_VERSION, DEFAULT_WRITE_VERSION);
  }

  /**
   * Returns a list of the domain metadatas to commit. This consists of the domain metadatas added
   * in the transaction using {@link TransactionBuilder#withDomainMetadata(String, String)} and the
   * tombstones for the domain metadatas removed in the transaction using {@link
   * TransactionBuilder#withDomainMetadataRemoved(String)}.
   */
  private List<DomainMetadata> getDomainMetadatasToCommit(SnapshotImpl snapshot) {
    // Add all domain metadatas added in the transaction
    List<DomainMetadata> finalDomainMetadatas = new ArrayList<>(domainMetadatasAdded.values());

    // Generate the tombstones for the removed domain metadatas
    Map<String, DomainMetadata> snapshotDomainMetadataMap = snapshot.getDomainMetadataMap();
    for (String domainName : domainMetadatasRemoved) {
      // Note: we know domainName is not already in finalDomainMetadatas because we do not allow
      // removing and adding a domain with the same identifier in a single txn!
      if (snapshotDomainMetadataMap.containsKey(domainName)) {
        DomainMetadata domainToRemove = snapshotDomainMetadataMap.get(domainName);
        if (domainToRemove.isRemoved()) {
          // If the domain is already removed we throw an error to avoid any inconsistencies or
          // ambiguity. The snapshot read by the connector is inconsistent with the snapshot
          // loaded here as the domain to remove no longer exists.
          throw new DomainDoesNotExistException(
              table.getDataPath().toString(), domainName, snapshot.getVersion());
        }
        finalDomainMetadatas.add(domainToRemove.removed());
      } else {
        // We must throw an error if the domain does not exist. Otherwise, there could be unexpected
        // behavior within conflict resolution. For example, consider the following
        // 1. Table has no domains set in V0
        // 2. txnA is started and wants to remove domain "foo"
        // 3. txnB is started and adds domain "foo" and commits V1 before txnA
        // 4. txnA needs to perform conflict resolution against the V1 commit from txnB
        // Conflict resolution should fail but since the domain does not exist we cannot create
        // a tombstone to mark it as removed and correctly perform conflict resolution.
        throw new DomainDoesNotExistException(
            table.getDataPath().toString(), domainName, snapshot.getVersion());
      }
    }
    return finalDomainMetadatas;
  }
}
