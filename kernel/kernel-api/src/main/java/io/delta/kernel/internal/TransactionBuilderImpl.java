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
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.icebergcompat.IcebergCompatV2MetadataValidatorAndUpdater;
import io.delta.kernel.internal.icebergcompat.IcebergWriterCompatV1MetadataValidatorAndUpdater;
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
  private Optional<StructType> schema = Optional.empty();
  private Optional<List<String>> partitionColumns = Optional.empty();
  private Optional<SetTransaction> setTxnOpt = Optional.empty();
  private Optional<Map<String, String>> tableProperties = Optional.empty();
  private boolean needDomainMetadataSupport = false;

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
    this.tableProperties =
        Optional.of(Collections.unmodifiableMap(TableConfig.validateDeltaProperties(properties)));
    return this;
  }

  @Override
  public TransactionBuilder withMaxRetries(int maxRetries) {
    checkArgument(maxRetries >= 0, "maxRetries must be >= 0");
    this.maxRetries = maxRetries;
    return this;
  }

  @Override
  public TransactionBuilder withDomainMetadataSupported() {
    needDomainMetadataSupport = true;
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
    validateTransactionInputs(engine, snapshot, isNewTable);

    Metadata snapshotMetadata = snapshot.getMetadata();
    Protocol snapshotProtocol = snapshot.getProtocol();
    Optional<Metadata> newMetadata = Optional.empty();
    Optional<Protocol> newProtocol = Optional.empty();

    // The metadata + protocol transformations get complex with the addition of IcebergCompat which
    // can mutate the configuration. We walk through an example of this for clarity.
    /* ----- 1: Update the METADATA with new table properties or schema set in the builder ----- */
    // Ex: User has set table properties = Map(delta.enableIcebergCompatV2 -> true)
    Map<String, String> newProperties =
        snapshotMetadata.filterOutUnchangedProperties(
            tableProperties.orElse(Collections.emptyMap()));

    if (!newProperties.isEmpty()) {
      newMetadata = Optional.of(snapshotMetadata.withMergedConfiguration(newProperties));
    }

    // TODO In the future update metadata with new schema if provided

    /* ----- 2: Update the PROTOCOL based on the table properties or schema ----- */
    // This is the only place we update the protocol action; takes care of any dependent features
    // Ex: We enable feature `icebergCompatV2` plus dependent features `columnMapping`
    Optional<Tuple2<Protocol, Set<TableFeature>>> newProtocolAndFeatures =
        TableFeatures.autoUpgradeProtocolBasedOnMetadata(
            newMetadata.orElse(snapshotMetadata), needDomainMetadataSupport, snapshotProtocol);
    if (newProtocolAndFeatures.isPresent()) {
      logger.info(
          "Automatically enabling table features: {}",
          newProtocolAndFeatures.get()._2.stream().map(TableFeature::featureName).collect(toSet()));

      newProtocol = Optional.of(newProtocolAndFeatures.get()._1);
      TableFeatures.validateKernelCanWriteToTable(
          newProtocol.orElse(snapshotProtocol),
          newMetadata.orElse(snapshotMetadata),
          table.getPath(engine));
    }

    /* 3: Validate the METADATA and PROTOCOL and possibly update the METADATA for IcebergCompat */
    // IcebergCompat validates that the current metadata and protocol is compatible (e.g. all the
    // required TF are present, no incompatible types, etc). It also updates the metadata for new
    // tables if needed (e.g. enables column mapping)
    // Ex: We enable column mapping mode in the configuration such that our properties now include
    // Map(delta.enableIcebergCompatV2 -> true, delta.columnMapping.mode -> name)

    // We must do our icebergWriterCompatV1 checks/updates FIRST since it has stricter column
    // mapping requirements (id mode) than icebergCompatV2. It also may enable icebergCompatV2.
    Optional<Metadata> icebergWriterCompatV1 =
        IcebergWriterCompatV1MetadataValidatorAndUpdater
            .validateAndUpdateIcebergWriterCompatV1Metadata(
                isNewTable,
                newMetadata.orElse(snapshotMetadata),
                newProtocol.orElse(snapshotProtocol));
    if (icebergWriterCompatV1.isPresent()) {
      newMetadata = icebergWriterCompatV1;
    }

    Optional<Metadata> icebergCompatV2Metadata =
        IcebergCompatV2MetadataValidatorAndUpdater.validateAndUpdateIcebergCompatV2Metadata(
            isNewTable, newMetadata.orElse(snapshotMetadata), newProtocol.orElse(snapshotProtocol));
    if (icebergCompatV2Metadata.isPresent()) {
      newMetadata = icebergCompatV2Metadata;
    }

    /* ----- 4: Update the METADATA with column mapping info if applicable ----- */
    // We update the column mapping info here after all configuration changes are finished
    Optional<Metadata> columnMappingMetadata =
        ColumnMapping.updateColumnMappingMetadataIfNeeded(
            newMetadata.orElse(snapshotMetadata), isNewTable);
    if (columnMappingMetadata.isPresent()) {
      newMetadata = columnMappingMetadata;
    }

    /* ----- 5: Validate the metadata change ----- */
    // Now that all the config and schema changes have been made validate the old vs new metadata
    newMetadata.ifPresent(
        metadata -> validateMetadataChange(snapshotMetadata, metadata, isNewTable));

    return new TransactionImpl(
        isNewTable,
        table.getDataPath(),
        table.getLogPath(),
        snapshot,
        engineInfo,
        operation,
        newProtocol.orElse(snapshotProtocol),
        newMetadata.orElse(snapshotMetadata),
        setTxnOpt,
        newMetadata.isPresent() /* shouldUpdateMetadata */,
        newProtocol.isPresent() /* shouldUpdateProtocol */,
        maxRetries,
        table.getClock());
  }

  /**
   * Validates the transaction as built given the parameters input by the user. This includes
   *
   * <ul>
   *   <li>Ensures that the table, as defined by the protocol and metadata of its latest version, is
   *       writable by Kernel
   *   <li>Partition columns are not specified for an existing table
   *   <li>The provided schema is valid (e.g. no duplicate columns, valid names)
   *   <li>Partition columns provided are valid (e.g. they exist, valid data types)
   *   <li>Concurrent txn has not already committed to the table with same txnId
   * </ul>
   */
  private void validateTransactionInputs(Engine engine, SnapshotImpl snapshot, boolean isNewTable) {
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

  /**
   * Validate that the change from oldMetadata to newMetadata is a valid change. For example, this
   * checks the following
   *
   * <ul>
   *   <li>Column mapping mode can only go from none->name for existing table
   *   <li>icebergWriterCompatV1 cannot be enabled on existing tables (only supported upon table
   *       creation)
   * </ul>
   */
  private void validateMetadataChange(
      Metadata oldMetadata, Metadata newMetadata, boolean isNewTable) {
    ColumnMapping.verifyColumnMappingChange(
        oldMetadata.getConfiguration(), newMetadata.getConfiguration(), isNewTable);
    IcebergWriterCompatV1MetadataValidatorAndUpdater.validateIcebergWriterCompatV1Change(
        oldMetadata.getConfiguration(), newMetadata.getConfiguration(), isNewTable);

    // TODO In the future validate any schema change
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
}
