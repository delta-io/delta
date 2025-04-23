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
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

import io.delta.kernel.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableAlreadyExistsException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.clustering.ClusteringUtils;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.icebergcompat.IcebergCompatV2MetadataValidatorAndUpdater;
import io.delta.kernel.internal.icebergcompat.IcebergUniversalFormatMetadataValidatorAndUpdater;
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
  private Optional<List<Column>> clusteringColumns = Optional.empty();
  private Optional<SetTransaction> setTxnOpt = Optional.empty();
  private Optional<Map<String, String>> tableProperties = Optional.empty();
  private Optional<Set<String>> unsetTablePropertiesKeys = Optional.empty();
  private boolean needDomainMetadataSupport = false;

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
    this.clusteringColumns = Optional.of(clusteringColumns);
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
    SnapshotImpl snapshot;
    try {
      snapshot = (SnapshotImpl) table.getLatestSnapshot(engine);
      if (operation == Operation.CREATE_TABLE) {
        throw new TableAlreadyExistsException(table.getPath(engine), "Operation = CREATE_TABLE");
      }
      return buildTransactionInternal(engine, false /* isNewTableDef */, Optional.of(snapshot));
    } catch (TableNotFoundException tblf) {
      String tablePath = table.getPath(engine);
      logger.info("Table {} doesn't exist yet. Trying to create a new table.", tablePath);
      schema.orElseThrow(() -> requiresSchemaForNewTable(tablePath));
      return buildTransactionInternal(engine, true /* isNewTableDef */, Optional.empty());
    }
  }

  /**
   * TODO docs!!
   * @param engine
   * @param isNewTableDef
   * @param existingSnapshot
   * @return
   */
  protected TransactionImpl buildTransactionInternal(
      Engine engine,
      boolean isNewTableDef,
      Optional<SnapshotImpl> existingSnapshot
      ) {
    checkArgument(isNewTableDef || existingSnapshot.isPresent(),
        "Existing snapshot must be provided if not defining a new table definition");
    Metadata baseMetadata;
    Protocol baseProtocol;
    if (isNewTableDef) {
      // For a new table definition start with an empty initial metadata
      baseMetadata = getInitialMetadata();
    } else {
      // Otherwise, use the existing table metadata
      baseMetadata = existingSnapshot.get().getMetadata();
    }
    if (existingSnapshot.isPresent()) {
      // If the table exists, start with the prior table protocol - this ensures we never downgrade
      baseProtocol = existingSnapshot.get().getProtocol();
    } else {
      // Otherwise, start with initial protocol
      baseProtocol = getInitialProtocol();
    }
    existingSnapshot.ifPresent(snapshot -> validateWriteToExistingTable(engine, snapshot));
    validateTransactionInputs(engine, isNewTableDef);

    boolean enablesDomainMetadataSupport =
        needDomainMetadataSupport
            && !baseProtocol.supportsFeature(TableFeatures.DOMAIN_METADATA_W_FEATURE);

    // If this a new table definition or there is a metadata or protocol update, we need to execute
    // our protocol & metadata validation/update logic
    if (isNewTableDef
        || schema.isPresent() // schema evolution
        || tableProperties.isPresent() // table properties updated
        || unsetTablePropertiesKeys.isPresent() // table properties updated
        || clusteringColumns.isPresent() // clustering columns have changed
        || enablesDomainMetadataSupport) {

      // We use the existing clustering columns to validate schema evolution
      Optional<List<Column>> existingClusteringCols =
          isNewTableDef
              ? Optional.empty()
              : ClusteringUtils.getClusteringColumnsOptional(existingSnapshot.get());
      Tuple2<Optional<Protocol>, Optional<Metadata>> updatedProtocolAndMetadata =
          validateAndUpdateProtocolAndMetadata(
              engine, baseMetadata, baseProtocol, isNewTableDef, existingClusteringCols);
      Optional<Protocol> newProtocol = updatedProtocolAndMetadata._1;
      Optional<Metadata> newMetadata = updatedProtocolAndMetadata._2;

      // TODO should we do the validation somewhere in previous fx and transform as part of
      //  generating the domain in the txn?
      StructType updatedSchema = newMetadata.orElse(baseMetadata).getSchema();
      Optional<List<Column>> casePreservingClusteringColumnsOpt =
          clusteringColumns.map(
              cols -> SchemaUtils.casePreservingEligibleClusterColumns(updatedSchema, cols));

      if (!existingSnapshot.isPresent()) {
        // For now, we generate an empty snapshot (with version -1) for a new table. In the future,
        // we should define an internal interface to expose just the information the transaction
        // needs instead of the entire SnapshotImpl class. This should also let us avoid creating
        // this fake empty initial snapshot.
        existingSnapshot = Optional.of(getInitialEmptySnapshot(engine, baseMetadata, baseProtocol));
      }

      return new TransactionImpl(
          isNewTableDef,
          table.getDataPath(),
          table.getLogPath(),
          existingSnapshot.get(),
          engineInfo,
          operation,
          newProtocol.orElse(baseProtocol),
          newMetadata.orElse(baseMetadata),
          setTxnOpt,
          casePreservingClusteringColumnsOpt,
          newMetadata.isPresent() || isNewTableDef /* shouldUpdateMetadata */,
          newProtocol.isPresent() || isNewTableDef /* shouldUpdateProtocol */,
          maxRetries,
          logCompactionInterval,
          table.getClock());

    } else {
      return new TransactionImpl(
          false, // isNewTableDef
          table.getDataPath(),
          table.getLogPath(),
          existingSnapshot.get(),
          engineInfo,
          operation,
          existingSnapshot.get().getProtocol(),
          existingSnapshot.get().getMetadata(),
          setTxnOpt,
          Optional.empty(), /* clustering cols */
          false /* shouldUpdateMetadata */,
          false /* shouldUpdateProtocol */,
          maxRetries,
          logCompactionInterval,
          table.getClock());
    }
  }

  /**
   * TODO docs
   * @param engine
   * @param baseMetadata
   * @param baseProtocol
   * @param isNewTableDef
   * @param existingClusteringCols
   * @return
   */
  protected Tuple2<Optional<Protocol>, Optional<Metadata>> validateAndUpdateProtocolAndMetadata(
      Engine engine,
      Metadata baseMetadata,
      Protocol baseProtocol,
      boolean isNewTableDef,
      Optional<List<Column>> existingClusteringCols) {
    if (isNewTableDef) {
      checkArgument(!existingClusteringCols.isPresent());
    }

    Optional<Metadata> newMetadata = Optional.empty();
    Optional<Protocol> newProtocol = Optional.empty();

    // The metadata + protocol transformations get complex with the addition of IcebergCompat which
    // can mutate the configuration. We walk through an example of this for clarity.
    /* ----- 1: Update the METADATA with new table properties or schema set in the builder ----- */
    // Ex: User has set table properties = Map(delta.enableIcebergCompatV2 -> true)
    Map<String, String> newProperties =
        baseMetadata.filterOutUnchangedProperties(tableProperties.orElse(Collections.emptyMap()));

    if (!newProperties.isEmpty()) {
      newMetadata = Optional.of(baseMetadata.withMergedConfiguration(newProperties));
    }

    if (unsetTablePropertiesKeys.isPresent()) {
      newMetadata =
          Optional.of(
              newMetadata
                  .orElse(baseMetadata)
                  .withConfigurationKeysUnset(unsetTablePropertiesKeys.get()));
    }

    if (schema.isPresent() && !isNewTableDef) {
      newMetadata = Optional.of(newMetadata.orElse(baseMetadata).withNewSchema(schema.get()));
    }

    /* ----- 2: Update the PROTOCOL based on the table properties or schema ----- */
    // This is the only place we update the protocol action; takes care of any dependent features
    // Ex: We enable feature `icebergCompatV2` plus dependent features `columnMapping`
    Set<TableFeature> manuallyEnabledFeatures = new HashSet<>();
    if (needDomainMetadataSupport) {
      manuallyEnabledFeatures.add(TableFeatures.DOMAIN_METADATA_W_FEATURE);
    }
    if (clusteringColumns.isPresent()) {
      manuallyEnabledFeatures.add(TableFeatures.CLUSTERING_W_FEATURE);
    }

    // This will remove feature properties (i.e. metadata properties in the form of
    // "delta.feature.*") from metadata. There should be one TableFeature in the returned set for
    // each property removed.
    Tuple2<Set<TableFeature>, Optional<Metadata>> newFeaturesAndMetadata =
        TableFeatures.extractFeaturePropertyOverrides(newMetadata.orElse(baseMetadata));
    manuallyEnabledFeatures.addAll(newFeaturesAndMetadata._1);
    if (newFeaturesAndMetadata._2.isPresent()) {
      newMetadata = newFeaturesAndMetadata._2;
    }

    Optional<Tuple2<Protocol, Set<TableFeature>>> newProtocolAndFeatures =
        TableFeatures.autoUpgradeProtocolBasedOnMetadata(
            newMetadata.orElse(baseMetadata), manuallyEnabledFeatures, baseProtocol);
    if (newProtocolAndFeatures.isPresent()) {
      logger.info(
          "Automatically enabling table features: {}",
          newProtocolAndFeatures.get()._2.stream().map(TableFeature::featureName).collect(toSet()));

      newProtocol = Optional.of(newProtocolAndFeatures.get()._1);
      TableFeatures.validateKernelCanWriteToTable(
          newProtocol.orElse(baseProtocol),
          newMetadata.orElse(baseMetadata),
          table.getPath(engine));
    }

    /* 3: Validate the METADATA and PROTOCOL and possibly update the METADATA for IcebergCompat */
    // IcebergCompat validates that the current metadata and protocol is compatible (e.g. all the
    // required TF are present, no incompatible types, etc). It also updates the metadata for new
    // tables if needed (e.g. enables column mapping)
    // Ex: We enable column mapping mode in the configuration such that our properties now include
    // Map(delta.enableIcebergCompatV2 -> true, delta.columnMapping.mode -> name)

    // Validate this is a valid config change earlier for a clearer error message
    newMetadata.ifPresent(
        metadata ->
            IcebergWriterCompatV1MetadataValidatorAndUpdater.validateIcebergWriterCompatV1Change(
                baseMetadata.getConfiguration(), metadata.getConfiguration(), isNewTableDef));

    // We must do our icebergWriterCompatV1 checks/updates FIRST since it has stricter column
    // mapping requirements (id mode) than icebergCompatV2. It also may enable icebergCompatV2.
    Optional<Metadata> icebergWriterCompatV1 =
        IcebergWriterCompatV1MetadataValidatorAndUpdater
            .validateAndUpdateIcebergWriterCompatV1Metadata(
                isNewTableDef, newMetadata.orElse(baseMetadata), newProtocol.orElse(baseProtocol));
    if (icebergWriterCompatV1.isPresent()) {
      newMetadata = icebergWriterCompatV1;
    }

    Optional<Metadata> icebergCompatV2Metadata =
        IcebergCompatV2MetadataValidatorAndUpdater.validateAndUpdateIcebergCompatV2Metadata(
            isNewTableDef, newMetadata.orElse(baseMetadata), newProtocol.orElse(baseProtocol));
    if (icebergCompatV2Metadata.isPresent()) {
      newMetadata = icebergCompatV2Metadata;
    }

    /* ----- 4: Update the METADATA with column mapping info if applicable ----- */
    // We update the column mapping info here after all configuration changes are finished
    Optional<Metadata> columnMappingMetadata =
        ColumnMapping.updateColumnMappingMetadataIfNeeded(
            newMetadata.orElse(baseMetadata), isNewTableDef);
    if (columnMappingMetadata.isPresent()) {
      newMetadata = columnMappingMetadata;
    }

    /* ----- 5: Validate the metadata change ----- */
    // Now that all the config and schema changes have been made validate the old vs new metadata
    if (newMetadata.isPresent()) {
      validateMetadataChange(
          existingClusteringCols, baseMetadata, newMetadata.get(), isNewTableDef);
    }

    return new Tuple2(newProtocol, newMetadata);
  }

  // TODO docs
  protected void validateWriteToExistingTable(Engine engine, SnapshotImpl snapshot) {
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
  }

  // TODO docs
  protected void validateTransactionInputs(Engine engine, boolean isNewTableDef) {
    String tablePath = table.getPath(engine);
    if (!isNewTableDef) {
      if (partitionColumns.isPresent()) {
        throw tableAlreadyExists(
            tablePath,
            "Table already exists, but provided new partition columns. "
                + "Partition columns can only be set on a new table.");
      }
    } else {
      checkArgument(
          !(partitionColumns.isPresent() && clusteringColumns.isPresent()),
          "Partition Columns and Clustering Columns cannot be set at the same time");

      // New table verify the given schema and partition columns
      ColumnMappingMode mappingMode =
          ColumnMapping.getColumnMappingMode(tableProperties.orElse(Collections.emptyMap()));

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

  // TODO docs updates
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
      Optional<List<Column>> existingClusteringCols,
      Metadata oldMetadata,
      Metadata newMetadata,
      boolean isNewTableDef) {
    ColumnMapping.verifyColumnMappingChange(
        oldMetadata.getConfiguration(), newMetadata.getConfiguration(), isNewTableDef);
    IcebergWriterCompatV1MetadataValidatorAndUpdater.validateIcebergWriterCompatV1Change(
        oldMetadata.getConfiguration(), newMetadata.getConfiguration(), isNewTableDef);
    IcebergUniversalFormatMetadataValidatorAndUpdater.validate(newMetadata);

    // Validate the conditions for schema evolution and the updated schema if applicable
    if (schema.isPresent() && !isNewTableDef) {
      ColumnMappingMode updatedMappingMode =
          ColumnMapping.getColumnMappingMode(newMetadata.getConfiguration());
      ColumnMappingMode currentMappingMode =
          ColumnMapping.getColumnMappingMode(oldMetadata.getConfiguration());
      if (currentMappingMode != updatedMappingMode) {
        throw new KernelException("Cannot update mapping mode and perform schema evolution");
      }

      // If the column mapping restriction is removed, clustering columns
      // will need special handling during schema evolution since they won't have physical names
      // ToDo: Support adding clustering columns

      if (!isColumnMappingModeEnabled(updatedMappingMode)) {
        throw new KernelException("Cannot update schema for table when column mapping is disabled");
      }

      // Clustering columns will be guaranteed to have physical names at this point
      // Only the leaf part of the overall column needs to be taken since
      // validation is performed on the leaf struct fields
      // E.g. getClusteringColumns returns <physical_name_of_struct>.<physical_name_inner>,
      // Only physical_name_inner is required for validation
      Set<String> clusteringColumnPhysicalNames =
          existingClusteringCols.orElse(Collections.emptyList()).stream()
              .map(col -> col.getNames()[col.getNames().length - 1])
              .collect(toSet());

      SchemaUtils.validateUpdatedSchema(
          oldMetadata.getSchema(),
          newMetadata.getSchema(),
          oldMetadata.getPartitionColNames(),
          clusteringColumnPhysicalNames,
          newMetadata);
    }
  }

  private SnapshotImpl getInitialEmptySnapshot(Engine engine, Metadata metadata, Protocol protocol) {
    SnapshotQueryContext snapshotContext =
        SnapshotQueryContext.forVersionSnapshot(table.getPath(engine), -1);
    LogReplay logReplay =
        getEmptyLogReplay(
            engine, metadata, protocol, snapshotContext.getSnapshotMetrics());
    return new InitialSnapshot(table.getDataPath(), logReplay, metadata, protocol, snapshotContext);
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
        casePreservingPartitionColNames(schema.get(), partitionColumns.orElse(emptyList()));

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
