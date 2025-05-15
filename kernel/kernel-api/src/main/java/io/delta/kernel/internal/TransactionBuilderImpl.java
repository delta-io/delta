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
import static java.util.Collections.emptySet;
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
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionBuilderImpl implements TransactionBuilder {
  private static final Logger logger = LoggerFactory.getLogger(TransactionBuilderImpl.class);

  private final long currentTimeMillis = System.currentTimeMillis();
  private final String engineInfo;
  private final Operation operation;
  private Optional<List<String>> partitionColumns = Optional.empty();
  private Optional<List<Column>> clusteringColumns = Optional.empty();
  private Optional<SetTransaction> setTxnOpt = Optional.empty();
  private Optional<Map<String, String>> tableProperties = Optional.empty();
  private Optional<Set<String>> unsetTablePropertiesKeys = Optional.empty();
  private boolean needDomainMetadataSupport = false;

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
    if (operation == Operation.REPLACE_TABLE) {
      throw new UnsupportedOperationException("REPLACE TABLE is not yet supported");
    }
    SnapshotImpl snapshot;
    try {
      snapshot = (SnapshotImpl) table.getLatestSnapshot(engine);
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
            || clusteringColumns.isPresent() // clustering columns changed
            || enablesDomainMetadataSupport; // domain metadata support added

    if (!needsMetadataOrProtocolUpdate) {
      // Return early if there is no metadata or protocol updates and isCreateOrReplace=false
      new TransactionImpl(
          false, // isCreateOrReplace
          table.getDataPath(),
          table.getLogPath(),
          latestSnapshot.get(),
          engineInfo,
          operation,
          latestSnapshot.get().getProtocol(), // reuse latest protocol
          latestSnapshot.get().getMetadata(), // reuse latest metadata
          setTxnOpt,
          Optional.empty(), /* clustering cols=empty */
          false /* shouldUpdateMetadata=false */,
          false /* shouldUpdateProtocol=false */,
          maxRetries,
          logCompactionInterval,
          table.getClock());
    }

    // Otherwise, if this is a new table definition or there is a metadata or protocol update, we
    // need to execute our protocol & metadata validation + update logic and possibly get an updated
    // protocol and/or metadata
    Metadata baseMetadata;
    Protocol baseProtocol;
    if (isCreateOrReplace) {
      // For a new table definition start with an empty initial metadata
      baseMetadata = getInitialMetadata();
      // In the case of Replace table there are a few delta-specific properties we want to preserve
      if (latestSnapshot.isPresent()) { // replace = isCreateOrReplace && latestSnapshot.isPresent
        Map<String, String> propertiesToPreserve =
            latestSnapshot.get().getMetadata().getConfiguration().entrySet().stream()
                .filter(
                    e ->
                        ReplaceTableTransactionBuilderImpl.TABLE_PROPERTY_KEYS_TO_PRESERVE.contains(
                            e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        baseMetadata = baseMetadata.withMergedConfiguration(propertiesToPreserve);
      }
    } else {
      // Otherwise, use the existing table metadata
      baseMetadata = latestSnapshot.get().getMetadata();
    }
    if (latestSnapshot.isPresent()) {
      // If latestSnapshot is present it is either a write to an existing table or a replace table.
      // In both cases we want to start with the prior table protocol to ensure we never downgrade
      // protocols.
      baseProtocol = latestSnapshot.get().getProtocol();
    } else {
      // Otherwise, start with initial protocol for a new table
      baseProtocol = getInitialProtocol();
    }

    // We use the existing clustering columns to validate schema evolution
    Optional<List<Column>> existingClusteringCols =
        isCreateOrReplace
            ? Optional.empty()
            : ClusteringUtils.getClusteringColumnsOptional(latestSnapshot.get());
    Tuple2<Optional<Protocol>, Optional<Metadata>> updatedProtocolAndMetadata =
        validateAndUpdateProtocolAndMetadata(
            engine,
            baseMetadata,
            baseProtocol,
            isCreateOrReplace,
            existingClusteringCols,
            latestSnapshot);
    Optional<Protocol> newProtocol = updatedProtocolAndMetadata._1;
    Optional<Metadata> newMetadata = updatedProtocolAndMetadata._2;

    // TODO should we do the validation in validateTransactionInputs and transform as part of
    //  generating the domain in the txn?
    StructType updatedSchema = newMetadata.orElse(baseMetadata).getSchema();
    Optional<List<Column>> casePreservingClusteringColumnsOpt =
        clusteringColumns.map(
            cols -> SchemaUtils.casePreservingEligibleClusterColumns(updatedSchema, cols));

    if (!latestSnapshot.isPresent()) {
      // For now, we generate an empty snapshot (with version -1) for a new table. In the future,
      // we should define an internal interface to expose just the information the transaction
      // needs instead of the entire SnapshotImpl class. This should also let us avoid creating
      // this fake empty initial snapshot.
      latestSnapshot = Optional.of(getInitialEmptySnapshot(engine, baseMetadata, baseProtocol));
    }

    // Block this for now - in a future PR we will enable this
    if (operation == Operation.REPLACE_TABLE) {
      if (newProtocol.orElse(baseProtocol).supportsFeature(TableFeatures.ROW_TRACKING_W_FEATURE)) {
        // Block this for now to be safe, we will return to this in the future
        throw new UnsupportedOperationException(
            "REPLACE TABLE is not yet supported on row tracking tables");
      }
    }

    return new TransactionImpl(
        isCreateOrReplace,
        table.getDataPath(),
        table.getLogPath(),
        latestSnapshot.get(),
        engineInfo,
        operation,
        newProtocol.orElse(baseProtocol),
        newMetadata.orElse(baseMetadata),
        setTxnOpt,
        casePreservingClusteringColumnsOpt,
        newMetadata.isPresent() || isCreateOrReplace /* shouldUpdateMetadata */,
        newProtocol.isPresent() || isCreateOrReplace /* shouldUpdateProtocol */,
        maxRetries,
        logCompactionInterval,
        table.getClock());
  }

  /**
   * Validates and makes any protocol or metadata updates as defined in this transaction builder.
   *
   * @param baseMetadata the starting metadata to update
   * @param baseProtocol the starting protocol to update
   * @param isCreateOrReplace whether we are defining a new table definition or not
   * @param existingClusteringCols the existing clustering columns for the table (if it exists)
   * @return an updated protocol and metadata if any updates are necessary
   */
  protected Tuple2<Optional<Protocol>, Optional<Metadata>> validateAndUpdateProtocolAndMetadata(
      Engine engine,
      Metadata baseMetadata,
      Protocol baseProtocol,
      boolean isCreateOrReplace,
      Optional<List<Column>> existingClusteringCols,
      Optional<SnapshotImpl> latestSnapshot) {
    if (isCreateOrReplace) {
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

    if (schema.isPresent() && !isCreateOrReplace) {
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
                baseMetadata.getConfiguration(), metadata.getConfiguration(), isCreateOrReplace));

    // We must do our icebergWriterCompatV1 checks/updates FIRST since it has stricter column
    // mapping requirements (id mode) than icebergCompatV2. It also may enable icebergCompatV2.
    Optional<Metadata> icebergWriterCompatV1 =
        IcebergWriterCompatV1MetadataValidatorAndUpdater
            .validateAndUpdateIcebergWriterCompatV1Metadata(
                isCreateOrReplace,
                newMetadata.orElse(baseMetadata),
                newProtocol.orElse(baseProtocol));
    if (icebergWriterCompatV1.isPresent()) {
      newMetadata = icebergWriterCompatV1;
    }

    Optional<Metadata> icebergCompatV2Metadata =
        IcebergCompatV2MetadataValidatorAndUpdater.validateAndUpdateIcebergCompatV2Metadata(
            isCreateOrReplace, newMetadata.orElse(baseMetadata), newProtocol.orElse(baseProtocol));
    if (icebergCompatV2Metadata.isPresent()) {
      newMetadata = icebergCompatV2Metadata;
    }

    /* ----- 4: Update the METADATA with column mapping info if applicable ----- */
    // We update the column mapping info here after all configuration changes are finished
    Optional<Metadata> columnMappingMetadata =
        ColumnMapping.updateColumnMappingMetadataIfNeeded(
            newMetadata.orElse(baseMetadata), isCreateOrReplace);
    if (columnMappingMetadata.isPresent()) {
      newMetadata = columnMappingMetadata;
    }

    /* ----- 5: Validate the metadata change ----- */
    // Now that all the config and schema changes have been made validate the old vs new metadata
    if (newMetadata.isPresent()) {
      validateMetadataChange(
          existingClusteringCols,
          baseMetadata,
          newMetadata.get(),
          isCreateOrReplace,
          latestSnapshot);
    }

    return new Tuple2(newProtocol, newMetadata);
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
        && clusteringColumns.isPresent()
        && snapshot.getMetadata().getPartitionColumns().getSize() != 0) {
      throw DeltaErrors.enablingClusteringOnPartitionedTableNotAllowed(
          table.getPath(engine),
          snapshot.getMetadata().getPartitionColNames(),
          clusteringColumns.get());
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

  /**
   * Validate that the change from oldMetadata to newMetadata is a valid change. For example, this
   * checks the following
   *
   * <ul>
   *   <li>Column mapping mode can only go from none->name for existing table
   *   <li>icebergWriterCompatV1 cannot be enabled on existing tables (only supported upon table
   *       creation)
   *   <li>Validates the universal format configs are valid.
   *   <li>If there is schema evolution validates
   *       <ul>
   *         <li>column mapping is enabled
   *         <li>column mapping mode is not changed in the same txn as schema change
   *         <li>the new schema is a valid schema
   *         <li>the schema change is a valid schema change
   *         <li>the schema change is a valid schema change given the tables partition and
   *             clustering columns
   *       </ul>
   * </ul>
   */
  private void validateMetadataChange(
      Optional<List<Column>> existingClusteringCols,
      Metadata oldMetadata,
      Metadata newMetadata,
      boolean isCreateOrReplace,
      Optional<SnapshotImpl> latestSnapshot) {
    ColumnMapping.verifyColumnMappingChange(
        oldMetadata.getConfiguration(), newMetadata.getConfiguration(), isCreateOrReplace);
    IcebergWriterCompatV1MetadataValidatorAndUpdater.validateIcebergWriterCompatV1Change(
        oldMetadata.getConfiguration(), newMetadata.getConfiguration(), isCreateOrReplace);
    IcebergUniversalFormatMetadataValidatorAndUpdater.validate(newMetadata);

    // Validate the conditions for schema evolution and the updated schema if applicable
    if (schema.isPresent() && !isCreateOrReplace) {
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
          oldMetadata,
          newMetadata,
          clusteringColumnPhysicalNames,
          false /* allowNewRequiredFields */);
    }

    // For replace table we need to do special validation in the case of fieldId re-use
    if (isCreateOrReplace && latestSnapshot.isPresent()) {
      // For now, we don't support changing column mapping mode during replace, in a future PR we
      // will loosen this restriction
      ColumnMappingMode oldMode =
          ColumnMapping.getColumnMappingMode(latestSnapshot.get().getMetadata().getConfiguration());
      ColumnMappingMode newMode =
          ColumnMapping.getColumnMappingMode(newMetadata.getConfiguration());
      if (oldMode != newMode) {
        throw new UnsupportedOperationException(
            String.format(
                "Changing column mapping mode from %s to %s is not currently supported in Kernel "
                    + "during REPLACE TABLE operations",
                oldMode, newMode));
      }

      // We only need to check fieldId re-use when cmMode != none
      if (newMode != ColumnMappingMode.NONE) {
        SchemaUtils.validateUpdatedSchema(
            latestSnapshot.get().getMetadata(),
            newMetadata,
            // We already validate clustering columns elsewhere for isCreateOrReplace no need to
            // duplicate this check here
            emptySet() /* clusteringCols */,
            // We allow new non-null fields in REPLACE since we know all existing data is removed
            true /* allowNewRequiredFields */);
      }
    }
  }

  private SnapshotImpl getInitialEmptySnapshot(
      Engine engine, Metadata metadata, Protocol protocol) {
    SnapshotQueryContext snapshotContext =
        SnapshotQueryContext.forVersionSnapshot(table.getPath(engine), -1);
    LogReplay logReplay =
        getEmptyLogReplay(engine, metadata, protocol, snapshotContext.getSnapshotMetrics());
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
