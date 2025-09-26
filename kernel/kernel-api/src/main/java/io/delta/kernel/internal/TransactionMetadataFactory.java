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

import static io.delta.kernel.internal.TransactionImpl.DEFAULT_READ_VERSION;
import static io.delta.kernel.internal.TransactionImpl.DEFAULT_WRITE_VERSION;
import static io.delta.kernel.internal.util.ColumnMapping.isColumnMappingModeEnabled;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;
import static io.delta.kernel.internal.util.SchemaUtils.casePreservingPartitionColNames;
import static io.delta.kernel.internal.util.VectorUtils.buildArrayValue;
import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;

import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.icebergcompat.*;
import io.delta.kernel.internal.rowtracking.MaterializedRowTrackingColumn;
import io.delta.kernel.internal.rowtracking.RowTracking;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for building the Protocol, Metadata, and ResolvedClusteringColumns to commit in a
 * transaction. This means it validates and updates the protocol and metadata according to the
 * inputs.
 */
public class TransactionMetadataFactory {
  private static final Logger logger = LoggerFactory.getLogger(TransactionMetadataFactory.class);

  /**
   * Expectations with respect to the given operation:
   *
   * <ul>
   *   <li>Create table: both protocol and metadata will be present
   *   <li>Replace table: both protocol and metadata will be present
   *   <li>Update table: metadata and protocol may or may not be present depending on whether there
   *       should be a metadata or protocol update
   * </ul>
   */
  public static class Output {
    /* New metadata, present if the transaction should commit a new metadata action */
    public final Optional<Protocol> newProtocol;
    /* New protocol, present if the transaction should commit a new protocol action */
    public final Optional<Metadata> newMetadata;
    /* Resolved _new_ clustering columns if the transaction should update the clustering columns */
    public final Optional<List<Column>> physicalNewClusteringColumns;

    public Output(
        Optional<Protocol> newProtocol,
        Optional<Metadata> newMetadata,
        Optional<List<Column>> physicalNewClusteringColumns) {
      this.newProtocol = newProtocol;
      this.newMetadata = newMetadata;
      this.physicalNewClusteringColumns = physicalNewClusteringColumns;
    }
  }

  ////////////////////////////
  // Static factory methods //
  ////////////////////////////

  static Output buildCreateTableMetadata(
      String tablePath,
      StructType schema,
      Map<String, String> tableProperties,
      Optional<List<String>> partitionColumns,
      Optional<List<Column>> clusteringColumns) {
    checkArgument(
        !partitionColumns.isPresent() || !clusteringColumns.isPresent(),
        "Cannot provide both partition columns and clustering columns");
    validateSchemaAndPartColsCreateOrReplace(
        tableProperties, schema, partitionColumns.orElse(emptyList()));
    Output output =
        new TransactionMetadataFactory(
                tablePath,
                Optional.empty() /* readSnapshot */,
                Optional.of(
                    getInitialMetadata(
                        schema, tableProperties, partitionColumns.orElse(emptyList()))),
                Optional.of(getInitialProtocol()),
                tableProperties,
                true /* isCreateOrReplace */,
                clusteringColumns,
                false /* isSchemaEvolultion */)
            .finalOutput;
    checkState(
        output.newMetadata.isPresent() && output.newProtocol.isPresent(),
        "Expected non-null metadata and protocol for create table");
    return output;
  }

  static Output buildReplaceTableMetadata(
      String tablePath,
      SnapshotImpl readSnapshot,
      StructType schema,
      Map<String, String> userInputTableProperties,
      Optional<List<String>> partitionColumns,
      Optional<List<Column>> clusteringColumns) {
    checkArgument(
        !partitionColumns.isPresent() || !clusteringColumns.isPresent(),
        "Cannot provide both partition columns and clustering columns");
    validateSchemaAndPartColsCreateOrReplace(
        userInputTableProperties, schema, partitionColumns.orElse(emptyList()));
    validateNotEnablingCatalogManagedOnReplace(userInputTableProperties);
    // In the case of Replace table there are a few delta-specific properties we want to preserve
    Map<String, String> replaceTableProperties =
        readSnapshot.getMetadata().getConfiguration().entrySet().stream()
            .filter(
                e ->
                    ReplaceTableTransactionBuilderV2Impl.TABLE_PROPERTY_KEYS_TO_PRESERVE.contains(
                        e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    replaceTableProperties.putAll(userInputTableProperties);
    Output output =
        new TransactionMetadataFactory(
                tablePath,
                Optional.of(readSnapshot),
                Optional.of(
                    getInitialMetadata(
                        schema, replaceTableProperties, partitionColumns.orElse(emptyList()))),
                Optional.of(readSnapshot.getProtocol()),
                userInputTableProperties,
                true /* isCreateOrReplace */,
                clusteringColumns,
                false /* isSchemaEvolultion */)
            .finalOutput;
    // TODO: reconsider whether we should always commit a new Protocol action regardless of whether
    //   there is a protocol upgrade
    checkState(
        output.newMetadata.isPresent() && output.newProtocol.isPresent(),
        "Expected non-null metadata and protocol for replace table");
    if (output
        .newProtocol
        .orElse(readSnapshot.getProtocol())
        .supportsFeature(TableFeatures.ICEBERG_COMPAT_V3_W_FEATURE)) {
      // Block this for now to be safe, we will return to this in the future
      // once replace for rowTracking is enabled
      throw new UnsupportedOperationException(
          "REPLACE TABLE is not yet supported on IcebergCompatV3 tables");
    }
    return output;
  }

  static Output buildUpdateTableMetadata(
      String tablePath,
      SnapshotImpl readSnapshot,
      Optional<Map<String, String>> propertiesAdded,
      Optional<Set<String>> propertyKeysRemoved,
      Optional<StructType> newSchema,
      Optional<List<Column>> clusteringColumns) {
    if (propertiesAdded.isPresent() && propertyKeysRemoved.isPresent()) {
      Set<String> overlappingPropertyKeys =
          propertyKeysRemoved.get().stream()
              .filter(key -> propertiesAdded.get().containsKey(key))
              .collect(toSet());
      if (!overlappingPropertyKeys.isEmpty()) {
        throw DeltaErrors.overlappingTablePropertiesSetAndUnset(overlappingPropertyKeys);
      }
    }
    Optional<Metadata> newMetadata = Optional.empty();

    Map<String, String> newProperties =
        readSnapshot
            .getMetadata()
            .filterOutUnchangedProperties(propertiesAdded.orElse(Collections.emptyMap()));

    if (!newProperties.isEmpty()) {
      newMetadata = Optional.of(readSnapshot.getMetadata().withMergedConfiguration(newProperties));
    }

    if (propertyKeysRemoved.isPresent()) {
      newMetadata =
          Optional.of(
              newMetadata
                  .orElse(readSnapshot.getMetadata())
                  .withConfigurationKeysUnset(propertyKeysRemoved.get()));
    }

    if (newSchema.isPresent()) {
      newMetadata =
          Optional.of(
              newMetadata.orElse(readSnapshot.getMetadata()).withNewSchema(newSchema.get()));
    }

    return new TransactionMetadataFactory(
            tablePath,
            Optional.of(readSnapshot),
            newMetadata,
            Optional.empty(),
            propertiesAdded.orElse(Collections.emptyMap()),
            false /* isCreateOrReplace */,
            clusteringColumns,
            newSchema.isPresent() /* isSchemaEvolultion */)
        .finalOutput;
  }

  ///////////////////////////////
  // Instance Fields / Methods //
  ///////////////////////////////

  // ===== Fields that set by input =====
  private final String tablePath;
  private final Optional<SnapshotImpl> latestSnapshotOpt;

  /**
   * The table properties provided in this transaction. i.e. excludes any properties in the read
   * snapshot.
   *
   * <p>This helps validation code understand what the user is trying to do in *this* transaction,
   * as opposed to what is the current state already in the table.
   */
  private final Map<String, String> originalUserInputProperties;

  private final boolean isCreateOrReplace;
  private final boolean isSchemaEvolution;

  // ===== Fields that are updated by helper methods when updating and validating the metadata =====
  private Optional<Metadata> newMetadata;
  private Optional<Protocol> newProtocol;
  private Optional<List<Column>> physicalNewClusteringColumns;

  // ===== Fields that are fixed after validation and updates are finished =====
  private final Output finalOutput;

  /**
   * @param initialNewMetadata the initial metadata that we should validate and transform. It is a
   *     function of the readSnapshot's metadata (if applicable) joined with any _user provided_
   *     table property updates. Specifically:
   *     <ul>
   *       <li>CREATE: default empty metadata merged with schema, partCols, and user-specified table
   *           properties
   *       <li>UPDATE: readSnapshot's metadata merged wth user-specified added/removed table
   *           properties
   *       <li>REPLACE: readSnapshot's metadata, with all table properties removed except for those
   *           that are included in TABLE_PROPERTY_KEYS_TO_PRESERVE, merged with schema, partCols,
   *           and user-specified table properties
   *     </ul>
   *     <p>This class may apply additional updates to transform the {@code initialNewMetadata} the
   *     final output (e.g. auto-enabling column mapping for iceberg compat, adding column mapping
   *     metadata to the schema, etc.)
   */
  private TransactionMetadataFactory(
      String tablePath,
      Optional<SnapshotImpl> latestSnapshotOpt,
      Optional<Metadata> initialNewMetadata,
      Optional<Protocol> initialNewProtocol,
      Map<String, String> originalUserInputProperties,
      boolean isCreateOrReplace,
      Optional<List<Column>> userProvidedLogicalClusteringColumns,
      boolean isSchemaEvolution) {
    checkArgument(
        (initialNewMetadata.isPresent() && initialNewProtocol.isPresent())
            || latestSnapshotOpt.isPresent(),
        "initial protocol and metadata must be present for create table");
    checkArgument(
        !isSchemaEvolution || !isCreateOrReplace,
        "isSchemaEvolution can only be true for update table");
    checkArgument(
        isCreateOrReplace || latestSnapshotOpt.isPresent(),
        "update table must provide a latest snapshot");
    this.tablePath = tablePath;
    this.latestSnapshotOpt = latestSnapshotOpt;
    this.originalUserInputProperties = originalUserInputProperties;
    this.isCreateOrReplace = isCreateOrReplace;
    this.isSchemaEvolution = isSchemaEvolution;
    this.newMetadata = initialNewMetadata;
    this.newProtocol = initialNewProtocol;

    performProtocolUpgrades(userProvidedLogicalClusteringColumns.isPresent());
    handleCatalogManagedEnablement();
    handleInCommitTimestampDisablement();
    performIcebergCompatUpgradesAndValidation();
    updateColumnMappingMetadataAndResolveClusteringColumns(userProvidedLogicalClusteringColumns);
    updateRowTrackingMetadata();
    validateMetadataChangeAndApplyTypeWidening();
    this.finalOutput = new Output(newProtocol, newMetadata, physicalNewClusteringColumns);
  }

  private Metadata getEffectiveMetadata() {
    // Fact: either newMetadata is defined upon initiation or latestSnapshotOpt is present
    return newMetadata.orElseGet(() -> latestSnapshotOpt.get().getMetadata());
  }

  private Protocol getEffectiveProtocol() {
    // Fact: either newProtocol is defined upon initiation or latestSnapshotOpt is present
    return newProtocol.orElseGet(() -> latestSnapshotOpt.get().getProtocol());
  }

  private void validateForUpdateTableUsingOldMetadata(Consumer<Metadata> validateFn) {
    if (!isCreateOrReplace) {
      // For update table we know latestSnapshotOpt is present
      Metadata oldMetadata = latestSnapshotOpt.get().getMetadata();
      validateFn.accept(oldMetadata);
    }
  }

  /** STEP 1: Update the PROTOCOL based on the table properties or schema */
  // TODO: if you only update the feature properties we currently write a new Metadata even though
  //  this should just be a protocol upgrade (this could be an issue if for example you use
  //  .withDomainMetadata supported in every txn --- we always write a new Metadata action)
  private void performProtocolUpgrades(boolean clusteringRequired) {
    // This is the only place we update the protocol action; takes care of any dependent features
    // Ex: We enable feature `icebergCompatV2` plus dependent features `columnMapping`

    // This will remove feature properties (i.e. metadata properties in the form of
    // "delta.feature.*") from metadata. There should be one TableFeature in the returned set for
    // each property removed.
    Tuple2<Set<TableFeature>, Optional<Metadata>> newFeaturesAndMetadata =
        TableFeatures.extractFeaturePropertyOverrides(getEffectiveMetadata());
    if (newFeaturesAndMetadata._2.isPresent()) {
      newMetadata = newFeaturesAndMetadata._2;
    }

    // Enable clustering if not already enabled and clustering columns are set. This isn't handled
    // in autoUpgradeProtocolBasedOnMetadata since clustering is stored in the domain metadata
    if (clusteringRequired
        && !getEffectiveProtocol().supportsFeature(TableFeatures.CLUSTERING_W_FEATURE)) {
      newFeaturesAndMetadata._1.add(TableFeatures.CLUSTERING_W_FEATURE);
    }

    Optional<Tuple2<Protocol, Set<TableFeature>>> newProtocolAndFeatures =
        TableFeatures.autoUpgradeProtocolBasedOnMetadata(
            getEffectiveMetadata(), newFeaturesAndMetadata._1, getEffectiveProtocol());
    if (newProtocolAndFeatures.isPresent()) {
      logger.info(
          "Automatically enabling table features: {}",
          newProtocolAndFeatures.get()._2.stream().map(TableFeature::featureName).collect(toSet()));

      newProtocol = Optional.of(newProtocolAndFeatures.get()._1);
      TableFeatures.validateKernelCanWriteToTable(
          getEffectiveProtocol(), getEffectiveMetadata(), tablePath);
    }
  }

  /** STEP 1.1: Handle catalogManaged enablement. Updates the METADATA if applicable. */
  private void handleCatalogManagedEnablement() {
    final boolean readVersionSupportsCatalogManaged =
        latestSnapshotOpt
            .map(x -> TableFeatures.isCatalogManagedSupported(x.getProtocol()))
            .orElse(false);
    final boolean writeVersionSupportsCatalogManaged =
        TableFeatures.isCatalogManagedSupported(getEffectiveProtocol());
    final boolean txnEnablesCatalogManaged =
        !readVersionSupportsCatalogManaged && writeVersionSupportsCatalogManaged;

    // Case 1: Txn is not enabling catalogManaged. Exit.
    if (!txnEnablesCatalogManaged) {
      return;
    }

    // catalogManaged is being enabled in this transaction. catalogManaged dependsOn
    // inCommitTimestamp. The inCommitTimestamp feature should have been auto-supported in the
    // protocol by now.
    checkState(
        getEffectiveProtocol().supportsFeature(TableFeatures.IN_COMMIT_TIMESTAMP_W_FEATURE),
        "inCommitTimestamp feature expected to have already been supported");

    // Case 2: Txn is explicitly disabling ICT. Throw.
    final String txnIctEnabledValue =
        originalUserInputProperties.get(TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey());
    final boolean txnExplicitlyDisablesICT =
        txnIctEnabledValue != null && txnIctEnabledValue.equalsIgnoreCase("false");
    if (txnExplicitlyDisablesICT) {
      throw new KernelException("Cannot disable inCommitTimestamp when enabling catalogManaged");
    }

    // Case 3: ICT already enabled. Exit.
    final boolean isIctAlreadyEnabled =
        TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(getEffectiveMetadata());
    if (isIctAlreadyEnabled) {
      return;
    }

    // Case 4: ICT is not enabled. Enable it.
    newMetadata =
        Optional.of(
            getEffectiveMetadata()
                .withMergedConfiguration(
                    Collections.singletonMap(
                        TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey(), "true")));
  }

  /**
   * Step 1.2: Handle inCommitTimestamp disablement. Updates the METADATA if applicable.
   *
   * <p>If the user explicitly disables inCommitTimestamp in this transaction, we then also
   * explicitly remove the ICT enablement version and timestamp properties from the metadata.
   */
  private void handleInCommitTimestampDisablement() {
    final String txnIctEnabledValue =
        originalUserInputProperties.get(TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey());
    final boolean txnExplicitlyDisablesICT =
        txnIctEnabledValue != null && txnIctEnabledValue.equalsIgnoreCase("false");

    // Case 1: Txn is not explicitly disabling ICT. Exit.
    if (!txnExplicitlyDisablesICT) {
      return;
    }

    // Case 2: Txn is explicitly disabling ICT on a catalogManaged table. Throw.
    if (getEffectiveProtocol().supportsFeature(TableFeatures.CATALOG_MANAGED_R_W_FEATURE_PREVIEW)) {
      throw new KernelException("Cannot disable inCommitTimestamp on a catalogManaged table");
    }

    // Case 3 (normal case): Txn is explicitly disabling ICT. Remove the ICT enablement properties.
    final Set<String> ictKeysToRemove =
        new HashSet<>(
            Arrays.asList(
                TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey(),
                TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey()));

    newMetadata = Optional.of(getEffectiveMetadata().withConfigurationKeysUnset(ictKeysToRemove));
  }

  /**
   * STEP 2: Validate the METADATA and PROTOCOL and possibly update the METADATA for IcebergCompat
   */
  private void performIcebergCompatUpgradesAndValidation() {
    // IcebergCompat validates that the current metadata and protocol is compatible (e.g. all the
    // required TF are present, no incompatible types, etc). It also updates the metadata for new
    // tables if needed (e.g. enables column mapping)
    // Ex: We enable column mapping mode in the configuration such that our properties now include
    // Map(delta.enableIcebergCompatV2 -> true, delta.columnMapping.mode -> name)

    // Validate this is a valid config change earlier for a clearer error message
    validateForUpdateTableUsingOldMetadata(
        oldMetadata -> {
          newMetadata.ifPresent(
              metadata ->
                  IcebergWriterCompatV1MetadataValidatorAndUpdater
                      .validateIcebergWriterCompatV1Change(
                          oldMetadata.getConfiguration(), metadata.getConfiguration()));
          newMetadata.ifPresent(
              metadata ->
                  IcebergCompatV3MetadataValidatorAndUpdater.validateIcebergCompatV3Change(
                      oldMetadata.getConfiguration(), metadata.getConfiguration()));
        });

    // We must do our icebergWriterCompatV1 checks/updates FIRST since it has stricter column
    // mapping requirements (id mode) than icebergCompatV2. It also may enable icebergCompatV2.
    Optional<Metadata> icebergWriterCompatV1 =
        IcebergWriterCompatV1MetadataValidatorAndUpdater
            .validateAndUpdateIcebergWriterCompatV1Metadata(
                isCreateOrReplace, getEffectiveMetadata(), getEffectiveProtocol());
    if (icebergWriterCompatV1.isPresent()) {
      newMetadata = icebergWriterCompatV1;
    }

    Optional<Metadata> icebergWriterCompatV3 =
        IcebergWriterCompatV3MetadataValidatorAndUpdater
            .validateAndUpdateIcebergWriterCompatV3Metadata(
                isCreateOrReplace, getEffectiveMetadata(), getEffectiveProtocol());
    if (icebergWriterCompatV3.isPresent()) {
      newMetadata = icebergWriterCompatV3;
    }

    // TODO: refactor this method to use a single validator and updater.
    Optional<Metadata> icebergCompatV2Metadata =
        IcebergCompatV2MetadataValidatorAndUpdater.validateAndUpdateIcebergCompatV2Metadata(
            isCreateOrReplace, getEffectiveMetadata(), getEffectiveProtocol());
    if (icebergCompatV2Metadata.isPresent()) {
      newMetadata = icebergCompatV2Metadata;
    }
    Optional<Metadata> icebergCompatV3Metadata =
        IcebergCompatV3MetadataValidatorAndUpdater.validateAndUpdateIcebergCompatV3Metadata(
            isCreateOrReplace, getEffectiveMetadata(), getEffectiveProtocol());
    if (icebergCompatV3Metadata.isPresent()) {
      newMetadata = icebergCompatV3Metadata;
    }
  }

  /**
   * STEP 3: Update the METADATA with column mapping info if applicable, and resolve the provided
   * clustering columns using the updated metadata.
   */
  private void updateColumnMappingMetadataAndResolveClusteringColumns(
      Optional<List<Column>> userProvidedClusteringColumns) {
    // We update the column mapping info here after all configuration changes are finished
    Optional<Metadata> columnMappingMetadata =
        ColumnMapping.updateColumnMappingMetadataIfNeeded(
            getEffectiveMetadata(), isCreateOrReplace);
    if (columnMappingMetadata.isPresent()) {
      newMetadata = columnMappingMetadata;
    }
    // We also resolve the user provided clustering columns here using the updated schema
    StructType updatedSchema = getEffectiveMetadata().getSchema();
    this.physicalNewClusteringColumns =
        userProvidedClusteringColumns.map(
            cols -> SchemaUtils.casePreservingEligibleClusterColumns(updatedSchema, cols));
  }

  /** STEP 4: Update the METADATA with materialized row tracking column name if applicable */
  private void updateRowTrackingMetadata() {
    if (isCreateOrReplace) {
      // For new tables, assign materialized column names if row tracking is enabled
      Optional<Metadata> rowTrackingMetadata =
          MaterializedRowTrackingColumn.assignMaterializedColumnNamesIfNeeded(
              getEffectiveMetadata());
      if (rowTrackingMetadata.isPresent()) {
        newMetadata = rowTrackingMetadata;
      }
    }
    validateForUpdateTableUsingOldMetadata(
        oldMetadata -> {
          // For existing tables, we block enabling/disabling row tracking because:
          // 1. Enabling requires backfilling row IDs/commit versions, which is not supported in
          // Kernel
          // 2. Disabling is irreversible in Kernel (re-enabling not supported)
          newMetadata.ifPresent(
              metadata -> RowTracking.throwIfRowTrackingToggled(oldMetadata, metadata));

          // For existing tables, validate that row tracking configs are present when row tracking
          // is enabled
          MaterializedRowTrackingColumn.validateRowTrackingConfigsNotMissing(
              getEffectiveMetadata(), tablePath);
        });
  }

  /**
   * STEP 5: Validate the metadata change and update the type widening metadata if needed. Note: we
   * update the type widening info at the same time to avoid traversing the schema more than
   * required.
   *
   * <p>Validate that the change from oldMetadata to newMetadata is a valid change. For example,
   * this checks the following
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
   *   <li>Materialized row tracking column names do not conflict with schema
   * </ul>
   */
  private void validateMetadataChangeAndApplyTypeWidening() {
    validateForUpdateTableUsingOldMetadata(
        oldMetadata -> {
          ColumnMapping.verifyColumnMappingChange(
              oldMetadata.getConfiguration(), getEffectiveMetadata().getConfiguration());
          IcebergWriterCompatV1MetadataValidatorAndUpdater.validateIcebergWriterCompatV1Change(
              oldMetadata.getConfiguration(), getEffectiveMetadata().getConfiguration());
          IcebergCompatV3MetadataValidatorAndUpdater.validateIcebergCompatV3Change(
              oldMetadata.getConfiguration(), getEffectiveMetadata().getConfiguration());
        });
    IcebergUniversalFormatMetadataValidatorAndUpdater.validate(getEffectiveMetadata());

    validateForUpdateTableUsingOldMetadata(
        oldMetadata -> {
          if (isSchemaEvolution) {
            ColumnMappingMode updatedMappingMode =
                ColumnMapping.getColumnMappingMode(getEffectiveMetadata().getConfiguration());
            ColumnMappingMode currentMappingMode =
                ColumnMapping.getColumnMappingMode(oldMetadata.getConfiguration());
            if (currentMappingMode != updatedMappingMode) {
              throw new KernelException("Cannot update mapping mode and perform schema evolution");
            }

            // If the column mapping restriction is removed, clustering columns
            // will need special handling during schema evolution since they won't have physical
            // names
            // ToDo: Support adding clustering columns

            if (!isColumnMappingModeEnabled(updatedMappingMode)) {
              throw new KernelException(
                  "Cannot update schema for table when column mapping is disabled");
            }

            // Clustering columns will be guaranteed to have physical names at this point
            // Only the leaf part of the overall column needs to be taken since
            // validation is performed on the leaf struct fields
            // E.g. getClusteringColumns returns <physical_name_of_struct>.<physical_name_inner>,
            // Only physical_name_inner is required for validation
            Optional<List<Column>> effectiveClusteringCols =
                physicalNewClusteringColumns.isPresent()
                    ? physicalNewClusteringColumns
                    : latestSnapshotOpt.get().getPhysicalClusteringColumns();
            Set<String> clusteringColumnPhysicalNames =
                effectiveClusteringCols.orElse(Collections.emptyList()).stream()
                    .map(col -> col.getNames()[col.getNames().length - 1])
                    .collect(toSet());

            Optional<StructType> schemaWithTypeWidening =
                SchemaUtils.validateUpdatedSchemaAndGetUpdatedSchema(
                    oldMetadata,
                    getEffectiveMetadata(),
                    clusteringColumnPhysicalNames,
                    false /* allowNewRequiredFields */);

            schemaWithTypeWidening.ifPresent(
                structType ->
                    newMetadata = Optional.of(getEffectiveMetadata().withNewSchema(structType)));
          }
        });

    // For replace table we need to do special validation in the case of fieldId re-use
    if (isCreateOrReplace && latestSnapshotOpt.isPresent()) {
      // For now, we don't support changing column mapping mode during replace, in a future PR we
      // will loosen this restriction
      ColumnMappingMode oldMode =
          ColumnMapping.getColumnMappingMode(
              latestSnapshotOpt.get().getMetadata().getConfiguration());
      ColumnMappingMode newMode =
          ColumnMapping.getColumnMappingMode(getEffectiveMetadata().getConfiguration());
      if (oldMode != newMode) {
        throw new UnsupportedOperationException(
            String.format(
                "Changing column mapping mode from %s to %s is not currently supported in Kernel "
                    + "during REPLACE TABLE operations",
                oldMode, newMode));
      }

      // We only need to check fieldId re-use when cmMode != none
      if (newMode != ColumnMappingMode.NONE) {
        Optional<StructType> schemaWithTypeWidening =
            SchemaUtils.validateUpdatedSchemaAndGetUpdatedSchema(
                latestSnapshotOpt.get().getMetadata(),
                getEffectiveMetadata(),
                // We already validate clustering columns elsewhere for isCreateOrReplace no
                // need to
                // duplicate this check here
                emptySet() /* clusteringCols */,
                // We allow new non-null fields in REPLACE since we know all existing data is
                // removed
                true /* allowNewRequiredFields */);
        schemaWithTypeWidening.ifPresent(
            structType ->
                newMetadata = Optional.of(getEffectiveMetadata().withNewSchema(structType)));
      }
    }

    MaterializedRowTrackingColumn.throwIfColumnNamesConflictWithSchema(getEffectiveMetadata());
  }

  private static Metadata getInitialMetadata(
      StructType schema, Map<String, String> tableProperties, List<String> partitionColumns) {
    return new Metadata(
        java.util.UUID.randomUUID().toString(), /* id */
        Optional.empty(), /* name */
        Optional.empty(), /* description */
        new Format(), /* format */
        schema.toJson(), /* schemaString */
        schema, /* schema */
        buildArrayValue(
            casePreservingPartitionColNames(schema, partitionColumns),
            StringType.STRING), /* partitionColumns */
        Optional.of(System.currentTimeMillis()), /* createdTime */
        stringStringMapValue(tableProperties) /* configuration */);
  }

  private static Protocol getInitialProtocol() {
    return new Protocol(DEFAULT_READ_VERSION, DEFAULT_WRITE_VERSION);
  }

  private static void validateSchemaAndPartColsCreateOrReplace(
      Map<String, String> tableProperties, StructType schema, List<String> partitionColumns) {
    // New table verify the given schema and partition columns
    ColumnMappingMode mappingMode = ColumnMapping.getColumnMappingMode(tableProperties);

    SchemaUtils.validateSchema(schema, isColumnMappingModeEnabled(mappingMode));
    SchemaUtils.validatePartitionColumns(schema, partitionColumns);
  }

  private static void validateNotEnablingCatalogManagedOnReplace(
      Map<String, String> userInputTableProperties) {
    if (TableFeatures.isPropertiesManuallySupportingTableFeature(
        userInputTableProperties, TableFeatures.CATALOG_MANAGED_R_W_FEATURE_PREVIEW)) {
      throw new UnsupportedOperationException(
          "Cannot enable the catalogManaged feature during a REPLACE command.");
    }
  }
}
