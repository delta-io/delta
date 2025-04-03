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
package io.delta.kernel.internal.icebergcompat;

import static io.delta.kernel.internal.tablefeatures.TableFeatures.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.*;
import java.util.*;
import java.util.stream.Stream;

/**
 * Performs the validations and updates necessary to support the table feature IcebergWriterCompatV1
 * when it is enabled by the table property "delta.enableIcebergWriterCompatV1".
 *
 * <p>Requires that the following table properties are set to the specified values. If they are set
 * to an invalid value, throws an exception. If they are not set, enable them if possible.
 *
 * <ul>
 *   <li>Requires ID column mapping mode (cannot be enabled on existing table).
 *   <li>Requires icebergCompatV2 to be enabled.
 * </ul>
 *
 * <p>Checks that required table features are enabled: icebergCompatWriterV1, icebergCompatV2,
 * columnMapping
 *
 * <p>Checks the following:
 *
 * <ul>
 *   <li>Checks that all table features supported in the table's protocol are in the allow-list of
 *       table features. This simultaneously ensures that any unsupported features are not present
 *       (e.g. CDF, variant type, etc).
 *   <li>Checks that there are no fields with data type byte or short.
 *   <li>Checks that the table feature `invariants` is not active in the table (i.e. there are no
 *       invariants in the table schema). This is a special case where the incompatible feature
 *       `invariants` is in the allow-list of features since it is included by default in the table
 *       protocol for new tables. Since it is incompatible we must verify that it is inactive in the
 *       table.
 * </ul>
 *
 * TODO additional enforcements coming in (ie physicalName=fieldId)
 */
public class IcebergWriterCompatV1MetadataValidatorAndUpdater
    extends IcebergCompatMetadataValidatorAndUpdater {

  /**
   * Validates that any change to property {@link TableConfig#ICEBERG_WRITER_COMPAT_V1_ENABLED} is
   * valid. Currently, the changes we support are
   *
   * <ul>
   *   <li>No change in enablement (true to true or false to false)
   *   <li>Enabling but only on a new table (false to true)
   *   <li>Disabling (true to false)
   * </ul>
   *
   * If enabling (false to true) on an existing table we throw an {@link KernelException}.
   */
  public static void validateIcebergWriterCompatV1Change(
      Map<String, String> oldConfig, Map<String, String> newConfig, boolean isNewTable) {
    if (!isNewTable) {
      boolean wasEnabled = TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.fromMetadata(oldConfig);
      boolean isEnabled = TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.fromMetadata(newConfig);
      if (!wasEnabled && isEnabled) {
        throw DeltaErrors.enablingIcebergWriterCompatV1OnExistingTable(
            TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey());
      }
    }
  }

  /**
   * Validate and update the given Iceberg Writer Compat V1 metadata.
   *
   * @param newMetadata Metadata after the current updates
   * @param newProtocol Protocol after the current updates
   * @return The updated metadata if the metadata is valid and updated, otherwise empty.
   * @throws UnsupportedOperationException if the metadata is not compatible with Iceberg Writer V1
   *     requirements
   */
  public static Optional<Metadata> validateAndUpdateIcebergWriterCompatV1Metadata(
      boolean isCreatingNewTable, Metadata newMetadata, Protocol newProtocol) {
    return INSTANCE.validateAndUpdateMetadata(
        new IcebergCompatInputContext(isCreatingNewTable, newMetadata, newProtocol));
  }

  /// //////////////////////////////////////////////////////////////////////////////
  /// Define the compatibility and update checks for icebergWriterCompatV1       ///
  /// //////////////////////////////////////////////////////////////////////////////

  private static final IcebergWriterCompatV1MetadataValidatorAndUpdater INSTANCE =
      new IcebergWriterCompatV1MetadataValidatorAndUpdater();

  private static final IcebergCompatRequiredTablePropertyEnforcer CM_ID_MODE_ENABLED =
      new IcebergCompatRequiredTablePropertyEnforcer<>(
          TableConfig.COLUMN_MAPPING_MODE,
          (value) -> ColumnMapping.ColumnMappingMode.ID == value,
          ColumnMapping.ColumnMappingMode.ID.value);

  private static final IcebergCompatRequiredTablePropertyEnforcer ICEBERG_COMPAT_V2_ENABLED =
      new IcebergCompatRequiredTablePropertyEnforcer<>(
          TableConfig.ICEBERG_COMPAT_V2_ENABLED,
          (value) -> value,
          "true",
          (inputContext) ->
              IcebergCompatV2MetadataValidatorAndUpdater.validateAndUpdateIcebergCompatV2Metadata(
                  inputContext.isCreatingNewTable,
                  inputContext.newMetadata,
                  inputContext.newProtocol));

  /**
   * Current set of allowed table features. This may evolve as the protocol evolves. This includes
   * `invariants` because it is auto-enabled for tables due to the default writer protocol version =
   * 2. Below in INVARIANTS_INACTIVE_CHECK we check that there are no invariants in the table
   * schema.
   *
   * <p>Notably, we do NOT include the other legacy table features here (such as CDF) because since
   * we only support enabling IcebergWriterCompatV1 on *new* tables, if those features are present
   * in the protocol they must be active (since they were enabled by the metadata). Thus, we know
   * any protocol with those features we are incompatible with. `invariants` is a special case since
   * it may be present but inactive even for new tables.
   */
  private static Set<TableFeature> ALLOWED_TABLE_FEATURES =
      Stream.of(
              INVARIANTS_W_FEATURE,
              APPEND_ONLY_W_FEATURE,
              COLUMN_MAPPING_RW_FEATURE,
              ICEBERG_COMPAT_V2_W_FEATURE,
              ICEBERG_WRITER_COMPAT_V1,
              DOMAIN_METADATA_W_FEATURE,
              VACUUM_PROTOCOL_CHECK_RW_FEATURE,
              CHECKPOINT_V2_RW_FEATURE,
              IN_COMMIT_TIMESTAMP_W_FEATURE,
              CLUSTERING_W_FEATURE,
              TIMESTAMP_NTZ_RW_FEATURE,
              TYPE_WIDENING_RW_FEATURE,
              TYPE_WIDENING_PREVIEW_TABLE_FEATURE)
          .collect(toSet());

  /** Checks that all features supported in the protocol are in {@link #ALLOWED_TABLE_FEATURES} */
  private static final IcebergCompatCheck UNSUPPORTED_FEATURES_CHECK =
      (inputContext) -> {
        // TODO include additional information in inputContext so that we can throw different errors
        //   for blocking enablement of icebergWriterCompatV1 versus enablement of the incompatible
        //   feature
        if (!ALLOWED_TABLE_FEATURES.containsAll(
            inputContext.newProtocol.getImplicitlyAndExplicitlySupportedFeatures())) {
          Set<TableFeature> incompatibleFeatures =
              inputContext.newProtocol.getImplicitlyAndExplicitlySupportedFeatures();
          incompatibleFeatures.removeAll(ALLOWED_TABLE_FEATURES);
          throw DeltaErrors.icebergCompatIncompatibleTableFeatures(
              INSTANCE.compatFeatureName(), incompatibleFeatures);
        }
      };

  /**
   * Checks that there are no unsupported types in the schema. Data types {@link ByteType} and
   * {@link ShortType} are unsupported for IcebergWriterCompatV1 tables.
   */
  private static final IcebergCompatCheck UNSUPPORTED_TYPES_CHECK =
      (inputContext) -> {
        List<Tuple2<List<String>, StructField>> matches =
            SchemaUtils.filterRecursively(
                inputContext.newMetadata.getSchema(),
                /* recurseIntoMapAndArrayTypes= */ true,
                /* stopOnFirstMatch = */ false,
                field -> {
                  DataType dataType = field.getDataType();
                  return (dataType instanceof ByteType || dataType instanceof ShortType);
                });

        if (!matches.isEmpty()) {
          throw DeltaErrors.icebergCompatUnsupportedTypeColumns(
              INSTANCE.compatFeatureName(),
              matches.stream().map(tuple -> tuple._2.getDataType()).collect(toList()));
        }
      };

  /**
   * Checks that the table feature `invariants` is not active in the table, meaning there are no
   * invariants stored in the table schema.
   */
  private static final IcebergCompatCheck INVARIANTS_INACTIVE_CHECK =
      // Note - since Kernel currently does not support the table feature `invariants` we will not
      // hit this check for E2E writes since we will fail early due to unsupported write
      // If Kernel starts supporting the feature `invariants` this check will become applicable
      (inputContext) -> {
        if (TableFeatures.hasInvariants(inputContext.newMetadata.getSchema())) {
          throw DeltaErrors.icebergCompatIncompatibleTableFeatures(
              INSTANCE.compatFeatureName(), Collections.singleton(INVARIANTS_W_FEATURE));
        }
      };

  @Override
  String compatFeatureName() {
    return "icebergWriterCompatV1";
  }

  @Override
  TableConfig<Boolean> requiredDeltaTableProperty() {
    return TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED;
  }

  @Override
  List<IcebergCompatRequiredTablePropertyEnforcer> requiredDeltaTableProperties() {
    return Stream.of(CM_ID_MODE_ENABLED, ICEBERG_COMPAT_V2_ENABLED).collect(toList());
  }

  @Override
  List<TableFeature> requiredDependencyTableFeatures() {
    return Stream.of(
            ICEBERG_WRITER_COMPAT_V1, ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)
        .collect(toList());
  }

  @Override
  List<IcebergCompatCheck> icebergCompatChecks() {
    return Stream.of(UNSUPPORTED_FEATURES_CHECK, UNSUPPORTED_TYPES_CHECK, INVARIANTS_INACTIVE_CHECK)
        .collect(toList());
  }
}
