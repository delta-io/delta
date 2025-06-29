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
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.tablefeatures.TableFeature;
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
    extends IcebergWriterCompatMetadataValidatorAndUpdater {

  /**
   * Validates that any change to property {@link TableConfig#ICEBERG_WRITER_COMPAT_V1_ENABLED} is
   * valid. Currently, the changes we support are
   *
   * <ul>
   *   <li>No change in enablement (true to true or false to false)
   *   <li>Enabling but only on a new table (false to true)
   * </ul>
   *
   * The changes that we do not support and for which we throw an {@link KernelException} are
   *
   * <ul>
   *   <li>Disabling on an existing table (true to false)
   *   <li>Enabling on an existing table (false to true)
   * </ul>
   */
  public static void validateIcebergWriterCompatV1Change(
      Map<String, String> oldConfig, Map<String, String> newConfig, boolean isNewTable) {
    blockConfigChangeOnExistingTable(
        TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED, oldConfig, newConfig, isNewTable);
  }

  /// //////////////////////////////////////////////////////////////////////////////
  /// Define the compatibility and update checks for icebergWriterCompatV1       ///
  /// //////////////////////////////////////////////////////////////////////////////

  private static final IcebergWriterCompatV1MetadataValidatorAndUpdater INSTANCE =
      new IcebergWriterCompatV1MetadataValidatorAndUpdater();

  public static IcebergWriterCompatMetadataValidatorAndUpdater
      getIcebergWriterCompatV1MetadataValidatorAndUpdaterInstance() {
    return INSTANCE;
  }

  private static final IcebergCompatRequiredTablePropertyEnforcer ICEBERG_COMPAT_V2_ENABLED =
      new IcebergCompatRequiredTablePropertyEnforcer<>(
          TableConfig.ICEBERG_COMPAT_V2_ENABLED,
          (value) -> value,
          "true",
          (inputContext) ->
              IcebergCompatV2MetadataValidatorAndUpdater.validateAndUpdateIcebergCompatMetadata(
                  inputContext.isCreatingNewTable,
                  inputContext.newMetadata,
                  inputContext.newProtocol,
                  INSTANCE));

  /**
   * Current set of allowed table features. This may evolve as the protocol evolves. This includes
   * the incompatible legacy features (invariants, changeDataFeed, checkConstraints,
   * identityColumns, generatedColumns) because they may be present in the table protocol even when
   * they are not in use. In later checks we validate that these incompatible features are inactive
   * in the table. See the protocol spec for more details.
   */
  private static Set<TableFeature> ALLOWED_TABLE_FEATURES =
      Stream.of(
              // Incompatible legacy table features
              INVARIANTS_W_FEATURE,
              CHANGE_DATA_FEED_W_FEATURE,
              CONSTRAINTS_W_FEATURE,
              IDENTITY_COLUMNS_W_FEATURE,
              GENERATED_COLUMNS_W_FEATURE,
              // Compatible table features
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
              TYPE_WIDENING_RW_PREVIEW_FEATURE)
          .collect(toSet());

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
  protected Set<TableFeature> getAllowedTableFeatures() {
    return ALLOWED_TABLE_FEATURES;
  }

  @Override
  List<IcebergCompatCheck> icebergCompatChecks() {
    return Stream.of(
            createUnsupportedFeaturesCheck(this), // Pass 'this' instance
            UNSUPPORTED_TYPES_CHECK,
            PHYSICAL_NAMES_MATCH_FIELD_IDS_CHECK,
            INVARIANTS_INACTIVE_CHECK,
            CHANGE_DATA_FEED_INACTIVE_CHECK,
            CHECK_CONSTRAINTS_INACTIVE_CHECK,
            IDENTITY_COLUMNS_INACTIVE_CHECK,
            GENERATED_COLUMNS_INACTIVE_CHECK)
        .collect(toList());
  }
}
