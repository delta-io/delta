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

import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class IcebergWriterCompatV3MetadataValidatorAndUpdater
    extends IcebergWriterCompatMetadataValidatorAndUpdater {

  /**
   * Validates that any change to property {@link TableConfig#ICEBERG_WRITER_COMPAT_V3_ENABLED} is
   * valid. Currently, the changes we support are
   *
   * <ul>
   *   <li>No change in enablement (true to true or false to false)
   * </ul>
   *
   * The changes that we do not support and for which we throw an {@link KernelException} are
   *
   * <ul>
   *   <li>Disabling on an existing table (true to false)
   *   <li>Enabling on an existing table (false to true)
   * </ul>
   */
  public static void validateIcebergWriterCompatV3Change(
      Map<String, String> oldConfig, Map<String, String> newConfig) {
    blockConfigChangeOnExistingTable(
        TableConfig.ICEBERG_WRITER_COMPAT_V3_ENABLED, oldConfig, newConfig);
  }

  /**
   * Validate and update the given Iceberg Writer Compat V3 metadata.
   *
   * @param newMetadata Metadata after the current updates
   * @param newProtocol Protocol after the current updates
   * @return The updated metadata if the metadata is valid and updated, otherwise empty.
   * @throws UnsupportedOperationException if the metadata is not compatible with Iceberg Writer V3
   *     requirements
   */
  public static Optional<Metadata> validateAndUpdateIcebergWriterCompatV3Metadata(
      boolean isCreatingNewTable, Metadata newMetadata, Protocol newProtocol) {
    return INSTANCE.validateAndUpdateMetadata(
        new IcebergCompatInputContext(
            INSTANCE.compatFeatureName(), isCreatingNewTable, newMetadata, newProtocol));
  }

  /// //////////////////////////////////////////////////////////////////////////////
  /// Define the compatibility and update checks for icebergWriterCompatV3       ///
  /// //////////////////////////////////////////////////////////////////////////////

  private static final IcebergWriterCompatV3MetadataValidatorAndUpdater INSTANCE =
      new IcebergWriterCompatV3MetadataValidatorAndUpdater();

  /**
   * Enforcer for Iceberg compatibility V3. Ensures the ICEBERG_COMPAT_V3_ENABLED property is set to
   * "true" and delegates validation to the V3 metadata validator.
   */
  private static final IcebergCompatRequiredTablePropertyEnforcer<Boolean>
      ICEBERG_COMPAT_V3_ENABLED =
          createIcebergCompatEnforcer(
              TableConfig.ICEBERG_COMPAT_V3_ENABLED,
              (inputContext) ->
                  IcebergCompatV3MetadataValidatorAndUpdater
                      .validateAndUpdateIcebergCompatV3Metadata(
                          inputContext.isCreatingNewTable,
                          inputContext.newMetadata,
                          inputContext.newProtocol));

  /**
   * Current set of allowed table features for Iceberg writer compat V3. This combines the all v1
   * supported features with V3-specific features including variant support, deletion vectors, and
   * row tracking.
   */
  private static Set<TableFeature> ALLOWED_TABLE_FEATURES = V3_ALLOWED_FEATURES;

  @Override
  String compatFeatureName() {
    return "icebergWriterCompatV3";
  }

  @Override
  TableConfig<Boolean> requiredDeltaTableProperty() {
    return TableConfig.ICEBERG_WRITER_COMPAT_V3_ENABLED;
  }

  @Override
  List<IcebergCompatRequiredTablePropertyEnforcer> requiredDeltaTableProperties() {
    return Stream.of(CM_ID_MODE_ENABLED, ICEBERG_COMPAT_V3_ENABLED).collect(toList());
  }

  @Override
  List<TableFeature> requiredDependencyTableFeatures() {
    return Stream.of(
            ICEBERG_WRITER_COMPAT_V3, ICEBERG_COMPAT_V3_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)
        .collect(toList());
  }

  @Override
  List<IcebergCompatCheck> icebergCompatChecks() {
    return Stream.concat(Stream.of(createUnsupportedFeaturesCheck(this)), COMMON_CHECKS.stream())
        .collect(toList());
  }

  @Override
  protected Set<TableFeature> getAllowedTableFeatures() {
    return ALLOWED_TABLE_FEATURES;
  }
}
