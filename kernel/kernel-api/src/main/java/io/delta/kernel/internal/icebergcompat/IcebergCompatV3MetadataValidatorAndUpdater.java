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
package io.delta.kernel.internal.icebergcompat;

import static io.delta.kernel.internal.tablefeatures.TableFeatures.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.DataFileStatus;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/** Utility methods for validation and compatibility checks for Iceberg V3. */
public class IcebergCompatV3MetadataValidatorAndUpdater
    extends IcebergCompatMetadataValidatorAndUpdater {
  /**
   * Validate and update the given Iceberg V3 metadata.
   *
   * @param newMetadata Metadata after the current updates
   * @param newProtocol Protocol after the current updates
   * @return The updated metadata if the metadata is valid and updated, otherwise empty.
   * @throws UnsupportedOperationException if the metadata is not compatible with Iceberg V3
   *     requirements
   */
  public static Optional<Metadata> validateAndUpdateIcebergCompatV3Metadata(
      boolean isCreatingNewTable, Metadata newMetadata, Protocol newProtocol) {
    return INSTANCE.validateAndUpdateMetadata(
        new IcebergCompatInputContext(isCreatingNewTable, newMetadata, newProtocol));
  }

  /**
   * Validate the given {@link DataFileStatus} that is being added as a {@code add} action to Delta
   * Log. Currently, it checks that the statistics are not empty.
   *
   * @param dataFileStatus The {@link DataFileStatus} to validate.
   */
  public static void validateDataFileStatus(DataFileStatus dataFileStatus) {
    if (!dataFileStatus.getStatistics().isPresent()) {
      // presence of stats means always has a non-null `numRecords`
      throw DeltaErrors.icebergCompatMissingNumRecordsStats(
          INSTANCE.compatFeatureName(), dataFileStatus);
    }
  }

  /// //////////////////////////////////////////////////////////////////////////////
  /// Define the compatibility and update checks for icebergCompatV3             ///
  /// //////////////////////////////////////////////////////////////////////////////

  private static final IcebergCompatV3MetadataValidatorAndUpdater INSTANCE =
      new IcebergCompatV3MetadataValidatorAndUpdater();

  private static final IcebergCompatRequiredTablePropertyEnforcer ICEBERG_COMPAT_V3_CM_REQUIREMENT =
      new IcebergCompatRequiredTablePropertyEnforcer<>(
          TableConfig.COLUMN_MAPPING_MODE,
          (value) -> ColumnMappingMode.NAME == value || ColumnMappingMode.ID == value,
          ColumnMappingMode.NAME.value);

  private static final IcebergCompatCheck ICEBERG_COMPAT_V3_CHECK_NO_LOWER_COMPAT_ENABLED =
      (inputContext) -> {
        List<String> incompatibleProperties =
            Arrays.asList("delta.enableIcebergCompatV1", "delta.enableIcebergCompatV2");

        incompatibleProperties.forEach(
            prop -> {
              if (Boolean.parseBoolean(
                  inputContext.newMetadata.getConfiguration().getOrDefault(prop, "false"))) {
                throw DeltaErrors.icebergCompatIncompatibleVersionEnabled(
                    INSTANCE.compatFeatureName(), prop);
              }
            });
      };

  private static final IcebergCompatCheck ICEBERG_COMPAT_V3_CHECK_HAS_SUPPORTED_TYPES =
      (inputContext) -> {
        List<Tuple2<List<String>, StructField>> matches =
            SchemaUtils.filterRecursively(
                inputContext.newMetadata.getSchema(),
                /* recurseIntoMapAndArrayTypes= */ true,
                /* stopOnFirstMatch = */ false,
                field -> {
                  DataType dataType = field.getDataType();
                  // IcebergCompatV3 supports variants and all the IcebergCompatV2 supported types
                  return !isSupportedDataTypesForV2(dataType) && !(dataType instanceof VariantType);
                });

        if (!matches.isEmpty()) {
          throw DeltaErrors.icebergCompatUnsupportedTypeColumns(
              INSTANCE.compatFeatureName(),
              matches.stream().map(tuple -> tuple._2.getDataType()).collect(toList()));
        }
      };

  private static final IcebergCompatCheck ICEBERG_COMPAT_V3_CHECK_HAS_ALLOWED_PARTITION_TYPES =
      (inputContext) ->
          inputContext
              .newMetadata
              .getPartitionColNames()
              .forEach(
                  partitionCol -> {
                    int partitionFieldIndex =
                        inputContext.newMetadata.getSchema().indexOf(partitionCol);
                    checkArgument(
                        partitionFieldIndex != -1,
                        "Partition column %s not found in the schema",
                        partitionCol);
                    DataType dataType =
                        inputContext.newMetadata.getSchema().at(partitionFieldIndex).getDataType();
                    if (!isAllowedPartitionType(dataType)) {
                      throw DeltaErrors.icebergCompatUnsupportedTypePartitionColumn(
                          INSTANCE.compatFeatureName(), dataType);
                    }
                  });

  private static final IcebergCompatCheck ICEBERG_COMPAT_V3_CHECK_HAS_NO_PARTITION_EVOLUTION =
      (inputContext) -> {
        // TODO: Kernel doesn't support replace table yet. When it is supported, extend
        // this to allow checking the partition columns aren't changed
      };

  @Override
  String compatFeatureName() {
    return "icebergCompatV3";
  }

  @Override
  TableConfig<Boolean> requiredDeltaTableProperty() {
    return TableConfig.ICEBERG_COMPAT_V3_ENABLED;
  }

  @Override
  List<IcebergCompatRequiredTablePropertyEnforcer> requiredDeltaTableProperties() {
    return singletonList(ICEBERG_COMPAT_V3_CM_REQUIREMENT);
  }

  @Override
  List<TableFeature> requiredDependencyTableFeatures() {
    return Stream.of(ICEBERG_COMPAT_V3_W_FEATURE, COLUMN_MAPPING_RW_FEATURE).collect(toList());
  }

  @Override
  List<IcebergCompatCheck> icebergCompatChecks() {
    return Stream.of(
            ICEBERG_COMPAT_V3_CHECK_NO_LOWER_COMPAT_ENABLED,
            ICEBERG_COMPAT_V3_CHECK_HAS_SUPPORTED_TYPES,
            ICEBERG_COMPAT_V3_CHECK_HAS_ALLOWED_PARTITION_TYPES,
            ICEBERG_COMPAT_V3_CHECK_HAS_NO_PARTITION_EVOLUTION)
        .collect(toList());
  }
}
