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
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/** Utility methods for validation and compatibility checks for Iceberg V2. */
public class IcebergCompatV2MetadataValidatorAndUpdater
    extends IcebergCompatMetadataValidatorAndUpdater {
  /**
   * Validate and update the given Iceberg V2 metadata.
   *
   * @param newMetadata Metadata after the current updates
   * @param newProtocol Protocol after the current updates
   * @return The updated metadata if the metadata is valid and updated, otherwise empty.
   * @throws UnsupportedOperationException if the metadata is not compatible with Iceberg V2
   *     requirements
   */
  public static Optional<Metadata> validateAndUpdateIcebergCompatV2Metadata(
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
          INSTANCE.compatVersion(), dataFileStatus);
    }
  }

  /// //////////////////////////////////////////////////////////////////////////////
  /// Define the compatibility and update checks for icebergCompatV2             ///
  /// //////////////////////////////////////////////////////////////////////////////

  private static final IcebergCompatV2MetadataValidatorAndUpdater INSTANCE =
      new IcebergCompatV2MetadataValidatorAndUpdater();

  private static final IcebergCompatRequiredTablePropertyEnforcer ICEBERG_COMPAT_V2_CM_REQUIREMENT =
      new IcebergCompatRequiredTablePropertyEnforcer<>(
          TableConfig.COLUMN_MAPPING_MODE,
          (value) -> ColumnMappingMode.NAME == value || ColumnMappingMode.ID == value,
          ColumnMappingMode.NAME.value);

  private static final IcebergCompatCheck ICEBERG_COMPAT_V2_CHECK_NO_COMPAT_V1_ENABLED =
      (inputContext) -> {
        if (Boolean.valueOf(
            inputContext
                .newMetadata
                .getConfiguration()
                .getOrDefault("delta.enableIcebergCompatV1", "false"))) {
          throw DeltaErrors.icebergCompatIncompatibleVersionEnabled(
              INSTANCE.compatVersion(), "delta.enableIcebergCompatV1");
        }
      };

  private static final IcebergCompatCheck ICEBERG_COMPAT_V2_CHECK_HAS_SUPPORTED_TYPES =
      (inputContext) -> {
        List<Tuple2<List<String>, StructField>> matches =
            SchemaUtils.filterRecursively(
                inputContext.newMetadata.getSchema(),
                /* recurseIntoMapAndArrayTypes= */ true,
                /* stopOnFirstMatch = */ false,
                field -> {
                  DataType dataType = field.getDataType();
                  return !(dataType instanceof ByteType
                      || dataType instanceof ShortType
                      || dataType instanceof IntegerType
                      || dataType instanceof LongType
                      || dataType instanceof FloatType
                      || dataType instanceof DoubleType
                      || dataType instanceof DecimalType
                      || dataType instanceof StringType
                      || dataType instanceof BinaryType
                      || dataType instanceof BooleanType
                      || dataType instanceof DateType
                      || dataType instanceof TimestampType
                      || dataType instanceof TimestampNTZType
                      || dataType instanceof ArrayType
                      || dataType instanceof MapType
                      || dataType instanceof StructType);
                });

        if (!matches.isEmpty()) {
          throw DeltaErrors.icebergCompatUnsupportedTypeColumns(
              INSTANCE.compatVersion(),
              matches.stream().map(tuple -> tuple._2.getDataType()).collect(toList()));
        }
      };

  private static final IcebergCompatCheck ICEBERG_COMPAT_V2_CHECK_HAS_ALLOWED_PARTITION_TYPES =
      (inputContext) ->
          inputContext
              .newMetadata
              .getPartitionColNames()
              .forEach(
                  partitonCol -> {
                    DataType dataType =
                        inputContext.newMetadata.getSchema().get(partitonCol).getDataType();
                    boolean validType =
                        dataType instanceof ByteType
                            || dataType instanceof ShortType
                            || dataType instanceof IntegerType
                            || dataType instanceof LongType
                            || dataType instanceof FloatType
                            || dataType instanceof DoubleType
                            || dataType instanceof DecimalType
                            || dataType instanceof StringType
                            || dataType instanceof BinaryType
                            || dataType instanceof BooleanType
                            || dataType instanceof DateType
                            || dataType instanceof TimestampType
                            || dataType instanceof TimestampNTZType;
                    if (!validType) {
                      throw DeltaErrors.icebergCompatUnsupportedTypePartitionColumn(
                          INSTANCE.compatVersion(), dataType);
                    }
                  });

  private static final IcebergCompatCheck ICEBERG_COMPAT_V2_CHECK_HAS_NO_PARTITION_EVOLUTION =
      (inputContext) -> {
        // TODO: Kernel doesn't support replace table yet. When it is supported, extend
        // this to allow checking the partition columns aren't changed
      };

  private static final IcebergCompatCheck ICEBERG_COMPAT_V2_CHECK_HAS_NO_DELETION_VECTORS =
      (inputContext) -> {
        if (inputContext.newProtocol.supportsFeature(DELETION_VECTORS_RW_FEATURE)) {
          throw DeltaErrors.icebergCompatDeletionVectorsUnsupported(INSTANCE.compatVersion());
        }
      };

  private static final IcebergCompatCheck ICEBERG_COMPAT_V2_CHECK_HAS_SUPPORTED_TYPE_WIDENING =
      (inputContext) -> {
        if (inputContext.newProtocol.supportsFeature(TYPE_WIDENING_RW_FEATURE)) {
          // TODO: Currently Kernel has no support for writing with type widening. When it is
          //  supported extend this to allow a whitelist of supported type widening in Iceberg
          throw DeltaErrors.unsupportedTableFeature(TYPE_WIDENING_RW_FEATURE.featureName());
        } else if (inputContext.newProtocol.supportsFeature(TYPE_WIDENING_PREVIEW_TABLE_FEATURE)) {
          throw DeltaErrors.unsupportedTableFeature(
              TYPE_WIDENING_PREVIEW_TABLE_FEATURE.featureName());
        }
      };

  @Override
  String compatVersion() {
    return "icebergCompatV2";
  }

  @Override
  TableConfig<Boolean> requiredDeltaTableProperty() {
    return TableConfig.ICEBERG_COMPAT_V2_ENABLED;
  }

  @Override
  List<IcebergCompatRequiredTablePropertyEnforcer> requiredDeltaTableProperties() {
    return singletonList(ICEBERG_COMPAT_V2_CM_REQUIREMENT);
  }

  @Override
  List<TableFeature> requiredDependencyTableFeatures() {
    return Stream.of(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE).collect(toList());
  }

  @Override
  List<IcebergCompatCheck> icebergCompatChecks() {
    return Stream.of(
            ICEBERG_COMPAT_V2_CHECK_NO_COMPAT_V1_ENABLED,
            ICEBERG_COMPAT_V2_CHECK_HAS_SUPPORTED_TYPES,
            ICEBERG_COMPAT_V2_CHECK_HAS_ALLOWED_PARTITION_TYPES,
            ICEBERG_COMPAT_V2_CHECK_HAS_NO_PARTITION_EVOLUTION,
            ICEBERG_COMPAT_V2_CHECK_HAS_NO_DELETION_VECTORS,
            ICEBERG_COMPAT_V2_CHECK_HAS_SUPPORTED_TYPE_WIDENING)
        .collect(toList());
  }
}
