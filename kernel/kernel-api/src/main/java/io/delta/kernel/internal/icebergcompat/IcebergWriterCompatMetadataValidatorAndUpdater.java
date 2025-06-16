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
import static io.delta.kernel.internal.util.SchemaUtils.concatWithDot;
import static java.util.stream.Collectors.toList;

import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.*;
import java.util.*;

/**
 * Contains interfaces and common utility classes performing the validations and updates necessary
 * to support the table feature IcebergWriterCompats when it is enabled by the table properties such
 * as "delta.enableIcebergWriterCompatV3".
 */
abstract class IcebergWriterCompatMetadataValidatorAndUpdater
    extends IcebergCompatMetadataValidatorAndUpdater {
  /////////////////////////////////////////////////////////////////////////////////
  /// Interfaces for defining validations and updates necessary to support IcebergWriterCompats
  // ///
  /////////////////////////////////////////////////////////////////////////////////

  /**
   * Common property enforcer for Column Mapping ID mode requirement. This is identical across all
   * Writer Compat versions.
   */
  protected static final IcebergCompatRequiredTablePropertyEnforcer CM_ID_MODE_ENABLED =
      new IcebergCompatRequiredTablePropertyEnforcer<>(
          TableConfig.COLUMN_MAPPING_MODE,
          (value) -> ColumnMapping.ColumnMappingMode.ID == value,
          ColumnMapping.ColumnMappingMode.ID.value,
          // We need to update the CM info in the schema here because we check that the physical
          // name is correctly set as part of icebergWriterCompat checks
          (inputContext) ->
              ColumnMapping.updateColumnMappingMetadataIfNeeded(
                  inputContext.newMetadata, inputContext.isCreatingNewTable));

  protected static IcebergCompatCheck createUnsupportedFeaturesCheck(
      IcebergWriterCompatMetadataValidatorAndUpdater instance) {
    return (inputContext) -> {
      Set<TableFeature> allowedTableFeatures = instance.getAllowedTableFeatures();
      if (!allowedTableFeatures.containsAll(
          inputContext.newProtocol.getImplicitlyAndExplicitlySupportedFeatures())) {
        Set<TableFeature> incompatibleFeatures =
            inputContext.newProtocol.getImplicitlyAndExplicitlySupportedFeatures();
        incompatibleFeatures.removeAll(allowedTableFeatures);
        throw DeltaErrors.icebergCompatIncompatibleTableFeatures(
            inputContext.compatFeatureName, incompatibleFeatures);
      }
    };
  }

  /**
   * Checks that there are no unsupported types in the schema. Data types {@link ByteType} and
   * {@link ShortType} are unsupported for IcebergWriterCompatV1 and V3 tables.
   */
  protected static final IcebergCompatCheck UNSUPPORTED_TYPES_CHECK =
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
              inputContext.compatFeatureName,
              matches.stream().map(tuple -> tuple._2.getDataType()).collect(toList()));
        }
      };

  /**
   * Checks that in the schema column mapping is set up such that the physicalName is equal to
   * "col-[fieldId]". This check assumes column mapping is enabled (and so should be performed after
   * that check).
   */
  protected static final IcebergCompatCheck PHYSICAL_NAMES_MATCH_FIELD_IDS_CHECK =
      (inputContext) -> {
        List<Tuple2<List<String>, StructField>> invalidFields =
            SchemaUtils.filterRecursively(
                inputContext.newMetadata.getSchema(),
                /* recurseIntoMapAndArrayTypes= */ true,
                /* stopOnFirstMatch = */ false,
                field -> {
                  String physicalName = ColumnMapping.getPhysicalName(field);
                  long columnId = ColumnMapping.getColumnId(field);
                  return !physicalName.equals(String.format("col-%s", columnId));
                });
        if (!invalidFields.isEmpty()) {
          List<String> invalidFieldsFormatted =
              invalidFields.stream()
                  .map(
                      pair ->
                          String.format(
                              "%s(physicalName='%s', columnId=%s)",
                              concatWithDot(pair._1),
                              ColumnMapping.getPhysicalName(pair._2),
                              ColumnMapping.getColumnId(pair._2)))
                  .collect(toList());
          throw DeltaErrors.icebergWriterCompatInvalidPhysicalName(invalidFieldsFormatted);
        }
      };

  /**
   * Checks that the table feature `invariants` is not active in the table, meaning there are no
   * invariants stored in the table schema.
   */
  protected static final IcebergCompatCheck INVARIANTS_INACTIVE_CHECK =
      (inputContext) -> {
        // Note - since Kernel currently does not support the table feature `invariants` we will not
        // hit this check for E2E writes since we will fail early due to unsupported write
        // If Kernel starts supporting the feature `invariants` this check will become applicable
        if (TableFeatures.hasInvariants(inputContext.newMetadata.getSchema())) {
          throw DeltaErrors.icebergCompatIncompatibleTableFeatures(
              inputContext.compatFeatureName, Collections.singleton(INVARIANTS_W_FEATURE));
        }
      };

  /**
   * Checks that the table feature `changeDataFeed` is not active in the table, meaning the table
   * property `delta.enableChangeDataFeed` is not enabled.
   */
  protected static final IcebergCompatCheck CHANGE_DATA_FEED_INACTIVE_CHECK =
      (inputContext) -> {
        // Note - since Kernel currently does not support the table feature `changeDataFeed` we will
        // not hit this check for E2E writes since we will fail early due to unsupported write
        // If Kernel starts supporting the feature `changeDataFeed` this check will become
        // applicable
        if (TableConfig.CHANGE_DATA_FEED_ENABLED.fromMetadata(inputContext.newMetadata)) {
          throw DeltaErrors.icebergCompatIncompatibleTableFeatures(
              inputContext.compatFeatureName, Collections.singleton(CHANGE_DATA_FEED_W_FEATURE));
        }
      };

  /**
   * Checks that the table feature `checkConstraints` is not active in the table, meaning the table
   * has no check constraints stored in its metadata configuration.
   */
  protected static final IcebergCompatCheck CHECK_CONSTRAINTS_INACTIVE_CHECK =
      (inputContext) -> {
        // Note - since Kernel currently does not support the table feature `checkConstraints` we
        // will
        // not hit this check for E2E writes since we will fail early due to unsupported write
        // If Kernel starts supporting the feature `checkConstraints` this check will become
        // applicable
        if (TableFeatures.hasCheckConstraints(inputContext.newMetadata)) {
          throw DeltaErrors.icebergCompatIncompatibleTableFeatures(
              inputContext.compatFeatureName, Collections.singleton(CONSTRAINTS_W_FEATURE));
        }
      };

  /**
   * Checks that the table feature `identityColumns` is not active in the table, meaning no identity
   * columns exist in the table schema.
   */
  protected static final IcebergCompatCheck IDENTITY_COLUMNS_INACTIVE_CHECK =
      (inputContext) -> {
        // Note - since Kernel currently does not support the table feature `identityColumns` we
        // will
        // not hit this check for E2E writes since we will fail early due to unsupported write
        // If Kernel starts supporting the feature `identityColumns` this check will become
        // applicable
        if (TableFeatures.hasIdentityColumns(inputContext.newMetadata)) {
          throw DeltaErrors.icebergCompatIncompatibleTableFeatures(
              inputContext.compatFeatureName, Collections.singleton(IDENTITY_COLUMNS_W_FEATURE));
        }
      };

  /**
   * Checks that the table feature `generatedColumns` is not active in the table, meaning no
   * generated columns exist in the table schema.
   */
  protected static final IcebergCompatCheck GENERATED_COLUMNS_INACTIVE_CHECK =
      (inputContext) -> {
        // Note - since Kernel currently does not support the table feature `generatedColumns` we
        // will
        // not hit this check for E2E writes since we will fail early due to unsupported write
        // If Kernel starts supporting the feature `generatedColumns` this check will become
        // applicable
        if (TableFeatures.hasGeneratedColumns(inputContext.newMetadata)) {
          throw DeltaErrors.icebergCompatIncompatibleTableFeatures(
              inputContext.compatFeatureName, Collections.singleton(GENERATED_COLUMNS_W_FEATURE));
        }
      };

  @Override
  abstract String compatFeatureName();

  @Override
  abstract TableConfig<Boolean> requiredDeltaTableProperty();

  @Override
  abstract List<IcebergCompatRequiredTablePropertyEnforcer> requiredDeltaTableProperties();

  @Override
  abstract List<TableFeature> requiredDependencyTableFeatures();

  @Override
  abstract List<IcebergCompatCheck> icebergCompatChecks();

  abstract Set<TableFeature> getAllowedTableFeatures();
}
