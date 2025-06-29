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

import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.DataFileStatus;
import java.util.List;
import java.util.stream.Stream;

/** Utility methods for validation and compatibility checks for Iceberg V2. */
public class IcebergCompatV2MetadataValidatorAndUpdater
    extends IcebergCompatMetadataValidatorAndUpdater {

  /**
   * Validate the given {@link DataFileStatus} that is being added as a {@code add} action to Delta
   * Log. Currently, it checks that the statistics are not empty.
   *
   * @param dataFileStatus The {@link DataFileStatus} to validate.
   */
  public static void validateDataFileStatus(DataFileStatus dataFileStatus) {
    validateDataFileStatus(dataFileStatus, INSTANCE.compatFeatureName());
  }

  /// //////////////////////////////////////////////////////////////////////////////
  /// Define the compatibility and update checks for icebergCompatV2             ///
  /// //////////////////////////////////////////////////////////////////////////////

  private static final IcebergCompatV2MetadataValidatorAndUpdater INSTANCE =
      new IcebergCompatV2MetadataValidatorAndUpdater();

  public static IcebergCompatMetadataValidatorAndUpdater
      getIcebergCompatV2MetadataValidatorAndUpdaterInstance() {
    return INSTANCE;
  }

  @Override
  String compatFeatureName() {
    return "icebergCompatV2";
  }

  @Override
  TableConfig<Boolean> requiredDeltaTableProperty() {
    return TableConfig.ICEBERG_COMPAT_V2_ENABLED;
  }

  @Override
  List<IcebergCompatRequiredTablePropertyEnforcer> requiredDeltaTableProperties() {
    return singletonList(COLUMN_MAPPING_REQUIREMENT);
  }

  @Override
  List<TableFeature> requiredDependencyTableFeatures() {
    return Stream.of(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE).collect(toList());
  }

  @Override
  List<IcebergCompatCheck> icebergCompatChecks() {
    return Stream.of(
            CHECK_ONLY_ICEBERG_COMPAT_V2_ENABLED,
            V2_CHECK_HAS_SUPPORTED_TYPES,
            CHECK_HAS_ALLOWED_PARTITION_TYPES,
            CHECK_HAS_NO_PARTITION_EVOLUTION,
            CHECK_HAS_NO_DELETION_VECTORS,
            CHECK_HAS_SUPPORTED_TYPE_WIDENING)
        .collect(toList());
  }
}
