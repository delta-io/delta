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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode;
import io.delta.kernel.utils.DataFileStatus;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/** Utility methods for validation and compatibility checks for Iceberg V2. */
public class IcebergCompatV2Utils {
  private IcebergCompatV2Utils() {}

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

    if (!ICEBERG_COMPAT_V2_VALIDATOR_AND_UPDATER.isEnabled(newMetadata)) {
      return Optional.empty();
    }

    return ICEBERG_COMPAT_V2_VALIDATOR_AND_UPDATER.validateAndUpdateMetadata(
        isCreatingNewTable, newMetadata, newProtocol);
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
      throw DeltaErrors.missingNumRecordsStatsForIcebergCompatV2(dataFileStatus);
    }
  }

  /////////////////////////////////////////////////////////////////////////////////
  /// Private static variables and methods                                      ///
  /////////////////////////////////////////////////////////////////////////////////
  /** Defines a callback to post-process the metadata. */
  private abstract static class PostMetadataProcessor {
    abstract Optional<Metadata> postProcess(Metadata newMetadata, boolean isCreatingNewTable);
  }

  /** Wrapper class for table property validation */
  private static class RequiredDeltaTablePropertyEnforcer {
    public final String propertyName;
    public final Predicate<String> validator;
    public final String autoSetValue;
    public final PostMetadataProcessor postMetadataProcessor;

    /**
     * Constructor for RequiredDeltaTableProperty
     *
     * @param propertyName DeltaConfig we are checking
     * @param validator A generic method to validate the given value
     * @param autoSetValue The value to set if we can auto-set this value (e.g. during table
     *     creation)
     * @param postMetadataProcessor A callback to post-process the metadata
     */
    RequiredDeltaTablePropertyEnforcer(
        String propertyName,
        Predicate<String> validator,
        String autoSetValue,
        PostMetadataProcessor postMetadataProcessor) {
      this.propertyName = propertyName;
      this.validator = validator;
      this.autoSetValue = autoSetValue;
      this.postMetadataProcessor = postMetadataProcessor;
    }

    Optional<Metadata> validateAndUpdate(boolean isCreatingTable, Metadata newMetadata) {
      String newestValue = newMetadata.getConfiguration().get(propertyName);
      boolean newestValueOkay = validator.test(newestValue);
      boolean newestValueExplicitlySet = newMetadata.getConfiguration().containsKey(propertyName);

      if (!newestValueOkay) {
        if (!newestValueExplicitlySet && isCreatingTable) {
          // Covers the case CREATE that did not explicitly specify the required table property.
          // In these cases, we set the property automatically.
          newMetadata = newMetadata.withNewConfiguration(singletonMap(propertyName, autoSetValue));
          return Optional.of(newMetadata);
        } else {
          // In all other cases, if the property value is not compatible
          // with the IcebergV1 requirements, we fail
          throw new UnsupportedOperationException(
              String.format(
                  "The value '%s' for the property '%s' is not compatible with "
                      + "IcebergV2 requirements",
                  newestValue, propertyName));
        }
      }

      return Optional.empty();
    }
  }

  private abstract static class IcebergCompatCheck {
    abstract void check(boolean isCreatingNewTable, Metadata newMetadata, Protocol newProtocol);
  }

  private static final RequiredDeltaTablePropertyEnforcer ICEBERG_COMPAT_V2_CM_REQUIREMENT =
      new RequiredDeltaTablePropertyEnforcer(
          TableConfig.COLUMN_MAPPING_MODE.getKey(),
          (value) ->
              value.equals(ColumnMappingMode.NAME.name())
                  || value.equals(ColumnMappingMode.ID.name()),
          ColumnMappingMode.NAME.value,
          new PostMetadataProcessor() {
            @Override
            Optional<Metadata> postProcess(Metadata newMetadata, boolean isCreatingNewTable) {
              // TODO: assign column mapping field ids
              return Optional.empty();
            }
          });

  private static final IcebergCompatCheck ICEBERG_COMPAT_V2_CHECK_NO_COMPAT_V1_ENABLED =
      new IcebergCompatCheck() {
        @Override
        void check(boolean isCreatingNewTable, Metadata newMetadata, Protocol newProtocol) {
          if (Boolean.valueOf(
              newMetadata
                  .getConfiguration()
                  .getOrDefault("delta.enableIcebergCompatV1", "false"))) {
            throw new IllegalArgumentException(
                "Not expected to have delta.enableIcebergCompatV1 enabled");
          }
        }
      };

  private static final IcebergCompatCheck ICEBERG_COMPAT_V2_CHECK_HAS_SUPPORTED_TYPES =
      new IcebergCompatCheck() {
        @Override
        void check(boolean isCreatingNewTable, Metadata newMetadata, Protocol newProtocol) {
          // TODO
        }
      };

  private static final IcebergCompatCheck ICEBERG_COMPAT_V2_CHECK_HAS_ALLOWED_PARTITION_TYPES =
      new IcebergCompatCheck() {
        @Override
        void check(boolean isCreatingNewTable, Metadata newMetadata, Protocol newProtocol) {
          // TODO
        }
      };

  private static final IcebergCompatCheck ICEBERG_COMPAT_V2_CHECK_HAS_NO_PARTITION_EVOLUTION =
      new IcebergCompatCheck() {
        @Override
        void check(boolean isCreatingNewTable, Metadata newMetadata, Protocol newProtocol) {
          // TODO
        }
      };

  private static final IcebergCompatCheck ICEBERG_COMPAT_V2_CHECK_HAS_NO_DELETION_VECTORS =
      new IcebergCompatCheck() {
        @Override
        void check(boolean isCreatingNewTable, Metadata newMetadata, Protocol newProtocol) {
          // TODO
        }
      };

  private static class IcebergCompatMetadataValidatorAndUpdater {
    private final TableConfig<Boolean> requiredDeltaTableProperty;
    private final List<TableFeature> requiredDependencyTableFeatures;
    private final List<RequiredDeltaTablePropertyEnforcer> requiredDeltaTableProperties;
    private final List<IcebergCompatCheck> icebergCompatChecks;

    IcebergCompatMetadataValidatorAndUpdater(
        TableConfig<Boolean> requiredDeltaTableProperty,
        List<TableFeature> requiredDependencyTableFeatures,
        List<RequiredDeltaTablePropertyEnforcer> requiredDeltaTableProperties,
        List<IcebergCompatCheck> icebergCompatChecks) {
      this.requiredDeltaTableProperty = requiredDeltaTableProperty;
      this.requiredDependencyTableFeatures = requiredDependencyTableFeatures;
      this.requiredDeltaTableProperties = requiredDeltaTableProperties;
      this.icebergCompatChecks = icebergCompatChecks;
    }

    boolean isEnabled(Metadata metadata) {
      return requiredDeltaTableProperty.fromMetadata(metadata);
    }

    Optional<Metadata> validateAndUpdateMetadata(
        boolean isCreatingNewTable, Metadata newMetadata, Protocol newProtocol) {
      Optional<Metadata> updatedMetadata = Optional.empty();

      // table property checks and metadata updates
      for (RequiredDeltaTablePropertyEnforcer requiredDeltaTableProperty :
          requiredDeltaTableProperties) {
        Optional<Metadata> updated =
            requiredDeltaTableProperty.validateAndUpdate(
                isCreatingNewTable, updatedMetadata.orElse(newMetadata));

        if (updated.isPresent()) {
          updatedMetadata = updated;
        }
      }

      // post-process metadata after the table property checks are done and updated
      for (RequiredDeltaTablePropertyEnforcer requiredDeltaTableProperty :
          requiredDeltaTableProperties) {
        Optional<Metadata> updated =
            requiredDeltaTableProperty.postMetadataProcessor.postProcess(
                updatedMetadata.orElse(newMetadata), isCreatingNewTable);
        if (updated.isPresent()) {
          updatedMetadata = updated;
        }
      }

      // check for required dependency table features
      for (TableFeature requiredDependencyTableFeature : requiredDependencyTableFeatures) {
        if (!newProtocol
            .getImplicitlyAndExplicitlySupportedFeatures()
            .contains(requiredDependencyTableFeature)) {
          throw new UnsupportedOperationException(
              String.format(
                  "The table feature '%s' is required for IcebergV2 compatibility",
                  requiredDependencyTableFeature.featureName()));
        }
      }

      // check for IcebergV2 compatibility checks
      for (IcebergCompatCheck icebergCompatCheck : icebergCompatChecks) {
        icebergCompatCheck.check(isCreatingNewTable, newMetadata, newProtocol);
      }

      return updatedMetadata;
    }
  }

  private static final IcebergCompatMetadataValidatorAndUpdater
      ICEBERG_COMPAT_V2_VALIDATOR_AND_UPDATER =
          new IcebergCompatMetadataValidatorAndUpdater(
              TableConfig.ICEBERG_COMPAT_V2_ENABLED,
              Stream.of(
                      TableFeatures.ICEBERG_COMPAT_V2_W_FEATURE,
                      TableFeatures.COLUMN_MAPPING_RW_FEATURE)
                  .collect(toList()),
              singletonList(ICEBERG_COMPAT_V2_CM_REQUIREMENT),
              Stream.of(
                      ICEBERG_COMPAT_V2_CHECK_NO_COMPAT_V1_ENABLED,
                      ICEBERG_COMPAT_V2_CHECK_HAS_SUPPORTED_TYPES,
                      ICEBERG_COMPAT_V2_CHECK_HAS_ALLOWED_PARTITION_TYPES,
                      ICEBERG_COMPAT_V2_CHECK_HAS_NO_PARTITION_EVOLUTION,
                      ICEBERG_COMPAT_V2_CHECK_HAS_NO_DELETION_VECTORS)
                  .collect(toList()));
}
