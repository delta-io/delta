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

import static java.util.Collections.singletonMap;

import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Contains interfaces and common utility classes for defining the iceberg conversion compatibility
 * checks and metadata updates.
 *
 * <p>Main class is {@link IcebergCompatMetadataValidatorAndUpdater} which takes:
 *
 * <ul>
 *   <li>{@link TableConfig} to check if the table is enabled iceberg compat property enabled. When
 *       enabled, the metadata will be validated and updated.
 *   <li>List of {@link TableFeature}s expected to be supported by the protocol
 *   <li>List of {@link IcebergCompatRequiredTablePropertyEnforcer} that enforce certain properties
 *       must be set for IcebergV2 compatibility. If the property is not set, we will set it to a
 *       default value. It will also update the metadata to make it compatible with Iceberg compat
 *       version targeted.
 *   <li>List of {@link IcebergCompatCheck} to validate the metadata and protocol. The checks can be
 *       like what are the table features not supported and in what cases a certain table feature is
 *       supported (e.g. type widening is enabled, but iceberg compat only if the widening is
 *       supported in the Iceberg).
 * </ul>
 */
public abstract class IcebergCompatMetadataValidatorAndUpdater {
  /////////////////////////////////////////////////////////////////////////////////
  /// Interfaces for defining checks for the compat validation and updating     ///
  /////////////////////////////////////////////////////////////////////////////////
  /** Defines the input context for the metadata validator and updater. */
  public static class IcebergCompatInputContext {
    final boolean isCreatingNewTable;
    final Metadata newMetadata;
    final Protocol newProtocol;

    public IcebergCompatInputContext(
        boolean isCreatingNewTable, Metadata newMetadata, Protocol newProtocol) {
      this.isCreatingNewTable = isCreatingNewTable;
      this.newMetadata = newMetadata;
      this.newProtocol = newProtocol;
    }

    public IcebergCompatInputContext withUpdatedMetadata(Metadata newMetadata) {
      return new IcebergCompatInputContext(isCreatingNewTable, newMetadata, newProtocol);
    }
  }

  /** Defines a callback to post-process the metadata. */
  interface PostMetadataProcessor {
    Optional<Metadata> postProcess(IcebergCompatInputContext inputContext);
  }

  /**
   * Defines a required table property that must be set for IcebergV2 compatibility. If the property
   * is not set, we will set it to a default value. It will also update the metadata to make it
   * compatible with Iceberg compat version targeted.
   */
  static class IcebergCompatRequiredTablePropertyEnforcer<T> {
    public final TableConfig<T> property;
    public final Predicate<T> validator;
    public final String autoSetValue;
    public final PostMetadataProcessor postMetadataProcessor;

    /**
     * Constructor for RequiredDeltaTableProperty
     *
     * @param property DeltaConfig we are checking
     * @param validator A generic method to validate the given value
     * @param autoSetValue The value to set if we can auto-set this value (e.g. during table
     *     creation)
     * @param postMetadataProcessor A callback to post-process the metadata
     */
    IcebergCompatRequiredTablePropertyEnforcer(
        TableConfig<T> property,
        Predicate<T> validator,
        String autoSetValue,
        PostMetadataProcessor postMetadataProcessor) {
      this.property = property;
      this.validator = validator;
      this.autoSetValue = autoSetValue;
      this.postMetadataProcessor = postMetadataProcessor;
    }

    /**
     * Constructor for RequiredDeltaTableProperty
     *
     * @param property DeltaConfig we are checking
     * @param validator A generic method to validate the given value
     * @param autoSetValue The value to set if we can auto-set this value (e.g. during table
     *     creation)
     */
    IcebergCompatRequiredTablePropertyEnforcer(
        TableConfig<T> property, Predicate<T> validator, String autoSetValue) {
      this(property, validator, autoSetValue, (c) -> Optional.empty());
    }

    Optional<Metadata> validateAndUpdate(
        IcebergCompatInputContext inputContext, String compatVersion) {
      Metadata newMetadata = inputContext.newMetadata;
      T newestValue = property.fromMetadata(newMetadata);
      boolean newestValueOkay = validator.test(newestValue);
      boolean newestValueExplicitlySet =
          newMetadata.getConfiguration().containsKey(property.getKey());

      if (!newestValueOkay) {
        if (!newestValueExplicitlySet && inputContext.isCreatingNewTable) {
          // Covers the case CREATE that did not explicitly specify the required table property.
          // In these cases, we set the property automatically.
          newMetadata =
              newMetadata.withMergedConfiguration(singletonMap(property.getKey(), autoSetValue));
          return Optional.of(newMetadata);
        } else {
          // In all other cases, if the property value is not compatible
          throw new KernelException(
              String.format(
                  "The value '%s' for the property '%s' is not compatible with "
                      + "%s requirements",
                  newestValue, property.getKey(), compatVersion));
        }
      }

      return Optional.empty();
    }
  }

  /**
   * Defines checks for compatibility with the targeted iceberg features (icebergCompatV1 or
   * icebergCompatV2 etc.)
   */
  interface IcebergCompatCheck {
    void check(IcebergCompatInputContext inputContext);
  }

  /////////////////////////////////////////////////////////////////////////////////
  /// Implementation of {@link IcebergCompatMetadataValidatorAndUpdater}        ///
  /////////////////////////////////////////////////////////////////////////////////

  /**
   * If the iceberg compat is enabled, validate and update the metadata for Iceberg compatibility.
   *
   * @param inputContext input containing the metadata, protocol, if the table is being created etc.
   * @return the updated metadata. If no updates are done, then returns empty
   * @throws {@link io.delta.kernel.exceptions.KernelException} for any validation errors
   */
  Optional<Metadata> validateAndUpdateMetadata(IcebergCompatInputContext inputContext) {
    if (!requiredDeltaTableProperty().fromMetadata(inputContext.newMetadata)) {
      return Optional.empty();
    }

    boolean metadataUpdated = false;

    // table property checks and metadata updates
    List<IcebergCompatRequiredTablePropertyEnforcer> requiredDeltaTableProperties =
        requiredDeltaTableProperties();
    for (IcebergCompatRequiredTablePropertyEnforcer requiredDeltaTableProperty :
        requiredDeltaTableProperties) {
      Optional<Metadata> updated =
          requiredDeltaTableProperty.validateAndUpdate(inputContext, compatVersion());

      if (updated.isPresent()) {
        inputContext = inputContext.withUpdatedMetadata(updated.get());
        metadataUpdated = true;
      }
    }

    // post-process metadata after the table property checks are done and updated
    for (IcebergCompatRequiredTablePropertyEnforcer requiredDeltaTableProperty :
        requiredDeltaTableProperties) {
      Optional<Metadata> updated =
          requiredDeltaTableProperty.postMetadataProcessor.postProcess(inputContext);
      if (updated.isPresent()) {
        metadataUpdated = true;
        inputContext = inputContext.withUpdatedMetadata(updated.get());
      }
    }

    // check for required dependency table features
    for (TableFeature requiredDependencyTableFeature : requiredDependencyTableFeatures()) {
      if (!inputContext.newProtocol.supportsFeature(requiredDependencyTableFeature)) {
        throw DeltaErrors.icebergCompatRequiredFeatureMissing(
            compatVersion(), requiredDependencyTableFeature.featureName());
      }
    }

    // check for IcebergV2 compatibility checks
    for (IcebergCompatCheck icebergCompatCheck : icebergCompatChecks()) {
      icebergCompatCheck.check(inputContext);
    }

    return metadataUpdated ? Optional.of(inputContext.newMetadata) : Optional.empty();
  }

  abstract String compatVersion();

  abstract TableConfig<Boolean> requiredDeltaTableProperty();

  abstract List<IcebergCompatRequiredTablePropertyEnforcer> requiredDeltaTableProperties();

  abstract List<TableFeature> requiredDependencyTableFeatures();

  abstract List<IcebergCompatCheck> icebergCompatChecks();
}
