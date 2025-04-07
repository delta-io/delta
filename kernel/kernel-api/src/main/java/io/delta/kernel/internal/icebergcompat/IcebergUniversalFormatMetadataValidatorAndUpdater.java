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

import io.delta.kernel.exceptions.InvalidConfigurationValueException;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Utility class that enforces dependencies of UNIVERSAL_FORMAT_* options. */
public class IcebergUniversalFormatMetadataValidatorAndUpdater {
  private IcebergUniversalFormatMetadataValidatorAndUpdater() {}

  /**
   * Ensures the newMetadata is consistent with the enabled Universal output targets.
   *
   * <p>If required {@linkplain TableConfig}s are no longer set to a supported a target format the
   * target format will be removed from the returned {@linkplain Metadata} object. If the required
   * configs were not set in {@argument currentMetadata} then an exception is raised.
   *
   * <p>"hudi" is trivially compatible with Metadata.
   *
   * <p>"iceberg" requires that {@linkplain TableConfig#ICEBERG_COMPAT_V2_ENABLED} is set to true.
   *
   * @return An updated newMetadata that removes invalid uniform values if the other required
   *     TableConfig properties are not set. Optional.empty() is returned if no changes are
   *     necessary.
   * @throws InvalidConfigurationValueException newMetadata has ICEBERG universal format enabled and
   *     {@linkplain TableConfig#ICEBERG_COMPAT_V2_ENABLED} is not enabled in newMetadata and not
   *     enabled in currentMetadata.
   */
  public static Optional<Metadata> validateAndUpdate(
      Metadata newMetadata, Metadata currentMetadata) {
    if (!newMetadata
        .getConfiguration()
        .containsKey(TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey())) {
      return Optional.empty();
    }
    Set<String> targetFormats =
        TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.fromMetadata(newMetadata);
    if (!targetFormats.contains(TableConfig.UNIVERSAL_FORMAT_ICEBERG)) {
      return Optional.empty();
    }

    if (TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(newMetadata)) {
      return Optional.empty();
    }

    boolean wasIcebergCompatEnabled =
        TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(currentMetadata);
    if (!wasIcebergCompatEnabled) {
      // The user is trying to turn on the new iceberg universal format without enabling iceberg v2.
      throw new InvalidConfigurationValueException(
          TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey(),
          TableConfig.UNIVERSAL_FORMAT_ICEBERG,
          String.format(
              "%s must be set to \"true\" to enable iceberg uniform format.",
              TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey()));
    }

    // The user disabled iceberg compat v2, so automatically remove iceberg from the list of target
    // formats.
    String updatedFormats =
        targetFormats.stream()
            .filter(s -> !TableConfig.UNIVERSAL_FORMAT_ICEBERG.equals(s))
            .collect(Collectors.joining(","));

    if (updatedFormats.isEmpty()) {
      // No reason to keep the key in the map.
      return Optional.of(
          newMetadata.withReplacedConfiguration(
              newMetadata.getConfiguration().entrySet().stream()
                  .filter(
                      e ->
                          !e.getKey().equals(TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey()))
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
    }
    Map<String, String> updatedTargetMap =
        Collections.singletonMap(
            TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey(), updatedFormats);
    return Optional.of(newMetadata.withMergedConfiguration(updatedTargetMap));
  }
}
