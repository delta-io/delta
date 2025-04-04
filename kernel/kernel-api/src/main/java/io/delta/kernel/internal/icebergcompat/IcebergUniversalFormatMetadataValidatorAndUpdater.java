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
   * Ensures the metadata is consistent with the enabled Universal output targets.
   *
   * <p>If required {@linkplain TableConfig}s are not set to a supported a target format the target
   * format will be removed from the returned {@linkplain Metadata} object.
   *
   * <p>"hudi" is trivially compatible with Metadata.
   *
   * <p>"iceberg" requires that {@linkplain TableConfig#ICEBERG_COMPAT_V2_ENABLED} is set to true.
   *
   * @return An updated metadata that removes invalid uniform values if the other required
   *     TableConfig properties are not set. Optional.empty() is returned if no changes are
   *     necessary.
   */
  public static Optional<Metadata> validateAndUpdate(Metadata metadata) {
    if (!metadata
        .getConfiguration()
        .containsKey(TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey())) {
      return Optional.empty();
    }
    Set<String> targetFormats = TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.fromMetadata(metadata);
    if (!targetFormats.contains(TableConfig.UNIVERSAL_FORMAT_ICEBERG)) {
      return Optional.empty();
    }

    if (TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata)) {
      return Optional.empty();
    }

    String updatedFormats =
        targetFormats.stream()
            .filter(s -> !TableConfig.UNIVERSAL_FORMAT_ICEBERG.equals(s))
            .collect(Collectors.joining(","));

    if (updatedFormats.isEmpty()) {
      // No reason to keep the key in the map.
      return Optional.of(
          metadata.withReplacedConfiguration(
              metadata.getConfiguration().entrySet().stream()
                  .filter(
                      e ->
                          !e.getKey().equals(TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey()))
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
    }
    Map<String, String> updatedTargetMap =
        Collections.singletonMap(
            TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey(), updatedFormats);
    return Optional.of(metadata.withMergedConfiguration(updatedTargetMap));
  }
}
