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
import java.util.Set;

/**
 * Utility class that enforces dependencies of UNIVERSAL_FORMAT_* options.
 *
 * <p>This class currently only does validation, in the future it might also update metadata to be
 * conformant and thus has "Updater suffix".
 */
public class IcebergUniversalFormatMetadataValidatorAndUpdater {
  private IcebergUniversalFormatMetadataValidatorAndUpdater() {}

  /**
   * Ensures the metadata is consistent with the enabled Universal output targets.
   *
   * <p>If required dependent {@linkplain TableConfig}s are not set in {@code metadata} then an
   * exception is raised.
   *
   * <p>"hudi" is trivially compatible with Metadata.
   *
   * <p>"iceberg" requires that {@linkplain TableConfig#ICEBERG_COMPAT_V2_ENABLED} is set to true.
   *
   * @throws InvalidConfigurationValueException metadata has ICEBERG universal format enabled and
   *     {@linkplain TableConfig#ICEBERG_COMPAT_V2_ENABLED} is not enabled in metadata
   */
  public static void validate(Metadata metadata) {
    if (!metadata
        .getConfiguration()
        .containsKey(TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey())) {
      return;
    }

    Set<String> targetFormats = TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.fromMetadata(metadata);
    boolean isIcebergEnabled = targetFormats.contains(TableConfig.UniversalFormats.FORMAT_ICEBERG);
    boolean isIcebergCompatV2Enabled = TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata);
    if (isIcebergEnabled && !isIcebergCompatV2Enabled) {
      throw new InvalidConfigurationValueException(
          TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey(),
          TableConfig.UniversalFormats.FORMAT_ICEBERG,
          String.format(
              "%s must be set to \"true\" to enable iceberg uniform format.",
              TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey()));
    }
  }
}
