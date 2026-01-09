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

package io.delta.kernel.unitycatalog.adapters;

import io.delta.storage.commit.actions.AbstractIceberg;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter for Delta Uniform Iceberg metadata to {@link
 * io.delta.storage.commit.actions.AbstractIceberg}.
 *
 * <p>This adapter extracts Iceberg metadata from committer properties and provides it in the format
 * expected by Unity Catalog.
 */
public class IcebergAdapter implements AbstractIceberg {
  private static final Logger logger = LoggerFactory.getLogger(IcebergAdapter.class);

  // Keys for extracting Iceberg metadata from committer properties
  public static final String ICEBERG_METADATA_LOCATION_KEY =
      "delta.uniform.iceberg.metadataLocation";
  public static final String ICEBERG_CONVERTED_DELTA_VERSION_KEY =
      "delta.uniform.iceberg.convertedDeltaVersion";
  public static final String ICEBERG_CONVERTED_DELTA_TIMESTAMP_KEY =
      "delta.uniform.iceberg.convertedDeltaTimestamp";

  private final String metadataLocation;
  private final long convertedDeltaVersion;
  private final String convertedDeltaTimestamp;

  private IcebergAdapter(
      String metadataLocation, long convertedDeltaVersion, String convertedDeltaTimestamp) {
    this.metadataLocation = Objects.requireNonNull(metadataLocation, "metadataLocation is null");
    this.convertedDeltaVersion = convertedDeltaVersion;
    this.convertedDeltaTimestamp =
        Objects.requireNonNull(convertedDeltaTimestamp, "convertedDeltaTimestamp is null");
  }

  /**
   * Extracts Iceberg metadata from committer properties.
   *
   * @param properties the committer properties map
   * @return an Optional containing the IcebergAdapter if all required fields are present,
   *     Optional.empty() otherwise
   */
  public static Optional<AbstractIceberg> fromCommitterProperties(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return Optional.empty();
    }

    String metadataLocation = properties.get(ICEBERG_METADATA_LOCATION_KEY);
    String convertedVersionStr = properties.get(ICEBERG_CONVERTED_DELTA_VERSION_KEY);
    String convertedTimestamp = properties.get(ICEBERG_CONVERTED_DELTA_TIMESTAMP_KEY);

    // All three fields must be present
    if (metadataLocation == null || convertedVersionStr == null || convertedTimestamp == null) {
      return Optional.empty();
    }

    try {
      long convertedVersion = Long.parseLong(convertedVersionStr);
      return Optional.of(
          new IcebergAdapter(metadataLocation, convertedVersion, convertedTimestamp));
    } catch (NumberFormatException e) {
      logger.warn(
          "Invalid converted delta version in committer properties: {}", convertedVersionStr, e);
      return Optional.empty();
    }
  }

  @Override
  public String getMetadataLocation() {
    return metadataLocation;
  }

  @Override
  public long getConvertedDeltaVersion() {
    return convertedDeltaVersion;
  }

  @Override
  public String getConvertedDeltaTimestamp() {
    return convertedDeltaTimestamp;
  }
}
