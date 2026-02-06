/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.storage.commit.uniform;

import java.util.Objects;

/**
 * Metadata for Delta Uniform Iceberg conversion.
 *
 * <p>This class contains information about the latest Iceberg conversion for a Delta table,
 * which is sent to Unity Catalog to track the Iceberg metadata state.
 */
public class IcebergMetadata {
  private final String metadataLocation;
  private final long convertedDeltaVersion;
  private final String convertedDeltaTimestamp;

  /**
   * Constructs IcebergMetadata with the specified conversion details.
   *
   * @param metadataLocation The Iceberg metadata file location (e.g., "s3://bucket/metadata/v1.json")
   * @param convertedDeltaVersion The Delta version that was converted (e.g., 1044)
   * @param convertedDeltaTimestamp The timestamp of the conversion (e.g., "2025-01-04T03:13:11.423")
   */
  public IcebergMetadata(
      String metadataLocation, long convertedDeltaVersion, String convertedDeltaTimestamp) {
    this.metadataLocation = Objects.requireNonNull(metadataLocation, "metadataLocation is null");
    this.convertedDeltaVersion = convertedDeltaVersion;
    this.convertedDeltaTimestamp =
        Objects.requireNonNull(convertedDeltaTimestamp, "convertedDeltaTimestamp is null");
  }

  /** Returns the Iceberg metadata file location. */
  public String getMetadataLocation() {
    return metadataLocation;
  }

  /** Returns the Delta version that was converted to Iceberg. */
  public long getConvertedDeltaVersion() {
    return convertedDeltaVersion;
  }

  /** Returns the timestamp when the conversion occurred. */
  public String getConvertedDeltaTimestamp() {
    return convertedDeltaTimestamp;
  }
}
