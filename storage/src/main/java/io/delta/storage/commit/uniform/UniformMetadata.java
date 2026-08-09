/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import java.util.Optional;

/**
 * Metadata for Delta Universal Format (UniForm) conversions.
 *
 * <p>UniForm allows Delta tables to be read by other table formats. This class contains
 * conversion metadata for supported formats that is sent to Unity Catalog.
 */
public class UniformMetadata {
  private final IcebergMetadata icebergMetadata;
  // Future: private final HudiMetadata hudiMetadata;

  /**
   * Constructs UniformMetadata with Iceberg conversion metadata.
   *
   * @param icebergMetadata The Iceberg conversion metadata (can be null if not enabled)
   */
  public UniformMetadata(IcebergMetadata icebergMetadata) {
    this.icebergMetadata = icebergMetadata;
  }

  /**
   * Returns the Iceberg metadata if Iceberg conversion is enabled.
   *
   * @return Optional containing Iceberg metadata, or empty if not enabled
   */
  public Optional<IcebergMetadata> getIcebergMetadata() {
    return Optional.ofNullable(icebergMetadata);
  }

  // Future: public Optional<HudiMetadata> getHudiMetadata() { ... }
}
