/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.spark.read;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import org.apache.spark.sql.delta.DeltaParquetFileFormatBase;
import scala.Option;

/**
 * Kernel-specific implementation of DeltaParquetFileFormat.
 *
 * <p>This class extends DeltaParquetFileFormatBase and uses Kernel's Protocol and Metadata through
 * the KernelDeltaProtocolMetadataWrapper abstraction. This enables kernel-spark to leverage
 * Delta-specific Parquet reading logic (column mapping, row tracking) without depending on
 * delta-spark's Protocol and Metadata action classes.
 *
 * <p>Example usage in kernel-spark:
 *
 * <pre>
 *   Protocol protocol = snapshot.getProtocol();
 *   Metadata metadata = snapshot.getMetadata();
 *   KernelDeltaParquetFileFormat fileFormat = new KernelDeltaParquetFileFormat(protocol, metadata);
 * </pre>
 */
public class KernelDeltaParquetFileFormat extends DeltaParquetFileFormatBase {
  private static final long serialVersionUID = 1L;

  /**
   * Creates a KernelDeltaParquetFileFormat with default settings.
   *
   * @param protocol Kernel's Protocol
   * @param metadata Kernel's Metadata
   */
  public KernelDeltaParquetFileFormat(Protocol protocol, Metadata metadata) {
    super(
        new KernelDeltaProtocolMetadataWrapper(protocol, metadata),
        false, // nullableRowTrackingConstantFields
        false, // nullableRowTrackingGeneratedFields
        true, // optimizationsEnabled
        Option.empty(), // tablePath
        false // isCDCRead
        );
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof KernelDeltaParquetFileFormat) {
      return super.equals(other);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getClass().getCanonicalName().hashCode();
  }
}
