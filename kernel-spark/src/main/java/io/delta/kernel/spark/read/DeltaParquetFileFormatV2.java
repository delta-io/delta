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
package io.delta.kernel.spark.read;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import org.apache.spark.sql.delta.DeltaParquetFileFormatBase;
import scala.Option;

/**
 * Kernel-based implementation of DeltaParquetFileFormat that uses Kernel's Protocol and Metadata.
 *
 * <p>This class enables kernel-spark to leverage delta-spark's Parquet reading logic without
 * depending on delta-spark's Protocol and Metadata action classes.
 */
public class DeltaParquetFileFormatV2 extends DeltaParquetFileFormatBase {
  private static final long serialVersionUID = 1L;

  /**
   * Constructs a DeltaParquetFileFormatV2 with Kernel's Protocol and Metadata.
   *
   * @param protocol Kernel Protocol containing reader/writer versions and table features
   * @param metadata Kernel Metadata containing schema, partition columns, and configuration
   * @param tablePath Optional table path for deletion vector resolution
   * @param optimizationsEnabled Whether to enable Delta-specific read optimizations
   */
  public DeltaParquetFileFormatV2(
      Protocol protocol,
      Metadata metadata,
      Option<String> tablePath,
      boolean optimizationsEnabled) {
    super(
        new ProtocolMetadataAdapterV2(protocol, metadata),
        /* nullableRowTrackingConstantFields */ false,
        /* nullableRowTrackingGeneratedFields */ false,
        optimizationsEnabled,
        tablePath,
        /* isCDCRead */ false);
  }
}
