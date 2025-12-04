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

/** V2 implementation (kernel-based) of DeltaParquetFileFormat. */
public class DeltaParquetFileFormatV2 extends DeltaParquetFileFormatBase {
  private static final long serialVersionUID = 1L;

  /**
   * Creates a DeltaParquetFileFormatV2 with table path for deletion vector support.
   *
   * @param protocol Kernel's Protocol
   * @param metadata Kernel's Metadata
   * @param tablePath The table path (required for deletion vector support)
   * @param optimizationsEnabled Whether to enable optimizations (splits, predicate pushdown)
   */
  public DeltaParquetFileFormatV2(
      Protocol protocol, Metadata metadata, String tablePath, boolean optimizationsEnabled) {
    super(
        new ProtocolAndMetadataAdapterV2(protocol, metadata),
        false, // nullableRowTrackingConstantFields
        false, // nullableRowTrackingGeneratedFields
        optimizationsEnabled,
        Option.apply(tablePath),
        false); // isCDCRead
  }
}
