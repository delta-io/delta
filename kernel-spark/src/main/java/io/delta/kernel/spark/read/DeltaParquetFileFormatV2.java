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
 * V2 implementation of DeltaParquetFileFormat using Kernel's Protocol and Metadata.
 *
 * <p>This class enables kernel-spark to reuse delta-spark's DeltaParquetFileFormatBase for reading
 * Parquet files with Delta-specific features like column mapping.
 */
public class DeltaParquetFileFormatV2 extends DeltaParquetFileFormatBase {

  private final Protocol protocol;
  private final Metadata metadata;

  public DeltaParquetFileFormatV2(
      Protocol protocol,
      Metadata metadata,
      boolean nullableRowTrackingConstantFields,
      boolean nullableRowTrackingGeneratedFields,
      boolean optimizationsEnabled,
      Option<String> tablePath,
      boolean isCDCRead) {
    super(
        new ProtocolMetadataAdapterV2(protocol, metadata),
        nullableRowTrackingConstantFields,
        nullableRowTrackingGeneratedFields,
        optimizationsEnabled,
        tablePath,
        isCDCRead);
    this.protocol = protocol;
    this.metadata = metadata;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof DeltaParquetFileFormatV2)) return false;

    DeltaParquetFileFormatV2 that = (DeltaParquetFileFormatV2) other;
    return this.columnMappingMode().equals(that.columnMappingMode())
        && this.referenceSchema().equals(that.referenceSchema())
        && this.optimizationsEnabled() == that.optimizationsEnabled()
        && this.tablePath().equals(that.tablePath())
        && this.isCDCRead() == that.isCDCRead();
  }
}
