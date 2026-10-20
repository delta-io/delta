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
package io.delta.spark.internal.v2.read

import io.delta.kernel.internal.actions.{Metadata => KernelMetadata, Protocol => KernelProtocol}

import org.apache.spark.sql.delta.DeltaParquetFileFormatBase

/**
 * V2 implementation of DeltaParquetFileFormat using Kernel's Protocol and Metadata.
 *
 * This class enables the V2 connector to reuse delta-spark-v1's DeltaParquetFileFormatBase for
 * reading Parquet files with Delta-specific features like column mapping.
 *
 * @param kernelProtocol Kernel's Protocol
 * @param kernelMetadata Kernel's Metadata
 * @param nullableRowTrackingConstantFields if true, row tracking constant fields (e.g., base row
 *     ID, default row commit version) will be created as nullable in the schema
 * @param nullableRowTrackingGeneratedFields if true, row tracking generated fields will be
 *     created as nullable in the schema
 * @param optimizationsEnabledInput whether to enable optimizations (splits, predicate pushdown)
 * @param tablePathInput table path for deletion vector support
 * @param isCDCReadInput whether this is a CDC read
 * @param useMetadataRowIndex V2: explicit control over _metadata.row_index usage for DV filtering
 */
// Preserve the existing inherited hashCode while converting the equals override.
// scalastyle:off equals.hash.code
@SerialVersionUID(1L)
private[v2] class DeltaParquetFileFormatV2(
    kernelProtocol: KernelProtocol,
    kernelMetadata: KernelMetadata,
    nullableRowTrackingConstantFields: Boolean,
    nullableRowTrackingGeneratedFields: Boolean,
    optimizationsEnabledInput: Boolean,
    tablePathInput: Option[String],
    isCDCReadInput: Boolean,
    useMetadataRowIndex: Option[Boolean])
    extends DeltaParquetFileFormatBase(
      new ProtocolMetadataAdapterV2(kernelProtocol, kernelMetadata),
      nullableRowTrackingConstantFields,
      nullableRowTrackingGeneratedFields,
      optimizationsEnabledInput,
      tablePathInput,
      isCDCReadInput,
      useMetadataRowIndex) {

  override def equals(other: Any): Boolean = {
    if (this eq other.asInstanceOf[AnyRef]) return true
    other match {
      case that: DeltaParquetFileFormatV2 =>
        this.columnMappingMode.equals(that.columnMappingMode) &&
        this.referenceSchema.equals(that.referenceSchema) &&
        this.optimizationsEnabled == that.optimizationsEnabled &&
        this.tablePath.equals(that.tablePath) &&
        this.isCDCRead == that.isCDCRead
      case _ => false
    }
  }
}
// scalastyle:on equals.hash.code
