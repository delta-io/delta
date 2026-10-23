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
package org.apache.spark.sql.delta.v2.read

import scala.collection.JavaConverters.asScalaBuffer

import org.apache.spark.sql.delta.{
  DeltaColumnMappingMode,
  IdMapping,
  NameMapping,
  NoMapping,
  ProtocolMetadataAdapter
}
import org.apache.spark.sql.delta.v2.utils.{RowTrackingUtils, SchemaUtils}
import io.delta.kernel.internal.{TableConfig => KernelTableConfig}
import io.delta.kernel.internal.actions.{
  Metadata => KernelMetadata,
  Protocol => KernelProtocol
}
import io.delta.kernel.internal.rowtracking.{RowTracking => KernelRowTracking}
import io.delta.kernel.internal.tablefeatures.{
  TableFeatures => KernelTableFeatures
}
import io.delta.kernel.internal.util.{ColumnMapping => KernelColumnMapping}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Implementation of ProtocolMetadataAdapter for Delta Kernel's Protocol and Metadata.
 *
 * This class adapts Kernel's Protocol and Metadata to the ProtocolMetadataAdapter interface,
 * enabling the V2 connector to reuse delta-spark-v1's DeltaParquetFileFormat for reading Parquet
 * files.
 *
 * Key responsibilities:
 *  - Bridge Kernel's Protocol/Metadata to delta-spark's ProtocolMetadataAdapter interface
 *  - Convert column mapping modes between Kernel and delta-spark representations
 *  - Provide Delta-aware feature checks (deletion vectors, row tracking, Iceberg compatibility)
 *  - Convert schemas between Kernel and Spark formats
 */
private[delta] case class ProtocolMetadataAdapterV2(
    private val kernelProtocol: KernelProtocol,
    private val kernelMetadata: KernelMetadata)
    extends ProtocolMetadataAdapter {

  override def columnMappingMode: DeltaColumnMappingMode = {
    val kernelMode = KernelColumnMapping.getColumnMappingMode(kernelMetadata.getConfiguration)
    kernelMode match {
      case KernelColumnMapping.ColumnMappingMode.NONE => NoMapping
      case KernelColumnMapping.ColumnMappingMode.ID => IdMapping
      case KernelColumnMapping.ColumnMappingMode.NAME => NameMapping
      case _ =>
        throw new UnsupportedOperationException("Unsupported column mapping mode: " + kernelMode)
    }
  }

  override def getReferenceSchema: StructType = {
    SchemaUtils.convertKernelSchemaToSparkSchema(kernelMetadata.getSchema)
  }

  override def isRowIdEnabled: Boolean = KernelRowTracking.isEnabled(kernelProtocol, kernelMetadata)

  override def isDeletionVectorReadable: Boolean = {
    kernelProtocol.supportsFeature(KernelTableFeatures.DELETION_VECTORS_RW_FEATURE) &&
    "parquet".equalsIgnoreCase(kernelMetadata.getFormat.getProvider)
  }

  override def isIcebergCompatAnyEnabled: Boolean = {
    KernelTableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(kernelMetadata).booleanValue() ||
    KernelTableConfig.ICEBERG_COMPAT_V3_ENABLED.fromMetadata(kernelMetadata).booleanValue()
  }

  override def isIcebergCompatGeqEnabled(version: Int): Boolean = {
    val v2Enabled =
      KernelTableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(kernelMetadata).booleanValue()
    val v3Enabled =
      KernelTableConfig.ICEBERG_COMPAT_V3_ENABLED.fromMetadata(kernelMetadata).booleanValue()
    // IcebergCompatV1 is not supported in Kernel, so V2 is the minimum version for v2 connector
    // until kernel supports IcebergCompatV1.
    // For version 1 or 2, we return true if V2 or V3 is enabled.
    version match {
      case 1 | 2 => v2Enabled || v3Enabled
      case 3 => v3Enabled
      case _ => false
    }
  }

  override def assertTableReadable(sparkSession: SparkSession): Unit = {
    // TODO(delta-io/delta#5649): Add type widening validation.
  }


  override def createRowTrackingMetadataFields(
      nullableRowTrackingConstantFields: Boolean,
      nullableRowTrackingGeneratedFields: Boolean): Iterable[StructField] = {
    // Use RowTrackingUtils.createMetadataStructFields which handles:
    // - Checking if row tracking is enabled
    // - Creating fields with proper Spark metadata attributes
    // - Handling materialized column names
    asScalaBuffer(
      RowTrackingUtils.createMetadataStructFields(
        /* protocol = */ kernelProtocol,
        /* metadata = */ kernelMetadata,
        /* nullableConstantFields = */ nullableRowTrackingConstantFields,
        /* nullableGeneratedFields = */ nullableRowTrackingGeneratedFields
      )
    ).toSeq
  }
}
