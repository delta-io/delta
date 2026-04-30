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
package io.delta.spark.internal.v2.read;

import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.rowtracking.RowTracking;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.spark.internal.v2.utils.RowTrackingUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.io.Serializable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaColumnMappingMode;
import org.apache.spark.sql.delta.IdMapping$;
import org.apache.spark.sql.delta.NameMapping$;
import org.apache.spark.sql.delta.NoMapping$;
import org.apache.spark.sql.delta.ProtocolMetadataAdapter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Implementation of ProtocolMetadataAdapter for Delta Kernel's Protocol and Metadata.
 *
 * <p>This class adapts Kernel's Protocol and Metadata to the ProtocolMetadataAdapter interface,
 * enabling the V2 connector to reuse delta-spark-v1's DeltaParquetFileFormat for reading Parquet
 * files.
 *
 * <p>Key responsibilities:
 *
 * <ul>
 *   <li>Bridge Kernel's Protocol/Metadata to delta-spark's ProtocolMetadataAdapter interface
 *   <li>Convert column mapping modes between Kernel and delta-spark representations
 *   <li>Provide Delta-aware feature checks (deletion vectors, row tracking, Iceberg compatibility)
 *   <li>Convert schemas between Kernel and Spark formats
 * </ul>
 */
public class ProtocolMetadataAdapterV2 implements ProtocolMetadataAdapter, Serializable {
  private static final long serialVersionUID = 1L;

  private final Protocol protocol;
  private final Metadata metadata;

  public ProtocolMetadataAdapterV2(Protocol protocol, Metadata metadata) {
    this.protocol = protocol;
    this.metadata = metadata;
  }

  @Override
  public DeltaColumnMappingMode columnMappingMode() {
    ColumnMapping.ColumnMappingMode kernelMode =
        ColumnMapping.getColumnMappingMode(metadata.getConfiguration());
    switch (kernelMode) {
      case NONE:
        return NoMapping$.MODULE$;
      case ID:
        return IdMapping$.MODULE$;
      case NAME:
        return NameMapping$.MODULE$;
      default:
        throw new UnsupportedOperationException("Unsupported column mapping mode: " + kernelMode);
    }
  }

  @Override
  public StructType getReferenceSchema() {
    return SchemaUtils.convertKernelSchemaToSparkSchema(metadata.getSchema());
  }

  @Override
  public boolean isRowIdEnabled() {
    return RowTracking.isEnabled(protocol, metadata);
  }

  @Override
  public boolean isDeletionVectorReadable() {
    return protocol.supportsFeature(TableFeatures.DELETION_VECTORS_RW_FEATURE)
        && "parquet".equalsIgnoreCase(metadata.getFormat().getProvider());
  }

  @Override
  public boolean isIcebergCompatAnyEnabled() {
    return TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata)
        || TableConfig.ICEBERG_COMPAT_V3_ENABLED.fromMetadata(metadata);
  }

  @Override
  public boolean isIcebergCompatGeqEnabled(int version) {
    boolean v2Enabled = TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata);
    boolean v3Enabled = TableConfig.ICEBERG_COMPAT_V3_ENABLED.fromMetadata(metadata);
    // IcebergCompatV1 is not supported in Kernel, so V2 is the minimum version for v2 connector
    // until kernel supports IcebergCompatV1.
    // For version 1 or 2, we return true if V2 or V3 is enabled.
    switch (version) {
      case 1:
      case 2:
        return v2Enabled || v3Enabled;
      case 3:
        return v3Enabled;
      default:
        return false;
    }
  }

  @Override
  public void assertTableReadable(SparkSession sparkSession) {
    // TODO(delta-io/delta#5649): Add type widening validation.
  }

  @Override
  public scala.collection.Iterable<StructField> createRowTrackingMetadataFields(
      boolean nullableRowTrackingConstantFields, boolean nullableRowTrackingGeneratedFields) {
    // Use RowTrackingUtils.createMetadataStructFields which handles:
    // - Checking if row tracking is enabled
    // - Creating fields with proper Spark metadata attributes
    // - Handling materialized column names
    return CollectionConverters.asScala(
            RowTrackingUtils.createMetadataStructFields(
                protocol,
                metadata,
                nullableRowTrackingConstantFields,
                nullableRowTrackingGeneratedFields))
        .toSeq();
  }
}
