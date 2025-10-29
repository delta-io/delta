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

import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.icebergcompat.IcebergCompatMetadataValidatorAndUpdater;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.spark.utils.SchemaUtils;
import io.delta.kernel.spark.utils.SerializableKernelRowWrapper;
import org.apache.spark.sql.delta.DeltaColumnMappingMode;
import org.apache.spark.sql.delta.DeltaWrapper;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterable;

/**
 * Implementation of DeltaWrapper that uses Kernel's Protocol and Metadata.
 *
 * <p>This allows kernel-spark connector to use DeltaParquetFileFormatBase without depending on
 * spark's actions.Protocol/Metadata classes.
 *
 * <p>Uses SerializableKernelRowWrapper to wrap Protocol and Metadata for serialization support,
 * enabling DeltaParquetFileFormat to be distributed to Spark executors.
 *
 * <p>Note: This is a work-in-progress implementation. Some advanced features (Row Tracking, Type
 * Widening) require more Spark-Kernel integration work.
 */
public class KernelDeltaWrapper implements DeltaWrapper {
  private final SerializableKernelRowWrapper protocolWrapper;
  private final SerializableKernelRowWrapper metadataWrapper;

  public KernelDeltaWrapper(Protocol protocol, Metadata metadata) {
    this.protocolWrapper = new SerializableKernelRowWrapper(protocol.toRow());
    this.metadataWrapper = new SerializableKernelRowWrapper(metadata.toRow());
  }

  private Protocol getProtocol() {
    return Protocol.fromRow(protocolWrapper.getRow());
  }

  private Metadata getMetadata() {
    return Metadata.fromRow(metadataWrapper.getRow());
  }

  @Override
  public DeltaColumnMappingMode columnMappingMode() {
    // Convert Kernel's ColumnMappingMode to Spark's DeltaColumnMappingMode
    ColumnMapping.ColumnMappingMode kernelMode =
        ColumnMapping.getColumnMappingMode(getMetadata().getConfiguration());
    return org.apache.spark.sql.delta.DeltaColumnMappingMode.apply(kernelMode.toString());
  }

  @Override
  public StructType getReferenceSchema() {
    // Convert Kernel StructType to Spark StructType
    return SchemaUtils.convertKernelSchemaToSparkSchema(getMetadata().getSchema());
  }

  @Override
  public boolean isRowIdEnabled() {
    return TableConfig.ROW_TRACKING_ENABLED.fromMetadata(getMetadata());
  }

  @Override
  public boolean isDeletionVectorReadable() {
    return TableConfig.DELETION_VECTORS_CREATION_ENABLED.fromMetadata(getMetadata());
  }

  @Override
  public boolean isIcebergCompatEnabled() {
    return IcebergCompatMetadataValidatorAndUpdater.isIcebergCompatEnabled(getMetadata());
  }

  @Override
  public boolean isIcebergCompatV2OrAbove() {
    // Check if icebergCompatV2 or above is enabled
    Protocol protocol = getProtocol();
    return protocol.supportsFeature(TableFeatures.ICEBERG_COMPAT_V2_W_FEATURE)
        || protocol.supportsFeature(TableFeatures.ICEBERG_COMPAT_V3_W_FEATURE)
        || protocol.supportsFeature(TableFeatures.ICEBERG_WRITER_COMPAT_V3);
  }

  @Override
  public void validateTableReadableForTypeWidening(SQLConf conf) {
    // TODO: Implement type widening support in kernel-spark when available
  }

  @Override
  public Iterable<StructField> createRowTrackingMetadataFields(
      boolean nullableConstantFields, boolean nullableGeneratedFields) {
    // TODO: Implement full row tracking support in kernel-spark
    throw new UnsupportedOperationException("Row tracking is not supported yet");
  }
}
