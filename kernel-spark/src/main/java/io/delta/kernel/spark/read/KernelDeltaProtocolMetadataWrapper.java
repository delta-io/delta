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
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.icebergcompat.IcebergCompatMetadataValidatorAndUpdater;
import io.delta.kernel.internal.rowtracking.RowTracking;
import io.delta.kernel.internal.types.TypeWideningChecker;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.spark.utils.SchemaUtils;
import io.delta.kernel.types.StructField;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.*;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Implementation of DeltaProtocolMetadataWrapper for Kernel's Protocol and Metadata.
 *
 * <p>This class adapts Kernel's Protocol and Metadata to the DeltaProtocolMetadataWrapper
 * interface, enabling kernel-spark to reuse DeltaParquetFileFormat from delta-spark.
 */
public class KernelDeltaProtocolMetadataWrapper
    implements DeltaProtocolMetadataWrapper, Serializable {
  private static final long serialVersionUID = 1L;

  private final Protocol protocol;
  private final Metadata metadata;

  public KernelDeltaProtocolMetadataWrapper(Protocol protocol, Metadata metadata) {
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
    // Note: Delta-Spark V1 calls RowId.isEnabled(protocol, metadata), but in Kernel
    // RowId functionality is consolidated in RowTracking class. Both have identical logic.
    return RowTracking.isEnabled(protocol, metadata);
  }

  @Override
  public boolean isDeletionVectorReadable() {
    return DeletionVectorUtils.isReadable(protocol, metadata);
  }

  @Override
  public boolean isIcebergCompatAnyEnabled() {
    return IcebergCompatMetadataValidatorAndUpdater.isAnyVersionEnabled(protocol, metadata);
  }

  @Override
  public boolean isIcebergCompatGeqEnabled(int version) {
    return IcebergCompatMetadataValidatorAndUpdater.isVersionGeqEnabled(
        protocol, metadata, version);
  }

  @Override
  public void assertTableReadable(SparkSession sparkSession) {
    // Check if bypass configuration is enabled (SQLConf check done in wrapper)
    // Note: Using getConfString instead of getConf due to Scala/Java interop type erasure issues
    org.apache.spark.sql.internal.SQLConf sqlConf = sparkSession.sessionState().conf();
    boolean bypass =
        Boolean.parseBoolean(
            sqlConf.getConfString(
                DeltaSQLConf.DELTA_TYPE_WIDENING_BYPASS_UNSUPPORTED_TYPE_CHANGE_CHECK().key(),
                "false"));
    if (bypass) {
      return;
    }

    // Delegate to Kernel's type widening validation logic
    TypeWideningChecker.assertTableReadable(protocol, metadata);
  }

  @Override
  public Seq<org.apache.spark.sql.types.StructField> createRowTrackingMetadataFields(
      boolean nullableRowTrackingConstantFields, boolean nullableRowTrackingGeneratedFields) {
    // Get Kernel's StructFields and convert to Spark's StructFields
    List<StructField> kernelFields =
        RowTracking.createMetadataStructFields(
            protocol,
            metadata,
            nullableRowTrackingConstantFields,
            nullableRowTrackingGeneratedFields);

    List<org.apache.spark.sql.types.StructField> sparkFields =
        kernelFields.stream()
            .map(
                field ->
                    new org.apache.spark.sql.types.StructField(
                        field.getName(),
                        SchemaUtils.convertKernelDataTypeToSparkDataType(field.getDataType()),
                        field.isNullable(),
                        org.apache.spark.sql.types.Metadata.empty()))
            .collect(Collectors.toList());

    return JavaConverters.asScalaBuffer(sparkFields).toSeq();
  }
}
