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

import static org.junit.Assert.*;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.StructType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.delta.DeltaColumnMappingMode;
import org.junit.Test;

/** Unit tests for KernelDeltaProtocolMetadataWrapper. */
public class KernelDeltaProtocolMetadataWrapperTest {

  @Test
  public void testColumnMappingModeNone() {
    Protocol protocol = new Protocol(1, 2);
    StructType schema = new StructType().add("id", io.delta.kernel.types.IntegerType.INTEGER);
    Map<String, String> config = new HashMap<>();
    Metadata metadata = createMetadata(schema, config);

    KernelDeltaProtocolMetadataWrapper wrapper =
        new KernelDeltaProtocolMetadataWrapper(protocol, metadata);

    assertEquals(DeltaColumnMappingMode.NoMapping(), wrapper.columnMappingMode());
  }

  @Test
  public void testColumnMappingModeId() {
    Protocol protocol = new Protocol(2, 5);
    StructType schema = new StructType().add("id", io.delta.kernel.types.IntegerType.INTEGER);
    Map<String, String> config = new HashMap<>();
    config.put(ColumnMapping.COLUMN_MAPPING_MODE_KEY, "id");
    Metadata metadata = createMetadata(schema, config);

    KernelDeltaProtocolMetadataWrapper wrapper =
        new KernelDeltaProtocolMetadataWrapper(protocol, metadata);

    assertEquals(DeltaColumnMappingMode.IdMapping(), wrapper.columnMappingMode());
  }

  @Test
  public void testColumnMappingModeName() {
    Protocol protocol = new Protocol(2, 5);
    StructType schema = new StructType().add("id", io.delta.kernel.types.IntegerType.INTEGER);
    Map<String, String> config = new HashMap<>();
    config.put(ColumnMapping.COLUMN_MAPPING_MODE_KEY, "name");
    Metadata metadata = createMetadata(schema, config);

    KernelDeltaProtocolMetadataWrapper wrapper =
        new KernelDeltaProtocolMetadataWrapper(protocol, metadata);

    assertEquals(DeltaColumnMappingMode.NameMapping(), wrapper.columnMappingMode());
  }

  @Test
  public void testRowTrackingDisabled() {
    Protocol protocol = new Protocol(1, 2);
    StructType schema = new StructType().add("id", io.delta.kernel.types.IntegerType.INTEGER);
    Metadata metadata = createMetadata(schema, new HashMap<>());

    KernelDeltaProtocolMetadataWrapper wrapper =
        new KernelDeltaProtocolMetadataWrapper(protocol, metadata);

    assertFalse(wrapper.isRowIdEnabled());
  }

  @Test
  public void testRowTrackingEnabled() {
    // Row tracking requires protocol version (1, 7) with rowTracking feature
    Set<String> writerFeatures = new HashSet<>();
    writerFeatures.add("rowTracking");
    Protocol protocol = new Protocol(1, 7, new HashSet<>(), writerFeatures);
    StructType schema = new StructType().add("id", io.delta.kernel.types.IntegerType.INTEGER);
    Metadata metadata = createMetadata(schema, new HashMap<>());

    KernelDeltaProtocolMetadataWrapper wrapper =
        new KernelDeltaProtocolMetadataWrapper(protocol, metadata);

    assertTrue(wrapper.isRowIdEnabled());
  }

  @Test
  public void testDeletionVectorNotReadable() {
    Protocol protocol = new Protocol(1, 2);
    StructType schema = new StructType().add("id", io.delta.kernel.types.IntegerType.INTEGER);
    Metadata metadata = createMetadata(schema, new HashMap<>());

    KernelDeltaProtocolMetadataWrapper wrapper =
        new KernelDeltaProtocolMetadataWrapper(protocol, metadata);

    assertFalse(wrapper.isDeletionVectorReadable());
  }

  @Test
  public void testDeletionVectorReadable() {
    // Deletion vectors requires protocol version (3, 7) with deletionVectors feature
    Set<String> readerFeatures = new HashSet<>();
    readerFeatures.add("deletionVectors");
    Set<String> writerFeatures = new HashSet<>();
    writerFeatures.add("deletionVectors");
    Protocol protocol = new Protocol(3, 7, readerFeatures, writerFeatures);
    StructType schema = new StructType().add("id", io.delta.kernel.types.IntegerType.INTEGER);
    Metadata metadata = createMetadata(schema, new HashMap<>());

    KernelDeltaProtocolMetadataWrapper wrapper =
        new KernelDeltaProtocolMetadataWrapper(protocol, metadata);

    assertTrue(wrapper.isDeletionVectorReadable());
  }

  @Test
  public void testIcebergCompatNotEnabled() {
    Protocol protocol = new Protocol(1, 2);
    StructType schema = new StructType().add("id", io.delta.kernel.types.IntegerType.INTEGER);
    Metadata metadata = createMetadata(schema, new HashMap<>());

    KernelDeltaProtocolMetadataWrapper wrapper =
        new KernelDeltaProtocolMetadataWrapper(protocol, metadata);

    assertFalse(wrapper.isIcebergCompatAnyEnabled());
    assertFalse(wrapper.isIcebergCompatGeqEnabled(2));
  }

  @Test
  public void testIcebergCompatV2Enabled() {
    Set<String> writerFeatures = new HashSet<>();
    writerFeatures.add("icebergCompatV2");
    Protocol protocol = new Protocol(1, 7, new HashSet<>(), writerFeatures);
    StructType schema = new StructType().add("id", io.delta.kernel.types.IntegerType.INTEGER);
    Metadata metadata = createMetadata(schema, new HashMap<>());

    KernelDeltaProtocolMetadataWrapper wrapper =
        new KernelDeltaProtocolMetadataWrapper(protocol, metadata);

    assertTrue(wrapper.isIcebergCompatAnyEnabled());
    assertTrue(wrapper.isIcebergCompatGeqEnabled(2));
    assertFalse(wrapper.isIcebergCompatGeqEnabled(3));
  }

  @Test
  public void testGetReferenceSchema() {
    Protocol protocol = new Protocol(1, 2);
    StructType kernelSchema =
        new StructType()
            .add("id", io.delta.kernel.types.IntegerType.INTEGER)
            .add("name", io.delta.kernel.types.StringType.STRING);
    Metadata metadata = createMetadata(kernelSchema, new HashMap<>());

    KernelDeltaProtocolMetadataWrapper wrapper =
        new KernelDeltaProtocolMetadataWrapper(protocol, metadata);

    org.apache.spark.sql.types.StructType sparkSchema = wrapper.getReferenceSchema();
    assertNotNull(sparkSchema);
    assertEquals(2, sparkSchema.fields().length);
    assertEquals("id", sparkSchema.fields()[0].name());
    assertEquals("name", sparkSchema.fields()[1].name());
  }

  /** Helper method to create a Metadata instance for testing. */
  private Metadata createMetadata(StructType schema, Map<String, String> config) {
    return new Metadata(
        "test-id",
        java.util.Optional.of("test-table"),
        java.util.Optional.of("test description"),
        new io.delta.kernel.internal.actions.Format("parquet", new HashMap<>()),
        schema.toJson(),
        schema,
        io.delta.kernel.internal.util.VectorUtils.buildArrayValue(
            new java.util.ArrayList<>(), io.delta.kernel.types.StringType.STRING),
        java.util.Optional.of(System.currentTimeMillis()),
        io.delta.kernel.internal.util.VectorUtils.stringStringMapValue(config));
  }
}
