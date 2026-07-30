/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.adapters;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.*;
import org.apache.spark.sql.delta.IdMapping$;
import org.apache.spark.sql.delta.NameMapping$;
import org.apache.spark.sql.delta.NoMapping$;
import org.junit.jupiter.api.Test;
import scala.jdk.javaapi.CollectionConverters;

/** Unit tests for {@link KernelMetadataAdapter} and {@link KernelProtocolAdapter}. */
public class ActionAdaptersTest {

  // ===== KernelProtocolAdapter =====

  @Test
  public void testProtocolAdapterWithTableFeatures() {
    // Reader features: supported but empty (version >= 3 means features are supported, even with
    // an empty set). Writer features: supported and populated.
    Set<String> readerFeatures = Collections.emptySet();
    Set<String> writerFeatures = new HashSet<>(Arrays.asList("v2Checkpoint", "rowTracking"));
    Protocol kernelProtocol = new Protocol(3, 7, readerFeatures, writerFeatures);

    KernelProtocolAdapter adapter = new KernelProtocolAdapter(kernelProtocol);

    assertEquals(3, adapter.minReaderVersion());
    assertEquals(7, adapter.minWriterVersion());
    assertTrue(adapter.readerFeatures().isDefined());
    assertTrue(CollectionConverters.asJava(adapter.readerFeatures().get()).isEmpty());
    assertTrue(adapter.writerFeatures().isDefined());
    assertEquals(
        new HashSet<>(Arrays.asList("v2Checkpoint", "rowTracking")),
        CollectionConverters.asJava(adapter.writerFeatures().get()));
  }

  @Test
  public void testProtocolAdapterLegacyProtocol() {
    Protocol kernelProtocol = new Protocol(1, 2);

    KernelProtocolAdapter adapter = new KernelProtocolAdapter(kernelProtocol);

    assertEquals(1, adapter.minReaderVersion());
    assertEquals(2, adapter.minWriterVersion());
    assertTrue(adapter.readerFeatures().isEmpty());
    assertTrue(adapter.writerFeatures().isEmpty());
  }

  @Test
  public void testProtocolAdapterNullThrows() {
    assertThrows(NullPointerException.class, () -> new KernelProtocolAdapter(null));
  }

  @Test
  public void testProtocolAdapterEqualsByFields() {
    Set<String> rf = new HashSet<>(Arrays.asList("v2Checkpoint"));
    Set<String> wf = new HashSet<>(Arrays.asList("rowTracking"));
    KernelProtocolAdapter base = new KernelProtocolAdapter(new Protocol(3, 7, rf, wf));

    // Identical fields (built from a fresh Protocol instance) compare equal both directions.
    KernelProtocolAdapter same =
        new KernelProtocolAdapter(
            new Protocol(
                3,
                7,
                new HashSet<>(Arrays.asList("v2Checkpoint")),
                new HashSet<>(Arrays.asList("rowTracking"))));
    assertTrue(base.equalsByFields(same));
    assertTrue(same.equalsByFields(base));

    // Each field difference flips the result to false.
    assertFalse(
        base.equalsByFields(new KernelProtocolAdapter(new Protocol(2, 7, rf, wf))),
        "minReaderVersion mismatch should not be equal");
    assertFalse(
        base.equalsByFields(new KernelProtocolAdapter(new Protocol(3, 6, rf, wf))),
        "minWriterVersion mismatch should not be equal");
    assertFalse(
        base.equalsByFields(
            new KernelProtocolAdapter(
                new Protocol(3, 7, new HashSet<>(Arrays.asList("columnMapping")), wf))),
        "readerFeatures mismatch should not be equal");
    assertFalse(
        base.equalsByFields(
            new KernelProtocolAdapter(
                new Protocol(3, 7, rf, new HashSet<>(Arrays.asList("columnMapping"))))),
        "writerFeatures mismatch should not be equal");

    // Some(empty) vs None — features defined only at table-features versions; the helper must
    // distinguish (3,7,Some(empty),Some(empty)) from legacy (1,2,None,None).
    KernelProtocolAdapter emptyFeatures =
        new KernelProtocolAdapter(
            new Protocol(3, 7, Collections.emptySet(), Collections.emptySet()));
    KernelProtocolAdapter legacy = new KernelProtocolAdapter(new Protocol(1, 2));
    assertFalse(emptyFeatures.equalsByFields(legacy));
    assertTrue(legacy.equalsByFields(new KernelProtocolAdapter(new Protocol(1, 2))));
  }

  // ===== KernelMetadataAdapter =====

  @Test
  public void testMetadataAdapter() {
    ArrayValue partCols =
        VectorUtils.buildArrayValue(Arrays.asList("part1", "part2"), StringType.STRING);
    Map<String, String> formatOptions = Collections.singletonMap("foo", "bar");
    Format format = new Format("parquet", formatOptions);
    Map<String, String> configuration = new HashMap<>();
    configuration.put("zip", "zap");
    configuration.put("delta.columnMapping.mode", "name");

    Metadata kernelMetadata =
        new Metadata(
            "id",
            Optional.of("name"),
            Optional.of("description"),
            format,
            "{\"type\":\"struct\",\"fields\":"
                + "[{\"name\":\"part1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"part2\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},"
                + "{\"name\":\"col1\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}",
            new StructType()
                .add("part1", IntegerType.INTEGER)
                .add("part2", StringType.STRING, false /* nullable */)
                .add("col1", StringType.STRING, false /* nullable */),
            partCols,
            Optional.of(42L),
            VectorUtils.stringStringMapValue(configuration));

    KernelMetadataAdapter adapter = new KernelMetadataAdapter(kernelMetadata);

    assertEquals("id", adapter.id());
    assertEquals("name", adapter.name());
    assertEquals("description", adapter.description());
    assertEquals(3, adapter.schema().fields().length);
    assertEquals("integer", adapter.schema().apply("part1").dataType().typeName());
    assertTrue(adapter.schema().apply("part1").nullable());
    assertEquals("string", adapter.schema().apply("part2").dataType().typeName());
    assertFalse(adapter.schema().apply("part2").nullable());
    assertEquals("string", adapter.schema().apply("col1").dataType().typeName());
    assertFalse(adapter.schema().apply("col1").nullable());
    assertEquals(
        Arrays.asList("part1", "part2"), CollectionConverters.asJava(adapter.partitionColumns()));
    org.apache.spark.sql.types.StructType partSchema = adapter.partitionSchema();
    assertEquals(2, partSchema.fields().length);
    assertEquals("part1", partSchema.fields()[0].name());
    assertEquals("integer", partSchema.fields()[0].dataType().typeName());
    assertTrue(partSchema.fields()[0].nullable());
    assertEquals("part2", partSchema.fields()[1].name());
    assertEquals("string", partSchema.fields()[1].dataType().typeName());
    assertFalse(partSchema.fields()[1].nullable());
    assertEquals(configuration, CollectionConverters.asJava(adapter.configuration()));
    assertEquals(NameMapping$.MODULE$, adapter.columnMappingMode());
  }

  @Test
  public void testMetadataAdapterWithNullOptionalFields() {
    ArrayValue emptyPartCols =
        VectorUtils.buildArrayValue(Collections.emptyList(), StringType.STRING);
    Format format = new Format("parquet", Collections.emptyMap());

    Metadata kernelMetadata =
        new Metadata(
            "id2",
            Optional.empty(),
            Optional.empty(),
            format,
            "{\"type\":\"struct\",\"fields\":[]}",
            new StructType(),
            emptyPartCols,
            Optional.empty(),
            VectorUtils.stringStringMapValue(Collections.emptyMap()));

    KernelMetadataAdapter adapter = new KernelMetadataAdapter(kernelMetadata);

    assertEquals("id2", adapter.id());
    assertNull(adapter.name());
    assertNull(adapter.description());
    assertEquals(0, adapter.schema().fields().length);
    assertTrue(CollectionConverters.asJava(adapter.partitionColumns()).isEmpty());
    assertEquals(0, adapter.partitionSchema().fields().length);
    assertTrue(CollectionConverters.asJava(adapter.configuration()).isEmpty());
    assertEquals(NoMapping$.MODULE$, adapter.columnMappingMode());
  }

  @Test
  public void testMetadataAdapterNullThrows() {
    assertThrows(NullPointerException.class, () -> new KernelMetadataAdapter(null));
  }

  @Test
  public void testMetadataAdapterIdColumnMappingMode() {
    KernelMetadataAdapter adapter =
        new KernelMetadataAdapter(
            buildMinimalKernelMetadata(Collections.singletonMap("delta.columnMapping.mode", "id")));

    assertEquals(IdMapping$.MODULE$, adapter.columnMappingMode());
  }

  @Test
  public void testMetadataAdapterUnknownColumnMappingModeThrows() {
    KernelMetadataAdapter adapter =
        new KernelMetadataAdapter(
            buildMinimalKernelMetadata(
                Collections.singletonMap("delta.columnMapping.mode", "bogus")));

    assertThrows(UnsupportedOperationException.class, adapter::columnMappingMode);
  }

  @Test
  public void testMetadataAdapterPartitionColumnCaseMismatch() {
    // Kernel stores partition column names verbatim; a connector may persist a different
    // capitalization than the schema field. The adapter must not silently normalize the case,
    // and the default Spark `StructType.apply` lookup is case-sensitive — exercise both.
    ArrayValue partCols =
        VectorUtils.buildArrayValue(Collections.singletonList("Part1"), StringType.STRING);
    Format format = new Format("parquet", Collections.emptyMap());

    Metadata kernelMetadata =
        new Metadata(
            "id",
            Optional.empty(),
            Optional.empty(),
            format,
            "{\"type\":\"struct\",\"fields\":"
                + "[{\"name\":\"part1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
            new StructType().add("part1", IntegerType.INTEGER),
            partCols,
            Optional.empty(),
            VectorUtils.stringStringMapValue(Collections.emptyMap()));

    KernelMetadataAdapter adapter = new KernelMetadataAdapter(kernelMetadata);

    assertEquals(
        Collections.singletonList("Part1"),
        CollectionConverters.asJava(adapter.partitionColumns()));
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, adapter::partitionSchema);
    assertTrue(
        ex.getMessage().contains("[FIELD_NOT_FOUND]")
            && ex.getMessage().contains("`Part1`")
            && ex.getMessage().contains("`part1`"),
        "Unexpected exception message: " + ex.getMessage());
  }

  private static Metadata buildMinimalKernelMetadata(Map<String, String> configuration) {
    ArrayValue emptyPartCols =
        VectorUtils.buildArrayValue(Collections.emptyList(), StringType.STRING);
    return new Metadata(
        "id",
        Optional.empty(),
        Optional.empty(),
        new Format("parquet", Collections.emptyMap()),
        "{\"type\":\"struct\",\"fields\":[]}",
        new StructType(),
        emptyPartCols,
        Optional.empty(),
        VectorUtils.stringStringMapValue(configuration));
  }
}
