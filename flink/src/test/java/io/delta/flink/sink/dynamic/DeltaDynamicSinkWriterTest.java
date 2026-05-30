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

package io.delta.flink.sink.dynamic;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.flink.sink.TestSinkWriterContext;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

/** JUnit tests for {@link DeltaDynamicSinkWriter} using stub tables (no Hadoop). */
class DeltaDynamicSinkWriterTest {

  @Test
  void testUnpartitionedWritePassesEmptyPartitionMapToTable() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("msg", StringType.STRING);
    String schemaJson = DataTypeJsonSerDe.serializeStructType(schema);
    ProviderFixture fx = new ProviderFixture(schema);
    DeltaDynamicSinkWriter writer = newWriter(fx.provider());

    writer.write(
        dynamicRow(
            "catalog.db.t1",
            schemaJson,
            new String[0],
            null,
            GenericRowData.of(1, StringData.fromString("a"))),
        new TestSinkWriterContext(0, 0));

    writer.prepareCommit();
    writer.close();
    assertEquals(1, fx.stubFor("catalog.db.t1").partitionMaps.size());
    assertTrue(fx.stubFor("catalog.db.t1").partitionMaps.get(0).isEmpty());
  }

  @Test
  void testPartitionLiteralsFromRowNotRoutingStrings() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    String schemaJson = DataTypeJsonSerDe.serializeStructType(schema);
    ProviderFixture fx = new ProviderFixture(schema);
    DeltaDynamicSinkWriter writer = newWriter(fx.provider());

    writer.write(
        dynamicRow(
            "t",
            schemaJson,
            new String[] {"part"},
            new String[] {"routing-only-should-not-be-used-as-literal"},
            GenericRowData.of(0, StringData.fromString("actual-from-row"))),
        new TestSinkWriterContext(0, 0));

    writer.prepareCommit();
    writer.close();
    Map<String, Literal> map = fx.stubFor("t").partitionMaps.get(0);
    assertEquals(1, map.size());
    assertEquals(Literal.ofString("actual-from-row"), map.get("part"));
  }

  @Test
  void testMultiplePartitionsFlushedAsSeparateWrites() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    String schemaJson = DataTypeJsonSerDe.serializeStructType(schema);
    ProviderFixture fx = new ProviderFixture(schema);
    DeltaDynamicSinkWriter writer = newWriter(fx.provider());

    writer.write(
        dynamicRow(
            "t",
            schemaJson,
            new String[] {"part"},
            new String[] {"east"},
            GenericRowData.of(1, StringData.fromString("east"))),
        new TestSinkWriterContext(10, 10));
    writer.write(
        dynamicRow(
            "t",
            schemaJson,
            new String[] {"part"},
            new String[] {"west"},
            GenericRowData.of(2, StringData.fromString("west"))),
        new TestSinkWriterContext(20, 20));

    writer.prepareCommit();
    writer.close();
    StubDeltaTable stub = fx.stubFor("t");
    assertEquals(2, stub.partitionMaps.size());
    Set<Literal> parts =
        stub.partitionMaps.stream().map(m -> m.get("part")).collect(Collectors.toSet());
    assertEquals(Set.of(Literal.ofString("east"), Literal.ofString("west")), parts);
  }

  @Test
  void testGetOrCreateInvokedOnceForRepeatedWritesSameTable() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    String schemaJson = DataTypeJsonSerDe.serializeStructType(schema);
    ProviderFixture fx = new ProviderFixture(schema);
    DeltaDynamicSinkWriter writer = newWriter(fx.provider());

    writer.write(
        dynamicRow(
            "t",
            schemaJson,
            new String[] {"part"},
            new String[] {"p"},
            GenericRowData.of(1, StringData.fromString("p"))),
        new TestSinkWriterContext(0, 0));
    writer.write(
        dynamicRow(
            "t",
            schemaJson,
            new String[] {"part"},
            new String[] {"p"},
            GenericRowData.of(2, StringData.fromString("p"))),
        new TestSinkWriterContext(1, 1));

    assertEquals(1, fx.creates.get());
    writer.prepareCommit();
    writer.close();
  }

  @Test
  void testGetOrCreateInvokedAgainAfterClose() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    String schemaJson = DataTypeJsonSerDe.serializeStructType(schema);
    ProviderFixture fx = new ProviderFixture(schema);

    DeltaDynamicSinkWriter w1 = newWriter(fx.provider());
    w1.write(
        dynamicRow(
            "t",
            schemaJson,
            new String[] {"part"},
            new String[] {"p"},
            GenericRowData.of(1, StringData.fromString("p"))),
        new TestSinkWriterContext(0, 0));
    w1.prepareCommit();
    w1.close();
    assertEquals(1, fx.creates.get());

    DeltaDynamicSinkWriter w2 = newWriter(fx.provider());
    w2.write(
        dynamicRow(
            "t",
            schemaJson,
            new String[] {"part"},
            new String[] {"p"},
            GenericRowData.of(2, StringData.fromString("p"))),
        new TestSinkWriterContext(0, 0));
    w2.prepareCommit();
    w2.close();
    assertEquals(2, fx.creates.get());
  }

  @Test
  void testPartitionRoutingCountMismatchThrows() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    String schemaJson = DataTypeJsonSerDe.serializeStructType(schema);
    ProviderFixture fx = new ProviderFixture(schema);
    DeltaDynamicSinkWriter writer = newWriter(fx.provider());

    DynamicRow bad =
        dynamicRow(
            "t",
            schemaJson,
            new String[] {"part"},
            new String[0],
            GenericRowData.of(1, StringData.fromString("p")));

    assertThrows(IllegalArgumentException.class, () -> writer.write(bad, dummyCtx()));
    writer.close();
  }

  @Test
  void testWriterKeyEquality() {
    Map<String, String> m1 = new LinkedHashMap<>();
    m1.put("a", "1");
    Map<String, String> m2 = new LinkedHashMap<>();
    m2.put("a", "1");
    DeltaDynamicSinkWriter.WriterKey k1 = new DeltaDynamicSinkWriter.WriterKey("t", m1);
    DeltaDynamicSinkWriter.WriterKey k2 = new DeltaDynamicSinkWriter.WriterKey("t", m2);
    assertEquals(k1, k2);
    assertEquals(k1.hashCode(), k2.hashCode());

    Map<String, String> m3 = new LinkedHashMap<>();
    m3.put("a", "2");
    DeltaDynamicSinkWriter.WriterKey k3 = new DeltaDynamicSinkWriter.WriterKey("t", m3);
    assertNotEquals(k1, k3);
  }

  private static DeltaDynamicSinkWriter newWriter(DeltaTableProvider provider) {
    return new DeltaDynamicSinkWriter.Builder()
        .withJobId("dynamic-writer-test")
        .withSubtaskId(0)
        .withAttemptNumber(0)
        .withTableProvider(provider)
        .withConf(Collections.emptyMap())
        .withMetricGroup(UnregisteredMetricsGroup.createSinkWriterMetricGroup())
        .build();
  }

  private static DynamicRow dynamicRow(
      String tableName,
      String schemaJson,
      String[] partitionColumns,
      String[] routingPartitionValues,
      GenericRowData row) {
    return new DynamicRow() {
      @Override
      public String getTableName() {
        return tableName;
      }

      @Override
      public String getSchemaStr() {
        return schemaJson;
      }

      @Override
      public RowData getRow() {
        return row;
      }

      @Override
      public String[] getPartitionColumns() {
        return partitionColumns;
      }

      @Override
      public String[] getPartitionValues() {
        return routingPartitionValues;
      }
    };
  }

  private static TestSinkWriterContext dummyCtx() {
    return new TestSinkWriterContext(0, 0);
  }
}
