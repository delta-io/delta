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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.flink.TestHelper;
import io.delta.flink.sink.WriterResultContext;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** JUnit tests for {@link DeltaDynamicWriterResult} and its versioned serializer. */
class DeltaDynamicWriterResultTest extends TestHelper {

  @Test
  void testSerializerGetVersionIs2() {
    assertEquals(2, new DeltaDynamicWriterResult.Serializer().getVersion());
  }

  @Test
  void testSerializerRoundTripPreservesFieldsContextAndActions() throws IOException {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

    List<Row> actions =
        IntStream.rangeClosed(1, 3)
            .mapToObj(
                value -> dummyAddFileRow(schema, 10, Map.of("part", Literal.ofString("p" + value))))
            .collect(Collectors.toList());

    WriterResultContext ctx = new WriterResultContext(7L, 77L);
    DeltaDynamicWriterResult origin =
        new DeltaDynamicWriterResult("warehouse.db.events", actions, ctx);

    DeltaDynamicWriterResult.Serializer serde = new DeltaDynamicWriterResult.Serializer();
    DeltaDynamicWriterResult deserialized =
        serde.deserialize(serde.getVersion(), serde.serialize(origin));

    assertEquals(origin.getTableName(), deserialized.getTableName());
    assertEquals(
        origin.getContext().getLowWatermark(), deserialized.getContext().getLowWatermark());
    assertEquals(
        origin.getContext().getHighWatermark(), deserialized.getContext().getHighWatermark());

    List<String> expectedJson =
        actions.stream().map(JsonUtils::rowToJson).collect(Collectors.toList());
    List<String> actualJson =
        deserialized.getDeltaActions().stream()
            .map(JsonUtils::rowToJson)
            .collect(Collectors.toList());
    assertEquals(expectedJson, actualJson);
  }

  @Test
  void testSerializerRoundTripEmptyActions() throws IOException {
    DeltaDynamicWriterResult origin =
        new DeltaDynamicWriterResult("t", Collections.emptyList(), new WriterResultContext());

    DeltaDynamicWriterResult.Serializer serde = new DeltaDynamicWriterResult.Serializer();
    DeltaDynamicWriterResult d = serde.deserialize(serde.getVersion(), serde.serialize(origin));

    assertEquals(0, d.getDeltaActions().size());
    assertEquals("t", d.getTableName());
  }

  @Test
  void testMergeCombinesActionsAndMergesWatermarksForSameTable() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    Row action1 = dummyAddFileRow(schema, 1, Map.of());
    Row action2 = dummyAddFileRow(schema, 2, Map.of());

    DeltaDynamicWriterResult first =
        new DeltaDynamicWriterResult(
            "same.table", new ArrayList<>(List.of(action1)), new WriterResultContext(100L, 200L));
    DeltaDynamicWriterResult second =
        new DeltaDynamicWriterResult(
            "same.table", new ArrayList<>(List.of(action2)), new WriterResultContext(50L, 300L));

    DeltaDynamicWriterResult merged = first.merge(second);

    assertEquals(merged, first);
    assertEquals(2, first.getDeltaActions().size());
    assertEquals(50L, first.getContext().getLowWatermark());
    assertEquals(300L, first.getContext().getHighWatermark());

    List<String> json =
        first.getDeltaActions().stream().map(JsonUtils::rowToJson).collect(Collectors.toList());
    assertEquals(List.of(JsonUtils.rowToJson(action1), JsonUtils.rowToJson(action2)), json);
  }

  @Test
  void testMergeThrowsWhenTableNamesDiffer() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    Row action = dummyAddFileRow(schema, 1, Map.of());

    DeltaDynamicWriterResult a =
        new DeltaDynamicWriterResult(
            "t1", new ArrayList<>(List.of(action)), new WriterResultContext());
    DeltaDynamicWriterResult b =
        new DeltaDynamicWriterResult(
            "t2", new ArrayList<>(List.of(action)), new WriterResultContext());

    assertThrows(IllegalArgumentException.class, () -> a.merge(b));
  }
}
