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

import io.delta.flink.TestHelper;
import io.delta.flink.sink.WriterResultContext;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** JUnit tests for {@link DeltaDynamicCommittable} and its versioned serializer. */
class DeltaDynamicCommittableTest extends TestHelper {

  @Test
  void testSerializerGetVersionIs2() {
    assertEquals(2, new DeltaDynamicCommittable.Serializer().getVersion());
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

    WriterResultContext ctx = new WriterResultContext(42L, 99L);
    DeltaDynamicCommittable origin =
        new DeltaDynamicCommittable("job-1", "op-1", 100L, "catalog.schema.table", actions, ctx);

    DeltaDynamicCommittable.Serializer serde = new DeltaDynamicCommittable.Serializer();
    DeltaDynamicCommittable deserialized =
        serde.deserialize(serde.getVersion(), serde.serialize(origin));

    assertEquals(origin.getJobId(), deserialized.getJobId());
    assertEquals(origin.getOperatorId(), deserialized.getOperatorId());
    assertEquals(origin.getCheckpointId(), deserialized.getCheckpointId());
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
    DeltaDynamicCommittable origin =
        new DeltaDynamicCommittable(
            "j", "o", 1L, "my.table", Collections.emptyList(), new WriterResultContext());

    DeltaDynamicCommittable.Serializer serde = new DeltaDynamicCommittable.Serializer();
    DeltaDynamicCommittable d = serde.deserialize(serde.getVersion(), serde.serialize(origin));

    assertEquals(0, d.getDeltaActions().size());
    assertEquals("my.table", d.getTableName());
    assertEquals("j", d.getJobId());
    assertEquals("o", d.getOperatorId());
    assertEquals(1L, d.getCheckpointId());
  }
}
