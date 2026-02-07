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

package io.delta.flink.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.flink.TestHelper;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.types.*;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for DeltaCommittable serialization and deserialization. */
class DeltaCommittableTest extends TestHelper {

  @Test
  void testSerializeAndDeserialize() throws IOException {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

    List<Row> actions =
        IntStream.rangeClosed(1, 10)
            .mapToObj(
                value ->
                    dummyAddFileRow(
                        schema,
                        10,
                        Map.of("part", io.delta.kernel.expressions.Literal.ofInt(value))))
            .collect(Collectors.toList());

    DeltaCommittable origin =
        new DeltaCommittable("jobId", "operatorId", 100, actions, new WriterResultContext());

    DeltaCommittable.Serializer serde = new DeltaCommittable.Serializer();
    DeltaCommittable deserialized = serde.deserialize(1, serde.serialize(origin));

    assertEquals(origin.getJobId(), deserialized.getJobId());
    assertEquals(origin.getOperatorId(), deserialized.getOperatorId());
    assertEquals(origin.getCheckpointId(), deserialized.getCheckpointId());

    List<String> expectedJson =
        actions.stream().map(JsonUtils::rowToJson).collect(Collectors.toList());
    List<String> actualJson =
        deserialized.getDeltaActions().stream()
            .map(JsonUtils::rowToJson)
            .collect(Collectors.toList());

    assertEquals(expectedJson, actualJson);
  }
}
