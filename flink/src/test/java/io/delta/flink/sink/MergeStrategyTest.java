/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.StringType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** JUnit test suite for {@link MergeStrategy}. */
class MergeStrategyTest {

  @Test
  void testKeyStringDistinguishesNullLiteralFromStringNull() {
    String nullLiteralKey = MergeStrategy.keyString(List.of(Literal.ofNull(StringType.STRING)));
    String stringNullKey = MergeStrategy.keyString(List.of(Literal.ofString("null")));
    assertNotEquals(nullLiteralKey, stringNullKey);

    // Encoding is deterministic for equal inputs.
    assertEquals(
        nullLiteralKey, MergeStrategy.keyString(List.of(Literal.ofNull(StringType.STRING))));
    assertEquals(stringNullKey, MergeStrategy.keyString(List.of(Literal.ofString("null"))));
  }

  @Test
  void testKeyStringRejectsNullLiteral() {
    assertThrows(
        NullPointerException.class,
        () -> MergeStrategy.keyString(Collections.singletonList((Literal) null)));
  }

  @Test
  void testWriterKeyDistinguishesNullLiteralFromStringNull() {
    Map<String, String> nullPartition =
        MergeStrategy.writerKey(Map.of("part", Literal.ofNull(StringType.STRING)));
    Map<String, String> stringNullPartition =
        MergeStrategy.writerKey(Map.of("part", Literal.ofString("null")));
    assertNotEquals(nullPartition, stringNullPartition);
  }
}
