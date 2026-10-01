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
package io.delta.spark.internal.v2.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ScalaUtilsTest {

  @Test
  void testToJavaMap_NullInput_ReturnsNull() {
    assertNull(ScalaUtils.toJavaMap(null), "Null scala maps should return null");
  }

  @Test
  void testToJavaMap_EmptyInput_ReturnsEmptyMap() {
    scala.collection.immutable.Map<String, String> emptyScalaMap =
        ScalaUtils.toScalaMap(Collections.emptyMap());

    Map<String, String> javaMap = ScalaUtils.toJavaMap(emptyScalaMap);

    assertTrue(javaMap.isEmpty(), "Empty scala maps should convert to empty java maps");
  }

  @Test
  void testToJavaMap_PopulatedInput_PreservesEntries() {
    scala.collection.immutable.Map<String, String> scalaMap =
        ScalaUtils.toScalaMap(Map.of("foo", "bar"));

    Map<String, String> javaMap = ScalaUtils.toJavaMap(scalaMap);

    assertEquals(Map.of("foo", "bar"), javaMap, "Scala map entries should be preserved");
  }
}
