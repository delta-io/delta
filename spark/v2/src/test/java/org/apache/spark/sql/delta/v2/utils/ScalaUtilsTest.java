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
package org.apache.spark.sql.delta.v2.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import org.apache.spark.sql.delta.storage.ClosableIterator.IteratorCloseOps;
import org.junit.jupiter.api.Test;
import scala.Option;

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

  @Test
  void testToJavaOptionalLong() {
    long value = 1234567890123L;

    assertEquals(
        OptionalLong.of(value), ScalaUtils.toJavaOptionalLong(Option.apply((Object) value)));
    assertEquals(OptionalLong.empty(), ScalaUtils.toJavaOptionalLong(Option.empty()));
  }

  @Test
  void testWithClosePreservesJavaIteratorCloseAfterEarlyStop() throws IOException {
    AtomicInteger closes = new AtomicInteger();
    ClosableIterator<Integer> input =
        ScalaUtils.toClosableIterator(new TrackingJavaIterator(List.of(1, 2, 3), closes));
    try (ClosableIterator<Integer> limited =
        new IteratorCloseOps<>(input)
            .withClose(iterator -> iterator.takeWhile(value -> value < 2))) {
      assertEquals(1, limited.next());
      assertEquals(0, closes.get());
    }
    assertEquals(1, closes.get());
  }

  private static class TrackingJavaIterator implements java.util.Iterator<Integer>, Closeable {
    private final java.util.Iterator<Integer> values;
    private final AtomicInteger closes;

    TrackingJavaIterator(List<Integer> values, AtomicInteger closes) {
      this.values = values.iterator();
      this.closes = closes;
    }

    @Override
    public boolean hasNext() {
      return values.hasNext();
    }

    @Override
    public Integer next() {
      return values.next();
    }

    @Override
    public void close() {
      closes.incrementAndGet();
    }
  }
}
