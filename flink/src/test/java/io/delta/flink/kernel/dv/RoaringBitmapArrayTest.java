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

package io.delta.flink.kernel.dv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Tests for {@link RoaringBitmapArray}. */
class RoaringBitmapArrayTest {

  @Test
  void testEmptyBitmap() {
    RoaringBitmapArray rb = new RoaringBitmapArray();
    assertTrue(rb.isEmpty());
    assertEquals(0L, rb.cardinality());
    assertArrayEquals(new long[0], rb.toArray());
    assertFalse(rb.contains(0L));
  }

  @Test
  void testAddAndContains() {
    RoaringBitmapArray rb = RoaringBitmapArray.create(1L, 5L, 100L, (1L << 32) + 7L);
    assertTrue(rb.contains(1L));
    assertTrue(rb.contains(5L));
    assertTrue(rb.contains(100L));
    assertTrue(rb.contains((1L << 32) + 7L));
    assertFalse(rb.contains(0L));
    assertFalse(rb.contains((1L << 32))); // adjacent but absent
    assertEquals(4L, rb.cardinality());
    assertFalse(rb.isEmpty());
  }

  @Test
  void testAddRejectsNegative() {
    RoaringBitmapArray rb = new RoaringBitmapArray();
    assertThrows(IllegalArgumentException.class, () -> rb.add(-1L));
  }

  @Test
  void testToArrayIsSortedAscending() {
    RoaringBitmapArray rb =
        RoaringBitmapArray.create(
            (5L << 32) + 1L, 0L, (1L << 32) + 13L, 7L, (100L << 32) + 999_999L, (5L << 32));
    assertArrayEquals(
        new long[] {0L, 7L, (1L << 32) + 13L, (5L << 32), (5L << 32) + 1L, (100L << 32) + 999_999L},
        rb.toArray());
  }

  @Test
  void testMergeIsUnion() {
    RoaringBitmapArray a = RoaringBitmapArray.create(1L, 2L, 3L);
    RoaringBitmapArray b = RoaringBitmapArray.create(2L, 3L, 4L);
    a.merge(b);
    assertArrayEquals(new long[] {1L, 2L, 3L, 4L}, a.toArray());
  }

  @Test
  void testMergeExtendsHighKeys() {
    RoaringBitmapArray a = RoaringBitmapArray.create(1L);
    RoaringBitmapArray b = RoaringBitmapArray.create((3L << 32) + 5L);
    a.merge(b);
    assertArrayEquals(new long[] {1L, (3L << 32) + 5L}, a.toArray());
  }

  @Test
  void testEquality() {
    RoaringBitmapArray a = RoaringBitmapArray.create(1L, 5L, 100L);
    RoaringBitmapArray b = RoaringBitmapArray.create(100L, 1L, 5L);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());

    RoaringBitmapArray c = RoaringBitmapArray.create(1L, 5L, 101L);
    assertNotEquals(a, c);
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    RoaringBitmapArray original =
        RoaringBitmapArray.create(
            0L, 7L, (1L << 32) + 13L, (5L << 32), (5L << 32) + 1L, (100L << 32) + 999_999L);
    byte[] payload = original.serializeAsByteArray();
    RoaringBitmapArray restored = RoaringBitmapArray.readFrom(payload);
    assertArrayEquals(original.toArray(), restored.toArray());
    assertEquals(original, restored);
  }

  @Test
  void testEmptyBitmapRoundTrip() throws Exception {
    RoaringBitmapArray original = new RoaringBitmapArray();
    RoaringBitmapArray restored = RoaringBitmapArray.readFrom(original.serializeAsByteArray());
    assertEquals(original, restored);
    assertTrue(restored.isEmpty());
  }

  @Test
  void testPortableMagicNumberMatchesSpec() {
    // Locked in against accidental drift -- delta-spark and delta-kernel both hard-code this.
    assertEquals(1681511377, RoaringBitmapArray.PORTABLE_MAGIC_NUMBER);
  }
}
