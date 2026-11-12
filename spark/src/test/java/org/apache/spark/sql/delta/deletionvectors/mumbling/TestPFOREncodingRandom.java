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

package org.apache.spark.sql.delta.deletionvectors.mumbling;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Randomized round-trip tests for {@link PFOREncoding}. */
public class TestPFOREncodingRandom {
  private static final long SEED = 3546521684L;

  static Stream<Arguments> uniformRandomCases() {
    return Stream.of(
        Arguments.of("1-bit range, count=1", 1, 1),
        Arguments.of("1-bit range, count=7", 7, 1),
        Arguments.of("1-bit range, count=8", 8, 1),
        Arguments.of("1-bit range, count=9", 9, 1),
        Arguments.of("1-bit range, count=256", 256, 1),
        Arguments.of("2-bit range, count=7", 7, 3),
        Arguments.of("2-bit range, count=8", 8, 3),
        Arguments.of("2-bit range, count=9", 9, 3),
        Arguments.of("2-bit range, count=100", 100, 3),
        Arguments.of("2-bit range, count=256", 256, 3),
        Arguments.of("3-bit range, count=7", 7, 7),
        Arguments.of("3-bit range, count=24", 24, 7),
        Arguments.of("3-bit range, count=256", 256, 7),
        Arguments.of("6-bit range, count=7", 7, 32),
        Arguments.of("6-bit range, count=8", 8, 32),
        Arguments.of("6-bit range, count=63", 63, 32),
        Arguments.of("6-bit range, count=64", 64, 32),
        Arguments.of("6-bit range, count=256", 256, 32),
        Arguments.of("full range, count=1", 1, 255),
        Arguments.of("full range, count=7", 7, 255),
        Arguments.of("full range, count=8", 8, 255),
        Arguments.of("full range, count=9", 9, 255),
        Arguments.of("full range, count=15", 15, 255),
        Arguments.of("full range, count=16", 16, 255),
        Arguments.of("full range, count=100", 100, 255),
        Arguments.of("full range, count=255", 255, 255),
        Arguments.of("full range, count=256", 256, 255),
        Arguments.of("full range, count=257", 257, 255),
        Arguments.of("full range, count=512", 512, 255),
        Arguments.of("full range, count=513", 513, 255));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("uniformRandomCases")
  public void testUniformRandom(String name, int count, int maxValue) {
    assertRoundTrip(PFORRandomData.uniform(new Random(SEED + name.hashCode()), count, maxValue));
  }

  static Stream<Arguments> exceptionCases() {
    return Stream.of(
        Arguments.of("Exception 10%, count=7", 7, 0.10f),
        Arguments.of("Exception 10%, count=8", 8, 0.10f),
        Arguments.of("Exception 10%, count=9", 9, 0.10f),
        Arguments.of("Exception 10%, count=100", 100, 0.10f),
        Arguments.of("Exception 10%, count=256", 256, 0.10f),
        Arguments.of("Exception 25%, count=256", 256, 0.25f),
        Arguments.of("Exception 50%, count=256", 256, 0.50f),
        Arguments.of("Exception 10%, count=512", 512, 0.10f));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("exceptionCases")
  public void testRandomWithExceptions(String name, int count, float excPercent) {
    Random random = new Random(SEED + name.hashCode());
    assertRoundTrip(PFORRandomData.exceptions(random, count, excPercent));
  }

  static Stream<Arguments> offsetRangeCases() {
    return Stream.of(
        Arguments.of("offset min=100 rangeSize=3, count=8", 8, 100, 3),
        Arguments.of("offset min=100 rangeSize=3, count=256", 256, 100, 3),
        Arguments.of("offset min=50 rangeSize=31, count=64", 64, 50, 31),
        Arguments.of("offset min=200 rangeSize=55, count=100", 100, 200, 55),
        Arguments.of("offset min=128 rangeSize=127, count=256", 256, 128, 127));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("offsetRangeCases")
  public void testOffsetRandom(String name, int count, int minValue, int rangeSize) {
    Random random = new Random(SEED + name.hashCode());
    int[] values = PFORRandomData.uniform(random, count, rangeSize);
    for (int i = 0; i < values.length; i += 1) {
      values[i] += minValue;
    }

    assertRoundTrip(values);
  }

  @Test
  public void testFullRangeAndChunkBoundaries() {
    Random random = new Random(9876L);
    int[] lengths = {0, 1, 255, 256, 257, 511, 512, 1000};
    for (int length : lengths) {
      assertRoundTrip(PFORRandomData.uniform(random, length, 255));
    }
  }

  @Test
  public void testRandomWidthsBasesAndExceptionPatterns() {
    Random random = new Random(4242L);
    for (int iteration = 0; iteration < 500; iteration += 1) {
      int length = random.nextInt(600);
      int rangeSize = 1 + random.nextInt(256);
      int base = random.nextInt(256 - rangeSize + 1);
      int[] values = PFORRandomData.uniform(random, length, rangeSize - 1);
      for (int i = 0; i < values.length; i += 1) {
        values[i] += base;
      }
      assertRoundTrip(values);
    }
  }

  private static void assertRoundTrip(int[] values) {
    ByteBuffer encoded = PFOREncoding.encode(values, values.length);
    int[] decoded = PFOREncoding.decode(encoded, values.length);
    assertArrayEquals(values, decoded);
  }
}
