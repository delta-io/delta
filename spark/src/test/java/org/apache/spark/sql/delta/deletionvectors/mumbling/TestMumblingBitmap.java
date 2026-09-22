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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class TestMumblingBitmap {

  @Test
  public void testEmptyBitmap() {
    MumblingBitmap bitmap = bitmap();
    assertEquals(0, bitmap.cardinality());
    assertFalse(bitmap.isSet(0));
    assertFalse(bitmap.isSet(255));
    assertFalse(bitmap.isSet(256));
  }

  @Test
  public void testInvalidPosition() {
    MumblingBitmap bitmap = bitmap();
    assertEquals(0, bitmap.cardinality());
    assertFalse(bitmap.isSet(0));

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> bitmap.isSet(-1));
    assertEquals("Invalid bit position: -1 < 0", error.getMessage());
  }

  @Test
  public void testEmptySparseContainer() {
    MumblingBitmap bitmap = bitmap(sparse());
    assertEquals(0, bitmap.cardinality());
    assertFalse(bitmap.isSet(0));
    assertFalse(bitmap.isSet(100));
    assertFalse(bitmap.isSet(255));
  }

  @Test
  public void testSparseContainerSetPositions() {
    MumblingBitmap bitmap = bitmap(sparse(0, 5, 100, 255));
    assertEquals(4, bitmap.cardinality());

    assertTrue(bitmap.isSet(0));
    assertTrue(bitmap.isSet(5));
    assertTrue(bitmap.isSet(100));
    assertTrue(bitmap.isSet(255));
    assertFalse(bitmap.isSet(1));
    assertFalse(bitmap.isSet(4));
    assertFalse(bitmap.isSet(6));
    assertFalse(bitmap.isSet(99));
    assertFalse(bitmap.isSet(101));
    assertFalse(bitmap.isSet(254));
    assertFalse(bitmap.isSet(256));
  }

  @Test
  public void testFullSparseContainer() {
    int[] positions = new int[31];
    for (int i = 0; i < positions.length; i += 1) {
      positions[i] = i * 8;
    }

    MumblingBitmap bitmap = bitmap(sparse(positions));
    assertEquals(31, bitmap.cardinality());
    for (int position : positions) {
      assertTrue(bitmap.isSet(position));
    }
    assertFalse(bitmap.isSet(1));
    assertFalse(bitmap.isSet(7));
    assertFalse(bitmap.isSet(255));
  }

  @Test
  public void testFullDenseContainer() {
    byte[] container = new byte[32];
    Arrays.fill(container, (byte) 0xFF);

    MumblingBitmap bitmap = bitmap(dense(container));
    assertEquals(256, bitmap.cardinality());
    for (int i = 0; i < 256; i += 1) {
      assertTrue(bitmap.isSet(i));
    }
    assertFalse(bitmap.isSet(256));
  }

  @Test
  public void testDenseSpecExample1() {
    byte[] container = new byte[32];
    Arrays.fill(container, 0, 4, (byte) 0xFF);
    MumblingBitmap bitmap = bitmap(dense(container));
    assertEquals(32, bitmap.cardinality());

    for (int i = 0; i <= 31; i += 1) {
      assertTrue(bitmap.isSet(i));
    }
    assertFalse(bitmap.isSet(32));
    assertFalse(bitmap.isSet(255));
  }

  @Test
  public void testDenseSpecExample2() {
    byte[] container = new byte[32];
    Arrays.fill(container, 0, 4, (byte) 0xFF);
    container[4] = (byte) 0x80;
    MumblingBitmap bitmap = bitmap(dense(container));
    assertEquals(33, bitmap.cardinality());

    for (int i = 0; i <= 32; i += 1) {
      assertTrue(bitmap.isSet(i));
    }
    assertFalse(bitmap.isSet(33));
    assertFalse(bitmap.isSet(255));
  }

  @Test
  public void testDenseSpecExample3() {
    byte[] container = new byte[32];
    container[0] = (byte) 0xFF;
    container[1] = (byte) 0xFF;
    container[30] = (byte) 0xFF;
    container[31] = (byte) 0xFF;
    MumblingBitmap bitmap = bitmap(dense(container));
    assertEquals(32, bitmap.cardinality());

    for (int i = 0; i <= 15; i += 1) {
      assertTrue(bitmap.isSet(i));
    }
    for (int i = 240; i <= 255; i += 1) {
      assertTrue(bitmap.isSet(i));
    }
    assertFalse(bitmap.isSet(16));
    assertFalse(bitmap.isSet(239));
    assertFalse(bitmap.isSet(256));
  }

  @Test
  public void testDenseSpecExample4() {
    byte[] container = new byte[32];
    Arrays.fill(container, (byte) 0xAA);
    MumblingBitmap bitmap = bitmap(dense(container));
    assertEquals(128, bitmap.cardinality());

    for (int i = 0; i < 256; i += 1) {
      assertEquals(i % 2 == 0, bitmap.isSet(i));
    }
    assertFalse(bitmap.isSet(256));
  }

  @Test
  public void testMultipleContainers() {
    MumblingBitmap bitmap = bitmap(sparse(5), sparse(), sparse(10));
    assertEquals(2, bitmap.cardinality());
    assertTrue(bitmap.isSet(5));
    assertFalse(bitmap.isSet(256));
    assertTrue(bitmap.isSet(522));
    assertFalse(bitmap.isSet(512));
    assertFalse(bitmap.isSet(4));
    assertFalse(bitmap.isSet(265));
    assertFalse(bitmap.isSet(267));
  }

  @Test
  public void testMixedSparseAndDense() {
    byte[] denseContainer = new byte[32];
    Arrays.fill(denseContainer, 0, 4, (byte) 0xFF);
    MumblingBitmap bitmap = bitmap(dense(denseContainer), sparse(1));
    assertEquals(33, bitmap.cardinality());

    for (int i = 0; i < 32; i += 1) {
      assertTrue(bitmap.isSet(i));
    }
    assertFalse(bitmap.isSet(32));
    assertFalse(bitmap.isSet(256));
    assertTrue(bitmap.isSet(257));
    assertFalse(bitmap.isSet(258));
  }

  @Test
  public void testMixedSparseAndDenseWithPFORException() {
    byte[] denseContainer = new byte[32];
    Arrays.fill(denseContainer, 0, 4, (byte) 0xFF);

    MumblingBitmap bitmap =
        bitmap(
            sparse(0),
            sparse(1),
            sparse(2),
            sparse(3),
            sparse(4),
            sparse(5),
            sparse(6),
            dense(denseContainer),
            sparse(8),
            sparse(9),
            sparse(10),
            sparse(11),
            sparse(12),
            sparse(13),
            sparse(14));
    assertEquals(46, bitmap.cardinality());

    for (int i = 0; i < 15; i += 1) {
      if (i != 7) {
        assertTrue(bitmap.isSet(256 * i + i));
      }
    }
    for (int i = 0; i < 32; i += 1) {
      assertTrue(bitmap.isSet(256 * 7 + i));
    }
    assertFalse(bitmap.isSet(256 * 7 - 1));
    assertFalse(bitmap.isSet(256 * 7 + 32));
  }

  @Test
  public void testBufferWithOffset() {
    ByteBuffer buffer = build(sparse(42));
    byte[] rawBytes = new byte[buffer.remaining()];
    buffer.get(rawBytes);

    ByteBuffer padded = ByteBuffer.allocate(4 + rawBytes.length);
    padded.position(4);
    padded.put(rawBytes);
    padded.position(4);

    MumblingBitmap bitmap = new MumblingBitmap(padded);
    assertEquals(1, bitmap.cardinality());
    assertFalse(bitmap.isSet(41));
    assertTrue(bitmap.isSet(42));
    assertFalse(bitmap.isSet(43));
  }

  @Test
  public void testHeadersBeyondMumblingLimitsAreRejected() {
    ByteBuffer excessiveContainers = ByteBuffer.wrap(new byte[] {1, 0, 0, 0, 1, 32});
    ByteBuffer excessiveCardinality = ByteBuffer.wrap(new byte[] {1, 1, 0, 32, 0, 0});

    assertThrows(IllegalStateException.class, () -> new MumblingBitmap(excessiveContainers));
    assertThrows(IllegalStateException.class, () -> new MumblingBitmap(excessiveCardinality));
  }

  @Test
  public void testInvalidDescriptorsAreRejected() {
    byte[] bytes = {1, 0, 0, 0, 1, 0, 0, 0, 33};
    MumblingBitmap bitmap = new MumblingBitmap(ByteBuffer.wrap(bytes));
    IllegalStateException error = assertThrows(IllegalStateException.class, () -> bitmap.isSet(0));
    assertTrue(error.getMessage().contains("Invalid descriptor"));
  }

  private static Container sparse(int... positions) {
    byte[] bytes = new byte[positions.length];
    for (int i = 0; i < positions.length; i += 1) {
      Preconditions.checkArgument(
          positions[i] < 256, "Invalid position in container: %s", positions[i]);
      if (i > 0) {
        Preconditions.checkArgument(
            positions[i] > positions[i - 1],
            "Invalid sparse container: pos %s=%s >= pos %s=%s",
            i - 1,
            positions[i - 1],
            i,
            positions[i]);
      }
      bytes[i] = (byte) positions[i];
    }
    return new Container(bytes);
  }

  private static Container dense(byte[] container) {
    Preconditions.checkArgument(container.length == 32, "Dense container must be 32 bytes");
    return new Container(container);
  }

  private static MumblingBitmap bitmap(Container... containers) {
    return new MumblingBitmap(build(containers));
  }

  private static ByteBuffer build(Container... containers) {
    Preconditions.checkArgument(
        containers.length <= 8192,
        "Invalid container count (max 8192): %s",
        containers.length);

    int[] descriptors = new int[containers.length];
    int cardinality = 0;
    int sizeEstimate = 6;
    for (int i = 0; i < containers.length; i += 1) {
      descriptors[i] = containers[i].descriptor;
      cardinality += containers[i].cardinality;
      sizeEstimate += containers[i].bytes.length;
    }
    Preconditions.checkArgument(
        cardinality <= 2_097_152,
        "Invalid cardinality (max 2,097,152): %s",
        cardinality);

    sizeEstimate += PFOREncoding.estimateEncodedSize(containers.length);
    ByteBuffer buffer = ByteBuffer.allocate(sizeEstimate);
    buffer.put(0, (byte) 1);
    buffer.put(1, (byte) (cardinality & 0xFF));
    buffer.put(2, (byte) ((cardinality >>> 8) & 0xFF));
    buffer.put(3, (byte) ((cardinality >>> 16) & 0xFF));
    buffer.put(4, (byte) (containers.length & 0xFF));
    buffer.put(5, (byte) ((containers.length >>> 8) & 0xFF));

    int descriptorArraySize =
        PFOREncoding.encode(descriptors, 0, buffer, 6, descriptors.length);
    int containerOffset = 6 + descriptorArraySize;
    ByteBuffer containerData = buffer.duplicate();
    containerData.position(containerOffset);
    for (Container container : containers) {
      containerData.put(container.bytes);
    }
    buffer.limit(containerData.position());
    return buffer;
  }

  private static class Container {
    private final byte[] bytes;
    private final int descriptor;
    private final int cardinality;

    private Container(byte[] bytes) {
      this.bytes = bytes;
      this.descriptor = bytes.length;
      this.cardinality = cardinality(bytes);
    }

    private static int cardinality(byte[] bytes) {
      if (bytes.length < 32) {
        return bytes.length;
      } else if (bytes.length == 32) {
        int setBits = 0;
        for (byte value : bytes) {
          setBits += Integer.bitCount(value & 0xFF);
        }
        Preconditions.checkArgument(
            setBits > 31,
            "Invalid dense container: %s values should be sparse",
            setBits);
        return setBits;
      } else {
        throw new IllegalArgumentException("Invalid container: longer than 32 bytes");
      }
    }
  }
}
