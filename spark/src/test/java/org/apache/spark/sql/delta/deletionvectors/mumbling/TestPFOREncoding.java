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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class TestPFOREncoding {

  @Test
  public void testExample1AllZeros() {
    int[] values = new int[256];
    Arrays.fill(values, 0);

    ByteBuffer encoded = PFOREncoding.encode(values, values.length);
    assertArrayEquals(bytes(0x00, 0x00, 0x00), toByteArray(encoded));
    assertArrayEquals(values, PFOREncoding.decode(encoded, values.length));
  }

  @Test
  public void testExample2AllFives() {
    int[] values = new int[51];
    Arrays.fill(values, 5);

    ByteBuffer encoded = PFOREncoding.encode(values, values.length);
    assertArrayEquals(bytes(0x00, 0x00, 0x05), toByteArray(encoded));
    assertArrayEquals(values, PFOREncoding.decode(encoded, values.length));
  }

  @Test
  public void testExample3SparseExceptions() {
    int[] values = {0, 0, 0, 0, 0xFF, 0, 0, 0xFE};
    byte[] expected =
        bytes(0x80, 0x02, 0x00, /* offsets */ 0x04, 0x07, /* exceptions */ 0xFF, 0xFE);

    ByteBuffer encoded = PFOREncoding.encode(values, values.length);
    assertArrayEquals(expected, toByteArray(encoded));
    assertArrayEquals(values, PFOREncoding.decode(encoded, values.length));
  }

  @Test
  public void testExample4TwoBitsNoExceptions() {
    int[] values = {6, 7, 8};
    byte[] expected = bytes(0x02, 0x00, 0x06, 0x18);

    ByteBuffer encoded = PFOREncoding.encode(values, values.length);
    assertArrayEquals(expected, toByteArray(encoded));
    assertArrayEquals(values, PFOREncoding.decode(encoded, values.length));
  }

  @Test
  public void testExample5() {
    int[] values = {6, 34, 8, 7};
    byte[] expected = bytes(0x05, 0x00, 0x06, 0x07, 0x04, 0x10);
    byte[] fromSpec = bytes(0x32, 0x01, 0x06, 0x09, 0x01, 0xE0);

    ByteBuffer encoded = PFOREncoding.encode(values, values.length);
    assertArrayEquals(expected, toByteArray(encoded));
    assertArrayEquals(values, PFOREncoding.decode(encoded, values.length));
    assertArrayEquals(values, PFOREncoding.decode(ByteBuffer.wrap(fromSpec), values.length));
  }

  @Test
  public void testEncodeWithValueOffset() {
    int[] values = new int[400];
    for (int i = 0; i < values.length; i += 1) {
      values[i] = i % 251;
    }

    int valueOffset = 5;
    int count = 260;
    ByteBuffer out = ByteBuffer.allocate(PFOREncoding.estimateEncodedSize(count));
    int bytesWritten = PFOREncoding.encode(values, valueOffset, out, 0, count);

    int[] decoded = new int[count];
    int bytesRead = PFOREncoding.decode(out, 0, decoded, 0, count);

    assertEquals(bytesWritten, bytesRead);
    assertArrayEquals(Arrays.copyOfRange(values, valueOffset, valueOffset + count), decoded);
  }

  @Test
  public void testFullByteRangeUsesRawRepresentation() {
    int[] values = new int[256];
    for (int i = 0; i < values.length; i += 1) {
      values[i] = i;
    }

    ByteBuffer encoded = PFOREncoding.encode(values, values.length);
    byte[] actual = toByteArray(encoded);
    assertEquals(259, actual.length);
    assertArrayEquals(bytes(0x08, 0x00, 0x00), Arrays.copyOfRange(actual, 0, 3));
    for (int i = 0; i < values.length; i += 1) {
      assertEquals((byte) values[i], actual[i + 3]);
    }
    assertArrayEquals(values, PFOREncoding.decode(ByteBuffer.wrap(actual), values.length));
  }

  @Test
  public void testByteBufferOffsetsDoNotMovePositionOrLimit() {
    int[] values = {3, 3, 4, 7, 8, 129, 130};
    ByteBuffer buffer = ByteBuffer.allocate(64);
    buffer.position(7);
    buffer.limit(61);

    int positionBeforeEncode = buffer.position();
    int limitBeforeEncode = buffer.limit();
    int bytesWritten = PFOREncoding.encode(values, 0, buffer, 5, values.length);
    assertEquals(positionBeforeEncode, buffer.position());
    assertEquals(limitBeforeEncode, buffer.limit());

    int[] decoded = new int[values.length];
    int positionBeforeDecode = buffer.position();
    int limitBeforeDecode = buffer.limit();
    int bytesRead = PFOREncoding.decode(buffer, 5, decoded, 0, values.length);
    assertEquals(bytesWritten, bytesRead);
    assertArrayEquals(values, decoded);
    assertEquals(positionBeforeDecode, buffer.position());
    assertEquals(limitBeforeDecode, buffer.limit());
  }

  private static byte[] toByteArray(ByteBuffer buffer) {
    ByteBuffer copy = buffer.duplicate();
    byte[] result = new byte[copy.remaining()];
    copy.get(result);
    return result;
  }

  private static byte[] bytes(int... values) {
    byte[] result = new byte[values.length];
    for (int i = 0; i < values.length; i += 1) {
      result[i] = (byte) values[i];
    }
    return result;
  }
}
