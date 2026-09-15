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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.roaringbitmap.RoaringBitmap;

/** Ported from {@code org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray}. */
public final class RoaringBitmapArray {

  /** Magic number for the Portable serialization format (the only format we read/write). */
  static final int PORTABLE_MAGIC_NUMBER = 1681511377;

  /** The largest value a {@link RoaringBitmapArray} can possibly represent. */
  public static final long MAX_REPRESENTABLE_VALUE =
      composeFromHighLowBytes(Integer.MAX_VALUE - 1, Integer.MIN_VALUE);

  private RoaringBitmap[] bitmaps = new RoaringBitmap[0];

  public static RoaringBitmapArray create(long... values) {
    RoaringBitmapArray bitmap = new RoaringBitmapArray();
    bitmap.addAll(values);
    return bitmap;
  }

  public static RoaringBitmapArray readFrom(byte[] bytes) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    RoaringBitmapArray bitmap = new RoaringBitmapArray();
    bitmap.deserialize(buffer);
    return bitmap;
  }

  public void add(long value) {
    requireRepresentable(value);
    int high = highBytes(value);
    int low = lowBytes(value);
    if (high >= bitmaps.length) {
      extendBitmaps(high + 1);
    }
    bitmaps[high].add(low);
  }

  public void addAll(long... values) {
    for (long v : values) {
      add(v);
    }
  }

  /** Convenience overload for {@link Integer} collections (callers using {@code Set<Integer>}). */
  public void addAll(Collection<Integer> values) {
    for (Integer v : values) {
      add(v.longValue());
    }
  }

  public boolean contains(long value) {
    requireRepresentable(value);
    int high = highBytes(value);
    if (high >= bitmaps.length) {
      return false;
    }
    return bitmaps[high].contains(lowBytes(value));
  }

  public long cardinality() {
    long total = 0L;
    for (RoaringBitmap bitmap : bitmaps) {
      total += bitmap.getLongCardinality();
    }
    return total;
  }

  public boolean isEmpty() {
    for (RoaringBitmap bitmap : bitmaps) {
      if (!bitmap.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  /** In-place union: {@code this |= other}. */
  public void merge(RoaringBitmapArray other) {
    if (this.bitmaps.length < other.bitmaps.length) {
      extendBitmaps(other.bitmaps.length);
    }
    for (int i = 0; i < other.bitmaps.length; i++) {
      this.bitmaps[i].or(other.bitmaps[i]);
    }
  }

  /** Materialize the set as a sorted {@code long[]}. */
  public long[] toArray() {
    long cardinality = cardinality();
    if (cardinality > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Cardinality " + cardinality + " exceeds Integer.MAX_VALUE; cannot materialize toArray");
    }
    long[] values = new long[(int) cardinality];
    int[] cursor = new int[] {0};
    for (int high = 0; high < bitmaps.length; high++) {
      final long composed = ((long) high) << 32;
      bitmaps[high].forEach(
          (org.roaringbitmap.IntConsumer)
              low -> values[cursor[0]++] = composed | toUnsignedLong(low));
    }
    return values;
  }

  /**
   * Serialize this bitmap to a freshly-allocated byte array using the Portable format. Layout:
   *
   * <pre>
   *   [4 LE]  magic = PORTABLE_MAGIC_NUMBER
   *   [8 LE]  number of inner bitmaps
   *   for each inner bitmap (index = high 32-bit key):
   *     [4 LE] key
   *     [..]   RoaringBitmap.serialize output
   * </pre>
   */
  public byte[] serializeAsByteArray() {
    long size = 4L /* magic */ + 8L /* count */;
    for (RoaringBitmap b : bitmaps) {
      size += 4L /* key */ + b.serializedSizeInBytes();
    }
    if (size > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Bitmap too big to serialize into a single byte[] (" + size + " bytes)");
    }
    ByteBuffer buffer = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(PORTABLE_MAGIC_NUMBER);
    buffer.putLong((long) bitmaps.length);
    for (int i = 0; i < bitmaps.length; i++) {
      buffer.putInt(i);
      bitmaps[i].serialize(buffer);
    }
    return buffer.array();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof RoaringBitmapArray)) {
      return false;
    }
    return Arrays.deepEquals(this.bitmaps, ((RoaringBitmapArray) other).bitmaps);
  }

  @Override
  public int hashCode() {
    return 131 * Arrays.deepHashCode(bitmaps);
  }

  // ---------------------------------------------------------------------------------------------
  // Internal: Portable-format deserialization. Native is intentionally not supported -- Spark
  // emits Portable for every DV write, so any file we read came through that path.
  // ---------------------------------------------------------------------------------------------

  private void deserialize(ByteBuffer buffer) throws IOException {
    if (buffer.order() != ByteOrder.LITTLE_ENDIAN) {
      throw new IllegalArgumentException(
          "RoaringBitmapArray must be deserialized using a little-endian buffer");
    }
    int magic = buffer.getInt();
    if (magic != PORTABLE_MAGIC_NUMBER) {
      throw new IOException(
          "Unsupported RoaringBitmapArray magic number "
              + magic
              + " (expected "
              + PORTABLE_MAGIC_NUMBER
              + "); only the Portable format is supported");
    }
    long numberOfBitmaps = buffer.getLong();
    if (numberOfBitmaps < 0L || numberOfBitmaps > Integer.MAX_VALUE) {
      throw new IOException("Invalid RoaringBitmapArray length: " + numberOfBitmaps);
    }
    // Portable allows sparse keys; numberOfBitmaps is only a lower bound on the array size.
    ArrayList<RoaringBitmap> out = new ArrayList<>((int) numberOfBitmaps);
    int lastIndex = 0;
    for (long i = 0; i < numberOfBitmaps; i++) {
      int key = buffer.getInt();
      if (key < 0) {
        throw new IOException("Invalid unsigned entry in RoaringBitmapArray: " + key);
      }
      if (key < lastIndex) {
        throw new IOException(
            "RoaringBitmapArray keys must be ascending; got " + key + " after " + lastIndex);
      }
      while (lastIndex < key) {
        out.add(new RoaringBitmap());
        lastIndex++;
      }
      RoaringBitmap bitmap = new RoaringBitmap();
      bitmap.deserialize(buffer);
      out.add(bitmap);
      lastIndex++;
      // RoaringBitmap.deserialize does not advance the buffer's position.
      buffer.position(buffer.position() + bitmap.serializedSizeInBytes());
    }
    bitmaps = out.toArray(new RoaringBitmap[0]);
  }

  private void extendBitmaps(int newLength) {
    if (bitmaps.length == 0 && newLength == 1) {
      bitmaps = new RoaringBitmap[] {new RoaringBitmap()};
      return;
    }
    RoaringBitmap[] newBitmaps = new RoaringBitmap[newLength];
    System.arraycopy(bitmaps, 0, newBitmaps, 0, bitmaps.length);
    for (int i = bitmaps.length; i < newLength; i++) {
      newBitmaps[i] = new RoaringBitmap();
    }
    bitmaps = newBitmaps;
  }

  private static int highBytes(long value) {
    return (int) (value >> 32);
  }

  private static int lowBytes(long value) {
    return (int) value;
  }

  private static long composeFromHighLowBytes(int high, int low) {
    return (((long) high) << 32) | toUnsignedLong(low);
  }

  private static long toUnsignedLong(int v) {
    return ((long) v) & 0xFFFFFFFFL;
  }

  private static void requireRepresentable(long value) {
    if (value < 0 || value > MAX_REPRESENTABLE_VALUE) {
      throw new IllegalArgumentException(
          "Value out of range: "
              + value
              + " (must be 0 <= v <= MAX_REPRESENTABLE_VALUE="
              + MAX_REPRESENTABLE_VALUE
              + ")");
    }
  }
}
