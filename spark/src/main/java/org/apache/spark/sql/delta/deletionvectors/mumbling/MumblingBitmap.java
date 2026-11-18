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

import java.nio.ByteBuffer;

import org.apache.spark.sql.delta.deletionvectors.MutableMumblingBitmap;
import com.google.common.base.Preconditions;

/**
 * Immutable Mumbling compressed bitmap read from a {@link ByteBuffer}.
 *
 * <p>The supplied bytes are copied, but descriptor decoding remains lazy. On the first call to
 * {@link #isSet}, the PFOR-encoded descriptor array is decoded and used to build an offsets array
 * that maps each container index to its absolute byte position in the buffer. This offsets array is
 * the only derived state kept by this class.
 *
 * <p>Format (all integers unsigned, little-endian):
 *
 * <ul>
 *   <li>Header (6 bytes): version (1), cardinality (3), container count (2)
 *   <li>Descriptor array: PFOR-encoded, one byte per container
 *   <li>Containers: concatenated sparse (0-31 bytes) or dense (32 bytes) containers
 * </ul>
 */
public final class MumblingBitmap {
  private final ByteBuffer data;
  private final int cardinality;
  private final int containerCount;
  private volatile int[] descriptors = null;
  private volatile int[] offsets = null;

  public MumblingBitmap(ByteBuffer data) {
    this(copyByteBuffer(data));
  }

  MumblingBitmap(byte[] data) {
    this.data = ByteBuffer.wrap(data).asReadOnlyBuffer();

    int version = this.data.get(0) & 0xFF;
    if (version != MumblingFormat.VERSION) {
      throw new UnsupportedOperationException("Unsupported Mumbling bitmap version: " + version);
    }

    this.cardinality =
        (this.data.get(1) & 0xFF)
            | ((this.data.get(2) & 0xFF) << 8)
            | ((this.data.get(3) & 0xFF) << 16);
    this.containerCount =
        (this.data.get(4) & 0xFF) | ((this.data.get(5) & 0xFF) << 8);

    Preconditions.checkState(
        containerCount <= MumblingFormat.MAX_CONTAINERS,
        "Invalid container count: %s > %s (max)",
        containerCount,
        MumblingFormat.MAX_CONTAINERS);
    Preconditions.checkState(
        cardinality <= MumblingFormat.MAX_POSITION_EXCLUSIVE,
        "Invalid cardinality: %s > %s (max)",
        cardinality,
        MumblingFormat.MAX_POSITION_EXCLUSIVE);
  }

  public ByteBuffer buffer() {
    return data.asReadOnlyBuffer();
  }

  /** Returns an independent mutable copy of this bitmap. */
  public MutableMumblingBitmap toMutable() {
    return new MutableMumblingBitmap(this);
  }

  /** Returns the number of bits set in the bitmap. */
  public int cardinality() {
    return cardinality;
  }

  /**
   * Returns {@code true} if the bit at {@code pos} is set in the bitmap.
   *
   * <p>Positions beyond the range of any container are always unset.
   */
  public boolean isSet(int pos) {
    Preconditions.checkArgument(pos >= 0, "Invalid bit position: %s < 0", pos);
    int containerIndex = pos >>> 8;
    int posInContainer = pos & 0xFF;

    if (containerIndex >= containerCount) {
      return false;
    }

    int containerStart = offset(containerIndex);
    int descriptor = descriptor(containerIndex);

    if (MumblingFormat.isDense(descriptor)) {
      // Dense: 32-byte bitset, MSB of byte 0 is position 0
      int byteIndex = posInContainer >>> 3;
      int bitShift = 7 - (posInContainer & 0b111);
      return ((data.get(containerStart + byteIndex) >>> bitShift) & 0b1) == 0b1;

    } else {
      // Sparse: sorted list of set positions; scan until found or exceeded
      for (int i = 0; i < descriptor; i += 1) {
        int stored = data.get(containerStart + i) & 0xFF;
        if (stored == posInContainer) {
          return true;
        }

        if (stored > posInContainer) {
          return false;
        }
      }

      return false;
    }
  }

  private int descriptor(int containerIndex) {
    if (null == descriptors) {
      decodeDescriptors();
    }

    return descriptors[containerIndex];
  }

  private int offset(int containerIndex) {
    if (null == offsets) {
      decodeDescriptors();
    }

    return offsets[containerIndex];
  }

  /**
   * Decode the descriptor array and produce an array of absolute container offsets in the buffer.
   */
  private void decodeDescriptors() {
    int[] descriptorArray = new int[containerCount];
    int bytesRead =
        PFOREncoding.decode(
            data, MumblingFormat.HEADER_SIZE, descriptorArray, 0, containerCount);

    int[] offsetArray = new int[containerCount + 1];
    int firstContainerOffset = data.position() + MumblingFormat.HEADER_SIZE + bytesRead;
    descriptorsToOffsets(firstContainerOffset, descriptorArray, offsetArray);

    // update the references last so that only valid values are available
    this.descriptors = descriptorArray;
    this.offsets = offsetArray;
  }

  private static byte[] copyByteBuffer(ByteBuffer data) {
    ByteBuffer source = data.slice();
    byte[] bytes = new byte[source.remaining()];
    source.get(bytes);
    return bytes;
  }

  /**
   * Convert an array of lengths into an array of offsets starting at the given base.
   *
   * <p>For example, descriptorsToOffsets(0, [1, 1, 2]) produces [0, 1, 2, 4].
   *
   * @param baseOffset initial offset of the first container
   * @param descriptors an array of descriptor bytes
   * @param offsets output array of offsets
   */
  private static void descriptorsToOffsets(int baseOffset, int[] descriptors, int[] offsets) {
    Preconditions.checkArgument(
        offsets.length > descriptors.length,
        "Cannot decode %s lengths into %s offsets (not enough space)",
        descriptors.length,
        offsets.length);

    offsets[0] = baseOffset;
    for (int i = 0; i < descriptors.length; i += 1) {
      if (MumblingFormat.isDense(descriptors[i])) {
        offsets[i + 1] = offsets[i] + MumblingFormat.DENSE_CONTAINER_SIZE;
      } else if (MumblingFormat.isSparse(descriptors[i])) {
        offsets[i + 1] = offsets[i] + descriptors[i];
      } else {
        throw new IllegalStateException(
            "Invalid descriptor, not sparse or dense: " + descriptors[i]);
      }
    }
  }
}
