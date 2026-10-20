/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import com.google.common.base.Preconditions;

/**
 * Serializer for the Mumbling bitmap format read by {@link MumblingBitmap}.
 *
 * <p>Iceberg's Mumbling implementation ships only a read-only view; the byte layout produced here
 * is the write side, built directly on the (bidirectional) {@link PFOREncoding} descriptor codec so
 * it round-trips exactly through {@link MumblingBitmap}. The container assembly mirrors the
 * reference layout: a 6-byte header, a PFOR-encoded descriptor array (one descriptor per
 * container), then the concatenated container bytes.
 *
 * <p>TODO: Replace this class with Iceberg's Mumbling writer when one is available.
 */
public final class MumblingBitmapWriter {
  /**
   * Positions must be strictly less than this bound. The spec caps a Mumbling bitmap at {@code
   * MAX_CONTAINERS} containers of {@code CONTAINER_SIZE} positions (container index = position >>>
   * 8), which also bounds the cardinality since positions are distinct.
   */
  public static final int MAX_POSITION_EXCLUSIVE = MumblingFormat.MAX_POSITION_EXCLUSIVE;

  private MumblingBitmapWriter() {}

  /**
   * Serializes the given positions into a Mumbling bitmap.
   *
   * @param sortedPositions set positions in strictly ascending order, each in
   *     {@code [0, MAX_POSITION_EXCLUSIVE)}
   * @return the serialized bitmap bytes
   */
  public static byte[] serialize(int[] sortedPositions) {
    validateSortedPositions(sortedPositions);
    int cardinality = sortedPositions.length;
    if (cardinality == 0) {
      // Header only: version = 1, cardinality = 0, container count = 0.
      byte[] empty = new byte[MumblingFormat.HEADER_SIZE];
      empty[0] = (byte) MumblingFormat.VERSION;
      return empty;
    }

    int maxPosition = sortedPositions[cardinality - 1];
    int containerCount = (maxPosition >>> 8) + 1;
    int[] descriptors = new int[containerCount];
    byte[][] containers = new byte[containerCount][];
    int totalContainerBytes = 0;

    int index = 0;
    for (int container = 0; container < containerCount; container += 1) {
      int start = index;
      int upperExclusive = (container + 1) << 8;
      while (index < cardinality && sortedPositions[index] < upperExclusive) {
        index += 1;
      }
      int count = index - start;

      byte[] bytes;
      if (count <= MumblingFormat.MAX_SPARSE_COUNT) {
        // Sparse: descriptor = count, one ascending low-byte position per set bit.
        bytes = new byte[count];
        for (int i = 0; i < count; i += 1) {
          bytes[i] = (byte) (sortedPositions[start + i] & 0xFF);
        }
        descriptors[container] = count;
      } else {
        // Dense: 32-byte bitset, MSB of byte 0 is position 0.
        bytes = new byte[MumblingFormat.DENSE_CONTAINER_SIZE];
        for (int i = 0; i < count; i += 1) {
          int posInContainer = sortedPositions[start + i] & 0xFF;
          int byteIndex = posInContainer >>> 3;
          int bitShift = 7 - (posInContainer & 0b111);
          bytes[byteIndex] |= (byte) (1 << bitShift);
        }
        descriptors[container] = MumblingFormat.DENSE_CONTAINER_BIT;
      }
      containers[container] = bytes;
      totalContainerBytes += bytes.length;
    }

    return serializeContainers(cardinality, descriptors, containers, totalContainerBytes);
  }

  /**
   * Merges positions into an existing serialized Mumbling bitmap without materializing its set
   * positions. At most one decoded 256-position container is held at a time.
   *
   * @param sourceBitmap an existing Mumbling bitmap
   * @param additionContainers 32-byte addition bitsets indexed by container, with {@code null}
   *     entries for containers without additions
   * @return the merged bitmap
   */
  public static MumblingBitmap merge(
      MumblingBitmap sourceBitmap, byte[][] additionContainers) {
    Preconditions.checkArgument(
        additionContainers.length <= MumblingFormat.MAX_CONTAINERS,
        "Too many Mumbling addition containers: %s > %s",
        additionContainers.length,
        MumblingFormat.MAX_CONTAINERS);
    int additionContainerCount = additionContainers.length;
    while (additionContainerCount > 0
        && additionContainers[additionContainerCount - 1] == null) {
      additionContainerCount -= 1;
    }
    for (int container = 0; container < additionContainerCount; container += 1) {
      byte[] additionContainer = additionContainers[container];
      Preconditions.checkArgument(
          additionContainer == null
              || additionContainer.length == MumblingFormat.DENSE_CONTAINER_SIZE,
          "Mumbling addition container %s must contain exactly %s bytes",
          container,
          MumblingFormat.DENSE_CONTAINER_SIZE);
    }

    ByteBuffer source = sourceBitmap.buffer();
    int sourceContainerCount =
        (source.get(source.position() + 4) & 0xFF)
            | ((source.get(source.position() + 5) & 0xFF) << 8);
    int[] sourceDescriptors = new int[sourceContainerCount];
    int descriptorBytes =
        PFOREncoding.decode(
            source,
            MumblingFormat.HEADER_SIZE,
            sourceDescriptors,
            0,
            sourceContainerCount);
    int sourceContainerOffset =
        source.position() + MumblingFormat.HEADER_SIZE + descriptorBytes;

    int containerCount = Math.max(sourceContainerCount, additionContainerCount);
    int[] descriptors = new int[containerCount];
    byte[][] containers = new byte[containerCount][];
    int totalContainerBytes = 0;
    int sourceCardinality = 0;
    int mergedCardinality = 0;

    for (int container = 0; container < containerCount; container += 1) {
      byte[] additionContainer =
          container < additionContainerCount ? additionContainers[container] : null;

      // Fast path 1: copy an untouched source container without decoding and re-encoding it.
      if (container < sourceContainerCount && additionContainer == null) {
        int descriptor = sourceDescriptors[container];
        int encodedSize = encodedContainerSize(descriptor);
        byte[] encodedContainer = copyBytes(source, sourceContainerOffset, encodedSize);
        int containerCardinality =
            MumblingFormat.isDense(descriptor) ? cardinality(encodedContainer) : descriptor;

        sourceCardinality += containerCardinality;
        mergedCardinality += containerCardinality;
        descriptors[container] =
            MumblingFormat.isDense(descriptor)
                ? MumblingFormat.DENSE_CONTAINER_BIT
                : descriptor;
        containers[container] = encodedContainer;
        totalContainerBytes += encodedSize;
        sourceContainerOffset += encodedSize;
        continue;
      }

      // Fast path 2: emit an empty container for a gap before a later addition.
      if (container >= sourceContainerCount && additionContainer == null) {
        descriptors[container] = 0;
        containers[container] = new byte[0];
        continue;
      }

      byte[] dense = new byte[MumblingFormat.DENSE_CONTAINER_SIZE];
      if (container < sourceContainerCount) {
        int descriptor = sourceDescriptors[container];
        if (MumblingFormat.isDense(descriptor)) {
          for (int i = 0; i < MumblingFormat.DENSE_CONTAINER_SIZE; i += 1) {
            dense[i] = source.get(sourceContainerOffset + i);
          }
          sourceContainerOffset += MumblingFormat.DENSE_CONTAINER_SIZE;
        } else if (MumblingFormat.isSparse(descriptor)) {
          for (int i = 0; i < descriptor; i += 1) {
            setBit(dense, source.get(sourceContainerOffset + i) & 0xFF);
          }
          sourceContainerOffset += descriptor;
        } else {
          throw new IllegalStateException(
              "Invalid descriptor, not sparse or dense: " + descriptor);
        }
        sourceCardinality += cardinality(dense);
      }

      // Null addition containers take one of the fast paths above.
      for (int i = 0; i < MumblingFormat.DENSE_CONTAINER_SIZE; i += 1) {
        dense[i] |= additionContainer[i];
      }

      int containerCardinality = cardinality(dense);
      mergedCardinality += containerCardinality;
      byte[] encodedContainer = encodeContainer(dense, containerCardinality);
      descriptors[container] =
          containerCardinality <= MumblingFormat.MAX_SPARSE_COUNT
              ? containerCardinality
              : MumblingFormat.DENSE_CONTAINER_BIT;
      containers[container] = encodedContainer;
      totalContainerBytes += encodedContainer.length;
    }

    Preconditions.checkState(
        sourceCardinality == sourceBitmap.cardinality(),
        "Mumbling cardinality %s does not match decoded cardinality %s",
        sourceBitmap.cardinality(),
        sourceCardinality);
    return new MumblingBitmap(
        serializeContainers(mergedCardinality, descriptors, containers, totalContainerBytes));
  }

  private static int encodedContainerSize(int descriptor) {
    if (MumblingFormat.isDense(descriptor)) {
      return MumblingFormat.DENSE_CONTAINER_SIZE;
    } else if (MumblingFormat.isSparse(descriptor)) {
      return descriptor;
    }
    throw new IllegalStateException(
        "Invalid descriptor, not sparse or dense: " + descriptor);
  }

  private static byte[] copyBytes(ByteBuffer source, int offset, int length) {
    byte[] copy = new byte[length];
    for (int i = 0; i < length; i += 1) {
      copy[i] = source.get(offset + i);
    }
    return copy;
  }

  private static byte[] serializeContainers(
      int cardinality, int[] descriptors, byte[][] containers, int totalContainerBytes) {
    int containerCount = descriptors.length;
    int sizeEstimate =
        MumblingFormat.HEADER_SIZE
            + PFOREncoding.estimateEncodedSize(containerCount)
            + totalContainerBytes;
    ByteBuffer buffer = ByteBuffer.allocate(sizeEstimate);

    // Header: version (1 byte), cardinality (3 bytes LE), container count (2 bytes LE).
    buffer.put(0, (byte) MumblingFormat.VERSION);
    buffer.put(1, (byte) (cardinality & 0xFF));
    buffer.put(2, (byte) ((cardinality >>> 8) & 0xFF));
    buffer.put(3, (byte) ((cardinality >>> 16) & 0xFF));
    buffer.put(4, (byte) (containerCount & 0xFF));
    buffer.put(5, (byte) ((containerCount >>> 8) & 0xFF));

    // PFOR-encoded descriptor array.
    int descriptorArraySize =
        PFOREncoding.encode(
            descriptors, 0, buffer, MumblingFormat.HEADER_SIZE, containerCount);

    // Concatenated container bytes.
    int containerOffset = MumblingFormat.HEADER_SIZE + descriptorArraySize;
    for (byte[] bytes : containers) {
      for (int i = 0; i < bytes.length; i += 1) {
        buffer.put(containerOffset + i, bytes[i]);
      }
      containerOffset += bytes.length;
    }

    byte[] out = new byte[containerOffset];
    System.arraycopy(buffer.array(), 0, out, 0, containerOffset);
    return out;
  }

  private static byte[] encodeContainer(byte[] dense, int containerCardinality) {
    if (containerCardinality > MumblingFormat.MAX_SPARSE_COUNT) {
      return dense;
    }

    byte[] sparse = new byte[containerCardinality];
    int index = 0;
    for (int position = 0; position < MumblingFormat.CONTAINER_SIZE; position += 1) {
      if (isSet(dense, position)) {
        sparse[index] = (byte) position;
        index += 1;
      }
    }
    return sparse;
  }

  private static int cardinality(byte[] dense) {
    int cardinality = 0;
    for (byte value : dense) {
      cardinality += Integer.bitCount(value & 0xFF);
    }
    return cardinality;
  }

  private static void setBit(byte[] dense, int position) {
    int byteIndex = position >>> 3;
    int bitShift = 7 - (position & 0b111);
    dense[byteIndex] |= (byte) (1 << bitShift);
  }

  private static boolean isSet(byte[] dense, int position) {
    int byteIndex = position >>> 3;
    int bitShift = 7 - (position & 0b111);
    return ((dense[byteIndex] >>> bitShift) & 0b1) == 0b1;
  }

  private static void validateSortedPositions(int[] sortedPositions) {
    int previous = -1;
    for (int position : sortedPositions) {
      Preconditions.checkArgument(
          position >= 0 && position < MAX_POSITION_EXCLUSIVE,
          "Mumbling position must be within [0, %s): %s",
          MAX_POSITION_EXCLUSIVE,
          position);
      Preconditions.checkArgument(
          position > previous,
          "Mumbling positions must be strictly increasing: %s after %s",
          position,
          previous);
      previous = position;
    }
  }
}
