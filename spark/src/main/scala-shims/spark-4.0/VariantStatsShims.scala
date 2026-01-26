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

package org.apache.spark.sql.delta.shims

/**
 * Shim for variant stats functionality in Spark 4.0.
 * In Spark 4.0, VariantUtil.readUnsigned is a private member, so we provide our own
 * implementation here.
 */
object VariantStatsShims {

  private def checkIndex(pos: Int, length: Int): Unit = {
    if (pos < 0 || pos >= length) {
      throw new IllegalArgumentException(
        s"Malformed variant: invalid index $pos for length $length")
    }
  }

  /**
   * Read a little-endian unsigned int value from `bytes[pos, pos + numBytes)`.
   * The value must fit into a non-negative int (`[0, Integer.MAX_VALUE]`).
   *
   * This is a local implementation since VariantUtil.readUnsigned is private in Spark 4.0.
   */
  private def readUnsigned(bytes: Array[Byte], pos: Int, numBytes: Int): Int = {
    checkIndex(pos, bytes.length)
    checkIndex(pos + numBytes - 1, bytes.length)
    var result = 0
    // All bytes should be unsign-extended.
    var i = 0
    while (i < numBytes) {
      val unsignedByteValue = bytes(pos + i) & 0xFF
      result |= unsignedByteValue << (8 * i)
      i += 1
    }
    if (result < 0) {
      throw new IllegalArgumentException("Malformed variant: negative unsigned int")
    }
    result
  }

  /**
   * Calculate the size of the metadata section of a variant binary.
   *
   * @param metadata The variant metadata bytes (or combined metadata+value bytes)
   * @return The size of the metadata section in bytes
   */
  def metadataSize(metadata: Array[Byte]): Int = {
    checkIndex(0, metadata.length)
    val offsetSize = ((metadata(0) >> 6) & 0x3) + 1
    val dictSize = readUnsigned(metadata, 1, offsetSize)
    val lastOffset = readUnsigned(metadata, 1 + (dictSize + 1) * offsetSize, offsetSize)
    val size = 1 + (dictSize + 2) * offsetSize + lastOffset
    if (size > metadata.length) {
      throw new IllegalArgumentException(
        s"Malformed variant: metadata size $size exceeds buffer length ${metadata.length}")
    }
    size
  }
}
