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

import org.apache.spark.types.variant.VariantUtil

/**
 * Shim for variant stats functionality in Spark 4.2+.
 * In Spark 4.2, VariantUtil.readUnsigned is public, so we can use it directly.
 */
object VariantStatsShims {

  private def checkIndex(pos: Int, length: Int): Unit = {
    if (pos < 0 || pos >= length) {
      throw new IllegalArgumentException(
        s"Malformed variant: invalid index $pos for length $length")
    }
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
    val dictSize = VariantUtil.readUnsigned(metadata, 1, offsetSize)
    val lastOffset = VariantUtil.readUnsigned(metadata, 1 + (dictSize + 1) * offsetSize, offsetSize)
    val size = 1 + (dictSize + 2) * offsetSize + lastOffset
    if (size > metadata.length) {
      throw new IllegalArgumentException(
        s"Malformed variant: metadata size $size exceeds buffer length ${metadata.length}")
    }
    size
  }
}
