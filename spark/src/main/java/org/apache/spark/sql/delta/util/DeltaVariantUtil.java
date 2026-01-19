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

package org.apache.spark.sql.delta.util;

import org.apache.spark.types.variant.VariantUtil;

/**
 * Utility functions for working with Variant data types in Delta.
 * This class provides additional functionality beyond what's available in
 * org.apache.spark.types.variant.VariantUtil.
 */
public class DeltaVariantUtil {

  private static void checkIndex(int pos, int length) {
    if (pos < 0 || pos >= length) {
      throw new IllegalArgumentException(
        "Malformed variant: invalid index " + pos + " for length " + length);
    }
  }

  /**
   * Calculate the size of the metadata section of a variant binary.
   *
   * @param metadata The variant metadata bytes (or combined metadata+value bytes)
   * @return The size of the metadata section in bytes
   */
  public static int metadataSize(byte[] metadata) {
    checkIndex(0, metadata.length);
    int offsetSize = ((metadata[0] >> 6) & 0x3) + 1;
    int dictSize = VariantUtil.readUnsigned(metadata, 1, offsetSize);
    int lastOffset =
      VariantUtil.readUnsigned(metadata, 1 + (dictSize + 1) * offsetSize, offsetSize);
    int size = 1 + (dictSize + 2) * offsetSize + lastOffset;
    if (size > metadata.length) {
      throw new IllegalArgumentException(
        "Malformed variant: metadata size " + size + " exceeds buffer length " + metadata.length);
    }
    return size;
  }
}
