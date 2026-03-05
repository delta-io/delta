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

package org.apache.spark.sql.delta.shims;

import org.apache.spark.QueryContext;
import org.apache.spark.SparkRuntimeException;
import org.apache.spark.types.variant.VariantUtil;
import scala.collection.immutable.Map$;

/**
 * Shim for variant stats functionality in Spark 4.2+.
 * In Spark 4.2, VariantUtil.readUnsigned is public, so we can use it directly.
 */
public class VariantStatsShims {

  static SparkRuntimeException malformedVariant() {
    return new SparkRuntimeException("MALFORMED_VARIANT",
        Map$.MODULE$.<String, String>empty(), null, new QueryContext[]{}, "");
  }

  // Check the validity of an array index `pos`. Throw `MALFORMED_VARIANT` if it is out of bound,
  // meaning that the variant is malformed.
  private static void checkIndex(int pos, int length) {
    if (pos < 0 || pos >= length) throw malformedVariant();
  }

  // Get the length of metadata in the provided array. It is used to split metadata and value in
  // situations where they are serialized as a concatenated pair (e.g. Delta stats).
  public static int metadataSize(byte[] metadata) {
    checkIndex(0, metadata.length);
    // Similar to the logic from getMetadataKey where "id" is equal to "dictSize".
    int offsetSize = ((metadata[0] >> 6) & 0x3) + 1;
    int dictSize = VariantUtil.readUnsigned(metadata, 1, offsetSize);
    int lastOffset = VariantUtil.readUnsigned(metadata, 1 + (dictSize + 1) * offsetSize, offsetSize);
    int size = 1 + (dictSize + 2) * offsetSize + lastOffset;
    if (size > metadata.length) {
      throw malformedVariant();
    }
    return size;
  }
}
