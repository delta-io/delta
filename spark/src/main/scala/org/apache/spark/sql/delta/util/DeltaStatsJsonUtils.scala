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

package org.apache.spark.sql.delta.util

import org.apache.spark.sql.delta.shims.VariantStatsShims
import org.apache.spark.sql.delta.util.Codec.Base85Codec
import org.apache.spark.types.variant.{Variant, VariantUtil}
import org.apache.spark.unsafe.types.VariantVal

/**
 * Utility functions for encoding/decoding Variant values as Z85 strings.
 * This is used for storing variant statistics in Delta checkpoints and stats.
 */
object DeltaStatsJsonUtils {

  /**
   * Encode a Variant as a Z85 string.
   * The variant binary format stores metadata followed by value bytes.
   * This concatenates them and encodes as Z85.
   */
  def encodeVariantAsZ85(v: Variant): String = {
    val metadata = v.getMetadata
    val value = v.getValue

    val combined = new Array[Byte](metadata.length + value.length)
    System.arraycopy(metadata, 0, combined, 0, metadata.length)
    System.arraycopy(value, 0, combined, metadata.length, value.length)

    Base85Codec.encodeBytes(combined)
  }

  /**
   * Decode a Z85-encoded string back to a VariantVal.
   */
  def decodeVariantFromZ85(z85: String): VariantVal = {
    val decoded = Base85Codec.decodeBytes(z85, z85.length)
    val metadataSize = VariantStatsShims.metadataSize(decoded)
    val valueWithPadding = decoded.slice(metadataSize, decoded.length)
    val valueSize = VariantUtil.valueSize(valueWithPadding, 0)
    val value = valueWithPadding.slice(0, valueSize)
    val metadata = decoded.slice(0, metadataSize)
    new VariantVal(value, metadata)
  }
}
