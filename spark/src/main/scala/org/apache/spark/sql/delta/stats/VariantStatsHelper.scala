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

package org.apache.spark.sql.delta.stats

import org.apache.spark.types.variant.{Variant, VariantBuilder, VariantUtil}
import org.apache.spark.unsafe.types.VariantVal

object VariantStatsHelper {

  def trimVariant(maxFields: Int)(variantVal: VariantVal): VariantVal = {
    if (variantVal == null) {
      return null
    }
    val fullVariant = new Variant(variantVal.getValue, variantVal.getMetadata)
    if (fullVariant.getType != VariantUtil.Type.OBJECT) {
      return null
    }
    val trimmedVal = extractFirstNPaths(fullVariant, maxFields)
    new VariantVal(trimmedVal.getValue, trimmedVal.getMetadata)
  }

  private def extractFirstNPaths(fullVariant: Variant, maxFields: Int): Variant = {
    val numFields = fullVariant.objectSize
    if (numFields > maxFields) {
      val builder = new VariantBuilder(false)
      val start = builder.getWritePos
      val fields = new java.util.ArrayList[VariantBuilder.FieldEntry](maxFields)
      for (i <- 0 until maxFields) {
        val field = fullVariant.getFieldAtIndex(i)
        val id = builder.addKey(field.key)
        fields.add(new VariantBuilder.FieldEntry(field.key, id, builder.getWritePos - start))
        builder.appendVariant(field.value)
      }
      builder.finishWritingObject(start, fields)
      builder.result()
    } else {
      fullVariant
    }
  }
}
