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

import java.math.{BigDecimal => JBigDecimal}

import org.apache.spark.types.variant.{Variant, VariantBuilder, VariantUtil}
import org.apache.spark.unsafe.types.VariantVal

object VariantStatsHelper {

  private val EMPTY_METADATA: Array[Byte] = Array(VariantUtil.VERSION.toByte, 0, 0)

  def appendIntFixedPrecision(vb: VariantBuilder, l: Long, bitWidth: Int): Unit = {
    vb.appendVariant(new Variant(encodeIntFixedPrecision(l, bitWidth), EMPTY_METADATA))
  }

  def appendDecimalFixedPrecision(vb: VariantBuilder, d: JBigDecimal, bitWidth: Int): Unit = {
    vb.appendVariant(new Variant(encodeDecimalFixedPrecision(d, bitWidth), EMPTY_METADATA))
  }

  private def encodeIntFixedPrecision(l: Long, bitWidth: Int): Array[Byte] = {
    val (primType, numBytes) = bitWidth match {
      case 8 => (VariantUtil.INT1, 1)
      case 16 => (VariantUtil.INT2, 2)
      case 32 => (VariantUtil.INT4, 4)
      case 64 => (VariantUtil.INT8, 8)
      case _ => throw new IllegalArgumentException("Unexpected bit width of: " + bitWidth)
    }
    val buf = new Array[Byte](1 + numBytes)
    buf(0) = VariantUtil.primitiveHeader(primType)
    VariantUtil.writeLong(buf, 1, l, numBytes)
    buf
  }

  private def encodeDecimalFixedPrecision(d: JBigDecimal, bitWidth: Int): Array[Byte] = {
    val unscaled = d.unscaledValue
    bitWidth match {
      case 32 =>
        val buf = new Array[Byte](2 + 4)
        buf(0) = VariantUtil.primitiveHeader(VariantUtil.DECIMAL4)
        buf(1) = d.scale.toByte
        VariantUtil.writeLong(buf, 2, unscaled.intValueExact.toLong, 4)
        buf
      case 64 =>
        val buf = new Array[Byte](2 + 8)
        buf(0) = VariantUtil.primitiveHeader(VariantUtil.DECIMAL8)
        buf(1) = d.scale.toByte
        VariantUtil.writeLong(buf, 2, unscaled.longValueExact, 8)
        buf
      case 128 =>
        val buf = new Array[Byte](2 + 16)
        buf(0) = VariantUtil.primitiveHeader(VariantUtil.DECIMAL16)
        buf(1) = d.scale.toByte
        val bytes = unscaled.toByteArray
        var i = 0
        while (i < bytes.length) {
          buf(2 + i) = bytes(bytes.length - 1 - i)
          i += 1
        }
        val sign = if (bytes(0) < 0) (-1).toByte else 0.toByte
        while (i < 16) {
          buf(2 + i) = sign
          i += 1
        }
        buf
      case _ =>
        throw new IllegalArgumentException("Unexpected bit width of: " + bitWidth)
    }
  }

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
