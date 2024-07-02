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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSourceUtils._

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}

/**
 * Provide utility methods related to IDENTITY column support for Delta.
 */
object IdentityColumn extends DeltaLogging {
  case class IdentityInfo(start: Long, step: Long, highWaterMark: Option[Long])
  // Default start and step configuration if not specified by user.
  val defaultStart = 1
  val defaultStep = 1

  // Return true if `field` is an identity column that allows explicit insert. Caller must ensure
  // `isIdentityColumn(field)` is true.
  def allowExplicitInsert(field: StructField): Boolean = {
    field.metadata.getBoolean(IDENTITY_INFO_ALLOW_EXPLICIT_INSERT)
  }


  // Calculate the sync'ed IDENTITY high water mark based on actual data and returns a
  // potentially updated `StructField`.
  def syncIdentity(field: StructField, df: DataFrame): StructField = {
    // Round `value` to the next value that follows start and step configuration.
    def roundToNext(start: Long, step: Long, value: Long): Long = {
      if (Math.subtractExact(value, start) % step == 0) {
        value
      } else {
        // start + step * ((value - start) / step + 1)
        Math.addExact(
          Math.multiplyExact(Math.addExact(Math.subtractExact(value, start) / step, 1), step),
          start)
      }
    }

    assert(ColumnWithDefaultExprUtils.isIdentityColumn(field))
    // Run a query to get the actual high water mark (max or min value of the IDENTITY column) from
    // the actual data.
    val info = getIdentityInfo(field)
    val positiveStep = info.step > 0
    val expr = if (positiveStep) max(field.name) else min(field.name)
    val resultRow = df.select(expr).collect().head

    if (!resultRow.isNullAt(0)) {
      val result = resultRow.getLong(0)
      val isBeforeStart = if (positiveStep) result < info.start else result > info.start
      val newHighWaterMark = roundToNext(info.start, info.step, result)
      if (isBeforeStart || info.highWaterMark.contains(newHighWaterMark)) {
        field
      } else {
        val newMetadata = new MetadataBuilder().withMetadata(field.metadata)
          .putLong(IDENTITY_INFO_HIGHWATERMARK, newHighWaterMark)
          .build()
        field.copy(metadata = newMetadata)
      }
    } else {
      field
    }
  }


  // Return IDENTITY information of column `field`. Caller must ensure `isIdentityColumn(field)`
  // is true.
  def getIdentityInfo(field: StructField): IdentityInfo = {
    val md = field.metadata
    val start = md.getLong(IDENTITY_INFO_START)
    val step = md.getLong(IDENTITY_INFO_STEP)
    // If system hasn't generated IDENTITY values for this column (either it hasn't been
    // inserted into, or every inserts provided values for this IDENTITY column), high water mark
    // field will not present in column metadata. In this case, high water mark will be set to
    // (start - step) so that the first value generated is start (high water mark + step).
    val highWaterMark = if (md.contains(IDENTITY_INFO_HIGHWATERMARK)) {
        Some(md.getLong(IDENTITY_INFO_HIGHWATERMARK))
      } else {
        None
      }
    IdentityInfo(start, step, highWaterMark)
  }
}
