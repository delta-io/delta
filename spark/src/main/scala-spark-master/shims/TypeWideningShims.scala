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

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types._

/**
 * Type widening only supports a limited set of type changes with Spark 3.5 due to the parquet
 * readers lacking the corresponding conversions that were added in Spark 4.0.
 * This shim is for Delta on Spark 4.0 which supports:
 * - byte -> short -> int -> long.
 * - float -> double.
 * - date -> timestamp_ntz.
 * - {byte, short, int} -> double.
 * - decimal -> wider decimal.
 * - {byte, short, int} -> decimal(10, 0) and wider.
 * - long -> decimal(20, 0) and wider.
 */
object TypeWideningShims {

  /**
   * Returns whether the given type change is eligible for widening. This only checks atomic types.
   * It is the responsibility of the caller to recurse into structs, maps and arrays.
   */
  def isTypeChangeSupported(fromType: AtomicType, toType: AtomicType): Boolean =
    (fromType, toType) match {
      case (from, to) if from == to => true
      // All supported type changes below are supposed to be widening, but to be safe, reject any
      // non-widening change upfront.
      case (from, to) if !Cast.canUpCast(from, to) => false
      case (from: IntegralType, to: IntegralType) => from.defaultSize <= to.defaultSize
      case (FloatType, DoubleType) => true
      case (DateType, TimestampNTZType) => true
      case (ByteType | ShortType | IntegerType, DoubleType) => true
      case (from: DecimalType, to: DecimalType) => to.isWiderThan(from)
      // Byte, Short, Integer are all stored as INT32 in parquet. The parquet readers support
      // converting INT32 to Decimal(10, 0) and wider.
      case (ByteType | ShortType | IntegerType, d: DecimalType) => d.isWiderThan(IntegerType)
      // The parquet readers support converting INT64 to Decimal(20, 0) and wider.
      case (LongType, d: DecimalType) => d.isWiderThan(LongType)
      case _ => false
    }

  /**
   * Returns whether the given type change can be applied during schema evolution. Only a
   * subset of supported type changes are considered for schema evolution.
   */
  def isTypeChangeSupportedForSchemaEvolution(fromType: AtomicType, toType: AtomicType): Boolean =
    (fromType, toType) match {
      case (from, to) if from == to => true
      case (from, to) if !isTypeChangeSupported(from, to) => false
      case (from: IntegralType, to: IntegralType) => from.defaultSize <= to.defaultSize
      case (FloatType, DoubleType) => true
      case (from: DecimalType, to: DecimalType) => to.isWiderThan(from)
      case (DateType, TimestampNTZType) => true
      case _ => false
    }
}
