/*
 * Copyright (2024) The Delta Lake Project Authors.
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
 * This shim is for Delta on Spark 3.5 which supports:
 * - byte -> short -> int
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
      case (ByteType, ShortType) => true
      case (ByteType | ShortType, IntegerType) => true
      case _ => false
    }

  /** Whether we support reading the given type change. */
  def canReadTypeChange(fromType: AtomicType, toType: AtomicType): Boolean =
    isTypeChangeSupported(fromType, toType)
}
