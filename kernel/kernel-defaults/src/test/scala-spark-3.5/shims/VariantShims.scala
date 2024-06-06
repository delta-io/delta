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

package io.delta.kernel.defaults

import io.delta.kernel.defaults.internal.data.value.DefaultVariantValue

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

object VariantShims {

  /**
   * Spark's variant type is implemented for Spark 4.0 and is not implemented in Spark 3.5. Thus,
   * any Spark 3.5 DataType cannot be a variant type.
   */
  def isVariantType(dt: DataType): Boolean = false

  /**
   * Converts Spark's variant value to Kernel Default's variant value for testing.
   * This method should not be called when depending on Spark 3.5 because Spark 3.5 cannot create
   * variants.
   */
  def convertToKernelVariant(v: Any): DefaultVariantValue =
    throw new UnsupportedOperationException("Not supported")

  /**
   * Retrieves a Spark variant from a Spark row and converts it to Kernel Default's variant value
   * for testing.
   *
   * Should not be called when testing using Spark 3.5.
   */
  def getVariantAndConvertToKernel(r: Row, ordinal: Int): DefaultVariantValue =
    throw new UnsupportedOperationException("Not supported")

  /**
   * Returns Spark's variant type singleton. This should not be called when testing with Spark 3.5.
   */
  def getSparkVariantType(): DataType = throw new UnsupportedOperationException("Not supported")
}
