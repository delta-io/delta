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
import org.apache.spark.sql.types.{DataType, VariantType}
import org.apache.spark.unsafe.types.VariantVal

object VariantShims {

  /** Spark's variant type is only implemented in Spark 4.0 and above. */
  def isVariantType(dt: DataType): Boolean = dt.isInstanceOf[VariantType]

  /** Converts Spark's variant value to Kernel Default's variant value for testing. */
  def convertToKernelVariant(v: Any): DefaultVariantValue = {
    val sparkVariant = v.asInstanceOf[VariantVal]
    new DefaultVariantValue(sparkVariant.getValue(), sparkVariant.getMetadata())
  }

  /** 
   * Retrieves a Spark variant from a Spark row and converts it to Kernel Default's variant value
   * for testing.
   */
  def getVariantAndConvertToKernel(r: Row, ordinal: Int): DefaultVariantValue = {
    val sparkVariant = r.getAs[VariantVal](ordinal)
    new DefaultVariantValue(sparkVariant.getValue(), sparkVariant.getMetadata())
  }

  def getSparkVariantType(): DataType = VariantType
}
