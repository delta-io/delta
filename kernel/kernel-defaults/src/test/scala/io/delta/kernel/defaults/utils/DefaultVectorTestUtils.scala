/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults.utils

import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.data.{ColumnVector, ColumnarBatch}
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types._

trait DefaultVectorTestUtils extends VectorTestUtils {
  /**
   * Returns a [[ColumnarBatch]] with each given vector is a top-level column col_i where i is
   * the index of the vector in the input list.
   */
  protected def columnarBatch(vectors: ColumnVector*): ColumnarBatch = {
    val numRows = vectors.head.getSize
    vectors.tail.foreach(
      v => require(v.getSize == numRows, "All vectors should have the same size"))

    val schema = (0 until vectors.length)
      .foldLeft(new StructType())((s, i) => s.add(s"col_$i", vectors(i).getDataType))

    new DefaultColumnarBatch(numRows, schema, vectors.toArray)
  }
}
