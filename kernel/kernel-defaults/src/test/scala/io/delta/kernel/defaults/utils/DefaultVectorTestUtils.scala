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
