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

package org.apache.spark.sql.delta.util

import scala.collection.generic.Sizing
import scala.collection.mutable.ArrayBuffer

/**
 * Iterator that packs objects in `inputIter` to create bins that have a total size of
 * 'targetSize'. Each [[T]] object may contain multiple inputs that are always packed into a
 * single bin. [[T]] instances must inherit from [[Sizing]] and define what is their size.
 */
class BinPackingIterator[T <: Sizing](
    inputIter: Iterator[T],
    targetSize: Long)
  extends Iterator[Seq[T]] {

  private val currentBin = new ArrayBuffer[T]()
  private var sizeOfCurrentBin = 0L

  override def hasNext: Boolean = inputIter.hasNext || currentBin.nonEmpty

  override def next(): Seq[T] = {
    var resultBin: Seq[T] = null
    while (inputIter.hasNext && resultBin == null) {
      val input = inputIter.next()

      val sizeOfCurrentFile = input.size

      // Start a new bin if the deletion vectors for the current Parquet file corresponding to
      // `row` causes us to go over the target file size.
      if (currentBin.nonEmpty &&
        sizeOfCurrentBin + sizeOfCurrentFile > targetSize) {
        resultBin = currentBin.toVector
        sizeOfCurrentBin = 0L
        currentBin.clear()
      }

      currentBin += input
      sizeOfCurrentBin += sizeOfCurrentFile
    }

    // Finish the last bin.
    if (resultBin == null && !inputIter.hasNext) {
      resultBin = currentBin.toVector
      currentBin.clear()
    }

    resultBin
  }
}
