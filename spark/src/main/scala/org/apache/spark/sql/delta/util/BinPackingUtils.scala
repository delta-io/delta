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

import scala.collection.mutable.ArrayBuffer

object BinPackingUtils {
  /**
   * Takes a sequence of items and groups them such that the size of each group is
   * less than the specified maxBinSize.
   */
  @inline def binPackBySize[I, V](
      elements: Seq[I],
      sizeGetter: I => Long,
      valueGetter: I => V,
      maxBinSize: Long): Seq[Seq[V]] = {
    val bins = new ArrayBuffer[Seq[V]]()

    val currentBin = new ArrayBuffer[V]()
    var currentSize = 0L

    elements.sortBy(sizeGetter).foreach { element =>
      val size = sizeGetter(element)
      // Generally, a bin is a group of existing files, whose total size does not exceed the
      // desired maxFileSize. They will be coalesced into a single output file.
      if ((currentSize >= maxBinSize) || size + currentSize > maxBinSize) {
        if (currentBin.nonEmpty) {
          bins += currentBin.toVector
          currentBin.clear()
        }
        currentBin += valueGetter(element)
        currentSize = size
      } else {
        currentBin += valueGetter(element)
        currentSize += size
      }
    }

    if (currentBin.nonEmpty) {
      bins += currentBin.toVector
    }
    bins.toSeq
  }
}
