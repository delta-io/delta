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

import org.apache.spark.SparkFunSuite

class BinPackingUtilsSuite extends SparkFunSuite {
  test("test bin-packing") {
    val binSize = 5
    val cases = Seq[(Seq[Int], Seq[Seq[Int]])](
      (Seq(1, 2, 3, 4, 5), Seq(Seq(1, 2), Seq(3), Seq(4), Seq(5))),
      (Seq(5, 4, 3, 2, 1), Seq(Seq(1, 2), Seq(3), Seq(4), Seq(5))),
      // Naive coalescing returns 5 bins where sort-then-coalesce gets 4.
      (Seq(4, 2, 4, 2, 5), Seq(Seq(2, 2), Seq(4), Seq(4), Seq(5))),
      // The last element exceeds binSize and it's in its own bin.
      (Seq(1, 2, 4, 5, 6), Seq(Seq(1, 2), Seq(4), Seq(5), Seq(6))))

    for ((input, expect) <- cases) {
      assert(BinPackingUtils.binPackBySize(input, (x: Int) => x, (x: Int) => x, binSize) == expect)
    }
  }
}
