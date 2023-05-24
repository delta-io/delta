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

import org.apache.spark.sql.delta.CheckpointInstance.Format

import org.apache.spark.SparkFunSuite

class CheckpointInstanceSuite extends SparkFunSuite {

  test("checkpoint instance comparisons") {
    val ci1_single_1 = CheckpointInstance(1, Format.SINGLE, numParts = None)
    val ci1_withparts_2 = CheckpointInstance(1, Format.WITH_PARTS, numParts = Some(2))
    val ci1_sentinel = CheckpointInstance.sentinelValue(Some(1))

    val ci2_single_1 = CheckpointInstance(2, Format.SINGLE, numParts = None)
    val ci2_withparts_4 = CheckpointInstance(2, Format.WITH_PARTS, numParts = Some(4))
    val ci2_sentinel = CheckpointInstance.sentinelValue(Some(2))

    val ci3_single_1 = CheckpointInstance(3, Format.SINGLE, numParts = None)
    val ci3_withparts_2 = CheckpointInstance(3, Format.WITH_PARTS, numParts = Some(2))

    assert(ci1_single_1 < ci2_single_1) // version takes priority
    assert(ci1_single_1 < ci1_withparts_2) // parts takes priority when versions are same
    assert(ci2_withparts_4 < ci3_withparts_2) // version takes priority over parts

    // all checkpoint instances for version 1/2 are less than sentinel value for version 2.
    Seq(ci1_single_1, ci1_withparts_2, ci1_sentinel, ci2_single_1, ci2_withparts_4)
      .foreach(ci => assert(ci < ci2_sentinel))

    // all checkpoint instances for version 3 are greater than sentinel value for version 2.
    Seq(ci3_single_1, ci3_withparts_2).foreach(ci => assert(ci > ci2_sentinel))

    // Everything is less than CheckpointInstance.MaxValue
    Seq(
      ci1_single_1, ci1_withparts_2, ci1_sentinel,
      ci2_single_1, ci2_withparts_4, ci2_sentinel,
      ci3_single_1, ci3_withparts_2
    ).foreach(ci => assert(ci < CheckpointInstance.MaxValue))
  }
}
