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
package io.delta.kernel

import java.util.Optional

import org.scalatest.funsuite.AnyFunSuite
import io.delta.kernel.internal.checkpoints.CheckpointInstance

class CheckpointInstanceSuite extends AnyFunSuite {
  test("checkpoint instance comparisons") {
    val ci1_single_1 = new CheckpointInstance(1, Optional.empty())
    val ci1_withparts_2 = new CheckpointInstance(1, Optional.of(2))

    val ci2_single_1 = new CheckpointInstance(2, Optional.empty())
    val ci2_withparts_4 = new CheckpointInstance(2, Optional.of(4))

    val ci3_single_1 = new CheckpointInstance(3, Optional.empty())
    val ci3_withparts_2 = new CheckpointInstance(3, Optional.of(2))

    // version takes priority
    assert(ci1_single_1.compareTo(ci2_single_1) < 0)
    // parts takes priority when versions are same
    assert(ci1_single_1.compareTo(ci1_withparts_2) < 0)
    // version takes priority over parts
    assert(ci2_withparts_4.compareTo(ci3_withparts_2) < 0)

    // Everything is less than CheckpointInstance.MAX_VALUE
    Seq(
      ci1_single_1, ci1_withparts_2,
      ci2_single_1, ci2_withparts_4,
      ci3_single_1, ci3_withparts_2
    ).foreach(ci => assert(ci.compareTo(CheckpointInstance.MAX_VALUE) < 0))
  }
}
