/*
 * Copyright (2024-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

// scalastyle:off funsuite
import org.scalatest.FunSuite

import io.delta.standalone.internal.util.GoldenTableUtils._

class LogSegmentSuite extends FunSuite {
  test("getFileInfoString: single-part checkpoint") {
    withLogImplForGoldenTable("checkpoint") { log =>
      val logSegment = log.update().logSegment
      assert(logSegment.version == 14)
      assert(logSegment.deltas.size == 4)
      assert(logSegment.minDeltaVersion.contains(11))
      assert(logSegment.maxDeltaVersion.contains(14))
      assert(logSegment.minMultiPartCheckpointPart.isEmpty)
      assert(logSegment.maxMultiPartCheckpointPart.isEmpty)
      assert(logSegment.checkpointVersion.contains(10))
      assert(logSegment.checkpoints.size == 1)
      assert(logSegment.getFileInfoString ===
        s"numDeltas=4, minDeltaVersion=Some(11), maxDeltaVersion=Some(14), " +
          s"numCheckpointFiles=1, minMultiPartCheckpointPart=None, " +
          s"maxMultiPartCheckpointPart=None, startingCheckpointVersion=Some(10)")
    }
  }

  test("getFileInfoString: multi-part checkpoint (1)") {
    withLogImplForGoldenTable("multi-part-checkpoint") { log =>
      val logSegment = log.update().logSegment
      assert(logSegment.version == 1)
      assert(logSegment.deltas.isEmpty)
      assert(logSegment.minDeltaVersion.isEmpty)
      assert(logSegment.maxDeltaVersion.isEmpty)
      assert(logSegment.minMultiPartCheckpointPart.contains(1))
      assert(logSegment.maxMultiPartCheckpointPart.contains(2))
      assert(logSegment.checkpointVersion.contains(1))
      assert(logSegment.checkpoints.size == 2)
      assert(logSegment.getFileInfoString ===
          s"numDeltas=0, minDeltaVersion=None, maxDeltaVersion=None, " +
              s"numCheckpointFiles=2, minMultiPartCheckpointPart=Some(1), " +
              s"maxMultiPartCheckpointPart=Some(2), startingCheckpointVersion=Some(1)")
    }
  }

  test("getFileInfoString: multi-part checkpoint (2)") {
    withLogImplForGoldenTable("multi-part-checkpoint-2") { log =>
      val logSegment = log.update().logSegment
      assert(logSegment.version == 14)
      assert(logSegment.deltas.size == 4)
      assert(logSegment.minDeltaVersion.contains(11))
      assert(logSegment.maxDeltaVersion.contains(14))
      assert(logSegment.minMultiPartCheckpointPart.contains(1))
      assert(logSegment.maxMultiPartCheckpointPart.contains(10))
      assert(logSegment.checkpointVersion.contains(10))
      assert(logSegment.checkpoints.size == 10)
      assert(logSegment.getFileInfoString ===
        s"numDeltas=4, minDeltaVersion=Some(11), maxDeltaVersion=Some(14), " +
          s"numCheckpointFiles=10, minMultiPartCheckpointPart=Some(1), " +
          s"maxMultiPartCheckpointPart=Some(10), startingCheckpointVersion=Some(10)")
    }
  }
}
// scalastyle:on funsuite
