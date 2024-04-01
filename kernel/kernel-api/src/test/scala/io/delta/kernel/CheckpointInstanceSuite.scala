/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import scala.collection.JavaConverters._

import io.delta.kernel.internal.checkpoints.CheckpointInstance
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.test.MockFileSystemClientUtils
import org.scalatest.funsuite.AnyFunSuite

class CheckpointInstanceSuite extends AnyFunSuite with MockFileSystemClientUtils {

  private val FAKE_DELTA_LOG_PATH = new Path("/path/to/delta/log")

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

  test("checkpoint instance instantiation") {
    // classic checkpoint
    val classicCheckpoint = new CheckpointInstance(
      new Path(FAKE_DELTA_LOG_PATH, "00000000000000000010.checkpoint.parquet").toString)
    assert(classicCheckpoint.version == 10)
    assert(!classicCheckpoint.numParts.isPresent())
    assert(classicCheckpoint.format == CheckpointInstance.CheckpointFormat.CLASSIC)
    assert(classicCheckpoint.format.usesSidecars())

    // multi-part checkpoint
    val multipartCheckpoint = new CheckpointInstance(
      new Path(FAKE_DELTA_LOG_PATH,
        "00000000000000000010.checkpoint.0000000002.0000000003.parquet").toString)
    assert(multipartCheckpoint.version == 10)
    assert(multipartCheckpoint.numParts.isPresent() && multipartCheckpoint.numParts.get() == 3)
    assert(multipartCheckpoint.format == CheckpointInstance.CheckpointFormat.MULTI_PART)
    assert(!multipartCheckpoint.format.usesSidecars())

    // V2 checkpoint
    val v2Checkpoint = new CheckpointInstance(
      new Path(FAKE_DELTA_LOG_PATH,
        "00000000000000000010.checkpoint.abcda-bacbac.parquet").toString)
    assert(v2Checkpoint.version == 10)
    assert(!v2Checkpoint.numParts.isPresent())
    assert(v2Checkpoint.format == CheckpointInstance.CheckpointFormat.V2)
    assert(v2Checkpoint.format.usesSidecars())

    // invalid checkpoints
    intercept[RuntimeException] {
      new CheckpointInstance(
        new Path(FAKE_DELTA_LOG_PATH,
          "00000000000000000010.checkpoint.000000.a.parquet").toString)
    }
    intercept[RuntimeException] {
      new CheckpointInstance(
        new Path(FAKE_DELTA_LOG_PATH,
          "00000000000000000010.parquet").toString)
    }
  }

  test("checkpoint instance getCorrespondingFiles") {
    // classic checkpoint
    val classicCheckpoint0 = new CheckpointInstance(0)
    assert(classicCheckpoint0.getCorrespondingFiles(FAKE_DELTA_LOG_PATH).equals(
      Seq(new Path(FAKE_DELTA_LOG_PATH, "00000000000000000000.checkpoint.parquet")).asJava
    ))
    val classicCheckpoint10 = new CheckpointInstance(10)
    assert(classicCheckpoint10.getCorrespondingFiles(FAKE_DELTA_LOG_PATH).equals(
      Seq(new Path(FAKE_DELTA_LOG_PATH, "00000000000000000010.checkpoint.parquet")).asJava
    ))

    // multi-part checkpoint
    val multipartCheckpoint = new CheckpointInstance(10, Optional.of(3))
    val expectedResult = Seq(
      "00000000000000000010.checkpoint.0000000001.0000000003.parquet",
      "00000000000000000010.checkpoint.0000000002.0000000003.parquet",
      "00000000000000000010.checkpoint.0000000003.0000000003.parquet"
    ).map(new Path(FAKE_DELTA_LOG_PATH, _))
    assert(multipartCheckpoint.getCorrespondingFiles(FAKE_DELTA_LOG_PATH).equals(
      expectedResult.asJava))
  }
}
