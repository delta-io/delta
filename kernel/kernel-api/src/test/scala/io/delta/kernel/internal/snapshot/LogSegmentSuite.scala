/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.snapshot

import java.util.{Collections, Optional}

import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.FileStatus
import org.scalatest.funsuite.AnyFunSuite

class LogSegmentSuite extends AnyFunSuite {
  private val logPath = new Path("/a/_delta_log")
  private val checkpointFs10 = Collections.singletonList(
    FileStatus.of(FileNames.checkpointFileSingular(logPath, 10).toString, 0, 0))
  private val deltaFs11 = Collections.singletonList(
    FileStatus.of(FileNames.deltaFile(logPath, 11), 0, 0))
  private val badJson = Collections.singletonList(
    FileStatus.of(s"${logPath.toString}/gibberish.json", 0, 0))
  private val badCheckpoint = Collections.singletonList(
    FileStatus.of(s"${logPath.toString}/gibberish.checkpoint.parquet", 0, 0))

  test("LogSegment::constructor -- valid case (empty)") {
    LogSegment.empty(new Path("/a/_delta_log"))
  }

  test("LogSegment::constructor -- valid case (non-empty)") {
    val logPath = new Path("/a/_delta_log")
    new LogSegment(logPath, 11, deltaFs11, checkpointFs10, Optional.of(10), 1)
  }

  test("LogSegment::constructor -- null arguments") {
    // case: logPath is null
    intercept[NullPointerException] {
      new LogSegment(
        null, 1, Collections.emptyList(), Collections.emptyList(), Optional.empty(), -1)
    }
    // case: deltas is null
    intercept[NullPointerException] {
      new LogSegment(
        new Path("/a/_delta_log"), 1, null, Collections.emptyList(), Optional.empty(), -1)
    }
    // case: checkpoints is null
    intercept[NullPointerException] {
      new LogSegment(
        new Path("/a/_delta_log"), 1, Collections.emptyList(), null, Optional.empty(), -1)
    }
    // case: checkpointVersionOpt is null
    intercept[NullPointerException] {
      new LogSegment(
        new Path("/a/_delta_log"), 1, Collections.emptyList(), Collections.emptyList(), null, -1)
    }
  }

  test("LogSegment::constructor -- only one of checkpoints and checkpointVersionOpt is empty") {
    // case: checkpoints is empty but checkpointVersionOpt is not
    val exMsg1 = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 11, Collections.emptyList(), Collections.emptyList(), Optional.of(10), 1)
    }.getMessage
    assert(
      exMsg1 === "checkpoints and checkpointVersionOpt must either be both empty or both non-empty")

    // case: checkpoints is not empty but checkpointVersionOpt is
    val exMsg2 = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 11, Collections.emptyList(), checkpointFs10, Optional.empty(), 1)
    }.getMessage
    assert(
      exMsg2 === "checkpoints and checkpointVersionOpt must either be both empty or both non-empty")
  }

  test("LogSegment::constructor -- pass in non-delta file") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 11, badJson, checkpointFs10, Optional.of(10), 1)
    }.getMessage
    assert(exMsg === "deltas must all be actual delta (commit) files")
  }

  test("LogSegment::constructor -- pass in non-checkpoint file") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 11, deltaFs11, badCheckpoint, Optional.of(10), 1)
    }.getMessage
    assert(exMsg === "checkpoints must all be actual checkpoint files")
  }

  test("LogSegment::constructor -- checkpoint file version != checkpointVersionOpt") {
    // checkpointFs10 has version 10 but checkpointVersionOpt is 5
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 11, deltaFs11, checkpointFs10, Optional.of(5), 1)
    }.getMessage
    assert(exMsg === "All checkpoint files must have the same version as the checkpointVersionOpt")
  }
}
