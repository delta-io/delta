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

  test("constructor -- valid case (empty)") {
    LogSegment.empty(new Path("/a/_delta_log"))
  }

  test("constructor -- valid case (non-empty)") {
    val logPath = new Path("/a/_delta_log")
    new LogSegment(logPath, 11, deltaFs11, checkpointFs10, 1)
  }

  test("constructor -- null arguments => throw") {
    // logPath is null
    intercept[NullPointerException] {
      new LogSegment(
        null, 1, Collections.emptyList(), Collections.emptyList(), -1)
    }
    // deltas is null
    intercept[NullPointerException] {
      new LogSegment(
        new Path("/a/_delta_log"), 1, null, Collections.emptyList(), -1)
    }
    // checkpoints is null
    intercept[NullPointerException] {
      new LogSegment(
        new Path("/a/_delta_log"), 1, Collections.emptyList(), null, -1)
    }
  }

  test("constructor -- all deltas must be actual delta files") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 11, badJson, checkpointFs10, 1)
    }.getMessage
    assert(exMsg === "deltas must all be actual delta (commit) files")
  }

  test("constructor -- all checkpoints must be actual checkpoint files") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 11, deltaFs11, badCheckpoint, 1)
    }.getMessage
    assert(exMsg === "checkpoints must all be actual checkpoint files")
  }

  test("constructor -- if version >= 0 then both deltas and checkpoints cannot be empty") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 11, Collections.emptyList(), Collections.emptyList(), 1)
    }.getMessage
    assert(exMsg === "No files to read")
  }

  test("constructor -- if deltas non-empty then first delta must equal checkpointVersion + 1") {
    val deltaFs12 = Collections.singletonList(
      FileStatus.of(FileNames.deltaFile(logPath, 12), 0, 0))
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 12, deltaFs12, checkpointFs10, 1)
    }.getMessage
    assert(exMsg === "First delta file version must equal checkpointVersion + 1")
  }

  test("constructor -- if deltas non-empty then last delta must equal version") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 12, deltaFs11, checkpointFs10, 1)
    }.getMessage
    assert(exMsg === "Last delta file version must equal the version of this LogSegment")
  }

  test("constructor -- if no deltas then checkpointVersion must equal version") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 11, Collections.emptyList(), checkpointFs10, 1)
    }.getMessage
    assert(exMsg ===
      "If there are no deltas, then checkpointVersion must equal the version of this LogSegment")
  }
}
