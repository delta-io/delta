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

import java.util.Arrays
import java.util.{Collections, Optional}

import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.FileStatus
import org.scalatest.funsuite.AnyFunSuite

class LogSegmentSuite extends AnyFunSuite {
  private val logPath = new Path("/a/_delta_log")
  private val checkpointFs10 =
    FileStatus.of(FileNames.checkpointFileSingular(logPath, 10).toString, 1, 1)
  private val checkpointFs10List = Collections.singletonList(checkpointFs10)
  private val deltaFs11 = FileStatus.of(FileNames.deltaFile(logPath, 11), 1, 1)
  private val deltaFs11List = Collections.singletonList(deltaFs11)
  private val deltaFs12 = FileStatus.of(FileNames.deltaFile(logPath, 12), 1, 1)
  private val deltaFs12List = Collections.singletonList(deltaFs12)
  private val deltasFs11To12List = Arrays.asList(deltaFs11, deltaFs12)
  private val badJsonsList = Collections.singletonList(
    FileStatus.of(s"${logPath.toString}/gibberish.json", 1, 1))
  private val badCheckpointsList = Collections.singletonList(
    FileStatus.of(s"${logPath.toString}/gibberish.checkpoint.parquet", 1, 1))

  test("constructor -- valid case (empty)") {
    LogSegment.empty(new Path("/a/_delta_log"))
  }

  test("constructor -- valid case (non-empty)") {
    val logPath = new Path("/a/_delta_log")
    new LogSegment(logPath, 12, deltasFs11To12List, checkpointFs10List, 1)
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
        logPath, 12, badJsonsList, checkpointFs10List, 1)
    }.getMessage
    assert(exMsg === "deltas must all be actual delta (commit) files")
  }

  test("constructor -- all checkpoints must be actual checkpoint files") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 12, deltasFs11To12List, badCheckpointsList, 1)
    }.getMessage
    assert(exMsg === "checkpoints must all be actual checkpoint files")
  }

  test("constructor -- if version >= 0 then both deltas and checkpoints cannot be empty") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 12, Collections.emptyList(), Collections.emptyList(), 1)
    }.getMessage
    assert(exMsg === "No files to read")
  }

  test("constructor -- if deltas non-empty then first delta must equal checkpointVersion + 1") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 12, deltaFs12List, checkpointFs10List, 1)
    }.getMessage
    assert(exMsg === "First delta file version must equal checkpointVersion + 1")
  }

  test("constructor -- if deltas non-empty then last delta must equal version") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 12, deltaFs11List, checkpointFs10List, 1)
    }.getMessage
    assert(exMsg === "Last delta file version must equal the version of this LogSegment")
  }

  test("constructor -- if no deltas then checkpointVersion must equal version") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 11, Collections.emptyList(), checkpointFs10List, 1)
    }.getMessage
    assert(exMsg ===
      "If there are no deltas, then checkpointVersion must equal the version of this LogSegment")
  }
}
