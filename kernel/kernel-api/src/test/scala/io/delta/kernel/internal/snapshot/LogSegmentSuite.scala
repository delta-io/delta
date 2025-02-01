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
import scala.collection.JavaConverters._
import io.delta.kernel.test.MockFileSystemClientUtils
import io.delta.kernel.utils.FileStatus
import org.scalatest.funsuite.AnyFunSuite

class LogSegmentSuite extends AnyFunSuite with MockFileSystemClientUtils {
  private val checkpointFs10List = singularCheckpointFileStatuses(Seq(10)).toList.asJava
  private val checksumAtVersion10 = checksumFileStatus(10)
  private val deltaFs11List = deltaFileStatuses(Seq(11)).toList.asJava
  private val deltaFs12List = deltaFileStatuses(Seq(12)).toList.asJava
  private val deltasFs11To12List = deltaFileStatuses(Seq(11, 12)).toList.asJava
  private val badJsonsList = Collections.singletonList(
    FileStatus.of(s"${logPath.toString}/gibberish.json", 1, 1))
  private val badCheckpointsList = Collections.singletonList(
    FileStatus.of(s"${logPath.toString}/gibberish.checkpoint.parquet", 1, 1))

  test("constructor -- valid case (empty)") {
    LogSegment.empty(logPath)
  }

  test("constructor -- valid case (non-empty)") {
    new LogSegment(logPath, 12, deltasFs11To12List, checkpointFs10List, Optional.empty(), 1)
  }

  test("constructor -- null arguments => throw") {
    // logPath is null
    intercept[NullPointerException] {
      new LogSegment(
        null, 1, Collections.emptyList(), Collections.emptyList(), Optional.empty(), -1)
    }
    // deltas is null
    intercept[NullPointerException] {
      new LogSegment(logPath, 1, null, Collections.emptyList(), Optional.empty(), -1)
    }
    // checkpoints is null
    intercept[NullPointerException] {
      new LogSegment(logPath, 1, Collections.emptyList(), null, Optional.empty(), -1)
    }
  }

  test("constructor -- non-empty deltas or checkpoints with version -1 => throw") {
    val exMsg1 = intercept[IllegalArgumentException] {
      new LogSegment(logPath, -1, deltasFs11To12List, Collections.emptyList(), Optional.empty(), 1)
    }.getMessage
    assert(exMsg1 === "Version -1 should have no files")

    val exMsg2 = intercept[IllegalArgumentException] {
      new LogSegment(logPath, -1, Collections.emptyList(), checkpointFs10List, Optional.empty(), 1)
    }.getMessage
    assert(exMsg2 === "Version -1 should have no files")
  }

  test("constructor -- all deltas must be actual delta files") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(logPath, 12, badJsonsList, checkpointFs10List, Optional.empty(), 1)
    }.getMessage
    assert(exMsg === "deltas must all be actual delta (commit) files")
  }

  test("constructor -- all checkpoints must be actual checkpoint files") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(logPath, 12, deltasFs11To12List, badCheckpointsList, Optional.empty(), 1)
    }.getMessage
    assert(exMsg === "checkpoints must all be actual checkpoint files")
  }

  test("constructor -- if version >= 0 then both deltas and checkpoints cannot be empty") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath, 12, Collections.emptyList(), Collections.emptyList(), Optional.empty(), 1)
    }.getMessage
    assert(exMsg === "No files to read")
  }

  test("constructor -- if deltas non-empty then first delta must equal checkpointVersion + 1") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(logPath, 12, deltaFs12List, checkpointFs10List, Optional.empty(), 1)
    }.getMessage
    assert(exMsg === "First delta file version must equal checkpointVersion + 1")
  }

  test("constructor -- if deltas non-empty then last delta must equal version") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(logPath, 12, deltaFs11List, checkpointFs10List, Optional.empty(), 1)
    }.getMessage
    assert(exMsg === "Last delta file version must equal the version of this LogSegment")
  }

  test("constructor -- if no deltas then checkpointVersion must equal version") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(logPath, 11, Collections.emptyList(), checkpointFs10List, Optional.empty(), 1)
    }.getMessage
    assert(exMsg ===
      "If there are no deltas, then checkpointVersion must equal the version of this LogSegment")
  }

  test("constructor -- deltas not contiguous") {
    val deltas = deltaFileStatuses(Seq(11, 13)).toList.asJava
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(logPath, 13, deltas, checkpointFs10List, Optional.empty(), 1)
    }.getMessage
    assert(exMsg === "Delta versions must be contiguous: [11, 13]")
  }

  test("isComplete") {
    {
      // case 1: checkpoint and deltas => complete
      val logSegment =
        new LogSegment(logPath, 12, deltasFs11To12List, checkpointFs10List, Optional.empty(), 1)
      assert(logSegment.isComplete)
    }
    {
      // case 2: checkpoint only => complete
      val logSegment = new LogSegment(
        logPath,
        10,
        Collections.emptyList(),
        checkpointFs10List,
        Optional.empty(),
        1
      )
      assert(logSegment.isComplete)
    }
    {
      // case 3: deltas from 0 to N with no checkpoint => complete
      val deltaFiles = deltaFileStatuses((0L to 17L)).toList.asJava
      val logSegment =
        new LogSegment(logPath, 17, deltaFiles, Collections.emptyList(), Optional.empty(), 1)
      assert(logSegment.isComplete)
    }
    {
      // case 4: just deltas from 11 to 12 with no checkpoint => incomplete
      val logSegment = new LogSegment(
        logPath,
        12,
        deltasFs11To12List,
        Collections.emptyList(),
        Optional.empty(),
        1
      )
      assert(!logSegment.isComplete)
    }
    {
      // case 5: empty log segment => incomplete
      assert(!LogSegment.empty(logPath).isComplete)
    }
  }

  test("toString") {
    val logSegment = new LogSegment(
      logPath, 12, deltasFs11To12List, checkpointFs10List, Optional.of(checksumAtVersion10), 1)
    // scalastyle:off line.size.limit
    val expectedToString =
      """LogSegment {
        |  logPath='/fake/path/to/table/_delta_log',
        |  version=12,
        |  deltas=[
        |    FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000011.json', size=11, modificationTime=110},
        |    FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000012.json', size=12, modificationTime=120}
        |  ],
        |  checkpoints=[
        |    FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000010.checkpoint.parquet', size=10, modificationTime=100}
        |  ],
        |  lastSeenCheckSum=FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000010.crc', size=10, modificationTime=10},
        |  checkpointVersion=10,
        |  lastCommitTimestamp=1
        |}""".stripMargin
    // scalastyle:on line.size.limit
    assert(logSegment.toString === expectedToString)
  }

}
