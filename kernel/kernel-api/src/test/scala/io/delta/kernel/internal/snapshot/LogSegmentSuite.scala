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

import java.util.{Collections, List => JList, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.internal.fs.Path
import io.delta.kernel.test.MockFileSystemClientUtils
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class LogSegmentSuite extends AnyFunSuite with MockFileSystemClientUtils {
  private val checkpointFs10List = singularCheckpointFileStatuses(Seq(10)).toList.asJava
  private val checksumAtVersion10 = checksumFileStatus(10)
  private val deltaFs11List = deltaFileStatuses(Seq(11)).toList.asJava
  private val deltaFs12List = deltaFileStatuses(Seq(12)).toList.asJava
  private val deltasFs11To12List = deltaFileStatuses(Seq(11, 12)).toList.asJava
  private val compactionFs11To12List = compactedFileStatuses(Seq((11, 12))).toList.asJava
  private val badJsonsList = Collections.singletonList(
    FileStatus.of(s"${logPath.toString}/gibberish.json", 1, 1))
  private val badCheckpointsList = Collections.singletonList(
    FileStatus.of(s"${logPath.toString}/gibberish.checkpoint.parquet", 1, 1))
  private val logPath2 = new Path("/another/fake/path/to/table/", "_delta_log")

  test("constructor -- valid case (non-empty)") {
    new LogSegment(
      logPath,
      12,
      deltasFs11To12List,
      compactionFs11To12List,
      checkpointFs10List,
      Optional.empty(),
      1)
  }

  test("constructor -- null arguments => throw") {
    // logPath is null
    intercept[NullPointerException] {
      new LogSegment(
        null,
        1,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Optional.empty(),
        -1)
    }
    // deltas is null
    intercept[NullPointerException] {
      new LogSegment(
        logPath,
        1,
        null,
        Collections.emptyList(),
        Collections.emptyList(),
        Optional.empty(),
        -1)
    }
    // compactions is null
    intercept[NullPointerException] {
      new LogSegment(
        logPath,
        1,
        Collections.emptyList(),
        null,
        Collections.emptyList(),
        Optional.empty(),
        -1)
    }
    // checkpoints is null
    intercept[NullPointerException] {
      new LogSegment(
        logPath,
        1,
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        Optional.empty(),
        -1)
    }
  }

  test("constructor -- non-empty deltas or checkpoints with version -1 => throw") {
    val exMsg1 = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath,
        -1,
        deltasFs11To12List,
        Collections.emptyList(),
        Collections.emptyList(),
        Optional.empty(),
        1)
    }.getMessage
    assert(exMsg1 === "Version -1 should have no files")

    val exMsg2 = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath,
        -1,
        Collections.emptyList(),
        Collections.emptyList(),
        checkpointFs10List,
        Optional.empty(),
        1)
    }.getMessage
    assert(exMsg2 === "Version -1 should have no files")
  }

  test("constructor -- all deltas must be actual delta files") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath,
        12,
        badJsonsList,
        Collections.emptyList(),
        checkpointFs10List,
        Optional.empty(),
        1)
    }.getMessage
    assert(exMsg === "deltas must all be actual delta (commit) files")
  }

  test("constructor -- all checkpoints must be actual checkpoint files") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath,
        12,
        deltasFs11To12List,
        Collections.emptyList(),
        badCheckpointsList,
        Optional.empty(),
        1)
    }.getMessage
    assert(exMsg === "checkpoints must all be actual checkpoint files")
  }

  test("constructor -- if version >= 0 then both deltas and checkpoints cannot be empty") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath,
        12,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Optional.empty(),
        1)
    }.getMessage
    assert(exMsg === "No files to read")
  }

  test("constructor -- checksum version must be <= LogSegment version") {
    val checksumAtVersion13 = checksumFileStatus(13)

    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath,
        12, // LogSegment version is 12
        deltasFs11To12List,
        Collections.emptyList(),
        checkpointFs10List,
        Optional.of(checksumAtVersion13), // Checksum version is 13
        1)
    }.getMessage

    assert(exMsg.contains(
      "checksum file's version should be less than or equal to logSegment's version"))
  }

  test("constructor -- checksum version must be <= checkpoint version") {
    val checksumAtVersion9 = checksumFileStatus(9)

    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath,
        12,
        deltasFs11To12List,
        Collections.emptyList(),
        checkpointFs10List, // Checkpoint version is 10
        Optional.of(checksumAtVersion9), // Checksum version is 9
        1)
    }.getMessage

    assert(exMsg.contains(
      "checksum file's version 9 should be greater than or equal to checkpoint version 10"))
  }

  test("constructor -- if deltas non-empty then first delta must equal checkpointVersion + 1") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath,
        12,
        deltaFs12List,
        Collections.emptyList(),
        checkpointFs10List,
        Optional.empty(),
        1)
    }.getMessage
    assert(exMsg === "First delta file version must equal checkpointVersion + 1")
  }

  test("constructor -- if deltas non-empty then last delta must equal version") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath,
        12,
        deltaFs11List,
        Collections.emptyList(),
        checkpointFs10List,
        Optional.empty(),
        1)
    }.getMessage
    assert(exMsg === "Last delta file version must equal the version of this LogSegment")
  }

  test("constructor -- if no deltas then checkpointVersion must equal version") {
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath,
        11,
        Collections.emptyList(),
        Collections.emptyList(),
        checkpointFs10List,
        Optional.empty(),
        1)
    }.getMessage
    assert(exMsg ===
      "If there are no deltas, then checkpointVersion must equal the version of this LogSegment")
  }

  test("constructor -- deltas not contiguous") {
    val deltas = deltaFileStatuses(Seq(11, 13)).toList.asJava
    val exMsg = intercept[IllegalArgumentException] {
      new LogSegment(
        logPath,
        13,
        deltas,
        Collections.emptyList(),
        checkpointFs10List,
        Optional.empty(),
        1)
    }.getMessage
    assert(exMsg === "Delta versions must be contiguous: [11, 13]")
  }

  test("constructor -- delta commit files (JSON) outside of log path") {
    val deltasForDifferentTable =
      deltaFileStatuses(Seq(11, 12), logPath2).toList.asJava
    val ex = intercept[RuntimeException] {
      new LogSegment(
        logPath,
        12,
        deltasForDifferentTable,
        Collections.emptyList(),
        checkpointFs10List,
        Optional.empty(),
        1)
    }
    assert(ex.getMessage.contains("doesn't belong in the transaction log"))
  }

  test("constructor -- compaction log files outside of log path") {
    val compactionsForDifferentTable =
      compactedFileStatuses(Seq((11, 12)), logPath2).toList.asJava
    val ex = intercept[RuntimeException] {
      new LogSegment(
        logPath,
        12,
        deltasFs11To12List,
        compactionsForDifferentTable,
        checkpointFs10List,
        Optional.empty(),
        1)
    }
    assert(ex.getMessage.contains("doesn't belong in the transaction log"))
  }

  test("constructor -- checkpoint files (parquet) outside of log path") {
    val checkpointsForDifferentTable =
      singularCheckpointFileStatuses(Seq(10), logPath2).toList.asJava
    val ex = intercept[RuntimeException] {
      new LogSegment(
        logPath,
        12,
        deltasFs11To12List,
        Collections.emptyList(),
        checkpointsForDifferentTable,
        Optional.empty(),
        1)
    }
    assert(ex.getMessage.contains("doesn't belong in the transaction log"))
  }

  test("toString") {
    val logSegment = new LogSegment(
      logPath,
      12,
      deltasFs11To12List,
      Collections.emptyList(),
      checkpointFs10List,
      Optional.of(checksumAtVersion10),
      1)
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
        |  lastSeenChecksum=FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000010.crc', size=10, modificationTime=10},
        |  checkpointVersion=10,
        |  lastCommitTimestamp=1
        |}""".stripMargin
    // scalastyle:on line.size.limit
    assert(logSegment.toString === expectedToString)
  }

  private def parseExpectedString(expected: String): JList[FileStatus] = {
    expected.split(",").map(_.trim).map { item =>
      if (item.contains("-")) {
        // compaction file contains a -
        val parts = item.split("-").map(_.trim.toLong)
        logCompactionStatus(parts(0), parts(1))
      } else {
        // delta file does not
        deltaFileStatus(item.toLong)
      }
    }.toList.asJava
  }

  private def testCompactionCase(
      deltas: Seq[Long],
      compactions: Seq[(Long, Long)],
      expected: String): Unit = {
    val version = deltas.max
    val deltas_list = deltaFileStatuses(deltas).toList.asJava
    val compactions_list = compactedFileStatuses(compactions).toList.asJava
    val segment = new LogSegment(
      logPath,
      version,
      deltas_list,
      compactions_list,
      Collections.emptyList(),
      Optional.empty(),
      1)
    val expectedFiles = parseExpectedString(expected)
    assert(segment.allFilesWithCompactionsReversed() === expectedFiles)
  }

  test("allFilesWithCompactionsReversed -- 3 - 5 in middle") {
    testCompactionCase(
      Seq.range(0, 7),
      Seq((3, 5)),
      "6, 3-5, 2, 1, 0")
  }

  test("allFilesWithCompactionsReversed -- 3 - 5 at start") {
    testCompactionCase(
      Seq.range(3, 8),
      Seq((3, 5)),
      "7, 6, 3-5")
  }

  test("allFilesWithCompactionsReversed -- 3 - 5 at end") {
    testCompactionCase(
      Seq.range(0, 6),
      Seq((3, 5)),
      "3-5, 2, 1, 0")
  }

  test("allFilesWithCompactionsReversed -- 3 - 5 at second to last") {
    testCompactionCase(
      Seq.range(2, 7),
      Seq((3, 5)),
      "6, 3-5, 2")
  }

  test("allFilesWithCompactionsReversed -- 3 - 5, and 7 - 9") {
    testCompactionCase(
      Seq.range(1, 11),
      Seq((3, 5), (7, 9)),
      "10, 7-9, 6, 3-5, 2, 1")
  }

  test("allFilesWithCompactionsReversed -- 3 - 5, and 4 - 8 (overlap)") {
    testCompactionCase(
      Seq.range(2, 11),
      Seq((3, 5), (4, 8)),
      "10, 9, 4-8, 3, 2")
  }

  test("allFilesWithCompactionsReversed -- 3 - 5, whole range") {
    testCompactionCase(
      Seq.range(3, 6),
      Seq((3, 5)),
      "3-5")
  }

  test("allFilesWithCompactionsReversed -- consecutive compactions") {
    testCompactionCase(
      Seq.range(0, 13),
      Seq((3, 5), (6, 8), (9, 11)),
      "12, 9-11, 6-8, 3-5, 2, 1, 0")
  }

  test("allFilesWithCompactionsReversed -- contained range") {
    testCompactionCase(
      Seq.range(1, 12),
      Seq((2, 10), (4, 8)),
      "11, 2-10, 1")
  }

  test("allFilesWithCompactionsReversed -- complex ranges") {
    testCompactionCase(
      Seq.range(0, 21),
      Seq((1, 3), (1, 5), (7, 10), (11, 14), (11, 12), (16, 20), (18, 20)),
      "16-20, 15, 11-14, 7-10, 6, 1-5, 0")
  }

  test("assertLogFilesBelongToTable should pass for correct log paths") {
    val tablePath = new Path("s3://bucket/logPath")
    val logFiles = List(
      FileStatus.of("s3://bucket/logPath/deltafile1", 0L, 0L),
      FileStatus.of("s3://bucket/logPath/deltafile2", 0L, 0L),
      FileStatus.of("s3://bucket/logPath/checkpointfile1", 0L, 0L),
      FileStatus.of("s3://bucket/logPath/checkpointfile2", 0L, 0L)).asJava

    LogSegment.assertLogFilesBelongToTable(tablePath, logFiles)
  }

  test("assertLogFilesBelongToTable should fail for incorrect log paths") {
    val tablePath = new Path("s3://bucket/logPath")
    val logFiles = List(
      FileStatus.of("s3://bucket/logPath/deltafile1", 0L, 0L),
      FileStatus.of("s3://bucket/invalidLogPath/deltafile2", 0L, 0L),
      FileStatus.of("s3://bucket/logPath/checkpointfile1", 0L, 0L),
      FileStatus.of("s3://bucket/invalidLogPath/checkpointfile2", 0L, 0L)).asJava

    // Test that files with incorrect log paths trigger the assertion
    val ex = intercept[RuntimeException] {
      LogSegment.assertLogFilesBelongToTable(tablePath, logFiles)
    }
    assert(ex.getMessage.contains("File (s3://bucket/invalidLogPath/deltafile2) " +
      s"doesn't belong in the transaction log at $tablePath"))
  }

  test("getDeltaFileForVersion") {
    val logSegment = new LogSegment(
      logPath,
      12,
      deltasFs11To12List,
      Collections.emptyList(),
      checkpointFs10List,
      Optional.empty(),
      1)

    // Positive Cases
    assert(logSegment.getDeltaFileForVersion(11).get() === deltasFs11To12List.get(0)) // first
    assert(logSegment.getDeltaFileForVersion(12).get() === deltasFs11To12List.get(1)) // last

    // Negative Case
    assert(!logSegment.getDeltaFileForVersion(0).isPresent)
    assert(!logSegment.getDeltaFileForVersion(10).isPresent)

    // Illegal Cases
    val exMsg1 = intercept[IllegalArgumentException] {
      logSegment.getDeltaFileForVersion(-1)
    }.getMessage
    assert(exMsg1 === "deltaVersion must be non-negative")

    val exMsg2 = intercept[IllegalArgumentException] {
      logSegment.getDeltaFileForVersion(13)
    }.getMessage
    assert(exMsg2 === "deltaVersion must be <= logSegment version")
  }

}
