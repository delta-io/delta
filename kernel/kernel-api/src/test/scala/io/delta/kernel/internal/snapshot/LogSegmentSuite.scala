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

import io.delta.kernel.internal.files.ParsedDeltaData
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.test.{MockFileSystemClientUtils, VectorTestUtils}
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class LogSegmentSuite extends AnyFunSuite with MockFileSystemClientUtils with VectorTestUtils {
  private val checkpointFs10List = singularCheckpointFileStatuses(Seq(10)).toList.asJava
  private val checksumAtVersion10 = checksumFileStatus(10)
  private val deltaFs11List = deltaFileStatuses(Seq(11)).toList.asJava
  private val deltaFs12List = deltaFileStatuses(Seq(12)).toList.asJava
  private val deltasFs11To12List = deltaFileStatuses(Seq(11, 12)).toList.asJava
  private val parsedRatifiedCommits11To12List =
    Seq(11, 12).map(v => ParsedDeltaData.forFileStatus(stagedCommitFile(v))).asJava
  private val compactionFs11To12List = compactedFileStatuses(Seq((11, 12))).toList.asJava
  private val badJsonsList = Collections.singletonList(
    FileStatus.of(s"${logPath.toString}/gibberish.json", 1, 1))
  private val badCheckpointsList = Collections.singletonList(
    FileStatus.of(s"${logPath.toString}/gibberish.checkpoint.parquet", 1, 1))
  private val logPath2 = new Path("/another/fake/path/to/table/", "_delta_log")

  private def createLogSegmentForTest(
      logPath: Path = this.logPath,
      version: Long,
      deltas: JList[FileStatus] = Collections.emptyList(),
      compactions: JList[FileStatus] = Collections.emptyList(),
      checkpoints: JList[FileStatus] = Collections.emptyList(),
      deltaAtEndVersion: Option[FileStatus] = None,
      lastSeenChecksum: Optional[FileStatus] = Optional.empty()): LogSegment = {
    val finalDeltaAtEndVersion = deltaAtEndVersion.getOrElse {
      if (!deltas.isEmpty()) {
        // If we have deltas, use the last delta
        deltas.get(deltas.size() - 1)
      } else if (!checkpoints.isEmpty()) {
        // If we only have checkpoints, create a delta file for the checkpoint version
        val checkpointVersion = io.delta.kernel.internal.util.FileNames.checkpointVersion(
          new Path(checkpoints.get(0).getPath()))
        deltaFileStatus(checkpointVersion)
      } else {
        // If neither deltas nor checkpoints are provided, create a delta for the target version
        deltaFileStatus(version)
      }
    }

    new LogSegment(
      logPath,
      version,
      deltas,
      compactions,
      checkpoints,
      finalDeltaAtEndVersion,
      lastSeenChecksum)
  }

  test("constructor -- valid case (non-empty)") {
    createLogSegmentForTest(
      version = 12,
      deltas = deltasFs11To12List,
      compactions = compactionFs11To12List,
      checkpoints = checkpointFs10List)
  }

  test("constructor -- null arguments => throw") {
    // logPath is null
    intercept[NullPointerException] {
      createLogSegmentForTest(
        logPath = null,
        version = 1)
    }
    // deltas is null
    intercept[NullPointerException] {
      createLogSegmentForTest(
        version = 1,
        deltas = null)
    }
    // compactions is null
    intercept[NullPointerException] {
      createLogSegmentForTest(
        version = 1,
        compactions = null)
    }
    // checkpoints is null
    intercept[NullPointerException] {
      createLogSegmentForTest(
        version = 1,
        checkpoints = null)
    }
    // deltaAtEndVersion is null
    intercept[NullPointerException] {
      createLogSegmentForTest(
        version = 1,
        deltas = Collections.singletonList(deltaFileStatus(1)),
        deltaAtEndVersion = null)
    }
    // lastSeenChecksum is null
    intercept[NullPointerException] {
      createLogSegmentForTest(
        version = 1,
        deltas = Collections.singletonList(deltaFileStatus(1)),
        lastSeenChecksum = null)
    }
  }

  test("constructor -- version must be >= 0") {
    val exMsg = intercept[IllegalArgumentException] {
      createLogSegmentForTest(
        version = -1,
        deltaAtEndVersion = Some(deltaFileStatus(0))
      ) // dummy value
    }.getMessage
    assert(exMsg === "version must be >= 0")
  }

  test("constructor -- all deltas must be actual delta files") {
    val exMsg = intercept[IllegalArgumentException] {
      createLogSegmentForTest(
        version = 12,
        deltas = badJsonsList,
        checkpoints = checkpointFs10List)
    }.getMessage
    assert(exMsg === "deltas must all be actual delta (commit) files")
  }

  test("constructor -- all checkpoints must be actual checkpoint files") {
    val exMsg = intercept[IllegalArgumentException] {
      createLogSegmentForTest(
        version = 12,
        deltas = deltasFs11To12List,
        checkpoints = badCheckpointsList)
    }.getMessage
    assert(exMsg === "checkpoints must all be actual checkpoint files")
  }

  test("constructor -- deltas and checkpoints cannot be empty") {
    val exMsg = intercept[IllegalArgumentException] {
      createLogSegmentForTest(version = 12)
    }.getMessage
    assert(exMsg === "No files to read")
  }

  test("constructor -- checksum version must be <= LogSegment version") {
    val checksumAtVersion13 = checksumFileStatus(13)

    val exMsg = intercept[IllegalArgumentException] {
      createLogSegmentForTest(
        version = 12, // LogSegment version is 12
        deltas = deltasFs11To12List,
        checkpoints = checkpointFs10List,
        lastSeenChecksum = Optional.of(checksumAtVersion13)
      ) // Checksum version is 13
    }.getMessage

    assert(exMsg.contains(
      "checksum file's version should be less than or equal to logSegment's version"))
  }

  test("constructor -- deltaAtEndVersion must match version (checkpoint only)") {
    val exMsg = intercept[IllegalArgumentException] {
      createLogSegmentForTest(
        version = 10,
        checkpoints = checkpointFs10List,
        deltaAtEndVersion = Some(deltaFileStatus(9)) // Wrong version - should be 10
      )
    }.getMessage
    assert(exMsg === "deltaAtEndVersion must have version equal to the version of this LogSegment")
  }

  test("constructor -- deltaAtEndVersion must match version (checkpoint + deltas)") {
    val exMsg = intercept[IllegalArgumentException] {
      createLogSegmentForTest(
        version = 12,
        deltas = deltasFs11To12List,
        checkpoints = checkpointFs10List,
        deltaAtEndVersion = Some(deltaFileStatus(11)) // Wrong version - should be 12
      )
    }.getMessage
    assert(exMsg === "deltaAtEndVersion must have version equal to the version of this LogSegment")
  }

  test("constructor -- checksum version must be >= checkpoint version") {
    val checksumAtVersion9 = checksumFileStatus(9)

    val exMsg = intercept[IllegalArgumentException] {
      createLogSegmentForTest(
        version = 12,
        deltas = deltasFs11To12List,
        checkpoints = checkpointFs10List, // Checkpoint version is 10
        lastSeenChecksum = Optional.of(checksumAtVersion9)
      ) // Checksum version is 9
    }.getMessage

    assert(exMsg.contains(
      "checksum file's version 9 should be greater than or equal to checkpoint version 10"))
  }

  test("constructor -- if deltas non-empty then first delta must equal checkpointVersion + 1") {
    val exMsg = intercept[IllegalArgumentException] {
      createLogSegmentForTest(
        version = 12,
        deltas = deltaFs12List,
        checkpoints = checkpointFs10List)
    }.getMessage
    assert(exMsg === "First delta file version must equal checkpointVersion + 1")
  }

  test("constructor -- if deltas non-empty then last delta must equal version") {
    val exMsg = intercept[IllegalArgumentException] {
      createLogSegmentForTest(
        version = 12,
        deltas = deltaFs11List,
        checkpoints = checkpointFs10List)
    }.getMessage
    assert(exMsg === "Last delta file version must equal the version of this LogSegment")
  }

  test("constructor -- if no deltas then checkpointVersion must equal version") {
    val exMsg = intercept[IllegalArgumentException] {
      createLogSegmentForTest(
        version = 11,
        checkpoints = checkpointFs10List)
    }.getMessage
    assert(exMsg ===
      "If there are no deltas, then checkpointVersion must equal the version of this LogSegment")
  }

  test("constructor -- deltas not contiguous") {
    val deltas = deltaFileStatuses(Seq(11, 13)).toList.asJava
    val exMsg = intercept[IllegalArgumentException] {
      createLogSegmentForTest(
        version = 13,
        deltas = deltas,
        checkpoints = checkpointFs10List)
    }.getMessage
    assert(exMsg === "Delta versions must be contiguous: [11, 13]")
  }

  test("constructor -- delta commit files (JSON) outside of log path") {
    val deltasForDifferentTable =
      deltaFileStatuses(Seq(11, 12), logPath2).toList.asJava
    val ex = intercept[RuntimeException] {
      createLogSegmentForTest(
        version = 12,
        deltas = deltasForDifferentTable,
        checkpoints = checkpointFs10List)
    }
    assert(ex.getMessage.contains("doesn't belong in the transaction log"))
  }

  test("constructor -- compaction log files outside of log path") {
    val compactionsForDifferentTable =
      compactedFileStatuses(Seq((11, 12)), logPath2).toList.asJava
    val ex = intercept[RuntimeException] {
      createLogSegmentForTest(
        version = 12,
        deltas = deltasFs11To12List,
        compactions = compactionsForDifferentTable,
        checkpoints = checkpointFs10List)
    }
    assert(ex.getMessage.contains("doesn't belong in the transaction log"))
  }

  test("constructor -- checkpoint files (parquet) outside of log path") {
    val checkpointsForDifferentTable =
      singularCheckpointFileStatuses(Seq(10), logPath2).toList.asJava
    val ex = intercept[RuntimeException] {
      createLogSegmentForTest(
        version = 12,
        deltas = deltasFs11To12List,
        checkpoints = checkpointsForDifferentTable)
    }
    assert(ex.getMessage.contains("doesn't belong in the transaction log"))
  }

  test("toString") {
    val logSegment = createLogSegmentForTest(
      version = 12,
      deltas = deltasFs11To12List,
      checkpoints = checkpointFs10List,
      lastSeenChecksum = Optional.of(checksumAtVersion10))
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
        |  deltaAtEndVersion=FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000012.json', size=12, modificationTime=120},
        |  lastSeenChecksum=FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000010.crc', size=10, modificationTime=10},
        |  checkpointVersion=10
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
    val segment = createLogSegmentForTest(
      version = version,
      deltas = deltas_list,
      compactions = compactions_list)
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

  ////////////////////////////////////
  // copyWithAdditionalDeltas tests //
  ////////////////////////////////

  test("copyWithAdditionalDeltas: single additional delta") {
    val baseSegment = createLogSegmentForTest(
      version = 10,
      checkpoints = checkpointFs10List)

    val additionalDeltas = List(ParsedDeltaData.forFileStatus(stagedCommitFile(11))).asJava
    val updated = baseSegment.copyWithAdditionalDeltas(additionalDeltas)

    assert(updated.getVersion === 11)
    assert(updated.getDeltas.size() === 1)
  }

  test("copyWithAdditionalDeltas: multiple additional deltas") {
    val baseSegment = createLogSegmentForTest(
      version = 10,
      checkpoints = checkpointFs10List)

    val updated = baseSegment.copyWithAdditionalDeltas(parsedRatifiedCommits11To12List)

    assert(updated.getVersion === 12)
    assert(updated.getDeltas.size() === 2)
  }

  test("copyWithAdditionalDeltas: empty list returns same segment") {
    val baseSegment = createLogSegmentForTest(
      version = 10,
      checkpoints = checkpointFs10List)

    val updated = baseSegment.copyWithAdditionalDeltas(Collections.emptyList())
    assert(updated eq baseSegment)
  }

  test("copyWithAdditionalDeltas: first delta must be version + 1") {
    val baseSegment = createLogSegmentForTest(
      version = 10,
      checkpoints = checkpointFs10List)

    val wrongVersionDeltas = List(ParsedDeltaData.forFileStatus(stagedCommitFile(12))).asJava
    val exMsg = intercept[IllegalArgumentException] {
      baseSegment.copyWithAdditionalDeltas(wrongVersionDeltas)
    }.getMessage
    assert(exMsg.contains("First additional delta version 12 must equal current version + 1 (11)"))
  }

  test("copyWithAdditionalDeltas: deltas must be contiguous") {
    val baseSegment = createLogSegmentForTest(
      version = 10,
      checkpoints = checkpointFs10List)

    val nonContiguousDeltas = List(
      ParsedDeltaData.forFileStatus(stagedCommitFile(11)),
      ParsedDeltaData.forFileStatus(stagedCommitFile(13))).asJava
    val exMsg = intercept[IllegalArgumentException] {
      baseSegment.copyWithAdditionalDeltas(nonContiguousDeltas)
    }.getMessage
    assert(exMsg.contains("Delta versions must be contiguous. Expected 12 but got 13"))
  }

  test("copyWithAdditionalDeltas: inline delta fails") {
    val baseSegment = createLogSegmentForTest(
      version = 10,
      checkpoints = checkpointFs10List)

    // Create inline ParsedDeltaData (not file-based)
    val inlineDelta = ParsedDeltaData.forInlineData(11, emptyColumnarBatch)
    val inlineDeltas = List(inlineDelta).asJava

    val exMsg = intercept[IllegalArgumentException] {
      baseSegment.copyWithAdditionalDeltas(inlineDeltas)
    }.getMessage
    assert(exMsg.contains("Currently, only file-based deltas are supported"))
  }

  ///////////////////////////
  // fromSingleDelta tests //
  ///////////////////////////

  test("fromSingleDelta -- creates valid LogSegment") {
    val deltaData = ParsedDeltaData.forFileStatus(deltaFileStatus(0))
    val logSegment = LogSegment.fromSingleDelta(logPath, deltaData)

    assert(logSegment.getVersion === 0)
    assert(logSegment.getDeltas.size() === 1)
    assert(logSegment.getCheckpoints.isEmpty)
    assert(logSegment.getCompactions.isEmpty)
    assert(logSegment.getLastSeenChecksum === Optional.empty())
  }

  test("fromSingleDelta -- non-zero version fails") {
    val deltaData = ParsedDeltaData.forFileStatus(deltaFileStatus(1))
    val exMsg = intercept[IllegalArgumentException] {
      LogSegment.fromSingleDelta(logPath, deltaData)
    }.getMessage
    assert(exMsg.contains("Version must be 0 for a LogSegment with only a single delta"))
  }

  test("fromSingleDelta -- inline delta fails") {
    val inlineDelta = ParsedDeltaData.forInlineData(0, emptyColumnarBatch)
    val exMsg = intercept[IllegalArgumentException] {
      LogSegment.fromSingleDelta(logPath, inlineDelta)
    }.getMessage
    assert(exMsg.contains("Currently, only file-based deltas are supported"))
  }

}
