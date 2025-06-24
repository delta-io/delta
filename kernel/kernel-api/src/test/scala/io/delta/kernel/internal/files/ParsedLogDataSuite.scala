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

package io.delta.kernel.internal.files

import java.util.Optional

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.internal.files.ParsedLogData.{ParsedLogCategory, ParsedLogType}
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.test.{MockFileSystemClientUtils, VectorTestUtils}
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class ParsedLogDataSuite extends AnyFunSuite with MockFileSystemClientUtils with VectorTestUtils {

  /////////////
  // General //
  /////////////

  test("Throws on unknown log file") {
    val fileStatus = FileStatus.of("unknown", 0, 0)
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogData.forFileStatus(fileStatus)
    }.getMessage
    assert(exMsg.contains("Unknown log file type"))
  }

  test("Throws on version < 0") {
    val exMsg = intercept[IllegalArgumentException] {
      ParsedDeltaData.forInlineDelta(-1, emptyColumnarBatch)
    }.getMessage
    assert(exMsg === "version must be non-negative")
  }

  test("Throws on both fileStatusOpt and inlineDataOpt present") {
    val fileStatusOpt = Optional.of(deltaFileStatus(5))
    val inlineDataOpt = Optional.of(emptyColumnarBatch)
    val exMsg = intercept[IllegalArgumentException] {
      new ParsedLogData(10, ParsedLogType.DELTA, fileStatusOpt, inlineDataOpt)
    }.getMessage
    assert(exMsg === "Exactly one of fileStatusOpt or inlineDataOpt must be present")
  }

  test("Throws on both fileStatusOpt and inlineDataOpt empty") {
    val exMsg = intercept[IllegalArgumentException] {
      new ParsedLogData(10, ParsedLogType.DELTA, Optional.empty(), Optional.empty())
    }.getMessage
    assert(exMsg === "Exactly one of fileStatusOpt or inlineDataOpt must be present")
  }

  ////////////
  // Deltas //
  ////////////

  test("Can construct ratified inline commit using forInlineData") {
    val parsed = ParsedDeltaData.forInlineDelta(10, emptyColumnarBatch)
    assert(parsed.`type` == ParsedLogType.DELTA)
  }

  test("Correctly parses published delta file") {
    val fileStatus = deltaFileStatus(5)
    val parsed = ParsedLogData.forFileStatus(fileStatus)

    assert(parsed.version == 5)
    assert(parsed.`type` == ParsedLogType.DELTA)
    assert(parsed.getCategory == ParsedLogCategory.DELTA)
    assert(parsed.isFile)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("Correctly parses staged commit file") {
    val fileStatus = stagedCommitFile(5)
    val parsed = ParsedLogData.forFileStatus(fileStatus)

    assert(parsed.version == 5)
    assert(parsed.`type` == ParsedLogType.DELTA)
    assert(parsed.getCategory == ParsedLogCategory.DELTA)
    assert(parsed.isFile)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("Delta file equality") {
    val fileStatus1 = deltaFileStatus(5)
    val fileStatus2 = deltaFileStatus(5)
    val fileStatus3 = deltaFileStatus(6)

    val delta1 = ParsedLogData.forFileStatus(fileStatus1)
    val delta2 = ParsedLogData.forFileStatus(fileStatus2)
    val delta3 = ParsedLogData.forFileStatus(fileStatus3)

    assert(delta1 == delta1)
    assert(delta1 == delta2)
    assert(delta1 != delta3)
  }

  /////////////////
  // Checkpoints //
  /////////////////

  test("Can construct inline classic checkpoint using forInlineData") {
    val parsed = ParsedCheckpointData.forInlineClassicCheckpoint(10, emptyColumnarBatch)
    assert(parsed.`type` == ParsedLogType.CLASSIC_CHECKPOINT)
  }

  test("Can construct inline v2 checkpoint using forInlineData") {
    val parsed = ParsedCheckpointData.forInlineV2Checkpoint(10, emptyColumnarBatch)
    assert(parsed.`type` == ParsedLogType.V2_CHECKPOINT)
  }

  test("Correctly parses classic checkpoint file") {
    val fileStatus = classicCheckpointFileStatus(10)
    val parsed = ParsedLogData.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedCheckpointData])
    assert(parsed.version == 10)
    assert(parsed.`type` == ParsedLogType.CLASSIC_CHECKPOINT)
    assert(parsed.getCategory == ParsedLogCategory.CHECKPOINT)
    assert(parsed.isFile)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("Classic checkpoint file equality") {
    val cp1 = ParsedLogData.forFileStatus(classicCheckpointFileStatus(10))
    val cp2 = ParsedLogData.forFileStatus(classicCheckpointFileStatus(10))
    val cp3 = ParsedLogData.forFileStatus(classicCheckpointFileStatus(11))

    assert(cp1 == cp1)
    assert(cp1 == cp2)
    assert(cp1 != cp3)
  }

  test("Correctly parses V2 checkpoint file") {
    val fileStatus = v2CheckpointFileStatus(20)
    val parsed = ParsedLogData.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedCheckpointData])
    assert(parsed.version == 20)
    assert(parsed.`type` == ParsedLogType.V2_CHECKPOINT)
    assert(parsed.getCategory == ParsedLogCategory.CHECKPOINT)
    assert(parsed.isFile)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("V2 checkpoint file equality") {
    val parsed1 = ParsedLogData.forFileStatus(v2CheckpointFileStatus(20, useUUID = false))
    val parsed2 = ParsedLogData.forFileStatus(v2CheckpointFileStatus(20, useUUID = false))
    val parsed3 = ParsedLogData.forFileStatus(v2CheckpointFileStatus(21))

    assert(parsed1 == parsed1)
    assert(parsed1 == parsed2)
    assert(parsed1 != parsed3)
  }

  ////////////////////////////
  // Multi-Part Checkpoints //
  ////////////////////////////

  test("Can construct inline multi-part checkpoint using forInlineData") {
    val parsed =
      ParsedMultiPartCheckpointData.forInlineMultiPartCheckpoint(10, 1, 3, emptyColumnarBatch)
    assert(parsed.`type` == ParsedLogType.MULTIPART_CHECKPOINT)
  }

  test("Correctly parses multi-part checkpoint file") {
    val chkpt_15_1_3 = multiPartCheckpointFileStatus(15, 1, 3)
    val parsed = ParsedLogData
      .forFileStatus(chkpt_15_1_3)
      .asInstanceOf[ParsedMultiPartCheckpointData]

    assert(parsed.version == 15)
    assert(parsed.`type` == ParsedLogType.MULTIPART_CHECKPOINT)
    assert(parsed.getCategory == ParsedLogCategory.CHECKPOINT)
    assert(parsed.isFile)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == chkpt_15_1_3)
    assert(parsed.part == 1)
    assert(parsed.numParts == 3)
  }

  test("Throws on multi-part checkpoint with part > numParts") {
    val path = FileNames.multiPartCheckpointFile(logPath, 10, 5, 3) // part = 5, numParts = 3
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogData.forFileStatus(FileStatus.of(path.toString))
    }.getMessage
    assert(exMsg === "part must be between 1 and numParts")
  }

  test("Throws on multi-part checkpoint with numParts = 0") {
    val path = FileNames.multiPartCheckpointFile(logPath, 10, 0, 0) // part = 0, numParts = 0
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogData.forFileStatus(FileStatus.of(path.toString))
    }.getMessage
    assert(exMsg === "numParts must be greater than 0")
  }

  test("Throws on multi-part checkpoint with part = 0") {
    val path = FileNames.multiPartCheckpointFile(logPath, 10, 0, 3) // part = 0, numParts = 3
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogData.forFileStatus(FileStatus.of(path.toString))
    }.getMessage
    assert(exMsg === "part must be between 1 and numParts")
  }

  test("Multi-part checkpoint file equality") {
    val parsed_15_1_3_a = ParsedLogData.forFileStatus(multiPartCheckpointFileStatus(15, 1, 3))
    val parsed_15_1_3_b = ParsedLogData.forFileStatus(multiPartCheckpointFileStatus(15, 1, 3))
    val parsed_15_2_3 = ParsedLogData.forFileStatus(multiPartCheckpointFileStatus(15, 2, 3))
    val parsed_15_1_4 = ParsedLogData.forFileStatus(multiPartCheckpointFileStatus(15, 1, 4))
    val parsed_16_1_3 = ParsedLogData.forFileStatus(multiPartCheckpointFileStatus(16, 1, 3))

    assert(parsed_15_1_3_a == parsed_15_1_3_a)
    assert(parsed_15_1_3_a == parsed_15_1_3_b)
    assert(parsed_15_1_3_a != parsed_15_2_3)
    assert(parsed_15_1_3_a != parsed_15_1_4)
    assert(parsed_15_1_3_a != parsed_16_1_3)
  }

  /////////////////////////
  // Checkpoint ordering //
  /////////////////////////

  test("checkpoint ordering") {
    // _m means materialized, _i means inline

    val classic_12_m = ParsedCheckpointData.forFileStatus(classicCheckpointFileStatus(12))
    val multi_11_3_m = ParsedCheckpointData.forFileStatus(multiPartCheckpointFileStatus(11, 1, 3))
    val v2_10_m = ParsedCheckpointData.forFileStatus(v2CheckpointFileStatus(10))

    val multi_12_3_m = ParsedCheckpointData.forFileStatus(multiPartCheckpointFileStatus(12, 1, 3))
    val v2_12_m = ParsedCheckpointData.forFileStatus(v2CheckpointFileStatus(12))

    val classic_12_i = ParsedCheckpointData.forInlineClassicCheckpoint(12, emptyColumnarBatch)
    val v2_12_i = ParsedCheckpointData.forInlineV2Checkpoint(12, emptyColumnarBatch)

    val multi_12_3_i =
      ParsedMultiPartCheckpointData.forInlineMultiPartCheckpoint(12, 1, 3, emptyColumnarBatch)
    val multi_12_4_m = ParsedCheckpointData.forFileStatus(multiPartCheckpointFileStatus(12, 1, 4))

    val v2_aaa = ParsedCheckpointData.forFileStatus(
      FileStatus.of(FileNames.topLevelV2CheckpointFile(logPath, 10, "aaa", "json").toString))
    val v2_bbb = ParsedCheckpointData.forFileStatus(
      FileStatus.of(FileNames.topLevelV2CheckpointFile(logPath, 10, "bbb", "json").toString))

    // Case 1: Version priority
    classic_12_m should be > multi_11_3_m
    classic_12_m should be > v2_10_m
    multi_11_3_m should be > v2_10_m

    // Case 2: Type priority, when version is tied
    v2_12_m should be > classic_12_m
    v2_12_m should be > multi_12_3_m

    // Case 3: Inline priority, when version and type are tied (and parts are tied for multi)
    classic_12_i should be > classic_12_m
    v2_12_i should be > v2_12_m
    multi_12_3_i should be > multi_12_3_m

    // Case 4: Multi-part checkpoint with more parts has higher priority
    multi_12_4_m should be > multi_12_3_m

    // Case 5: For tied v2, filepath is tie-breaker
    v2_bbb should be > v2_aaa
  }

  /////////////////////
  // Log compactions //
  /////////////////////

  test("Can construct inline log compaction using forInlineData") {
    ParsedLogCompactionData.forInlineLogCompaction(10, 20, emptyColumnarBatch)
  }

  test("Correctly parses log compaction file") {
    val fileStatus = logCompactionStatus(25, 30)
    val parsed = ParsedLogData.forFileStatus(fileStatus).asInstanceOf[ParsedLogCompactionData]

    assert(parsed.version == 30)
    assert(parsed.`type` == ParsedLogType.LOG_COMPACTION)
    assert(parsed.getCategory == ParsedLogCategory.LOG_COMPACTION)
    assert(parsed.isFile)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)
    assert(parsed.startVersion == 25)
    assert(parsed.endVersion == 30)
  }

  test("Throws on log compaction with startVersion < 0") {
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogCompactionData.forInlineLogCompaction(-1, 3, emptyColumnarBatch)
    }.getMessage
    assert(exMsg === "startVersion and endVersion must be non-negative")
  }

  test("Throws on log compaction with endVersion < 0") {
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogCompactionData.forInlineLogCompaction(1, -1, emptyColumnarBatch)
    }.getMessage
    assert(exMsg === "version must be non-negative")
  }

  test("Throws on log compaction with startVersion > endVersion") {
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogCompactionData.forInlineLogCompaction(3, 1, emptyColumnarBatch)
    }.getMessage
    assert(exMsg === "startVersion must be less than endVersion")
  }

  test("Log compaction file equality") {
    val fileStatus1 = logCompactionStatus(25, 30)
    val fileStatus2 = logCompactionStatus(25, 30)
    val fileStatus3 = logCompactionStatus(31, 32)

    val parsed1 = ParsedLogData.forFileStatus(fileStatus1)
    val parsed2 = ParsedLogData.forFileStatus(fileStatus2)
    val parsed3 = ParsedLogData.forFileStatus(fileStatus3)

    assert(parsed1 == parsed1)
    assert(parsed1 == parsed2)
    assert(parsed1 != parsed3)
  }

  //////////////
  // Checksum //
  //////////////

  test("Can construct checksum file using forInlineData") {
    val parsed = ParsedChecksumData.forInlineChecksum(5, emptyColumnarBatch)
    assert(parsed.`type` == ParsedLogType.CHECKSUM)
  }

  test("Correctly parses checksum file") {
    val fileStatus = checksumFileStatus(5)
    val parsed = ParsedLogData.forFileStatus(fileStatus)

    assert(parsed.version == 5)
    assert(parsed.`type` == ParsedLogType.CHECKSUM)
    assert(parsed.getCategory == ParsedLogCategory.CHECKSUM)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("Checksum file equality") {
    val fileStatus1 = checksumFileStatus(5)
    val fileStatus2 = checksumFileStatus(5)
    val fileStatus3 = checksumFileStatus(6)

    val parsed1 = ParsedLogData.forFileStatus(fileStatus1)
    val parsed2 = ParsedLogData.forFileStatus(fileStatus2)
    val parsed3 = ParsedLogData.forFileStatus(fileStatus3)

    assert(parsed1 == parsed1)
    assert(parsed1 == parsed2)
    assert(parsed1 != parsed3)
  }

  //////////////
  // toString //
  //////////////

  test("published delta file toString") {
    val parsed = ParsedLogData.forFileStatus(deltaFileStatus(5))
    // scalastyle:off line.size.limit
    val expected =
      "ParsedDeltaData{version=5, type=DELTA, source=FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000005.json', size=5, modificationTime=50}}"
    // scalastyle:on line.size.limit
    assert(parsed.toString === expected)
  }

  test("multi-part checkpoint toString") {
    val parsed =
      ParsedMultiPartCheckpointData.forInlineMultiPartCheckpoint(10, 1, 3, emptyColumnarBatch)
    // scalastyle:off line.size.limit
    val expected =
      "ParsedMultiPartCheckpointData{version=10, type=MULTIPART_CHECKPOINT, source=inline, part=1, numParts=3}"
    // scalastyle:on line.size.limit
    assert(parsed.toString === expected)
  }

  test("log compaction toString") {
    val parsed = ParsedLogCompactionData.forInlineLogCompaction(10, 20, emptyColumnarBatch)
    // scalastyle:off line.size.limit
    val expected =
      "ParsedLogCompactionData{version=20, type=LOG_COMPACTION, source=inline, startVersion=10}"
    // scalastyle:on line.size.limit
    assert(parsed.toString === expected)
  }
}
