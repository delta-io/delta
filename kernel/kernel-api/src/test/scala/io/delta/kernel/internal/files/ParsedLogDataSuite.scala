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

import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.test.{MockFileSystemClientUtils, VectorTestUtils}
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class ParsedLogDataSuite extends AnyFunSuite with MockFileSystemClientUtils with VectorTestUtils {

  private val emptyInlineData = emptyColumnarBatch

  /////////////
  // General //
  /////////////

  test("ParsedLogData throws on unknown log file") {
    val fileStatus = FileStatus.of("unknown", 0, 0)
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogData.forFileStatus(fileStatus)
    }.getMessage
    assert(exMsg.contains("Unknown log file type"))
  }

  test("ParsedLogData (super) throws on version < 0") {
    val exMsg = intercept[IllegalArgumentException] {
      ParsedCatalogCommitData.forInlineData(-1, emptyInlineData)
    }.getMessage
    assert(exMsg === "version must be non-negative")
  }

  test("ParsedLogData: different types are not equal") {
    val delta = ParsedLogData.forFileStatus(deltaFileStatus(5))
    val checksum = ParsedLogData.forFileStatus(checksumFileStatus(5))
    val checkpoint = ParsedLogData.forFileStatus(classicCheckpointFileStatus(5))
    val logCompaction = ParsedLogData.forFileStatus(logCompactionStatus(5, 10))

    assert(delta != checksum)
    assert(delta != checkpoint)
    assert(delta != logCompaction)
    assert(checksum != checkpoint)
    assert(checksum != logCompaction)
    assert(checkpoint != logCompaction)
  }

  //////////////////////////////
  // ParsedPublishedDeltaData //
  //////////////////////////////

  test("ParsedLogData.forFileStatus(publishedDelta) creates a ParsedPublishedDeltaData") {
    val fileStatus = deltaFileStatus(5)
    val parsed = ParsedLogData.forFileStatus(fileStatus)
    assert(parsed.isInstanceOf[ParsedPublishedDeltaData])
  }

  test("ParsedPublishedDeltaData: correctly parses published delta file") {
    val fileStatus = deltaFileStatus(5)
    val parsed = ParsedPublishedDeltaData.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedDeltaData])
    assert(parsed.getVersion == 5)
    assert(parsed.isFile)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("ParsedPublishedDeltaData: throws on staged commit file") {
    val fileStatus = stagedCommitFile(5)
    val exMsg = intercept[IllegalArgumentException] {
      ParsedPublishedDeltaData.forFileStatus(fileStatus)
    }.getMessage
    assert(exMsg.contains("Expected a published Delta file but got"))
  }

  test("ParsedDeltaData: equality") {
    val fileStatus1 = deltaFileStatus(5)
    val fileStatus2 = deltaFileStatus(5)
    val fileStatus3 = deltaFileStatus(6)

    val delta1 = ParsedPublishedDeltaData.forFileStatus(fileStatus1)
    val delta2 = ParsedPublishedDeltaData.forFileStatus(fileStatus2)
    val delta3 = ParsedPublishedDeltaData.forFileStatus(fileStatus3)

    assert(delta1 == delta1)
    assert(delta1 == delta2)
    assert(delta1 != delta3)
  }

  /////////////////////////////
  // ParsedCatalogCommitData //
  /////////////////////////////

  test("ParsedLogData.forFileStatus(stagedCommit) creates a ParsedCatalogCommitData") {
    val fileStatus = stagedCommitFile(5)
    val parsed = ParsedLogData.forFileStatus(fileStatus)
    assert(parsed.isInstanceOf[ParsedCatalogCommitData])
  }

  test("ParsedCatalogCommitData: correctly parses staged commit file") {
    val fileStatus = stagedCommitFile(5)
    val parsed = ParsedCatalogCommitData.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedDeltaData])
    assert(parsed.getVersion == 5)
    assert(parsed.isFile)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("ParsedCatalogCommitData: can construct inline data") {
    val parsed = ParsedCatalogCommitData.forInlineData(10, emptyInlineData)
    assert(parsed.getVersion == 10)
    assert(parsed.isInline)
    assert(!parsed.isFile)
    assert(parsed.getInlineData == emptyInlineData)
  }

  test("ParsedCatalogCommitData: throws on published delta file") {
    val fileStatus = deltaFileStatus(5)
    val exMsg = intercept[IllegalArgumentException] {
      ParsedCatalogCommitData.forFileStatus(fileStatus)
    }.getMessage
    assert(exMsg.contains("Expected a staged commit file but got"))
  }

  test("ParsedCatalogCommitData: equality") {
    val fileStatus1 = stagedCommitFile(5)
    val fileStatus3 = stagedCommitFile(6)

    val catalogCommit1 = ParsedCatalogCommitData.forFileStatus(fileStatus1)
    val catalogCommit2 = ParsedCatalogCommitData.forFileStatus(fileStatus1)
    val catalogCommit3 = ParsedCatalogCommitData.forFileStatus(fileStatus3)

    assert(catalogCommit1 == catalogCommit1)
    assert(catalogCommit1 == catalogCommit2)
    assert(catalogCommit1 != catalogCommit3)
  }

  //////////////////////////
  // ParsedCheckpointData //
  //////////////////////////

  test("ParsedClassicCheckpointData: throws on non-classic checkpoint file") {
    val fileStatus = deltaFileStatus(5)
    val exMsg = intercept[IllegalArgumentException] {
      ParsedClassicCheckpointData.forFileStatus(fileStatus)
    }.getMessage
    assert(exMsg.contains("Expected a classic checkpoint file but got"))
  }

  test("ParsedClassicCheckpointData: correctly parses classic checkpoint file") {
    val fileStatus = classicCheckpointFileStatus(10)
    val parsed = ParsedLogData.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedClassicCheckpointData])
    assert(parsed.getVersion == 10)
    assert(parsed.getGroupByCategoryClass == classOf[ParsedCheckpointData])
    assert(parsed.isFile)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("ParsedClassicCheckpointData: equality") {
    val cp1 = ParsedLogData.forFileStatus(classicCheckpointFileStatus(10))
    val cp2 = ParsedLogData.forFileStatus(classicCheckpointFileStatus(10))
    val cp3 = ParsedLogData.forFileStatus(classicCheckpointFileStatus(11))

    assert(cp1 == cp1)
    assert(cp1 == cp2)
    assert(cp1 != cp3)
  }

  test("ParsedV2CheckpointData: throws on non-V2 checkpoint file") {
    val fileStatus = deltaFileStatus(5)
    val exMsg = intercept[IllegalArgumentException] {
      ParsedV2CheckpointData.forFileStatus(fileStatus)
    }.getMessage
    assert(exMsg.contains("Expected a V2 checkpoint file but got"))
  }

  test("ParsedV2CheckpointData: correctly parses V2 checkpoint file") {
    val fileStatus = v2CheckpointFileStatus(20)
    val parsed = ParsedLogData.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedCheckpointData])
    assert(parsed.getVersion == 20)
    assert(parsed.getGroupByCategoryClass == classOf[ParsedCheckpointData])
    assert(parsed.isFile)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("ParsedV2CheckpointData: equality") {
    val parsed1 = ParsedLogData.forFileStatus(v2CheckpointFileStatus(20, useUUID = false))
    val parsed2 = ParsedLogData.forFileStatus(v2CheckpointFileStatus(20, useUUID = false))
    val parsed3 = ParsedLogData.forFileStatus(v2CheckpointFileStatus(21))

    assert(parsed1 == parsed1)
    assert(parsed1 == parsed2)
    assert(parsed1 != parsed3)
  }

  ///////////////////////////////////
  // ParsedMultiPartCheckpointData //
  ///////////////////////////////////

  test("ParsedMultiPartCheckpointData: throws on non-multi-part checkpoint file") {
    val fileStatus = deltaFileStatus(5)
    val exMsg = intercept[IllegalArgumentException] {
      ParsedMultiPartCheckpointData.forFileStatus(fileStatus)
    }.getMessage
    assert(exMsg.contains("Expected a multi-part checkpoint file but got"))
  }

  test("ParsedMultiPartCheckpointData: correctly parses multi-part checkpoint file") {
    val chkpt_15_1_3 = multiPartCheckpointFileStatus(15, 1, 3)
    val parsed = ParsedLogData.forFileStatus(chkpt_15_1_3)

    assert(parsed.isInstanceOf[ParsedMultiPartCheckpointData])
    assert(parsed.getVersion == 15)
    assert(parsed.getGroupByCategoryClass == classOf[ParsedCheckpointData])
    assert(parsed.isFile)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == chkpt_15_1_3)

    val casted = parsed.asInstanceOf[ParsedMultiPartCheckpointData]
    assert(casted.part == 1)
    assert(casted.numParts == 3)
  }

  test("ParsedMultiPartCheckpointData: throws on part > numParts") {
    val path = FileNames.multiPartCheckpointFile(logPath, 10, 5, 3) // part = 5, numParts = 3
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogData.forFileStatus(FileStatus.of(path.toString))
    }.getMessage
    assert(exMsg === "part must be between 1 and numParts")
  }

  test("ParsedMultiPartCheckpointData: throws on numParts = 0") {
    val path = FileNames.multiPartCheckpointFile(logPath, 10, 0, 0) // part = 0, numParts = 0
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogData.forFileStatus(FileStatus.of(path.toString))
    }.getMessage
    assert(exMsg === "numParts must be greater than 0")
  }

  test("ParsedMultiPartCheckpointData: throws on part = 0") {
    val path = FileNames.multiPartCheckpointFile(logPath, 10, 0, 3) // part = 0, numParts = 3
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogData.forFileStatus(FileStatus.of(path.toString))
    }.getMessage
    assert(exMsg === "part must be between 1 and numParts")
  }

  test("ParsedMultiPartCheckpointData: equality") {
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

    val classic_12_m: ParsedCheckpointData =
      ParsedClassicCheckpointData.forFileStatus(classicCheckpointFileStatus(12))
    val multi_11_3_m: ParsedCheckpointData =
      ParsedMultiPartCheckpointData.forFileStatus(multiPartCheckpointFileStatus(11, 1, 3))
    val v2_10_m: ParsedCheckpointData =
      ParsedV2CheckpointData.forFileStatus(v2CheckpointFileStatus(10))
    val multi_12_3_m: ParsedCheckpointData =
      ParsedMultiPartCheckpointData.forFileStatus(multiPartCheckpointFileStatus(12, 1, 3))
    val v2_12_m: ParsedCheckpointData =
      ParsedV2CheckpointData.forFileStatus(v2CheckpointFileStatus(12))
    val multi_12_4_m: ParsedCheckpointData =
      ParsedMultiPartCheckpointData.forFileStatus(multiPartCheckpointFileStatus(12, 1, 4))
    val v2_aaa: ParsedCheckpointData = ParsedV2CheckpointData.forFileStatus(
      FileStatus.of(FileNames.topLevelV2CheckpointFile(logPath, 10, "aaa", "json").toString))
    val v2_bbb: ParsedCheckpointData = ParsedV2CheckpointData.forFileStatus(
      FileStatus.of(FileNames.topLevelV2CheckpointFile(logPath, 10, "bbb", "json").toString))

    // Case 1: Version priority
    classic_12_m should be > multi_11_3_m
    classic_12_m should be > v2_10_m
    multi_11_3_m should be > v2_10_m

    // Case 2: Type priority, when version is tied
    v2_12_m should be > classic_12_m
    v2_12_m should be > multi_12_3_m

    // Case 3: Inline priority, when version and type are tied (and parts are tied for multi)
    // TODO: Test this when we allow creating checkpoints with inline data

    // Case 4: Multi-part checkpoint with more parts has higher priority
    multi_12_4_m should be > multi_12_3_m

    // Case 5: For tied v2, filepath is tie-breaker
    v2_bbb should be > v2_aaa
  }

  ////////////////////////////
  // ParsedLogCompactionData //
  ////////////////////////////

  test("ParsedLogCompactionData: throws on non-log-compaction file") {
    val fileStatus = deltaFileStatus(5)
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogCompactionData.forFileStatus(fileStatus)
    }.getMessage
    assert(exMsg.contains("Expected a log compaction file but got"))
  }

  test("ParsedLogCompactionData: correctly parses log compaction file") {
    val fileStatus = logCompactionStatus(25, 30)
    val parsed = ParsedLogData.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedLogCompactionData])
    assert(parsed.getVersion == 30)
    assert(parsed.getGroupByCategoryClass == classOf[ParsedLogCompactionData])
    assert(parsed.isFile)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)

    val casted = parsed.asInstanceOf[ParsedLogCompactionData]
    assert(casted.startVersion == 25)
    assert(casted.endVersion == 30)
  }

  test("ParsedLogCompactionData: throws on startVersion > endVersion") {
    val exMsg = intercept[IllegalArgumentException] {
      val invalidFilePath = "00000000000000000003.00000000000000000001.compacted.json"
      ParsedLogCompactionData.forFileStatus(FileStatus.of(invalidFilePath))
    }.getMessage
    assert(exMsg === "startVersion must be less than endVersion")
  }

  test("ParsedLogCompactionData: equality") {
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

  ////////////////////////
  // ParsedChecksumData //
  ////////////////////////

  test("ParsedChecksumData: throws on non-checksum file") {
    val fileStatus = deltaFileStatus(5)
    val exMsg = intercept[IllegalArgumentException] {
      ParsedChecksumData.forFileStatus(fileStatus)
    }.getMessage
    assert(exMsg.contains("Expected a checksum file but got"))
  }

  test("ParsedChecksumData: correctly parses checksum file") {
    val fileStatus = checksumFileStatus(5)
    val parsed = ParsedLogData.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedChecksumData])
    assert(parsed.getVersion == 5)
    assert(parsed.getGroupByCategoryClass == classOf[ParsedChecksumData])
    assert(parsed.getFileStatus == fileStatus)
  }

  test("ParsedChecksumData: equality") {
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

  // scalastyle:off line.size.limit

  test("ParsedPublishedDeltaData: toString") {
    val parsed = ParsedPublishedDeltaData.forFileStatus(deltaFileStatus(5))
    val expected =
      "ParsedPublishedDeltaData{version=5, source=FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000005.json', size=5, modificationTime=50}}"
    assert(parsed.toString === expected)
  }

  test("ParsedCatalogCommitData: toString") {
    val parsed = ParsedCatalogCommitData.forFileStatus(stagedCommitFile(5))
    val expectedPattern =
      """ParsedCatalogCommitData\{version=5, source=FileStatus\{path='/fake/path/to/table/_delta_log/_staged_commits/00000000000000000005\.[^']+\.json', size=5, modificationTime=50\}\}""".r
    assert(expectedPattern.findFirstIn(parsed.toString).isDefined)
  }

  test("ParsedLogCompactionData: toString") {
    val fileStatus = logCompactionStatus(10, 20)
    val parsed = ParsedLogCompactionData.forFileStatus(fileStatus)
    val expected =
      "ParsedLogCompactionData{version=20, source=FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000010.00000000000000000020.compacted.json', size=10, modificationTime=100}, startVersion=10}"
    assert(parsed.toString === expected)
  }

  test("ParsedChecksumData: toString") {
    val parsed = ParsedLogData.forFileStatus(checksumFileStatus(5))
    val expected =
      "ParsedChecksumData{version=5, source=FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000005.crc', size=10, modificationTime=10}}"
    assert(parsed.toString === expected)
  }

  test("ParsedClassicCheckpointData: toString") {
    val parsed = ParsedLogData.forFileStatus(classicCheckpointFileStatus(10))
    val expected =
      "ParsedClassicCheckpointData{version=10, source=FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000010.checkpoint.parquet', size=10, modificationTime=100}}"
    assert(parsed.toString === expected)
  }

  test("ParsedMultiPartCheckpointData: toString") {
    val fileStatus = multiPartCheckpointFileStatus(10, 1, 3)
    val parsed = ParsedMultiPartCheckpointData.forFileStatus(fileStatus)
    val expected =
      "ParsedMultiPartCheckpointData{version=10, source=FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000010.checkpoint.0000000001.0000000003.parquet', size=10, modificationTime=100}, part=1, numParts=3}"
    assert(parsed.toString === expected)
  }

  test("ParsedV2CheckpointData: toString") {
    val parsed = ParsedLogData.forFileStatus(v2CheckpointFileStatus(20))
    val expectedPattern =
      """ParsedV2CheckpointData\{version=20, source=FileStatus\{path='/fake/path/to/table/_delta_log/00000000000000000020\.checkpoint\.[a-f0-9-]+\.json', size=20, modificationTime=200\}\}""".r
    assert(expectedPattern.findFirstIn(parsed.toString).isDefined)
  }

  // scalastyle:on line.size.limit
}
