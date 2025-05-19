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
import io.delta.kernel.test.MockFileSystemClientUtils
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class ParsedLogDataSuite extends AnyFunSuite with MockFileSystemClientUtils {

  private val emptyColumnarBatch = new ColumnarBatch {
    override def getSchema: StructType = null
    override def getColumnVector(ordinal: Int): ColumnVector = null
    override def getSize: Int = 0
  }

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
      ParsedLogData.forInlineData(-1, ParsedLogType.RATIFIED_INLINE_COMMIT, emptyColumnarBatch)
    }.getMessage
    assert(exMsg.contains("version must be non-negative"))
  }

  test("Throws on both fileStatusOpt and inlineDataOpt present") {
    val fileStatusOpt = Optional.of(deltaFileStatus(5))
    val inlineDataOpt = Optional.of(emptyColumnarBatch)
    val exMsg = intercept[IllegalArgumentException] {
      new ParsedLogData(10, ParsedLogType.PUBLISHED_DELTA, fileStatusOpt, inlineDataOpt)
    }.getMessage
    assert(exMsg.contains("Exactly one of fileStatusOpt or inlineDataOpt must be present"))
  }

  test("Throws on both fileStatusOpt and inlineDataOpt empty") {
    val exMsg = intercept[IllegalArgumentException] {
      new ParsedLogData(10, ParsedLogType.PUBLISHED_DELTA, Optional.empty(), Optional.empty())
    }.getMessage
    assert(exMsg.contains("Exactly one of fileStatusOpt or inlineDataOpt must be present"))
  }

  ////////////
  // Deltas //
  ////////////

  test("Correctly parses published delta file") {
    val fileStatus = deltaFileStatus(5)
    val parsed = ParsedLogData.forFileStatus(fileStatus)

    assert(parsed.version == 5)
    assert(parsed.`type` == ParsedLogType.PUBLISHED_DELTA)
    assert(parsed.getCategory == ParsedLogCategory.DELTA)
    assert(parsed.isMaterialized)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("Correctly parses staged commit file") {
    val fileStatus = stagedCommitFile(5)
    val parsed = ParsedLogData.forFileStatus(fileStatus)

    assert(parsed.version == 5)
    assert(parsed.`type` == ParsedLogType.STAGED_COMMIT)
    assert(parsed.getCategory == ParsedLogCategory.DELTA)
    assert(parsed.isMaterialized)
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

  test("Inline classic checkpoint") {
    ParsedLogData.forInlineData(10, ParsedLogType.CLASSIC_CHECKPOINT, emptyColumnarBatch)
    ParsedCheckpointData.forInlineData(10, ParsedLogType.CLASSIC_CHECKPOINT, emptyColumnarBatch)
  }

  test("Inline v2 checkpoint") {
    ParsedLogData.forInlineData(10, ParsedLogType.V2_CHECKPOINT, emptyColumnarBatch)
    ParsedCheckpointData.forInlineData(10, ParsedLogType.V2_CHECKPOINT, emptyColumnarBatch)
  }

  test("Correctly parses classic checkpoint file") {
    val fileStatus = singularCheckpointFileStatuses(Seq(10)).head
    val parsed = ParsedLogData.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedCheckpointData])
    assert(parsed.version == 10)
    assert(parsed.`type` == ParsedLogType.CLASSIC_CHECKPOINT)
    assert(parsed.getCategory == ParsedLogCategory.CHECKPOINT)
    assert(parsed.isMaterialized)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("Classic checkpoint file equality") {
    val fileStatus1 = singularCheckpointFileStatuses(Seq(10)).head
    val fileStatus2 = singularCheckpointFileStatuses(Seq(10)).head
    val fileStatus3 = singularCheckpointFileStatuses(Seq(11)).head

    val cp1 = ParsedLogData.forFileStatus(fileStatus1)
    val cp2 = ParsedLogData.forFileStatus(fileStatus2)
    val cp3 = ParsedLogData.forFileStatus(fileStatus3)

    assert(cp1 == cp1)
    assert(cp1 == cp2)
    assert(cp1 != cp3)
  }

  test("Correctly parses V2 checkpoint file") {
    val fileStatus = v2CheckpointFileStatuses(Seq((20, true, 0)), "parquet").head._1
    val parsed = ParsedLogData.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedCheckpointData])
    assert(parsed.version == 20)
    assert(parsed.`type` == ParsedLogType.V2_CHECKPOINT)
    assert(parsed.getCategory == ParsedLogCategory.CHECKPOINT)
    assert(parsed.isMaterialized)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("V2 checkpoint file equality") {
    val fileStatus1 = v2CheckpointFileStatuses(Seq((20, false, 0)), "parquet").head._1
    val fileStatus2 = v2CheckpointFileStatuses(Seq((20, false, 0)), "parquet").head._1
    val fileStatus3 = v2CheckpointFileStatuses(Seq((21, true, 0)), "parquet").head._1

    val parsed1 = ParsedLogData.forFileStatus(fileStatus1)
    val parsed2 = ParsedLogData.forFileStatus(fileStatus2)
    val parsed3 = ParsedLogData.forFileStatus(fileStatus3)

    assert(parsed1 == parsed1)
    assert(parsed1 == parsed2)
    assert(parsed1 != parsed3)
  }

  ////////////////////////////
  // Multi-Part Checkpoints //
  ////////////////////////////

  test("Inline multi-part checkpoint") {
    ParsedMultiPartCheckpointData.forInlineData(10, 1, 3, emptyColumnarBatch)
  }

  test("Throws on inline multi-part checkpoint using wrong factory") {
    val exMsg1 = intercept[IllegalArgumentException] {
      ParsedLogData.forInlineData(10, ParsedLogType.MULTIPART_CHECKPOINT, emptyColumnarBatch)
    }.getMessage
    assert(exMsg1 ==
      "For MULTIPART_CHECKPOINT, use ParsedMultiPartCheckpointData.forInlineData() instead")

    val exMsg2 = intercept[IllegalArgumentException] {
      ParsedCheckpointData.forInlineData(10, ParsedLogType.MULTIPART_CHECKPOINT, emptyColumnarBatch)
    }.getMessage
    assert(exMsg2 ==
      "For MULTIPART_CHECKPOINT, use ParsedMultiPartCheckpointData.forInlineData() instead")
  }

  test("Correctly parses multi-part checkpoint file") {
    val chkpt_15_1_3 = multiCheckpointFileStatuses(Seq(15), 3).head
    val parsed = ParsedLogData
      .forFileStatus(chkpt_15_1_3)
      .asInstanceOf[ParsedMultiPartCheckpointData]

    assert(parsed.version == 15)
    assert(parsed.`type` == ParsedLogType.MULTIPART_CHECKPOINT)
    assert(parsed.getCategory == ParsedLogCategory.CHECKPOINT)
    assert(parsed.isMaterialized)
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
    assert(exMsg.contains("part must be between 1 and numParts"))
  }

  test("Throws on multi-part checkpoint with numParts = 0") {
    val path = FileNames.multiPartCheckpointFile(logPath, 10, 0, 0) // part = 0, numParts = 0
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogData.forFileStatus(FileStatus.of(path.toString))
    }.getMessage
    assert(exMsg.contains("numParts must be greater than 0"))
  }

  test("Throws on multi-part checkpoint with part = 0") {
    val path = FileNames.multiPartCheckpointFile(logPath, 10, 0, 3) // part = 0, numParts = 3
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogData.forFileStatus(FileStatus.of(path.toString))
    }.getMessage
    assert(exMsg.contains("part must be between 1 and numParts"))
  }

  test("Multi-part checkpoint file equality") {
    val chkpt_15_1_3_a = multiCheckpointFileStatuses(Seq(15), 3).head
    val chkpt_15_1_3_b = multiCheckpointFileStatuses(Seq(15), 3).head
    val chkpt_15_2_3 = multiCheckpointFileStatuses(Seq(15), 3)(1)
    val chkpt_15_1_4 = multiCheckpointFileStatuses(Seq(15), 4).head
    val chkpt_16_1_3 = multiCheckpointFileStatuses(Seq(16), 3).head

    val parsed_15_1_3_a = ParsedLogData.forFileStatus(chkpt_15_1_3_a)
    val parsed_15_1_3_b = ParsedLogData.forFileStatus(chkpt_15_1_3_b)
    val parsed_15_2_3 = ParsedLogData.forFileStatus(chkpt_15_2_3)
    val parsed_15_1_4 = ParsedLogData.forFileStatus(chkpt_15_1_4)
    val parsed_16_1_3 = ParsedLogData.forFileStatus(chkpt_16_1_3)

    assert(parsed_15_1_3_a == parsed_15_1_3_a)
    assert(parsed_15_1_3_a == parsed_15_1_3_b)
    assert(parsed_15_1_3_a != parsed_15_2_3)
    assert(parsed_15_1_3_a != parsed_15_1_4)
    assert(parsed_15_1_3_a != parsed_16_1_3)
  }

  /////////////////////
  // Log compactions //
  /////////////////////

  test("inline log compaction") {
    ParsedLogCompactionData.forInlineData(10, 20, emptyColumnarBatch)
  }

  test("Throws on inline log compaction using wrong factory") {
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogData.forInlineData(10, ParsedLogType.LOG_COMPACTION, emptyColumnarBatch)
    }.getMessage
    assert(exMsg ==
      "For LOG_COMPACTION, use ParsedLogCompactionData.forInlineData() instead")
  }

  test("Correctly parses log compaction file") {
    val fileStatus = logCompactionStatus(25, 30)
    val parsed = ParsedLogData.forFileStatus(fileStatus).asInstanceOf[ParsedLogCompactionData]

    assert(parsed.version == 30)
    assert(parsed.`type` == ParsedLogType.LOG_COMPACTION)
    assert(parsed.getCategory == ParsedLogCategory.LOG_COMPACTION)
    assert(parsed.isMaterialized)
    assert(!parsed.isInline)
    assert(parsed.getFileStatus == fileStatus)
    assert(parsed.startVersion == 25)
    assert(parsed.endVersion == 30)
  }

  test("Throws on log compaction with startVersion < 0") {
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogCompactionData.forInlineData(-1, 3, emptyColumnarBatch)
    }.getMessage
    assert(exMsg.contains("startVersion and endVersion must be non-negative"))
  }

  test("Throws on log compaction with endVersion < 0") {
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogCompactionData.forInlineData(1, -1, emptyColumnarBatch)
    }.getMessage
    assert(exMsg.contains("version must be non-negative"))
  }

  test("Throws on log compaction with startVersion > endVersion") {
    val exMsg = intercept[IllegalArgumentException] {
      ParsedLogCompactionData.forInlineData(3, 1, emptyColumnarBatch)
    }.getMessage
    assert(exMsg.contains("startVersion must be less than endVersion"))
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
      "ParsedLogData{version=5, type=PUBLISHED_DELTA, source=FileStatus{path='/fake/path/to/table/_delta_log/00000000000000000005.json', size=5, modificationTime=50}}"
    // scalastyle:on line.size.limit
    assert(parsed.toString === expected)
  }

  test("multi-part checkpoint toString") {
    val parsed = ParsedMultiPartCheckpointData.forInlineData(10, 1, 3, emptyColumnarBatch)
    // scalastyle:off line.size.limit
    val expected =
      "ParsedMultiPartCheckpointData{version=10, type=MULTIPART_CHECKPOINT, source=inline, part=1, numParts=3}"
    // scalastyle:on line.size.limit
    assert(parsed.toString === expected)
  }

  test("log compaction toString") {
    val parsed = ParsedLogCompactionData.forInlineData(10, 20, emptyColumnarBatch)
    // scalastyle:off line.size.limit
    val expected =
      "ParsedLogCompactionData{version=20, type=LOG_COMPACTION, source=inline, startVersion=10}"
    // scalastyle:on line.size.limit
    assert(parsed.toString === expected)
  }
}
