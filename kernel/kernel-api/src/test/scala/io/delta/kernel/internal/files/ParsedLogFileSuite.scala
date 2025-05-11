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

import io.delta.kernel.test.MockFileSystemClientUtils
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class ParsedLogFileSuite extends AnyFunSuite with MockFileSystemClientUtils {
  test("Throws on unknown log file") {
    val fileStatus = FileStatus.of("unknown", 0, 0)
    val exception = intercept[IllegalArgumentException] {
      ParsedLogFile.forFileStatus(fileStatus)
    }
    assert(exception.getMessage.contains("Unknown log file type"))
  }

  test("Correctly parses delta file") {
    val fileStatus = deltaFileStatus(5)
    val parsed = ParsedLogFile.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedLogFile.PublishedDeltaFile])
    assert(parsed.getVersion == 5)
    assert(parsed.getCategory == ParsedLogFile.Category.DELTA)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("Delta file equality") {
    val fileStatus1 = deltaFileStatus(5)
    val fileStatus2 = deltaFileStatus(5)
    val fileStatus3 = deltaFileStatus(6)

    val delta1 = ParsedLogFile.forFileStatus(fileStatus1)
    val delta2 = ParsedLogFile.forFileStatus(fileStatus2)
    val delta3 = ParsedLogFile.forFileStatus(fileStatus3)

    assert(delta1 == delta1)
    assert(delta1 == delta2)
    assert(delta1 != delta3)
  }

  test("Correctly parses classic checkpoint file") {
    val fileStatus = singularCheckpointFileStatuses(Seq(10)).head
    val parsed = ParsedLogFile.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedLogFile.ClassicCheckpointFile])
    assert(parsed.getVersion == 10)
    assert(parsed.getCategory == ParsedLogFile.Category.CHECKPOINT)
    assert(parsed.getFileStatus == fileStatus)
    assert(parsed.asInstanceOf[ParsedLogFile.ClassicCheckpointFile].mayContainSidecarFiles)
  }

  test("Classic checkpoint file equality") {
    val fileStatus1 = singularCheckpointFileStatuses(Seq(10)).head
    val fileStatus2 = singularCheckpointFileStatuses(Seq(10)).head
    val fileStatus3 = singularCheckpointFileStatuses(Seq(11)).head

    val cp1 = ParsedLogFile.forFileStatus(fileStatus1)
    val cp2 = ParsedLogFile.forFileStatus(fileStatus2)
    val cp3 = ParsedLogFile.forFileStatus(fileStatus3)

    assert(cp1 == cp1)
    assert(cp1 == cp2)
    assert(cp1 != cp3)
  }

  test("Correctly parses multi-part checkpoint file") {
    val chkpt_15_1_3 = multiCheckpointFileStatuses(Seq(15), 3).head
    val parsed = ParsedLogFile.forFileStatus(chkpt_15_1_3)

    assert(parsed.isInstanceOf[ParsedLogFile.MultipartCheckpointFile])
    assert(parsed.getVersion == 15)
    assert(parsed.getCategory == ParsedLogFile.Category.CHECKPOINT)
    assert(parsed.getFileStatus == chkpt_15_1_3)

    val cast = parsed.asInstanceOf[ParsedLogFile.MultipartCheckpointFile]
    assert(cast.getPart == 1)
    assert(cast.getNumParts == 3)
    assert(!cast.mayContainSidecarFiles)
  }

  test("Multi-part checkpoint file equality") {
    val chkpt_15_1_3_a = multiCheckpointFileStatuses(Seq(15), 3).head
    val chkpt_15_1_3_b = multiCheckpointFileStatuses(Seq(15), 3).head
    val chkpt_15_2_3 = multiCheckpointFileStatuses(Seq(15), 3)(1)
    val chkpt_15_1_4 = multiCheckpointFileStatuses(Seq(15), 4).head
    val chkpt_16_1_3 = multiCheckpointFileStatuses(Seq(16), 3).head

    val parsed_15_1_3_a = ParsedLogFile.forFileStatus(chkpt_15_1_3_a)
    val parsed_15_1_3_b = ParsedLogFile.forFileStatus(chkpt_15_1_3_b)
    val parsed_15_2_3 = ParsedLogFile.forFileStatus(chkpt_15_2_3)
    val parsed_15_1_4 = ParsedLogFile.forFileStatus(chkpt_15_1_4)
    val parsed_16_1_3 = ParsedLogFile.forFileStatus(chkpt_16_1_3)

    assert(parsed_15_1_3_a == parsed_15_1_3_a)
    assert(parsed_15_1_3_a == parsed_15_1_3_b)
    assert(parsed_15_1_3_a != parsed_15_2_3)
    assert(parsed_15_1_3_a != parsed_15_1_4)
    assert(parsed_15_1_3_a != parsed_16_1_3)
  }

  test("Correctly parses V2 checkpoint file") {
    val fileStatus = v2CheckpointFileStatuses(Seq((20, true, 0)), "parquet").head._1
    val parsed = ParsedLogFile.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedLogFile.V2CheckpointFile])
    assert(parsed.getVersion == 20)
    assert(parsed.getCategory == ParsedLogFile.Category.CHECKPOINT)
    assert(parsed.getFileStatus == fileStatus)
    assert(parsed.asInstanceOf[ParsedLogFile.V2CheckpointFile].mayContainSidecarFiles)
  }

  test("V2 checkpoint file equality") {
    val fileStatus1 = v2CheckpointFileStatuses(Seq((20, false, 0)), "parquet").head._1
    val fileStatus2 = v2CheckpointFileStatuses(Seq((20, false, 0)), "parquet").head._1
    val fileStatus3 = v2CheckpointFileStatuses(Seq((21, true, 0)), "parquet").head._1

    val parsed1 = ParsedLogFile.forFileStatus(fileStatus1)
    val parsed2 = ParsedLogFile.forFileStatus(fileStatus2)
    val parsed3 = ParsedLogFile.forFileStatus(fileStatus3)

    assert(parsed1 == parsed1)
    assert(parsed1 == parsed2)
    assert(parsed1 != parsed3)
  }

  test("Correctly parses log compaction file") {
    val fileStatus = logCompactionStatus(25, 30)
    val parsed = ParsedLogFile.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedLogFile.LogCompactionFile])
    assert(parsed.getVersion == 30)
    assert(parsed.getCategory == ParsedLogFile.Category.LOG_COMPACTION)
    assert(parsed.getFileStatus == fileStatus)

    val cast = parsed.asInstanceOf[ParsedLogFile.LogCompactionFile]
    assert(cast.getStartVersion == 25)
    assert(cast.getEndVersion == 30)
  }

  test("Log compaction file equality") {
    val fileStatus1 = logCompactionStatus(25, 30)
    val fileStatus2 = logCompactionStatus(25, 30)
    val fileStatus3 = logCompactionStatus(31, 15)

    val parsed1 = ParsedLogFile.forFileStatus(fileStatus1)
    val parsed2 = ParsedLogFile.forFileStatus(fileStatus2)
    val parsed3 = ParsedLogFile.forFileStatus(fileStatus3)

    assert(parsed1 == parsed1)
    assert(parsed1 == parsed2)
    assert(parsed1 != parsed3)
  }

  test("Correctly parses checksum file") {
    val fileStatus = checksumFileStatus(5)
    val parsed = ParsedLogFile.forFileStatus(fileStatus)

    assert(parsed.isInstanceOf[ParsedLogFile.ChecksumFile])
    assert(parsed.getVersion == 5)
    assert(parsed.getCategory == ParsedLogFile.Category.CHECKSUM)
    assert(parsed.getFileStatus == fileStatus)
  }

  test("Checksum file equality") {
    val fileStatus1 = checksumFileStatus(5)
    val fileStatus2 = checksumFileStatus(5)
    val fileStatus3 = checksumFileStatus(6)

    val parsed1 = ParsedLogFile.forFileStatus(fileStatus1)
    val parsed2 = ParsedLogFile.forFileStatus(fileStatus2)
    val parsed3 = ParsedLogFile.forFileStatus(fileStatus3)

    assert(parsed1 == parsed1)
    assert(parsed1 == parsed2)
    assert(parsed1 != parsed3)
  }

}
