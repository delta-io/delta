/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.replay

import scala.collection.JavaConverters._
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.replay.LogReplayUtils.assertLogFilesBelongToTable
import io.delta.kernel.utils.FileStatus
import org.scalatest.funsuite.AnyFunSuite

class TestLogReplay extends AnyFunSuite {

  private val tablePath = new Path("s3://bucket/logPath")

  test("assertLogFilesBelongToTable should pass for correct log paths") {
    val logFiles = List(
      FileStatus.of("s3://bucket/logPath/deltafile1", 0L, 0L),
      FileStatus.of("s3://bucket/logPath/deltafile2", 0L, 0L),
      FileStatus.of("s3://bucket/logPath/checkpointfile1", 0L, 0L),
      FileStatus.of("s3://bucket/logPath/checkpointfile2", 0L, 0L)
    ).asJava

    assertLogFilesBelongToTable(tablePath, logFiles)
  }

  test("assertLogFilesBelongToTable should fail for incorrect log paths") {
    val logFiles = List(
      FileStatus.of("s3://bucket/logPath/deltafile1", 0L, 0L),
      FileStatus.of("s3://bucket/invalidLogPath/deltafile2", 0L, 0L),
      FileStatus.of("s3://bucket/logPath/checkpointfile1", 0L, 0L),
      FileStatus.of("s3://bucket/invalidLogPath/checkpointfile2", 0L, 0L)
    ).asJava

    // Test that files with incorrect log paths trigger the assertion
    val ex = intercept[RuntimeException] {
      assertLogFilesBelongToTable(tablePath, logFiles)
    }
    assert(ex.getMessage.contains("File (s3://bucket/invalidLogPath/deltafile2) " +
      s"doesn't belong in the transaction log at $tablePath"))
  }
}
