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

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.internal.fs.Path
import io.delta.kernel.utils.FileStatus
import io.delta.kernel.internal.snapshot.LogSegment
import io.delta.kernel.internal.replay.LogReplay

import org.scalatest.funsuite.AnyFunSuite
import org.junit.Assert.assertThrows

class TestLogReplay extends AnyFunSuite {

  test("assertLogFilesBelongToTable should pass for correct log paths") {
    /*
      Test public LogSegment constructor rather than assertLogFilesBelongToTable
      method directly because method is private
    */

    val logSegment = new LogSegment(
      new Path("s3://bucket/logPath"),
      0,
      List(
        FileStatus.of("s3://bucket/logPath/deltafile1", 0L, 0L),
        FileStatus.of("s3://bucket/logPath/deltafile2", 0L, 0L)
      ).asJava,
      List(
        FileStatus.of("s3://bucket/logPath/checkpointfile1", 0L, 0L),
        FileStatus.of("s3://bucket/logPath/checkpointfile2", 0L, 0L)
      ).asJava,
      Optional.of(0L),
      0L
    )

    // Test that files with the correct log path pass the assertion
    val logReplay = new LogReplay(
      new Path("s3://bucket/logPath"),
      new Path("s3://bucket/dataPath"),
      null,
      logSegment
    )
  }

  test("assertLogFilesBelongToTable should fail for incorrect log paths") {
    /*
      Test public LogSegment constructor rather than assertLogFilesBelongToTable
      method directly because method is private
    */
    val logSegment = new LogSegment(
      new Path("s3://bucket/logPath"),
      0,
      List(
        FileStatus.of("s3://bucket/invalidLogPath/deltafile1", 0L, 0L),
        FileStatus.of("s3://bucket/invalidLogPath/deltafile2", 0L, 0L)
      ).asJava,
      List(
        FileStatus.of("s3://bucket/invalidLogPath/checkpointfile1", 0L, 0L),
        FileStatus.of("s3://bucket/invalidLogPath/checkpointfile2", 0L, 0L)
      ).asJava,
      Optional.of(0L),
      0L
    )

    // Test that files with incorrect log paths trigger the assertion
    assertThrows[RuntimeException] {
      val logReplay = new LogReplay(
        new Path("s3://bucket/logPath"),
        new Path("s3://bucket/dataPath"),
        null,
        logSegment
      )
    }
  }
}
