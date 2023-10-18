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

import java.util

import scala.reflect.runtime.universe._
import scala.collection.JavaConverters._

import io.delta.kernel.internal.fs.Path
import io.delta.kernel.utils.FileStatus
import io.delta.kernel.internal.replay.LogReplay.assertLogFilesBelongToTable

import org.scalatest.funsuite.AnyFunSuite
import org.junit.Assert.assertThrows

class TestLogReplay extends AnyFunSuite {

  // Get a reference to the private method using reflection
  val methodSymbol = typeOf[LogReplay].decl(TermName("assertLogFilesBelongToTable")).asMethod
  val im = runtimeMirror(getClass.getClassLoader)
  val assertLogFilesBelongToTable = im.reflect(logReplay).reflectMethod(methodSymbol)

  test("assertLogFilesBelongToTable should pass for correct log paths") {
    // Create a test instance of LogReplay
    val logReplay = new LogReplay(new Path("s3://bucket/logPath"), new Path("s3://bucket/dataPath"), null, null)

    // Create a list of FileStatus objects representing log files with correct log paths
    val logFiles = List(
      new FileStatus.of("s3://bucket/logPath/logfile1"),
      new FileStatus.of("s3://bucket/logPath/logfile2"),
      new FileStatus.of("s3://bucket/logPath/logfile3")
    )

    // Test that files with the correct log path pass the assertion
    assertLogFilesBelongToTable(new Path("s3://bucket/logPath"), logFiles)
  }

  test("assertLogFilesBelongToTable should fail for incorrect log paths") {
    // Create a test instance of LogReplay
    val logReplay = new LogReplay(new Path("s3://bucket/logPath"), new Path("s3://bucket/dataPath"), null, null)

    // Create a list of FileStatus objects representing log files with incorrect log paths
    val invalidLogFiles = List(
      new FileStatus.of("s3://bucket/invalidPath/logfile1"),
      new FileStatus.of("s3://bucket/invalidPath/logfile2")
    )

    // Test that files with incorrect log paths trigger the assertion
    assertThrows[RuntimeException] {
      assertLogFilesBelongToTable(new Path("logPath"), invalidLogFiles)
    }
  }
}
