/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal

import io.delta.kernel.test.{MockFileSystemClientUtils, MockListFromResolvePathFileSystemClient}
import io.delta.kernel.utils.FileStatus
import io.delta.kernel.Table
import io.delta.kernel.exceptions.KernelException
import org.scalatest.funsuite.AnyFunSuite

class TableImplSuite extends AnyFunSuite with MockFileSystemClientUtils {

  def checkGetVersionBeforeOrAtTimestamp(
    fileList: Seq[FileStatus],
    timestamp: Long,
    expectedVersion: Option[Long] = None,
    expectedErrorMessageContains: Option[String] = None): Unit = {
    // Check our inputs are as expected
    assert(expectedVersion.isEmpty || expectedErrorMessageContains.isEmpty)
    assert(expectedVersion.nonEmpty || expectedErrorMessageContains.nonEmpty)

    val engine = mockEngine(fileSystemClient =
      new MockListFromResolvePathFileSystemClient(listFromProvider(fileList)))
    val table = Table.forPath(engine, dataPath.toString)

    expectedVersion.foreach { v =>
      assert(table.asInstanceOf[TableImpl].getVersionBeforeOrAtTimestamp(engine, timestamp) == v)
    }
    expectedErrorMessageContains.foreach { s =>
      assert(intercept[KernelException] {
        table.asInstanceOf[TableImpl].getVersionBeforeOrAtTimestamp(engine, timestamp)
      }.getMessage.contains(s))
    }
  }

  def checkGetVersionAtOrAfterTimestamp(
    fileList: Seq[FileStatus],
    timestamp: Long,
    expectedVersion: Option[Long] = None,
    expectedErrorMessageContains: Option[String] = None): Unit = {
    // Check our inputs are as expected
    assert(expectedVersion.isEmpty || expectedErrorMessageContains.isEmpty)
    assert(expectedVersion.nonEmpty || expectedErrorMessageContains.nonEmpty)

    val engine = mockEngine(fileSystemClient =
      new MockListFromResolvePathFileSystemClient(listFromProvider(fileList)))
    val table = Table.forPath(engine, dataPath.toString)

    expectedVersion.foreach { v =>
      assert(table.asInstanceOf[TableImpl].getVersionAtOrAfterTimestamp(engine, timestamp) == v)
    }
    expectedErrorMessageContains.foreach { s =>
      assert(intercept[KernelException] {
        table.asInstanceOf[TableImpl].getVersionAtOrAfterTimestamp(engine, timestamp)
      }.getMessage.contains(s))
    }
  }

  test("getVersionBeforeOrAtTimestamp: basic case from 0") {
    val deltaFiles = deltaFileStatuses(Seq(0L, 1L))
    checkGetVersionBeforeOrAtTimestamp(deltaFiles, -1,
      expectedErrorMessageContains = Some("is before the earliest available version 0")) // before 0
    checkGetVersionBeforeOrAtTimestamp(deltaFiles, 0, expectedVersion = Some(0)) // at 0
    checkGetVersionBeforeOrAtTimestamp(deltaFiles, 5, expectedVersion = Some(0)) // btw 0, 1
    checkGetVersionBeforeOrAtTimestamp(deltaFiles, 10, expectedVersion = Some(1)) // at 1
    checkGetVersionBeforeOrAtTimestamp(deltaFiles, 11, expectedVersion = Some(1)) // after 1
  }

  test("getVersionAtOrAfterTimestamp: basic case from 0") {
    val deltaFiles = deltaFileStatuses(Seq(0L, 1L))
    checkGetVersionAtOrAfterTimestamp(deltaFiles, -1, expectedVersion = Some(0)) // before 0
    checkGetVersionAtOrAfterTimestamp(deltaFiles, 0, expectedVersion = Some(0)) // at 0
    checkGetVersionAtOrAfterTimestamp(deltaFiles, 5, expectedVersion = Some(1)) // btw 0, 1
    checkGetVersionAtOrAfterTimestamp(deltaFiles, 10, expectedVersion = Some(1)) // at 1
    checkGetVersionAtOrAfterTimestamp(deltaFiles, 11,
      expectedErrorMessageContains = Some("is after the latest available version 1")) // after 1
  }

  test("getVersionBeforeOrAtTimestamp: w/ checkpoint + w/o checkpoint") {
    Seq(
      deltaFileStatuses(Seq(10L, 11L, 12L)) ++ singularCheckpointFileStatuses(Seq(10L)),
      deltaFileStatuses(Seq(10L, 11L, 12L)) // checks that does not need to be recreatable
    ).foreach { deltaFiles =>
      checkGetVersionBeforeOrAtTimestamp(deltaFiles, 99, // before 10
        expectedErrorMessageContains = Some("is before the earliest available version 10"))
      checkGetVersionBeforeOrAtTimestamp(deltaFiles, 100, expectedVersion = Some(10)) // at 10
      checkGetVersionBeforeOrAtTimestamp(deltaFiles, 105, expectedVersion = Some(10)) // btw 10, 11
      checkGetVersionBeforeOrAtTimestamp(deltaFiles, 110, expectedVersion = Some(11)) // at 11
      checkGetVersionBeforeOrAtTimestamp(deltaFiles, 115, expectedVersion = Some(11)) // btw 11, 12
      checkGetVersionBeforeOrAtTimestamp(deltaFiles, 120, expectedVersion = Some(12)) // at 12
      checkGetVersionBeforeOrAtTimestamp(deltaFiles, 125, expectedVersion = Some(12)) // after 12
    }
  }

  test("getVersionAtOrAfterTimestamp: w/ checkpoint + w/o checkpoint") {
    Seq(
      deltaFileStatuses(Seq(10L, 11L, 12L)) ++ singularCheckpointFileStatuses(Seq(10L)),
      deltaFileStatuses(Seq(10L, 11L, 12L)) // checks that does not need to be recreatable
    ).foreach { deltaFiles =>
      checkGetVersionAtOrAfterTimestamp(deltaFiles, 99, expectedVersion = Some(10)) // before 10
      checkGetVersionAtOrAfterTimestamp(deltaFiles, 100, expectedVersion = Some(10)) // at 10
      checkGetVersionAtOrAfterTimestamp(deltaFiles, 105, expectedVersion = Some(11)) // btw 10, 11
      checkGetVersionAtOrAfterTimestamp(deltaFiles, 110, expectedVersion = Some(11)) // at 11
      checkGetVersionAtOrAfterTimestamp(deltaFiles, 115, expectedVersion = Some(12)) // btw 11, 12
      checkGetVersionAtOrAfterTimestamp(deltaFiles, 120, expectedVersion = Some(12)) // at 12
      checkGetVersionAtOrAfterTimestamp(deltaFiles, 125,
        expectedErrorMessageContains = Some("is after the latest available version 12")) // after 12
    }
  }
}
