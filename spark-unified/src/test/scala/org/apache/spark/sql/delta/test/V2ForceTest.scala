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

package org.apache.spark.sql.delta.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.scalatest.Tag
import org.scalactic.source.Position

import scala.collection.mutable

/**
 * Trait that forces Delta V2 connector mode to STRICT, ensuring all operations
 * use the Kernel-based SparkTable implementation (V2 connector) instead of
 * DeltaTableV2 (V1 connector).
 *
 * See [[DeltaSQLConf.V2_ENABLE_MODE]] for V1 vs V2 connector definitions.
 *
 * Usage:
 * {{{
 * class MyKernelTest extends MyOriginalSuite with V2ForceTest {
 *   override protected def shouldPassTests: Set[String] = Set("supported test")
 *   override protected def shouldFailTests: Set[String] = Set("unsupported test")
 * }
 * }}}
 */
trait V2ForceTest extends DeltaSQLCommandTest {

  private val testsRun: mutable.Set[String] = mutable.Set.empty

  /**
   * Override `test` to apply the `shouldFail` logic.
   * Tests that are expected to fail are converted to ignored tests.
   */
  abstract override protected def test(
      testName: String,
      testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    if (shouldFail(testName)) {
      // TODO(#5754): Assert on test failure instead of ignoring
      super.ignore(
        s"$testName - expected to fail with Kernel-based V2 connector (not yet supported)")(testFun)
    } else {
      super.test(testName, testTags: _*) {
        testsRun.add(testName)
        testFun
      }
    }
  }

  /** Tests expected to pass under the V2 connector. Subclasses populate this set. */
  protected def shouldPassTests: Set[String] = Set.empty

  /** Tests expected to fail under the V2 connector. Subclasses populate this set. */
  protected def shouldFailTests: Set[String] = Set.empty

  /**
   * Determine if a test is expected to fail. Every test must appear in exactly one of
   * [[shouldPassTests]] or [[shouldFailTests]] so the V2 contract is explicit.
   */
  protected def shouldFail(testName: String): Boolean = {
    val inPassList = shouldPassTests.contains(testName)
    val inFailList = shouldFailTests.contains(testName)

    assert(inPassList || inFailList,
      s"Test '$testName' not in shouldPassTests or shouldFailTests")
    assert(!(inPassList && inFailList),
      s"Test '$testName' in both shouldPassTests and shouldFailTests")

    inFailList
  }

  /**
   * Override `sparkConf` to set V2_ENABLE_MODE to "STRICT".
   * This ensures all catalog operations use Kernel SparkTable (V2 connector).
   */
  abstract override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaSQLConf.V2_ENABLE_MODE.key, "STRICT")
  }

  /**
   * Run a SQL statement through the V1 connector by temporarily setting
   * V2_ENABLE_MODE to NONE. Useful for DDL/DML that SparkTable (V2) doesn't support.
   */
  protected def executeInV1Mode(sqlText: String): Unit = {
    withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
      sql(sqlText)
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}

