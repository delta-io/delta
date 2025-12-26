/*
 * Copyright (2021) The Delta Lake Project Authors.
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
import org.apache.spark.sql.delta.sources.DeltaSQLConfV2
import org.scalatest.Tag
import org.scalactic.source.Position

import scala.collection.mutable

/**
 * Trait that forces Delta V2 connector mode to STRICT, ensuring all operations
 * use the Kernel-based SparkTable implementation (V2 connector) instead of
 * DeltaTableV2 (V1 connector).
 *
 * See [[DeltaSQLConfV2.V2_ENABLE_MODE]] for V1 vs V2 connector definitions.
 *
 * Usage:
 * {{{
 * class MyKernelTest extends MyOriginalSuite with V2ForceTest {
 *   override protected def shouldSkipTest(testName: String): Boolean = {
 *     testName.contains("unsupported feature")
 *   }
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

  /**
   * Determine if a test is expected to fail based on the test name.
   * Subclasses should override this method to define which tests are expected to fail.
   * By default, no tests are expected to fail.
   *
   * @param testName The name of the test
   * @return true if the test is expected to fail, false otherwise
   */
  protected def shouldFail(testName: String): Boolean = false

  /**
   * Override `sparkConf` to set V2_ENABLE_MODE to "STRICT".
   * This ensures all catalog operations use Kernel SparkTable (V2 connector).
   */
  abstract override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaSQLConfV2.V2_ENABLE_MODE.key, "STRICT")
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}

