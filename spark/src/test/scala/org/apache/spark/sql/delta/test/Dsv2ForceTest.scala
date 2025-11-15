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
import org.apache.spark.sql.delta.DeltaDsv2EnableConf
import org.scalatest.Tag
import org.scalactic.source.Position

import scala.collection.mutable

/**
 * Trait that forces DataSourceV2 mode to STRICT, ensuring all operations
 * use the Kernel-based SparkTable implementation instead of DeltaTableV2.
 * 
 * Usage:
 * {{{
 * class MyKernelTest extends MyOriginalSuite with Dsv2ForceTest {
 *   override protected def shouldSkipTest(testName: String): Boolean = {
 *     testName.contains("unsupported feature")
 *   }
 * }
 * }}}
 */
trait Dsv2ForceTest extends DeltaSQLCommandTest {

  private val testsRun: mutable.Set[String] = mutable.Set.empty

  /**
   * Override `test` to apply the `shouldSkipTest` logic.
   * Tests that should be skipped are converted to ignored tests.
   */
  abstract override protected def test(
      testName: String,
      testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    if (shouldSkipTest(testName)) {
      super.ignore(s"$testName - skipped for Kernel based DSv2 (not yet supported)")(testFun)
    } else {
      super.test(testName, testTags: _*) {
        testsRun.add(testName)
        testFun
      }
    }
  }

  /**
   * Determine if a test should be skipped based on the test name.
   * Subclasses should override this method to define their skip logic.
   * By default, no tests are skipped.
   *
   * @param testName The name of the test
   * @return true if the test should be skipped, false otherwise
   */
  protected def shouldSkipTest(testName: String): Boolean = false

  /**
   * Override `sparkConf` to set the `DATASOURCEV2_ENABLE_MODE` to "STRICT".
   * This ensures all catalog operations use Kernel SparkTable.
   */
  abstract override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaDsv2EnableConf.DATASOURCEV2_ENABLE_MODE.key, "STRICT")
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}

