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

import scala.collection.mutable

import org.apache.spark.sql.delta.{DeltaColumnMappingTestUtils, DeltaConfigs, NoMapping}
import org.scalactic.source.Position
import org.scalatest.Tag
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SQLTestUtils

/**
 * A trait for selective enabling certain tests to run for column mapping modes
 */
trait DeltaColumnMappingSelectedTestMixin extends SparkFunSuite
  with SQLTestUtils with DeltaColumnMappingTestUtils {

  protected def runOnlyTests: Seq[String] = Seq()

  /**
   * If true, will run all tests.
   * Requires that `runOnlyTests` is empty.
   */
  protected def runAllTests: Boolean = false

  private val testsRun: mutable.Set[String] = mutable.Set.empty

  override protected def test(
      testName: String,
      testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    require(!runAllTests || runOnlyTests.isEmpty,
      "If `runAllTests` is true then `runOnlyTests` must be empty")

    if (runAllTests || runOnlyTests.contains(testName)) {
      super.test(s"$testName - column mapping $columnMappingMode mode", testTags: _*) {
        testsRun.add(testName)
        withSQLConf(
          DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> columnMappingMode) {
          testFun
        }
      }
    } else {
      super.ignore(s"$testName - ignored by DeltaColumnMappingSelectedTestMixin")(testFun)
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    val missingTests = runOnlyTests.toSet diff testsRun
    if (missingTests.nonEmpty) {
      throw new TestFailedException(
        Some("Not all selected column mapping tests were run. Missing: " +
          missingTests.mkString(", ")), None, 0)
    }
  }

}
